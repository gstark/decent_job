# frozen_string_literal: true

module DecentJob
  module Bulk
    def self.current_buffer
      Thread.current.thread_variable_get(:decent_job_bulk_buffer)
    end

    def self.current_buffer=(buffer)
      Thread.current.thread_variable_set(:decent_job_bulk_buffer, buffer)
    end

    def self.capture(active_jobs = nil, queue_adapter: nil, callbacks_run: false, &block)
      raise ArgumentError, "Use either jobs or a block, not both" if block && active_jobs

      if block
        buffer = Buffer.new
        capture_block(buffer, &block)
        buffer.active_jobs
      elsif current_buffer
        current_buffer.add(active_jobs, queue_adapter: queue_adapter, callbacks_run: callbacks_run)
        true
      end
    end

    def self.enqueue(*jobs, &block)
      raise ArgumentError, "Use either jobs or a block, not both" if block && !jobs.empty?

      parent = current_buffer
      buffer = Buffer.new
      jobs.flatten.each { |job| buffer.add(job, callbacks_run: job.is_a?(ActiveJob::Base) && job.successfully_enqueued?) }
      capture_block(buffer, &block) if block
      if parent
        parent.append(buffer)
      else
        with_buffer(nil) { buffer.enqueue }
      end
      buffer.active_jobs
    end

    def self.with_buffer(buffer)
      previous = current_buffer
      self.current_buffer = buffer
      yield
    ensure
      self.current_buffer = previous
    end

    def self.capture_block(buffer, &block)
      with_buffer(buffer, &block)
    rescue
      buffer.discard
      raise
    end
    private_class_method :capture_block

    class Buffer
      Entry = Data.define(:job, :adapter, :callbacks_run)

      attr_reader :active_jobs

      def initialize
        @entries = []
        @children = []
        @active_jobs = []
      end

      def add(jobs, queue_adapter: nil, callbacks_run: false)
        entries = Array(jobs).map do |job|
          raise ArgumentError, "Expected an ActiveJob::Base instance, got #{job.class}" unless job.is_a?(ActiveJob::Base)

          Entry.new(job: job, adapter: queue_adapter || job.queue_adapter, callbacks_run: callbacks_run)
        end
        @entries.concat(entries)
        @active_jobs.concat(entries.map(&:job))
      end

      def append(buffer)
        @entries.concat(buffer.entries)
        @active_jobs.concat(buffer.active_jobs)
        @children << buffer
      end

      def enqueue
        entries = @entries.uniq { |entry| entry.job.object_id }.reject { |entry| entry.job.provider_job_id }
        entries.group_by { |entry| [entry.adapter, entry.callbacks_run] }.each do |(adapter, callbacks_run), group|
          jobs = group.map(&:job)
          if adapter.is_a?(Adapter)
            adapter.enqueue_all(jobs, callbacks: !callbacks_run)
          elsif adapter.respond_to?(:enqueue_all)
            adapter.enqueue_all(jobs)
          else
            jobs.each(&:enqueue)
          end
        end
        retain_accepted
      end

      def retain_accepted
        @active_jobs.select! { |job| job.provider_job_id || job.successfully_enqueued? }
        @active_jobs.uniq!
        @children.each(&:retain_accepted)
      end

      def discard
        @active_jobs.each { |job| job.successfully_enqueued = false unless job.provider_job_id }
        @children.each(&:discard)
      end

      protected

      attr_reader :entries
    end
  end
end
