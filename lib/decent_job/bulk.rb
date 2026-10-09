# frozen_string_literal: true

module DecentJob
  module Bulk
    def self.current_buffer
      ExecutionState[:bulk_buffer]
    end

    def self.current_buffer=(buffer)
      ExecutionState[:bulk_buffer] = buffer
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
      ExecutionState.with(:bulk_buffer, buffer) { yield }
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

      def capture(&block)
        Bulk.with_buffer(self, &block)
      rescue
        discard
        raise
      end

      # Run controlled jobs' callbacks before acquiring a batch's complete set
      # of concurrency locks. Admission still happens when this buffer flushes.
      def prepare
        prepared = self.class.new
        pending = @entries.reverse
        callbacks_prepared = @entries.select(&:callbacks_run).to_h { |entry| [entry.job.object_id, true] }
        added = {}
        while (entry = pending.pop)
          identity = entry.job.object_id
          if !entry.callbacks_run && !entry.job.provider_job_id && entry.job.respond_to?(:decent_job_resolved_concurrency_rules)
            next if callbacks_prepared[identity]
            callbacks_prepared[identity] = true
            captured = self.class.new
            captured.capture { entry.job.enqueue }
            captured.entries.each { |child| callbacks_prepared[child.job.object_id] = true if child.callbacks_run }
            pending.concat(captured.entries.reverse)
            @children.concat(captured.children)
          else
            next if added[identity]
            added[identity] = true
            prepared.add(entry.job, queue_adapter: entry.adapter, callbacks_run: entry.callbacks_run)
          end
        end
        @entries = prepared.entries
        @active_jobs.replace(prepared.active_jobs)
        self
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

      attr_reader :entries, :children
    end
  end
end
