# frozen_string_literal: true

module DecentJob
  # Captures enqueues into a buffer and writes them together.
  #
  # While a buffer is current, {Adapter#enqueue} and {Adapter#enqueue_all}
  # add jobs to it instead of writing rows, and the jobs report
  # successfully_enqueued? as true. The flush later inserts plain jobs with
  # one SQL bulk insert per adapter and sends concurrency-controlled jobs
  # through single enqueue checks. After the flush, a job that was rejected
  # has successfully_enqueued? false, but its callback side effects stay.
  #
  # The current buffer lives in {ExecutionState}, so each thread (or fiber,
  # under the Rails isolation level) has its own. {Batch}, {CronEntry} and
  # {JobRetry} use {Buffer} directly to collect the jobs that their blocks
  # and callbacks enqueue, then validate them before any write.
  #
  # See the "Bulk capture" section of README.md.
  module Bulk
    # @return [Buffer, nil] the buffer of this thread, or nil when no capture
    #   is active
    def self.current_buffer
      ExecutionState[:bulk_buffer]
    end

    # @param buffer [Buffer, nil] the new buffer; nil turns capture off
    def self.current_buffer=(buffer)
      ExecutionState[:bulk_buffer] = buffer
    end

    # Captures jobs without writing them.
    #
    # With a block, enqueues in the block go to a new buffer, and the outer
    # buffer comes back afterward. Without a block, jobs go to the current
    # buffer, if there is one. Adapters call the form without a block.
    #
    # @param jobs [Base, Array, nil] jobs to add to the current buffer
    # @param queue_adapter [Object, nil] the adapter to flush through, or nil
    #   for each job's own adapter
    # @param callbacks_run [Boolean] true when the enqueue callbacks already
    #   ran, so the flush must not run them again
    # @yield enqueues to capture
    # @return [Array, Boolean, nil] the captured jobs with a block; true when
    #   a current buffer took the jobs; nil when no capture is active, and the
    #   caller must write the jobs itself
    # @raise [ArgumentError] when both jobs and a block are given
    # @note When the block raises, its jobs are marked not enqueued.
    def self.capture(jobs = nil, queue_adapter: nil, callbacks_run: false, &block)
      raise ArgumentError, "Use either jobs or a block, not both" if block && jobs

      if block
        buffer = Buffer.new
        capture_block(buffer, &block)
        buffer.jobs
      elsif current_buffer
        current_buffer.add(jobs, queue_adapter: queue_adapter, callbacks_run: callbacks_run)
        true
      end
    end

    # Captures jobs and writes them.
    #
    # Inside another capture, the jobs join the outer buffer and are written
    # when it flushes. Otherwise they are written now. Jobs that already have
    # a provider_job_id are not written again.
    #
    # @param jobs [Array<Base, Array>] jobs to add; nested arrays are flattened
    # @yield enqueues to capture
    # @return [Array] the accepted jobs after the write. Inside another
    #   capture, all captured jobs, because nothing was written yet.
    # @raise [ArgumentError] when both jobs and a block are given
    # @note SQL writes and NOTIFY run on each adapter's database. Wrap the
    #   call in a database transaction to make all writes atomic.
    def self.enqueue(*jobs, &block)
      raise ArgumentError, "Use either jobs or a block, not both" if block && !jobs.empty?

      parent = current_buffer
      buffer = Buffer.new
      jobs.flatten.each { |job| buffer.add(job, callbacks_run: Integrations.job?(job) && job.successfully_enqueued?) }
      capture_block(buffer, &block) if block

      if parent
        parent.append(buffer)
      else
        with_buffer(nil) { buffer.enqueue }
      end

      buffer.jobs
    end

    # Runs the block with buffer as the current buffer, then restores the old
    # one. Pass nil to write jobs directly inside an outer capture.
    #
    # @param buffer [Buffer, nil]
    # @return [Object] the block value
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

    # A list of captured jobs and the adapter to flush each one through.
    #
    # A buffer belongs to one thread. Nested buffers that join it are kept
    # as children, so their job lists also reflect the flush result.
    class Buffer
      Entry = Data.define(:job, :adapter, :callbacks_run)

      # @return [Array] the captured jobs, in capture order. After {#enqueue},
      #   only the accepted jobs.
      attr_reader :jobs

      # on_capture receives each job as the buffer accepts it.
      #
      # @param on_capture [#call, nil] called with each job as it is added
      def initialize(on_capture: nil)
        @entries = []
        @children = []
        @jobs = []
        @on_capture = on_capture
      end

      # @param jobs [Object, Array] job instances
      # @param queue_adapter [Object, nil] the adapter, or nil for each
      #   job's own adapter
      # @param callbacks_run [Boolean] true when enqueue callbacks already ran
      # @return [void]
      # @raise [ArgumentError] when an item is not a job instance
      def add(jobs, queue_adapter: nil, callbacks_run: false)
        entries = Array(jobs).map do |job|
          raise ArgumentError, "Expected a job instance, got #{job.class}" unless Integrations.job?(job)

          Entry.new(job: job, adapter: queue_adapter || job.queue_adapter, callbacks_run: callbacks_run)
        end

        @entries.concat(entries)
        @jobs.concat(entries.map(&:job))
        entries.each { |entry| @on_capture&.call(entry.job) }
      end

      # Moves the entries of a nested buffer into this one. The nested buffer
      # becomes a child.
      #
      # @param buffer [Buffer]
      # @return [void]
      def append(buffer)
        @entries.concat(buffer.entries)
        @jobs.concat(buffer.jobs)
        @children << buffer
      end

      # Runs the block with this buffer as the current buffer.
      #
      # @return [Object] the block value
      # @note When the block raises, the captured jobs are marked not enqueued.
      def capture(&block)
        Bulk.with_buffer(self, &block)
      rescue
        discard
        raise
      end

      # Run controlled jobs' callbacks before acquiring a batch's complete set
      # of concurrency locks. Admission still happens when this buffer flushes.
      #
      # Each unpersisted concurrency-controlled job whose callbacks did not run
      # is enqueued into a nested capture. That runs its callbacks once, and
      # the jobs those callbacks enqueue are prepared the same way. Duplicate
      # job objects are removed.
      #
      # @return [self]
      def prepare
        prepared = self.class.new
        pending = @entries.reverse
        callbacks_prepared = @entries.select(&:callbacks_run).to_h { |entry| [entry.job.object_id, true] }
        added = {}

        while (entry = pending.pop)
          identity = entry.job.object_id

          if !entry.callbacks_run && !entry.job.provider_job_id && ConcurrencyControls.controlled?(entry.job)
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
        @jobs.replace(prepared.jobs)
        self
      end

      # Writes the captured jobs, grouped by adapter and callback state.
      #
      # A DecentJob {Adapter} gets enqueue_all and runs callbacks only for
      # entries whose callbacks did not run. Another adapter gets enqueue_all,
      # or a single enqueue for each job when it has no enqueue_all.
      #
      # @return [void]
      # @note Call it inside Bulk.with_buffer(nil), or the adapters capture the
      #   jobs again instead of writing them.
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

      # Drops jobs that were not accepted from this buffer and its children.
      #
      # @return [void]
      def retain_accepted
        @jobs.select! { |job| job.provider_job_id || job.successfully_enqueued? }
        @jobs.uniq!
        @children.each(&:retain_accepted)
      end

      # Marks every unpersisted job in this buffer and its children as not
      # enqueued. Used when a capture block raises.
      #
      # @return [void]
      def discard
        @jobs.each { |job| job.successfully_enqueued = false unless job.provider_job_id }
        @children.each(&:discard)
      end

      protected

      attr_reader :entries, :children
    end
  end
end
