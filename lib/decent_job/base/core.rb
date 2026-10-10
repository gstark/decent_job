# frozen_string_literal: true

module DecentJob
  class Base
    # Job state, enqueue, perform and serialization for {Base}. Extensions included after
    # Core can wrap any of these methods with super.
    #
    # The instance API matches ActiveJob::Base: the same attribute names, the
    # same payload keys from {#serialize}, and the same return values from
    # {#enqueue} and {#perform_now}. A job instance is not thread safe. One
    # thread builds and enqueues it, or one worker thread runs it.
    #
    # Attributes:
    # - arguments [Array] the perform arguments. After {#deserialize} they stay
    #   serialized until {#perform_now} or an extension needs them.
    # - job_id [String] a UUID made in the constructor. It stays the same
    #   across retries. The decent_jobs.active_job_id column stores it.
    # - provider_job_id [String, nil] the decent_jobs.id of the stored row, or
    #   nil before the adapter accepts the job.
    # - scheduled_at [Time, nil] when the job becomes due, or nil for now.
    # - enqueued_at [Time, nil] read from the payload by {#deserialize}.
    # - executions [Integer] how many times perform_now started, counting
    #   earlier attempts from the payload.
    # - exception_executions [Hash{String => Integer}] retry_on attempt counts,
    #   keyed by the exception list of each retry_on call.
    # - enqueue_error [Exception, nil] why the last {#enqueue} failed in the
    #   adapter, or nil. It stays nil when a callback aborted.
    #
    # See docs/native-jobs.md.
    module Core
      attr_accessor :arguments, :job_id, :scheduled_at, :enqueued_at, :provider_job_id,
        :executions, :exception_executions, :enqueue_error
      attr_writer :queue_name, :priority, :successfully_enqueued, :serialized_arguments
      # The handled error and how it was handled (:retried, :discarded or
      # :retry_stopped) after the last perform_now, or nil.
      # {Integrations::Native#execute} reads it to fill the {Outcome}.
      #
      # @return [Array(Symbol, Exception), nil] a frozen [event, error] pair,
      #   or nil when no retry_on, discard_on or retry_job handled an error.
      #   A plain rescue_from handler does not set it.
      attr_reader :decent_job_outcome

      # @param arguments [Array] the perform arguments. A trailing keyword hash
      #   keeps its ruby2_keywords flag, so perform receives keywords.
      def initialize(*arguments)
        @arguments = arguments
        @job_id = SecureRandom.uuid
        @queue_name = self.class.queue_name || self.class.queue_name_from_part(nil)
        @priority = self.class.priority
        @scheduled_at = nil
        @executions = 0
        @exception_executions = {}
      end
      ruby2_keywords(:initialize)

      # Does the work of the job. Subclasses must override it.
      #
      # @raise [NotImplementedError] when the subclass does not define it
      def perform(*)
        raise NotImplementedError, "#{self.class.name} must implement perform"
      end

      # @return [String] the queue name. A Proc from queue_as runs once, on
      #   first read, and its result replaces it.
      def queue_name
        if @queue_name.is_a?(Proc)
          @queue_name = self.class.queue_name_from_part(instance_exec(&@queue_name))
        end

        @queue_name
      end

      # @return [Integer, nil] the priority, or nil for none. A Proc from
      #   queue_with_priority runs once, on first read, and its result
      #   replaces it.
      def priority
        if @priority.is_a?(Proc)
          @priority = instance_exec(&@priority)
        end

        @priority
      end

      # @return [Adapter, Object] the adapter that enqueues this job, from
      #   {Base.queue_adapter}
      def queue_adapter
        self.class.queue_adapter || DecentJob.default_adapter
      end

      # @return [Boolean, nil] true after the adapter accepted the job, false
      #   after a failed or aborted enqueue, nil before any enqueue
      def successfully_enqueued?
        @successfully_enqueued
      end

      # Applies enqueue options to this job. Unknown keys are ignored.
      #
      # @param options [Hash] the options:
      #   - :wait [Numeric, #to_f] delay in seconds from now
      #   - :wait_until [Time] the scheduled time; wins over :wait
      #   - :queue [String, Symbol] queue name part; queue_name_prefix applies
      #   - :priority [Integer, #to_i] the priority
      # @return [self]
      def set(options = {})
        self.scheduled_at = Time.now + options[:wait].to_f if options[:wait]
        self.scheduled_at = options[:wait_until] if options[:wait_until]
        self.queue_name = self.class.queue_name_from_part(options[:queue]) if options[:queue]
        self.priority = options[:priority].to_i if options[:priority]

        self
      end

      # Runs the enqueue callbacks and hands the job to the adapter.
      #
      # @param options [Hash] options for {#set}. Extensions also read
      #   :decent_job_labels and :decent_job_notify.
      # @return [self, false] the job, or false when a callback halts or the
      #   adapter rejects it
      # @note With {Adapter}, this inserts a decent_jobs row and can send
      #   NOTIFY in the same transaction. Inside a {Bulk} capture, the adapter
      #   buffers the job instead and reports success before the insert.
      # @note A scheduled_at value goes to the adapter as epoch seconds.
      def enqueue(options = {})
        set(options)

        self.successfully_enqueued = false
        self.enqueue_error = nil

        run_callbacks(:enqueue) do
          before_adapter_enqueue

          if scheduled_at
            queue_adapter.enqueue_at(self, scheduled_at.to_f)
          else
            queue_adapter.enqueue(self)
          end

          self.successfully_enqueued = true
        rescue EnqueueError => error
          self.enqueue_error = error
        end

        successfully_enqueued? ? self : false
      end

      # Runs the perform callbacks and #perform in this thread.
      #
      # Increments executions and loads serialized arguments first. When an
      # error escapes, the rescue handlers from {Exceptions} get it. Like Active
      # Job, this rescues Exception, not only StandardError.
      #
      # @return [Object, false] perform's value, the error when a handler
      #   handled it, or false when a before_perform callback threw :abort
      # @raise [Exception] the error when no handler handled it. The
      #   after_discard blocks run before it is raised.
      def perform_now
        @decent_job_outcome = nil

        self.executions = (executions || 0) + 1

        deserialize_arguments_if_needed

        run_callbacks(:perform) { perform(*arguments) }
      rescue Exception => exception # standard:disable Lint/RescueException
        handled = rescue_with_handler(exception)
        return handled if handled

        run_after_discard_procs(exception)
        raise
      end

      # Builds the stored payload. The keys match ActiveJob::Base#serialize.
      # Extensions merge their own "decent_job_*" keys into it.
      #
      # @return [Hash{String => Object}] a JSON-safe hash
      # @raise [SerializationError] when an argument cannot be encoded
      # @note enqueued_at is the time of this call. Arguments that were never
      #   loaded are written back in their stored form, so a job with
      #   arguments that no longer load can still be stored again.
      def serialize
        {
          "job_class" => self.class.name,
          "job_id" => job_id,
          "provider_job_id" => provider_job_id,
          "queue_name" => queue_name,
          "priority" => priority,
          "arguments" => @serialized_arguments || Arguments.serialize(arguments),
          "executions" => executions,
          "exception_executions" => exception_executions,
          "enqueued_at" => Time.now.utc.iso8601(9),
          "scheduled_at" => scheduled_at&.utc&.iso8601(9)
        }
      end

      # Loads state from a stored payload. Arguments stay serialized until
      # they are needed. Values from the payload replace class defaults, such
      # as a queue_as Proc.
      #
      # @param job_data [Hash{String => Object}] a payload from {#serialize}
      # @return [void]
      def deserialize(job_data)
        self.job_id = job_data["job_id"]
        self.provider_job_id = job_data["provider_job_id"]
        self.queue_name = job_data["queue_name"]
        self.priority = job_data["priority"]
        self.serialized_arguments = job_data["arguments"]
        self.executions = job_data["executions"]
        self.exception_executions = job_data["exception_executions"]
        self.enqueued_at = parse_time(job_data["enqueued_at"]) if job_data["enqueued_at"]
        self.scheduled_at = parse_time(job_data["scheduled_at"]) if job_data["scheduled_at"]
      end

      private

      # Runs after every enqueue callback, just before the adapter call.
      def before_adapter_enqueue
        # Nothing to do here
      end

      # Called by extensions and {Job#job_instance} through send. On failure
      # the stored arguments stay in place, so a later call can try again.
      def deserialize_arguments_if_needed
        return unless @serialized_arguments

        @arguments = Arguments.deserialize(@serialized_arguments)
        @serialized_arguments = nil
      end

      def parse_time(value)
        value.is_a?(Time) ? value : Time.iso8601(value)
      end
    end
  end
end
