# frozen_string_literal: true

module DecentJob
  class Base
    # Job state, enqueue, perform and serialization. Extensions included after
    # Core can wrap any of these methods with super.
    module Core
      attr_accessor :arguments, :job_id, :scheduled_at, :enqueued_at, :provider_job_id,
        :executions, :exception_executions, :enqueue_error
      attr_writer :queue_name, :priority, :successfully_enqueued, :serialized_arguments
      # The handled error and how it was handled (:retried, :discarded or
      # :retry_stopped) after the last perform_now, or nil.
      attr_reader :decent_job_outcome

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

      def perform(*)
        raise NotImplementedError, "#{self.class.name} must implement perform"
      end

      def queue_name
        @queue_name = self.class.queue_name_from_part(instance_exec(&@queue_name)) if @queue_name.is_a?(Proc)
        @queue_name
      end

      def priority
        @priority = instance_exec(&@priority) if @priority.is_a?(Proc)
        @priority
      end

      def queue_adapter
        self.class.queue_adapter || DecentJob.default_adapter
      end

      def successfully_enqueued?
        @successfully_enqueued
      end

      def set(options = {})
        self.scheduled_at = Time.now + options[:wait].to_f if options[:wait]
        self.scheduled_at = options[:wait_until] if options[:wait_until]
        self.queue_name = self.class.queue_name_from_part(options[:queue]) if options[:queue]
        self.priority = options[:priority].to_i if options[:priority]
        self
      end

      # Returns the job, or false when a callback halts or the adapter rejects it.
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

      # Returns perform's value, or the error when a handler handled it.
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
      end

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
