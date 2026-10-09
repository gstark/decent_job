# frozen_string_literal: true

module DecentJob
  module ActiveJobExtensions
    # Runs Active Job classes. Active Job reports how it handled an error only
    # through instrumentation, so execution listens to those events.
    class Integration
      EVENTS = /\A(?:perform|enqueue_retry|discard|retry_stopped)\.active_job\z/
      ERROR_EVENTS = {"enqueue_retry" => "retried", "discard" => "discarded", "retry_stopped" => "retry_stopped"}.freeze

      def handles?(job_class)
        job_class <= ::ActiveJob::Base
      end

      def deserialize(payload)
        ::ActiveJob::Base.deserialize(payload)
      end

      def execute(payload, outcome)
        current_thread = Thread.current
        subscriber = lambda do |name, _start, _finish, _id, event|
          next unless Thread.current == current_thread && event[:job]&.job_id == payload["job_id"]

          outcome.job_instance = event[:job]
          next if name == "perform.active_job"

          outcome.state = name.start_with?("enqueue_retry") ? "retried" : "discarded"
          outcome.error = event[:error]
          outcome.error_event = ERROR_EVENTS.fetch(name.split(".").first)
        end
        ::ActiveSupport::Notifications.subscribed(subscriber, EVENTS) { ::ActiveJob::Base.execute(payload) }
      end

      def serialize_core(job)
        ::ActiveJob::Core.instance_method(:serialize).bind_call(job)
      end

      # perform_later returns false only for Active Job's own error class.
      def enqueue_error(error)
        ::ActiveJob::EnqueueError.new(error.message)
      end

      def deserialization_error
        ::ActiveJob::DeserializationError
      end

      def with_context(job, &block)
        I18n.with_locale(job.locale, &block)
      end

      # Report an operator discard as Active Job's discard event.
      def discarding(job, error, &block)
        job.send(:instrument, :discard, error: error, &block)
      end
    end
  end
end
