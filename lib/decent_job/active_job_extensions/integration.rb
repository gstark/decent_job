# frozen_string_literal: true

module DecentJob
  module ActiveJobExtensions
    # Runs Active Job classes. Active Job reports how it handled an error only
    # through instrumentation, so execution listens to those events.
    #
    # This is the Active Job implementation of the interface that
    # {Integrations} documents. decent_job/active_job registers one instance,
    # ahead of {Integrations::Native}. The engine ({Performer}, {Adapter},
    # {Job}) calls it and never calls Active Job directly.
    #
    # Loaded only with Active Job. It also uses ActiveSupport::Notifications
    # and I18n, which Active Job loads.
    #
    # See docs/rails-integration.md.
    class Integration
      # Active Job events that report the outcome of one perform.
      EVENTS = /\A(?:perform|enqueue_retry|discard|retry_stopped)\.active_job\z/

      # Maps an Active Job event name to the decent_jobs.error_event value.
      ERROR_EVENTS = {"enqueue_retry" => "retried", "discard" => "discarded", "retry_stopped" => "retry_stopped"}.freeze

      # @param job_class [Class]
      # @return [Boolean] true for ActiveJob::Base and its subclasses
      def handles?(job_class)
        job_class <= ::ActiveJob::Base
      end

      # @param payload [Hash] the stored job payload
      # @return [ActiveJob::Base] the job, with arguments not yet deserialized
      def deserialize(payload)
        ::ActiveJob::Base.deserialize(payload)
      end

      # Runs the job through ActiveJob::Base.execute and records its outcome.
      #
      # The subscriber fills outcome as events arrive, so outcome stays
      # accurate when perform raises. A retry_on reschedule sets state
      # "retried". A discard or an exhausted retry sets state "discarded".
      #
      # @param payload [Hash] the stored job payload
      # @param outcome [Outcome] filled with the job instance, state, error,
      #   and error_event
      # @return [Object] the perform value, or the error a handler returned
      # @note The subscriber ignores events from other threads and other job
      #   IDs, because ActiveSupport subscriptions are process-wide.
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

      # Serializes with Active Job's core method only, so prepended extension
      # metadata is left out.
      #
      # @param job [ActiveJob::Base]
      # @return [Hash] the payload without extension keys
      def serialize_core(job)
        ::ActiveJob::Core.instance_method(:serialize).bind_call(job)
      end

      # perform_later returns false only for Active Job's own error class.
      #
      # @param error [EnqueueError] the engine error
      # @return [ActiveJob::EnqueueError] the error to raise or store on the job
      def enqueue_error(error)
        ::ActiveJob::EnqueueError.new(error.message)
      end

      # @return [Class] ActiveJob::DeserializationError
      def deserialization_error
        ::ActiveJob::DeserializationError
      end

      # Runs the block in the job's stored locale.
      #
      # @param job [ActiveJob::Base]
      # @yield the work to run
      # @return [Object] the block result
      def with_context(job, &block)
        I18n.with_locale(job.locale, &block)
      end

      # Report an operator discard as Active Job's discard event.
      #
      # @param job [ActiveJob::Base]
      # @param error [Exception] the discard reason
      # @yield the discard work
      # @return [Object] the block result
      # @note Calls Active Job's private instrument method, so subscribers to
      #   discard.active_job see operator discards.
      def discarding(job, error, &block)
        job.send(:instrument, :discard, error: error, &block)
      end
    end
  end
end
