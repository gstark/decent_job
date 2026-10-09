# frozen_string_literal: true

require "decent_job/base"
require "decent_job/support"

module DecentJob
  # What happened while a job instance ran. Integrations fill it in as the job
  # runs, so it stays accurate when the job raises.
  Outcome = Struct.new(:job_instance, :state, :error, :error_event)

  # The engine talks to job frameworks only through integrations. An
  # integration answers for the job classes it handles:
  #
  #   handles?(job_class)             => true or false
  #   deserialize(payload)            => job instance
  #   execute(payload, outcome)       => perform value, or the handled error
  #   serialize_core(job_instance)    => payload without extension metadata
  #   enqueue_error(error)            => the error raised to enqueue callers
  #   deserialization_error           => error class for unloadable arguments
  #   with_context(job_instance) { }  => runs the block in the job's context
  #   discarding(job_instance, error) { } => wraps an operator discard
  module Integrations
    class Native
      def handles?(job_class)
        job_class <= Base
      end

      def deserialize(payload)
        Base.deserialize(payload)
      end

      def execute(payload, outcome)
        job = deserialize(payload)
        outcome.job_instance = job
        job.perform_now
      ensure
        if (handled = job&.decent_job_outcome)
          event, error = handled
          outcome.state = (event == :retried) ? "retried" : "discarded"
          outcome.error = error
          outcome.error_event = event.to_s
        end
      end

      def serialize_core(job)
        Base.instance_method(:serialize).bind_call(job)
      end

      def enqueue_error(error)
        error
      end

      def deserialization_error
        DeserializationError
      end

      def with_context(_job)
        yield
      end

      def discarding(_job, _error)
        yield
      end
    end

    @integrations = [Native.new].freeze

    class << self
      def register(integration)
        @integrations = ([integration] + @integrations).freeze
      end

      def for_class(job_class)
        return unless job_class.is_a?(Class)

        @integrations.find { |integration| integration.handles?(job_class) }
      end

      def for_job(job)
        for_class(job.class) || raise(ArgumentError, "#{job.class} is not a supported job class")
      end

      def for_payload(payload)
        name = payload.fetch("job_class")
        job_class = begin
          Support.constantize(name)
        rescue NameError
          raise UnknownJobClassError, name
        end
        for_class(job_class) || raise(ArgumentError, "#{name} is not a supported job class")
      end

      def job_class?(job_class)
        !for_class(job_class).nil?
      end

      def job?(job)
        job_class?(job.class)
      end

      def deserialize(payload)
        for_payload(payload).deserialize(payload)
      end

      def deserialization_errors
        @integrations.map(&:deserialization_error).uniq
      end
    end
  end
end
