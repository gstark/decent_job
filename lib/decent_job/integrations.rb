# frozen_string_literal: true

require "decent_job/base"
require "decent_job/support"

module DecentJob
  # What happened while a job instance ran. Integrations fill it in as the job
  # runs, so it stays accurate when the job raises.
  #
  # {Performer} creates one per attempt with state "succeeded" and reads it
  # after the integration returns or raises. Fields:
  #
  # - job_instance: the deserialized job, or nil when deserialization failed.
  # - state: "succeeded", "retried", or "discarded".
  # - error: the handled exception, or nil.
  # - error_event: "retried", "discarded", or "retry_stopped" from the
  #   integration, "handled" when {Performer} records a returned error, or nil.
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
  #
  # {Native} handles {Base} subclasses. The Active Job bridge registers its
  # integration when Active Job loads. The registry is a frozen Array that
  # {.register} replaces as a whole, so readers on other threads never see a
  # partial list.
  #
  # See docs/native-jobs.md.
  module Integrations
    # The integration for native {Base} jobs. It is stateless.
    class Native
      # @param job_class [Class] the class to test
      # @return [Boolean] true for {Base} and its subclasses
      def handles?(job_class)
        job_class <= Base
      end

      # @param payload [Hash{String => Object}] the stored job payload
      # @return [Base] the job instance
      def deserialize(payload)
        Base.deserialize(payload)
      end

      # Deserializes and performs the job, and records the outcome.
      #
      # A retry or discard that the job handled itself is recorded in the
      # ensure block, so the outcome is correct also when perform_now raises.
      #
      # @param payload [Hash{String => Object}] the stored job payload
      # @param outcome [Outcome] receives the job instance and handled result
      # @return [Object] the perform_now value
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

      # Serializes with {Base#serialize} itself, so subclass overrides that add
      # extension metadata do not apply.
      #
      # @param job [Base] the job instance
      # @return [Hash{String => Object}] the core payload
      def serialize_core(job)
        Base.instance_method(:serialize).bind_call(job)
      end

      # @param error [Exception] the enqueue failure
      # @return [Exception] the same error; native jobs need no wrapper
      def enqueue_error(error)
        error
      end

      # @return [Class] the error raised for arguments that cannot load
      def deserialization_error
        DeserializationError
      end

      # Native jobs have no framework context, so this only yields.
      #
      # @param _job [Base] the job instance
      # @return [Object] the block result
      def with_context(_job)
        yield
      end

      # Native jobs have no discard callbacks, so this only yields.
      #
      # @param _job [Base] the job instance
      # @param _error [Exception] the discard reason
      # @return [Object] the block result
      def discarding(_job, _error)
        yield
      end
    end

    @integrations = [Native.new].freeze

    class << self
      # Adds an integration ahead of the existing ones.
      #
      # The newest integration is asked first, so it can claim classes that an
      # older integration also handles.
      #
      # @param integration [Object] an object with the methods listed above
      # @return [Array<Object>] the new frozen registry
      # @note Not synchronized against other register calls. Register at load
      #   time.
      def register(integration)
        @integrations = ([integration] + @integrations).freeze
      end

      # @param job_class [Object] the class to look up
      # @return [Object, nil] the first integration that handles the class, or
      #   nil when none does or the argument is not a Class
      def for_class(job_class)
        return unless job_class.is_a?(Class)

        @integrations.find { |integration| integration.handles?(job_class) }
      end

      # @param job [Object] a job instance
      # @return [Object] the integration for the job's class
      # @raise [ArgumentError] when no integration handles the class
      def for_job(job)
        for_class(job.class) || raise(ArgumentError, "#{job.class} is not a supported job class")
      end

      # Finds the integration for a stored payload.
      #
      # @param payload [Hash{String => Object}] a payload with a "job_class" name
      # @return [Object] the integration for the named class
      # @raise [UnknownJobClassError] when the class cannot be resolved
      # @raise [ArgumentError] when no integration handles the class
      # @raise [KeyError] when the payload has no "job_class"
      # @note Resolving the class can load application code through the host
      #   autoloader.
      def for_payload(payload)
        name = payload.fetch("job_class")
        job_class = begin
          Support.constantize(name)
        rescue NameError
          raise UnknownJobClassError, name
        end
        for_class(job_class) || raise(ArgumentError, "#{name} is not a supported job class")
      end

      # @param job_class [Object] the class to test
      # @return [Boolean] true when an integration handles the class
      def job_class?(job_class)
        !for_class(job_class).nil?
      end

      # @param job [Object] the object to test
      # @return [Boolean] true when the object is an instance of a handled class
      def job?(job)
        job_class?(job.class)
      end

      # @param payload [Hash{String => Object}] the stored job payload
      # @return [Object] the job instance
      # @raise (see .for_payload)
      def deserialize(payload)
        for_payload(payload).deserialize(payload)
      end

      # @return [Array<Class>] the unique deserialization error classes of all
      #   integrations, for use in a rescue clause
      def deserialization_errors
        @integrations.map(&:deserialization_error).uniq
      end
    end
  end
end
