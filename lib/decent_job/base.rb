# frozen_string_literal: true

require "securerandom"
require "time"
require "decent_job/arguments"
require "decent_job/support"

module DecentJob
  class EnqueueError < StandardError; end

  class UnknownJobClassError < NameError
    def initialize(job_class_name)
      super("Failed to instantiate job, class `#{job_class_name}` doesn't exist", job_class_name)
    end
  end

  # A job class without Active Job. The API, payload keys and argument
  # encoding follow Active Job, so moving a job between the two is a one-line
  # change of superclass.
  class Base
    # Class settings resolve through superclasses until a class assigns one.
    def self.setting(name, default)
      variable = :"@#{name}"
      define_singleton_method(name) do
        if instance_variable_defined?(variable)
          instance_variable_get(variable)
        elsif superclass.respond_to?(name)
          superclass.public_send(name)
        else
          default
        end
      end
      define_singleton_method(:"#{name}=") { |value| instance_variable_set(variable, value) }
    end
    private_class_method :setting

    setting :default_queue_name, "default"
    setting :queue_name, nil
    setting :queue_name_prefix, nil
    setting :queue_name_delimiter, "_"
    setting :priority, nil
    setting :retry_jitter, 0.0
    setting :rescue_handlers, [].freeze
    setting :after_discard_procs, [].freeze
    setting :queue_adapter, nil

    class ConfiguredJob
      def initialize(job_class, options)
        @job_class = job_class
        @options = options
      end

      def perform_now(...)
        @job_class.new(...).set(@options).perform_now
      end

      def perform_later(...)
        job = @job_class.new(...)
        result = job.enqueue(@options)
        yield job if block_given?
        result
      end
    end

    class << self
      def perform_later(...)
        job = job_or_instantiate(...)
        result = job.enqueue
        yield job if block_given?
        result
      end

      def perform_now(...)
        job_or_instantiate(...).perform_now
      end

      def set(options = {})
        ConfiguredJob.new(self, options)
      end

      def queue_as(part = nil, &block)
        self.queue_name = block || queue_name_from_part(part)
      end

      def queue_with_priority(priority = nil, &block)
        self.priority = block || priority
      end

      def queue_name_from_part(part)
        prefix = queue_name_prefix
        [(Support.blank?(prefix) ? nil : prefix), part || default_queue_name].compact.join(queue_name_delimiter)
      end

      def deserialize(job_data)
        name = job_data.fetch("job_class")
        job_class = begin
          Support.constantize(name)
        rescue NameError
          raise UnknownJobClassError, name
        end
        raise ArgumentError, "#{name} is not a DecentJob::Base job" unless job_class.is_a?(Class) && job_class <= Base

        job_class.new.tap { |job| job.deserialize(job_data) }
      end

      private

      def job_or_instantiate(*arguments)
        arguments.first.is_a?(self) ? arguments.first : new(*arguments)
      end
      ruby2_keywords(:job_or_instantiate)
    end

    require "decent_job/base/callbacks"
    require "decent_job/base/exceptions"
    include Callbacks
    include Exceptions

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

    def deserialize_arguments_if_needed
      return unless @serialized_arguments

      @arguments = Arguments.deserialize(@serialized_arguments)
      @serialized_arguments = nil
    end

    def parse_time(value)
      value.is_a?(Time) ? value : Time.iso8601(value)
    end
  end

  class << self
    # Enqueues jobs in bulk per adapter. Enqueue callbacks do not run.
    def perform_all_later(*jobs)
      jobs.flatten.group_by(&:queue_adapter).each { |adapter, group| adapter.enqueue_all(group) }
      nil
    end

    # Native jobs without their own queue_adapter share this one. reset!
    # retires it with the other adapters and clears it.
    def default_adapter
      @default_adapter_mutex.synchronize { @default_adapter ||= Adapter.new }
    end
  end

  @default_adapter_mutex = Mutex.new
end
