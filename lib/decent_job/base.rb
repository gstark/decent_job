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
    def self.setting(name, default)
      Support.class_setting(self, name, default)
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

    require "decent_job/base/core"
    require "decent_job/base/callbacks"
    require "decent_job/base/exceptions"
    include Core
    include Callbacks
    include Exceptions

    require "decent_job/job_extensions/batches"
    require "decent_job/job_extensions/concurrency"
    require "decent_job/job_extensions/notify_options"
    include JobExtensions::Labels
    include JobExtensions::NotifyOptions
    include JobExtensions::Concurrency
    include JobExtensions::Batches
    include JobExtensions::Cron

    private

    # Native jobs check concurrency after every enqueue callback has run, so
    # they need no callback ordering.
    def before_adapter_enqueue
      _decent_job_concurrency_before_enqueue
    end
  end

  class << self
    # Enqueues jobs in bulk per adapter. Enqueue callbacks do not run.
    def perform_all_later(*jobs)
      jobs = jobs.flatten
      CronEntry.validate_jobs(jobs) if CronEntry.current_context
      jobs.group_by(&:queue_adapter).each { |adapter, group| adapter.enqueue_all(group) }
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
