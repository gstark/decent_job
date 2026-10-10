# frozen_string_literal: true

require "securerandom"
require "time"
require "decent_job/arguments"
require "decent_job/support"

module DecentJob
  # The engine raises this error when it does not accept a job, for example a
  # PostgreSQL error during insert or a duplicate cron occurrence.
  # {Adapter} converts it to the integration's error class, so Active Job
  # callers get ActiveJob::EnqueueError. {Base::Core#enqueue} rescues it and
  # stores it in {Base::Core#enqueue_error}.
  class EnqueueError < StandardError; end

  # Raised when a stored payload names a job class that does not exist.
  # It is a NameError, so {Job#job_instance} with
  # ignore_deserialization_errors: true returns nil for it.
  class UnknownJobClassError < NameError
    # @param job_class_name [String] the job_class value from the payload
    def initialize(job_class_name)
      super("Failed to instantiate job, class `#{job_class_name}` doesn't exist", job_class_name)
    end
  end

  # A job class without Active Job. The API, payload keys and argument
  # encoding follow Active Job, so moving a job between the two is a one-line
  # change of superclass.
  #
  # Application job classes subclass Base and define #perform. The class DSL
  # comes from {Core}, {Callbacks} and {Exceptions}. The DecentJob extensions
  # (labels, notify options, concurrency, batches and cron validation) are
  # included by default. {JobExtensions::InterruptErrors} is opt-in.
  # {Integrations::Native} deserializes and runs stored native jobs.
  #
  # Class settings are inherited by subclasses until a subclass assigns its
  # own value (see {Support.class_setting}). Instances do not get writers for
  # class settings.
  #
  # Differences from Active Job:
  # - No locale or timezone payload keys.
  # - No report: option on retry_on or discard_on.
  # - Callback filters must be blocks or method names, not objects.
  # - retry_jitter defaults to 0.0. Active Job uses 0.15.
  #
  # See docs/native-jobs.md.
  class Base
    # Defines an inherited class setting with a reader and a writer.
    #
    # @param name [Symbol] the setting name
    # @param default [Object] the value when no class in the chain set one
    # @return [void]
    def self.setting(name, default)
      Support.class_setting(self, name, default)
    end
    private_class_method :setting

    # The queue name part used when queue_as was not called.
    setting :default_queue_name, "default"
    # A String, or a Proc that queue_as stored. Nil means default_queue_name.
    setting :queue_name, nil
    # Prepended to every queue name with queue_name_delimiter. Nil or blank
    # means no prefix.
    setting :queue_name_prefix, nil
    setting :queue_name_delimiter, "_"
    # An Integer, a Proc that queue_with_priority stored, or nil for no priority.
    setting :priority, nil
    # The fraction of a retry delay to add as random jitter. 0.0 disables it.
    setting :retry_jitter, 0.0
    # Frozen list of {Exceptions::Handler}. Do not mutate; assign a new array.
    setting :rescue_handlers, [].freeze
    # Frozen list of after_discard blocks. Do not mutate; assign a new array.
    setting :after_discard_procs, [].freeze
    # The adapter for this class. Nil means {DecentJob.default_adapter}.
    setting :queue_adapter, nil

    class << self
      alias_method :explicit_queue_adapter, :queue_adapter

      # Returns the adapter that enqueues jobs of this class.
      #
      # A class without its own adapter shares the default one, as its
      # instances do, so batch and cron validation see the same adapter.
      #
      # @return [Adapter, Object] the class adapter, or {DecentJob.default_adapter}
      def queue_adapter
        explicit_queue_adapter || DecentJob.default_adapter
      end
    end

    # A job class with enqueue options bound to it. {Base.set} returns one.
    # It matches ActiveJob::ConfiguredJob.
    class ConfiguredJob
      # @param job_class [Class<Base>] the job class
      # @param options [Hash] the options that {Core#set} accepts, plus
      #   :decent_job_labels and :decent_job_notify
      def initialize(job_class, options)
        @job_class = job_class
        @options = options
      end

      # Builds a job with the arguments and runs it in this thread.
      #
      # @return [Object] see {Core#perform_now}
      def perform_now(...)
        @job_class.new(...).set(@options).perform_now
      end

      # Builds a job with the arguments and enqueues it with the bound options.
      #
      # @yieldparam job [Base] the job, after the enqueue attempt
      # @return [Base, false] see {Core#enqueue}
      def perform_later(...)
        job = @job_class.new(...)
        result = job.enqueue(@options)
        yield job if block_given?
        result
      end
    end

    class << self
      # Builds a job and enqueues it. Enqueue callbacks run.
      #
      # @param arguments [Array] the perform arguments, or one job instance of
      #   this class to enqueue as is
      # @yieldparam job [Base] the job, after the enqueue attempt, also when
      #   the enqueue failed
      # @return [Base, false] the job, or false when a callback aborted or the
      #   adapter rejected it
      def perform_later(...)
        job = job_or_instantiate(...)
        result = job.enqueue
        yield job if block_given?
        result
      end

      # Builds a job and runs it in this thread without the adapter.
      #
      # @param arguments [Array] the perform arguments, or one job instance
      # @return [Object] see {Core#perform_now}
      def perform_now(...)
        job_or_instantiate(...).perform_now
      end

      # Binds enqueue options to the class.
      #
      # @param options [Hash] the options:
      #   - :wait [Numeric, #to_f] delay in seconds from now
      #   - :wait_until [Time] the scheduled time; wins over :wait
      #   - :queue [String, Symbol] queue name part; queue_name_prefix still applies
      #   - :priority [Integer, #to_i] the priority
      #   - :decent_job_labels [Array<String>] replaces the class labels
      #   - :decent_job_notify [Boolean, nil] overrides the class NOTIFY setting
      # @return [ConfiguredJob]
      def set(options = {})
        ConfiguredJob.new(self, options)
      end

      # Sets the queue for jobs of this class.
      #
      # @param part [String, Symbol, nil] queue name part, or nil for
      #   default_queue_name. The prefix is applied now.
      # @yieldreturn [String, Symbol, nil] the queue name part. The block runs
      #   with instance_exec on the job, the first time the job reads
      #   queue_name, and the prefix is applied then.
      # @return [void]
      def queue_as(part = nil, &block)
        self.queue_name = block || queue_name_from_part(part)
      end

      # Sets the priority for jobs of this class.
      #
      # @param priority [Integer, nil] the priority
      # @yieldreturn [Integer, nil] the priority. The block runs with
      #   instance_exec on the job, the first time the job reads priority.
      # @return [void]
      def queue_with_priority(priority = nil, &block)
        self.priority = block || priority
      end

      # Builds a full queue name from a part.
      #
      # @param part [String, Symbol, nil] the part, or nil for default_queue_name
      # @return [String] the part, with queue_name_prefix and
      #   queue_name_delimiter in front when a prefix is set
      def queue_name_from_part(part)
        prefix = queue_name_prefix
        [(Support.blank?(prefix) ? nil : prefix), part || default_queue_name].compact.join(queue_name_delimiter)
      end

      # Builds a job instance from a stored payload. Arguments stay serialized
      # until the job needs them.
      #
      # @param job_data [Hash{String => Object}] the payload from {Core#serialize}
      # @return [Base] an instance of the class that job_class names
      # @raise [UnknownJobClassError] when the class does not exist
      # @raise [ArgumentError] when the class is not a Base subclass
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

      # ruby2_keywords keeps a trailing keyword hash flagged, so perform still
      # receives keywords after the arguments are stored and loaded.
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
    require "decent_job/job_extensions/interrupt_errors"
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
    # Enqueues jobs in bulk, one adapter call for each adapter.
    #
    # Inside a cron entry, the jobs are checked against the cron database
    # first.
    #
    # @param jobs [Array<Base, Array<Base>>] the jobs; nested arrays are flattened
    # @return [nil]
    # @note Enqueue callbacks do not run, as in Active Job. The exception is a
    #   class with concurrency limits or rules: {Adapter#enqueue_all} enqueues
    #   it on its own, with callbacks, so admission sees the final key and
    #   labels.
    def perform_all_later(*jobs)
      jobs = jobs.flatten
      CronEntry.validate_jobs(jobs) if CronEntry.current_context
      jobs.group_by(&:queue_adapter).each { |adapter, group| adapter.enqueue_all(group) }
      nil
    end

    # Returns the adapter that native jobs without their own queue_adapter
    # share. It is created on first use and uses the global database.
    #
    # @return [Adapter]
    # @note Thread safe. reset! retires it with the other adapters and clears it.
    def default_adapter
      @default_adapter_mutex.synchronize { @default_adapter ||= Adapter.new }
    end
  end

  @default_adapter_mutex = Mutex.new
end
