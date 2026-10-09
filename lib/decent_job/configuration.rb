# frozen_string_literal: true

require "json"
require "logger"

module DecentJob
  class Configuration
    attr_accessor :database_url, :execution_mode, :queues, :max_threads,
      :handled_exceptions, :retry_on_unhandled_error, :poll_interval, :enable_listen_notify, :shutdown_timeout, :logger, :on_thread_error, :enable_pauses, :preserve_job_records, :cron, :enable_cron, :cron_graceful_restart_period

    attr_reader :advisory_lock_heartbeat, :process_heartbeat_interval, :cleanup_discarded_jobs, :cleanup_preserved_jobs_before_seconds_ago,
      :cleanup_interval_jobs, :cleanup_interval_seconds, :probe_port, :probe_handler, :probe_app

    alias_method :cleanup_discarded_jobs?, :cleanup_discarded_jobs

    def initialize
      self.subprocesses = Integer(ENV.fetch("DECENT_JOB_SUBPROCESSES", "0"), 10)
      self.probe_port = Integer(ENV["DECENT_JOB_PROBE_PORT"], 10) if ENV["DECENT_JOB_PROBE_PORT"]
      self.probe_handler = ENV["DECENT_JOB_PROBE_HANDLER"]
      self.advisory_lock_heartbeat = ENV.fetch("DECENT_JOB_ADVISORY_LOCK_HEARTBEAT", "false")
      self.process_heartbeat_interval = Float(ENV.fetch("DECENT_JOB_PROCESS_HEARTBEAT_INTERVAL", "30"))
      self.cleanup_discarded_jobs = ENV.fetch("DECENT_JOB_CLEANUP_DISCARDED_JOBS", "true")
      self.cleanup_preserved_jobs_before_seconds_ago = Float(ENV.fetch("DECENT_JOB_CLEANUP_PRESERVED_JOBS_BEFORE_SECONDS_AGO", "1209600"))
      self.cleanup_interval_jobs = ENV["DECENT_JOB_CLEANUP_INTERVAL_JOBS"]
      self.cleanup_interval_seconds = ENV["DECENT_JOB_CLEANUP_INTERVAL_SECONDS"]
      @handled_exceptions = [StandardError, NotImplementedError]
      @retry_on_unhandled_error = false
      @preserve_job_records = true
      @enable_cron = ENV.fetch("DECENT_JOB_ENABLE_CRON", "false") == "true"
      @cron_graceful_restart_period = Float(ENV["DECENT_JOB_CRON_GRACEFUL_RESTART_PERIOD"]) if ENV["DECENT_JOB_CRON_GRACEFUL_RESTART_PERIOD"]
      @cron = JSON.parse(ENV.fetch("DECENT_JOB_CRON", "{}"), symbolize_names: true)
      @database_url = ENV["DECENT_JOB_DATABASE_URL"] || ENV["DATABASE_URL"]
      @execution_mode = ENV.fetch("DECENT_JOB_EXECUTION_MODE", "external").to_sym
      @queues = ENV.fetch("DECENT_JOB_QUEUES", "*")
      @max_threads = Integer(ENV.fetch("DECENT_JOB_MAX_THREADS", "5"))
      @poll_interval = Float(ENV.fetch("DECENT_JOB_POLL_INTERVAL", "1"))
      @enable_listen_notify = ENV.fetch("DECENT_JOB_ENABLE_LISTEN_NOTIFY", "true") != "false"
      @enable_pauses = ENV.fetch("DECENT_JOB_ENABLE_PAUSES", "false") == "true"
      @shutdown_timeout = Float(ENV.fetch("DECENT_JOB_SHUTDOWN_TIMEOUT", "30"))
      @logger = Logger.new($stdout)
      @on_thread_error = ->(error) { @logger.error("#{error.class}: #{error.message}") }
    end

    def subprocesses=(value)
      raise ArgumentError, "subprocesses must be a nonnegative integer" unless value.is_a?(Integer) && value >= 0
      @subprocesses = value
    end

    def subprocesses
      groups = queue_groups
      (groups.size > 1) ? groups.size : @subprocesses
    end

    def subprocess_configs
      groups = queue_groups
      if groups.size > 1 && @subprocesses.positive?
        @logger.warn("Queue groups override subprocesses=#{@subprocesses} with #{groups.size}")
      end
      recipes = (groups.size > 1) ? groups : Array.new(subprocesses, groups.first)
      recipes.map { |queues| child_configuration(queues) }
    end

    def single_process_config
      child_configuration(queue_groups.join(";"))
    end

    def self.validate_process_options!(advisory, interval)
      raise ArgumentError, "advisory_lock_heartbeat must be true or false" unless [true, false].include?(advisory)
      unless interval.is_a?(Numeric) && interval.real? && interval.finite? && interval.positive? && interval < 300
        raise ArgumentError, "process_heartbeat_interval must be finite, positive, and less than 300"
      end
    end

    def probe_port=(value)
      if !value.nil? && (!value.is_a?(Integer) || !(0..65_535).cover?(value))
        raise ArgumentError, "probe_port must be nil or an integer between 0 and 65535"
      end
      @probe_port = value
    end

    def probe_handler=(value)
      unless value.nil? || value.is_a?(String) || value.is_a?(Symbol)
        raise ArgumentError, "probe_handler must be nil, a string, or a symbol"
      end
      @probe_handler = value&.to_sym
    end

    def probe_app=(value)
      raise ArgumentError, "probe_app must be callable or nil" unless value.nil? || value.respond_to?(:call)
      @probe_app = value
    end

    def advisory_lock_heartbeat=(value)
      value = true if value == "true"
      value = false if value == "false"
      self.class.validate_process_options!(value, @process_heartbeat_interval || 30)
      @advisory_lock_heartbeat = value
    end

    def process_heartbeat_interval=(value)
      self.class.validate_process_options!(@advisory_lock_heartbeat, value)
      @process_heartbeat_interval = value
    end

    def cleanup_discarded_jobs=(value)
      value = true if value == "true"
      value = false if value == "false"
      raise ArgumentError, "cleanup_discarded_jobs must be true or false" unless [true, false].include?(value)
      @cleanup_discarded_jobs = value
    end

    def cleanup_preserved_jobs_before_seconds_ago=(value)
      unless value.is_a?(Numeric) && value.real? && value.finite? && value >= 0
        raise ArgumentError, "cleanup_preserved_jobs_before_seconds_ago must be a finite nonnegative number"
      end
      @cleanup_preserved_jobs_before_seconds_ago = value
    end

    def cleanup_interval_jobs=(value)
      @cleanup_interval_jobs = cleanup_interval(value, default: 1000, integer: true)
    end

    def cleanup_interval_seconds=(value)
      @cleanup_interval_seconds = cleanup_interval(value, default: 600)
    end

    def handled_exception_classes
      validate_handled_exceptions!

      @handled_exceptions.map do |value|
        klass = value.is_a?(String) ? value.constantize : value
        raise ArgumentError, "handled_exceptions must contain exception classes or class names" unless klass.is_a?(Class) && klass <= Exception
        klass
      rescue NameError
        raise ArgumentError, "handled_exceptions contains an unknown exception class: #{value.inspect}"
      end
    end

    def validate!
      validate_handled_exceptions!
      raise ArgumentError, "retry_on_unhandled_error must be true or false" unless [true, false].include?(@retry_on_unhandled_error)
      raise ArgumentError, "execution_mode must be external, inline, or async" unless %i[external inline async].include?(@execution_mode)
      queue_groups.each { |group| QueueSelection.pools(group, default_threads: @max_threads) }
      raise ArgumentError, "poll_interval must be positive" unless @poll_interval.is_a?(Numeric) && @poll_interval.positive? && @poll_interval.finite?
      raise ArgumentError, "shutdown_timeout must be nonnegative" unless @shutdown_timeout.is_a?(Numeric) && @shutdown_timeout >= 0 && @shutdown_timeout.finite?
      raise ArgumentError, "on_thread_error must be callable" unless @on_thread_error.respond_to?(:call)
      raise ArgumentError, "enable_cron must be true or false" unless [true, false].include?(@enable_cron)
      period = @cron_graceful_restart_period
      if !period.nil? && (!period.is_a?(Numeric) || !period.finite? || !period.positive?)
        raise ArgumentError, "cron_graceful_restart_period must be a finite positive number or nil"
      end
      unless [true, false, :on_unhandled_error].include?(@preserve_job_records) || @preserve_job_records.respond_to?(:call)
        raise ArgumentError, "preserve_job_records must be true, false, :on_unhandled_error, or callable"
      end
      cron_entries
      self
    end

    def cron_entries(database: nil)
      raise ArgumentError, "cron must be a Hash" unless @cron.is_a?(Hash)

      @cron.map do |key, attributes|
        raise ArgumentError, "cron entry #{key} must be a Hash" unless attributes.is_a?(Hash)
        entry = CronEntry.new(attributes.merge(key: key), database: database)
        raise ArgumentError, "invalid cron entry #{key}: #{entry.errors.inspect}" unless entry.valid?
        entry
      end
    end

    def pool_size
      cron_connections = if @enable_cron
        @cron.size * (@cron_graceful_restart_period ? 4 : 3)
      else
        0
      end
      QueueSelection.pools(queue_groups.join(";"), default_threads: @max_threads).sum { |pool| pool.fetch(:threads) } + 2 + cron_connections
    end

    def cron_manager_options
      {graceful_restart_period: @cron_graceful_restart_period, on_thread_error: @on_thread_error}
    end

    def worker_options
      {advisory_lock_heartbeat: @advisory_lock_heartbeat, process_heartbeat_interval: @process_heartbeat_interval,
       queues: @queues, max_threads: @max_threads, poll_interval: @poll_interval,
       enable_listen_notify: @enable_listen_notify, on_thread_error: @on_thread_error,
       cleanup_interval_jobs: @cleanup_interval_jobs, cleanup_interval_seconds: @cleanup_interval_seconds,
       cleanup_preserved_jobs_before_seconds_ago: @cleanup_preserved_jobs_before_seconds_ago,
       cleanup_discarded_jobs: @cleanup_discarded_jobs}
    end

    private

    def queue_groups
      raise ArgumentError, "queues must be a nonempty string" unless @queues.is_a?(String) && !@queues.strip.empty?
      groups = @queues.split("|", -1).map(&:strip)
      raise ArgumentError, "queue groups must not be empty" if groups.any?(&:empty?)
      groups
    end

    def child_configuration(queues)
      dup.tap do |configuration|
        configuration.queues = queues
        configuration.subprocesses = 0
      end
    end

    def validate_handled_exceptions!
      valid = @handled_exceptions.is_a?(Array) && @handled_exceptions.all? do |value|
        (value.is_a?(Class) && value <= Exception) || (value.is_a?(String) && !value.empty?)
      end
      raise ArgumentError, "handled_exceptions must be an Array of exception classes or class names" unless valid
    end

    def cleanup_interval(value, default:, integer: false)
      return default if value.nil? || value == "" || value == true
      return false if [false, "false", 0, "0"].include?(value)
      value = integer ? Integer(value, 10) : Float(value) if value.is_a?(String)
      name = integer ? :cleanup_interval_jobs : :cleanup_interval_seconds
      CleanupTracker.new(**{name => value})
      value
    end
  end
end
