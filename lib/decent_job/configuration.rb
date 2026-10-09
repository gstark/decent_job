# frozen_string_literal: true

require "active_support/tagged_logging"
require "json"
require "logger"

module DecentJob
  class Configuration
    SERVER_ASYNC_MODES = %i[async async_server].freeze
    ASYNC_MODES = (SERVER_ASYNC_MODES + [:async_all]).freeze
    EXECUTION_MODES = (ASYNC_MODES + %i[external inline]).freeze

    def self.validate_execution_mode!(mode)
      raise ArgumentError, "execution_mode must be #{EXECUTION_MODES.join(", ")}" unless EXECUTION_MODES.include?(mode)
    end

    def in_webserver?
      return true if defined?(Rails::Server)
      caller.any? do |frame|
        frame.match?(%r{config\.ru|/puma/(?:request|response)\.rb|/puma/(?:cluster/worker|single)\.rb:\d+:|/rack/handler/}) ||
          (RUBY_ENGINE == "jruby" && frame.include?("jruby/rack/rails_booter"))
      end
    end

    attr_accessor :database_url, :queues, :max_threads,
      :handled_exceptions, :retry_on_unhandled_error, :enable_listen_notify, :shutdown_timeout, :enable_pauses, :preserve_job_records, :cron, :enable_cron, :cron_graceful_restart_period

    attr_reader :max_cache, :advisory_lock_heartbeat, :process_heartbeat_interval, :cleanup_discarded_jobs, :cleanup_preserved_jobs_before_seconds_ago,
      :cleanup_interval_jobs, :cleanup_interval_seconds, :probe_port, :probe_handler, :probe_app, :daemonize, :pidfile, :idle_timeout, :lower_thread_priority

    alias_method :cleanup_discarded_jobs?, :cleanup_discarded_jobs
    alias_method :daemonize?, :daemonize

    def initialize
      self.lower_thread_priority = ENV.fetch("DECENT_JOB_LOWER_THREAD_PRIORITY", "false") == "true"
      self.daemonize = ENV.fetch("DECENT_JOB_DAEMONIZE", "false")
      self.pidfile = ENV.fetch("DECENT_JOB_PIDFILE", File.expand_path("tmp/pids/decent_job.pid", Dir.pwd))
      self.idle_timeout = Float(ENV["DECENT_JOB_IDLE_TIMEOUT"]) if ENV["DECENT_JOB_IDLE_TIMEOUT"]
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
      @environment_execution_mode = ENV["DECENT_JOB_EXECUTION_MODE"]&.to_sym
      @queues = ENV.fetch("DECENT_JOB_QUEUES", "*")
      @max_threads = Integer(ENV.fetch("DECENT_JOB_MAX_THREADS", "5"))
      @environment_poll_interval = Float(ENV["DECENT_JOB_POLL_INTERVAL"]) if ENV["DECENT_JOB_POLL_INTERVAL"]
      self.max_cache = Integer(ENV.fetch("DECENT_JOB_MAX_CACHE", "10000"), 10)
      @enable_listen_notify = ENV.fetch("DECENT_JOB_ENABLE_LISTEN_NOTIFY", "true") != "false"
      @enable_pauses = ENV.fetch("DECENT_JOB_ENABLE_PAUSES", "false") == "true"
      @shutdown_timeout = Float(ENV.fetch("DECENT_JOB_SHUTDOWN_TIMEOUT", "30"))
      @default_logger = ActiveSupport::TaggedLogging.new(ActiveSupport::Logger.new($stdout))
    end

    attr_writer :execution_mode, :logger, :on_thread_error

    def execution_mode
      return @execution_mode unless @execution_mode.nil?
      @rails_execution_mode || @environment_execution_mode || @rails_default_execution_mode || :external
    end

    def on_thread_error
      return @on_thread_error if defined?(@on_thread_error)
      ->(error) { logger.error("#{error.class}: #{error.message}") }
    end

    def logger
      @logger || @rails_logger || @default_logger
    end

    def apply_rails_config(options, environment:, logger:)
      @rails_environment = environment
      @rails_poll_interval = options[:poll_interval]
      @rails_default_execution_mode = {"development" => :async, "test" => :inline}.fetch(environment, :external)
      @rails_execution_mode = options[:execution_mode]&.to_sym
      @rails_logger = options[:logger] || logger
      previous = @rails_options || {}
      options.each do |name, value|
        next if %i[execution_mode logger poll_interval].include?(name.to_sym) || (previous.key?(name) && previous[name] == value)
        public_send("#{name}=", value)
      end
      @rails_options = options.to_h.dup
      self
    end

    def poll_interval
      return @poll_interval unless @poll_interval.nil?
      return @rails_poll_interval unless @rails_poll_interval.nil?
      return @environment_poll_interval unless @environment_poll_interval.nil?
      (@rails_environment == "development" && ASYNC_MODES.include?(execution_mode)) ? -1 : 10
    end

    def poll_interval=(value)
      self.class.validate_poll_interval!(value)
      @poll_interval = value
    end

    def self.validate_poll_interval!(value)
      unless value.is_a?(Numeric) && value.real? && value.finite?
        raise ArgumentError, "poll_interval must be a finite real number"
      end
    end

    def max_cache=(value)
      raise ArgumentError, "max_cache must be a nonnegative integer" unless value.is_a?(Integer) && value >= 0
      @max_cache = value
    end

    def self.validate_lower_thread_priority!(value)
      raise ArgumentError, "lower_thread_priority must be true or false" unless [true, false].include?(value)
    end

    def lower_thread_priority=(value)
      self.class.validate_lower_thread_priority!(value)
      @lower_thread_priority = value
    end

    def daemonize=(value)
      value = true if value == "true"
      value = false if value == "false"
      raise ArgumentError, "daemonize must be true or false" unless [true, false].include?(value)
      @daemonize = value
    end

    def pidfile=(value)
      value = value.to_path if value.respond_to?(:to_path)
      unless value.is_a?(String) && !value.strip.empty? && !value.include?("\0")
        raise ArgumentError, "pidfile must be a nonempty path"
      end
      @pidfile = File.expand_path(value)
    end

    def self.validate_idle_timeout!(value)
      if !value.nil? && !(value.is_a?(Numeric) && value.real? && value.finite? && value >= 0)
        raise ArgumentError, "idle_timeout must be nil or a finite nonnegative number"
      end
    end

    def idle_timeout=(value)
      self.class.validate_idle_timeout!(value)
      @idle_timeout = value
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
        logger.warn("Queue groups override subprocesses=#{@subprocesses} with #{groups.size}")
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
      self.class.validate_execution_mode!(execution_mode)
      queue_groups.each { |group| QueueSelection.pools(group, default_threads: @max_threads) }
      self.class.validate_poll_interval!(poll_interval)
      raise ArgumentError, "shutdown_timeout must be nonnegative" unless @shutdown_timeout.is_a?(Numeric) && @shutdown_timeout >= 0 && @shutdown_timeout.finite?
      raise ArgumentError, "on_thread_error must be callable" unless on_thread_error.respond_to?(:call)
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
      {graceful_restart_period: @cron_graceful_restart_period, on_thread_error: on_thread_error}
    end

    def worker_options
      {advisory_lock_heartbeat: @advisory_lock_heartbeat, process_heartbeat_interval: @process_heartbeat_interval,
       queues: @queues, max_threads: @max_threads, poll_interval: poll_interval, max_cache: @max_cache,
       enable_listen_notify: @enable_listen_notify, lower_thread_priority: @lower_thread_priority, on_thread_error: on_thread_error,
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
