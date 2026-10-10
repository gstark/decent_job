# frozen_string_literal: true

require "decent_job/logging"
require "decent_job/support"
require "decent_job/configuration/errors"
require "decent_job/configuration/validator"
require "json"
require "logger"

module DecentJob
  # Holds every runtime setting and resolves each one from its sources.
  #
  # Each setting resolves in this order:
  #
  # 1. An explicit Ruby assignment, including CLI options.
  # 2. The current Rails `config.decent_job` options.
  # 3. The environment value captured when the object was constructed, then
  #    the library default.
  #
  # {DecentJob.configure}, {HostIntegration#configure_rails}, and
  # {ForkSafety#apply_cli_configuration} change a copy, validate it, and then
  # publish it under {ForkSafety#runtime_mutex}. A failed change never touches
  # the active object. A copy owns its nested Hash, Array, and String values,
  # but shares application objects such as classes, loggers, and callables.
  #
  # Ownership rules:
  #
  # - Setters in {GENERIC_OPTIONS} record an explicit assignment through
  #   {ExplicitAssignments}. {#apply_rails_config} never overwrites those.
  # - {SPECIAL_OPTIONS} keep one instance variable per source, for example
  #   @poll_interval, @rails_poll_interval, and @environment_poll_interval,
  #   and their getters pick the first non-nil value.
  #
  # Environment variables are read only in {#initialize}. Later ENV changes do
  # not affect an existing object.
  #
  # Thread safety: the object is not synchronized. Mutate only a candidate
  # copy, or the active object while no worker reads it.
  #
  # Workers copy {#worker_options} when they are built, so later changes to
  # those settings do not reach a running worker. Settings that are not in
  # {#worker_options}, such as {#lock_strategy} and {#enable_pauses}, are read
  # from {DecentJob.configuration} on each use.
  #
  # See docs/configuration.md and docs/configuration-reference.md.
  class Configuration
    # Modes that run jobs in background threads only inside a web server.
    SERVER_ASYNC_MODES = %i[async async_server].freeze
    # Modes that run jobs in background threads of the enqueuing process.
    ASYNC_MODES = (SERVER_ASYNC_MODES + [:async_all]).freeze
    # All valid values of {#execution_mode}.
    EXECUTION_MODES = (ASYNC_MODES + %i[external inline]).freeze

    # Settings with one instance variable for all sources. Rails options write
    # through the normal setter, and explicit assignments are recorded.
    #
    # These setters retain their own validation; the wrapper records ownership only
    # after a successful application assignment, never during initialization/host setup.
    GENERIC_OPTIONS = %i[database_url queues max_threads handled_exceptions retry_on_unhandled_error
      enable_listen_notify shutdown_timeout enable_pauses preserve_job_records cron enable_cron
      cron_graceful_restart_period max_cache advisory_lock_heartbeat process_heartbeat_interval
      cleanup_discarded_jobs cleanup_preserved_jobs_before_seconds_ago cleanup_interval_jobs
      cleanup_interval_seconds probe_port probe_handler probe_app daemonize pidfile idle_timeout
      lower_thread_priority subprocesses on_thread_error].freeze
    # Settings with a separate value per source and a resolving getter. A nil
    # assignment clears the explicit value, so the lower sources apply again.
    # poll_interval is the exception: its setter rejects nil.
    SPECIAL_OPTIONS = %i[execution_mode logger poll_interval dequeue_query_sort queue_select_limit lock_strategy].freeze

    # Marks a {GENERIC_OPTIONS} setting as explicit after its setter succeeds.
    #
    # @explicit_options is nil during {#initialize}, so environment defaults
    # are not explicit. @applying_rails_options is true inside
    # {#apply_rails_config}, so Rails values are not explicit either.
    module ExplicitAssignments
      GENERIC_OPTIONS.each do |name|
        define_method("#{name}=") do |value|
          result = super(value)
          @explicit_options[name] = true if @explicit_options && !@applying_rails_options
          result
        end
      end
    end
    prepend ExplicitAssignments

    # @param mode [Symbol] the mode to check
    # @return [nil]
    # @raise [ArgumentError] when the mode is not in {EXECUTION_MODES}
    def self.validate_execution_mode!(mode)
      raise ArgumentError, "execution_mode must be #{EXECUTION_MODES.join(", ")}" unless EXECUTION_MODES.include?(mode)
    end

    # Tells whether the current process serves web requests.
    #
    # The :async and :async_server modes run jobs only when this is true.
    # The result is computed on each call from Rails::Server or from caller
    # frames of config.ru, Puma request handling, Puma worker boot, Rack
    # handlers, and the JRuby Rack booter. A Puma cluster master does not
    # match.
    #
    # @return [Boolean] true when a web server is detected
    def in_webserver?
      return true if defined?(Rails::Server)

      caller.any? do |frame|
        frame.match?(%r{config\.ru|/puma/(?:request|response)\.rb|/puma/(?:cluster/worker|single)\.rb:\d+:|/rack/handler/}) ||
          (RUBY_ENGINE == "jruby" && frame.include?("jruby/rack/rails_booter"))
      end
    end

    # @!attribute database_url
    #   PostgreSQL connection input for the global {Database} pool.
    #   Default: DECENT_JOB_DATABASE_URL, then DATABASE_URL, then nil. nil
    #   means no global database; {DecentJob.database} raises. The value is
    #   read when the pool opens, and cannot change after that.
    #   @return [String, nil]
    # @!attribute queues
    #   Queue expression for workers. Semicolons separate queue pools, each
    #   with an optional thread count, for example "reports:2;*". Pipes
    #   separate subprocess groups. Default: DECENT_JOB_QUEUES, then "*".
    #   Blank values and empty groups are invalid.
    #   @return [String]
    # @!attribute max_threads
    #   Thread count for a queue pool that has no explicit count. Default:
    #   DECENT_JOB_MAX_THREADS, then RAILS_MAX_THREADS, then 5. Must be a
    #   positive Integer. It also sizes {#pool_size}.
    #   @return [Integer]
    # @!attribute handled_exceptions
    #   Exceptions that DecentJob catches and records as an attempt outcome.
    #   Default: [StandardError, NotImplementedError]. Entries are exception
    #   classes or class names; names resolve when an error is handled, see
    #   {#handled_exception_classes}. Other exceptions propagate, leave the
    #   attempt unfinished, and can end the worker thread.
    #   @return [Array<Class, String>]
    # @!attribute retry_on_unhandled_error
    #   When true, a caught error that the job did not handle makes the job
    #   ready to run again at once, with no limit or backoff. When false, the
    #   job is discarded. Default: false. Must be a Boolean.
    #   @return [Boolean]
    # @!attribute enable_listen_notify
    #   When true, enqueues send NOTIFY and each worker keeps one listener
    #   thread with its own dedicated session outside the pool. When false,
    #   workers find jobs only by polling. Default: true
    #   (DECENT_JOB_ENABLE_LISTEN_NOTIFY). Must be a Boolean.
    #   @return [Boolean]
    # @!attribute shutdown_timeout
    #   Seconds to wait for jobs to drain at process exit, in the CLI, in the
    #   supervisor, and before a fork. Default: 30
    #   (DECENT_JOB_SHUTDOWN_TIMEOUT). Must be finite and nonnegative. This
    #   is not the default of {DecentJob.shutdown}, which waits indefinitely.
    #   @return [Numeric]
    # @!attribute enable_pauses
    #   When true, claims skip jobs in paused queues, job classes, and labels.
    #   Default: false (DECENT_JOB_ENABLE_PAUSES). Read on each claim. Must be
    #   a Boolean.
    #   @return [Boolean]
    # @!attribute preserve_job_records
    #   Retention policy for finished jobs. true keeps all records, false
    #   deletes them, :on_unhandled_error keeps only jobs that ended with an
    #   unhandled error, and a callable decides per job. A callable that
    #   raises preserves the record. Default: true.
    #   @return [Boolean, Symbol, #call]
    # @!attribute cron
    #   Cron entry definitions, keyed by entry key. Each value is a Hash of
    #   {CronEntry} attributes. Default: DECENT_JOB_CRON parsed as JSON with
    #   Symbol keys, or {} when that is blank. A source replaces the whole
    #   Hash of a lower source; entries are not merged.
    #   @return [Hash{Symbol => Hash}]
    # @!attribute cron_graceful_restart_period
    #   Seconds of missed cron occurrences to catch up after cron starts. nil
    #   disables catch-up. Default: DECENT_JOB_CRON_GRACEFUL_RESTART_PERIOD,
    #   then nil. Must be finite and positive when set. Each cron entry needs
    #   one more pooled connection when set, see {#pool_size}.
    #   @return [Numeric, nil]
    attr_accessor :database_url, :queues, :max_threads,
      :handled_exceptions, :retry_on_unhandled_error, :enable_listen_notify, :shutdown_timeout, :enable_pauses, :preserve_job_records, :cron, :cron_graceful_restart_period

    # @!attribute [r] max_cache
    #   Maximum number of future scheduled times each queue pool caches.
    #   0 disables the cache. Default: 10000 (DECENT_JOB_MAX_CACHE).
    #   @return [Integer]
    # @!attribute [r] process_heartbeat_interval
    #   Seconds between process heartbeats. Default: 30
    #   (DECENT_JOB_PROCESS_HEARTBEAT_INTERVAL). Must be finite, positive, and
    #   less than 300.
    #   @return [Numeric]
    # @!attribute [r] cleanup_discarded_jobs
    #   When true, cleanup also deletes discarded jobs and their batches.
    #   Default: true (DECENT_JOB_CLEANUP_DISCARDED_JOBS).
    #   @return [Boolean]
    # @!attribute [r] cleanup_preserved_jobs_before_seconds_ago
    #   Age in seconds, measured from finished_at, after which cleanup deletes
    #   preserved records. Default: 1209600, which is 14 days
    #   (DECENT_JOB_CLEANUP_PRESERVED_JOBS_BEFORE_SECONDS_AGO).
    #   @return [Numeric]
    # @!attribute [r] cleanup_interval_jobs
    #   A worker runs automatic cleanup after more than this many completed
    #   attempts. -1 runs it after every attempt. false disables the count
    #   threshold. Default: 1000 (DECENT_JOB_CLEANUP_INTERVAL_JOBS).
    #   @return [Integer, false]
    # @!attribute [r] cleanup_interval_seconds
    #   A worker runs automatic cleanup when an attempt completes more than
    #   this many seconds after the last cleanup. -1 runs it after every
    #   attempt. false disables the time threshold. Default: 600
    #   (DECENT_JOB_CLEANUP_INTERVAL_SECONDS).
    #   @return [Numeric, false]
    # @!attribute [r] probe_port
    #   TCP port of the CLI health probe server. nil disables the server.
    #   0 picks a free port. Default: DECENT_JOB_PROBE_PORT, then nil.
    #   @return [Integer, nil]
    # @!attribute [r] probe_handler
    #   Probe server implementation. nil uses the built-in socket server;
    #   :webrick uses WEBrick when the host provides it. Default:
    #   DECENT_JOB_PROBE_HANDLER, then nil.
    #   @return [Symbol, nil]
    # @!attribute [r] probe_app
    #   Rack-style callable that answers probe requests. nil uses the default
    #   status routes. Default: nil.
    #   @return [#call, nil]
    # @!attribute [r] daemonize
    #   When true, the CLI start command detaches into the background and
    #   writes {#pidfile}. Default: false (DECENT_JOB_DAEMONIZE).
    #   @return [Boolean]
    # @!attribute [r] pidfile
    #   Absolute path of the daemon pidfile. Default: DECENT_JOB_PIDFILE, then
    #   tmp/pids/decent_job.pid under the working directory at construction.
    #   @return [String]
    # @!attribute [r] idle_timeout
    #   Seconds a single-process CLI worker can stay idle before it exits.
    #   nil disables idle exit. Cluster mode ignores it. Default:
    #   DECENT_JOB_IDLE_TIMEOUT, then nil.
    #   @return [Numeric, nil]
    # @!attribute [r] lower_thread_priority
    #   When true, worker execution threads run each attempt at Ruby thread
    #   priority -3. Default: false (DECENT_JOB_LOWER_THREAD_PRIORITY). Use
    #   {DecentJob.lower_thread_priority=} to change running workers too.
    #   @return [Boolean]
    attr_reader :max_cache, :process_heartbeat_interval, :cleanup_discarded_jobs, :cleanup_preserved_jobs_before_seconds_ago,
      :cleanup_interval_jobs, :cleanup_interval_seconds, :probe_port, :probe_handler, :probe_app, :daemonize, :pidfile, :idle_timeout, :lower_thread_priority

    alias_method :cleanup_discarded_jobs?, :cleanup_discarded_jobs
    alias_method :daemonize?, :daemonize

    # @return [Errors] the diagnostics from the last {#valid?} call
    attr_reader :errors

    # Builds a configuration from the environment and library defaults.
    #
    # @raise [ArgumentError] when an environment value is malformed, for
    #   example a non-numeric DECENT_JOB_MAX_THREADS
    # @raise [JSON::ParserError] when DECENT_JOB_CRON is not valid JSON
    def initialize
      @errors = Errors.new
      @environment_lock_strategy = normalize_lock_strategy(ENV["DECENT_JOB_LOCK_STRATEGY"])
      @environment_dequeue_query_sort = normalize_dequeue_query_sort(ENV["DECENT_JOB_DEQUEUE_QUERY_SORT"])
      @environment_queue_select_limit = normalize_queue_select_limit(ENV["DECENT_JOB_QUEUE_SELECT_LIMIT"])
      self.lower_thread_priority = environment_boolean("DECENT_JOB_LOWER_THREAD_PRIORITY", default: false)
      self.daemonize = environment_boolean("DECENT_JOB_DAEMONIZE", default: false)
      self.pidfile = ENV.fetch("DECENT_JOB_PIDFILE", File.expand_path("tmp/pids/decent_job.pid", Dir.pwd))
      self.idle_timeout = Float(ENV["DECENT_JOB_IDLE_TIMEOUT"]) if ENV["DECENT_JOB_IDLE_TIMEOUT"]
      self.subprocesses = Integer(ENV.fetch("DECENT_JOB_SUBPROCESSES", "0"), 10)
      self.probe_port = Integer(ENV["DECENT_JOB_PROBE_PORT"], 10) if ENV["DECENT_JOB_PROBE_PORT"]
      self.probe_handler = ENV["DECENT_JOB_PROBE_HANDLER"]
      @environment_advisory_lock_heartbeat = environment_boolean("DECENT_JOB_ADVISORY_LOCK_HEARTBEAT", default: nil)
      self.advisory_lock_heartbeat = @environment_advisory_lock_heartbeat || false
      self.process_heartbeat_interval = Float(ENV.fetch("DECENT_JOB_PROCESS_HEARTBEAT_INTERVAL", "30"))
      self.cleanup_discarded_jobs = environment_boolean("DECENT_JOB_CLEANUP_DISCARDED_JOBS", default: true)
      self.cleanup_preserved_jobs_before_seconds_ago = Float(ENV.fetch("DECENT_JOB_CLEANUP_PRESERVED_JOBS_BEFORE_SECONDS_AGO", "1209600"))
      self.cleanup_interval_jobs = ENV["DECENT_JOB_CLEANUP_INTERVAL_JOBS"]
      self.cleanup_interval_seconds = ENV["DECENT_JOB_CLEANUP_INTERVAL_SECONDS"]
      @handled_exceptions = [StandardError, NotImplementedError]
      @retry_on_unhandled_error = false
      @preserve_job_records = true
      @enable_cron = environment_boolean("DECENT_JOB_ENABLE_CRON", default: false)
      @cron_graceful_restart_period = Float(ENV["DECENT_JOB_CRON_GRACEFUL_RESTART_PERIOD"]) if ENV["DECENT_JOB_CRON_GRACEFUL_RESTART_PERIOD"]
      @cron = Support.present?(ENV["DECENT_JOB_CRON"]) ? JSON.parse(ENV.fetch("DECENT_JOB_CRON"), symbolize_names: true) : {}
      @database_url = ENV["DECENT_JOB_DATABASE_URL"] || ENV["DATABASE_URL"]
      @environment_execution_mode = ENV["DECENT_JOB_EXECUTION_MODE"]&.to_sym
      @queues = ENV.fetch("DECENT_JOB_QUEUES", "*")
      @max_threads = Integer(ENV["DECENT_JOB_MAX_THREADS"] || ENV["RAILS_MAX_THREADS"] || "5")
      @environment_poll_interval = Float(ENV["DECENT_JOB_POLL_INTERVAL"]) if ENV["DECENT_JOB_POLL_INTERVAL"]
      self.max_cache = Integer(ENV.fetch("DECENT_JOB_MAX_CACHE", "10000"), 10)
      @enable_listen_notify = environment_boolean("DECENT_JOB_ENABLE_LISTEN_NOTIFY", default: true)
      @enable_pauses = environment_boolean("DECENT_JOB_ENABLE_PAUSES", default: false)
      @shutdown_timeout = Float(ENV.fetch("DECENT_JOB_SHUTDOWN_TIMEOUT", "30"))
      @default_logger = Logging.tagged_logger($stdout)

      # The baseline is the value that comes back when a Rails option is removed.
      @baseline_options = GENERIC_OPTIONS.each_with_object({}) do |name, baseline|
        variable = "@#{name}"
        baseline[name] = copy_containers(instance_variable_get(variable)) if instance_variable_defined?(variable)
      end

      @explicit_options = {}
    end

    # Copies nested containers, so changes to the copy never reach the
    # original. Explicit-assignment marks and source values are copied too.
    def initialize_copy(original)
      super
      copies = {}.compare_by_identity

      original.instance_variables.each do |variable|
        instance_variable_set(variable, copy_containers(original.instance_variable_get(variable), copies))
      end

      @errors = original.errors.dup
    end

    # @!attribute [w] execution_mode
    #   @param value [Symbol, nil] an explicit mode, or nil to clear it
    # @!attribute [w] logger
    #   @param value [Logger, nil] an explicit logger, or nil to clear it
    # @!attribute [w] on_thread_error
    #   @param value [#call, nil] the reporter; nil disables reporting
    # @!attribute [w] enable_cron
    #   @param value [Boolean] the cron enable flag
    attr_writer :execution_mode, :logger, :on_thread_error, :enable_cron

    # Tells whether configured cron entries run with {DecentJob.start} and
    # async adapters.
    #
    # Default flag: DECENT_JOB_ENABLE_CRON, then false. The result is false
    # when {#cron} has no entries, even when the flag is true.
    #
    # @return [Boolean, Object] the effective value; a non-Boolean flag is
    #   returned as is, so validation can reject it
    def enable_cron
      return @enable_cron unless @enable_cron == true
      @cron.is_a?(Hash) && !@cron.empty?
    end

    alias_method :enable_cron?, :enable_cron

    # Tells whether process heartbeats use a session advisory lock instead of
    # timestamp updates.
    #
    # Default: true in Rails development, false otherwise. Any explicit,
    # Rails, or DECENT_JOB_ADVISORY_LOCK_HEARTBEAT value wins, also false.
    # Advisory heartbeats need direct connections or session pooling, not
    # PgBouncer transaction pooling.
    #
    # @return [Boolean]
    def advisory_lock_heartbeat
      if @explicit_options&.key?(:advisory_lock_heartbeat) || @rails_options&.key?(:advisory_lock_heartbeat) || !@environment_advisory_lock_heartbeat.nil?
        @advisory_lock_heartbeat
      else
        @rails_environment == "development"
      end
    end

    # Returns how adapters run enqueued jobs.
    #
    # - :external stores jobs for a separate worker process.
    # - :inline runs due jobs in the enqueuing thread after commit.
    # - :async and :async_server run jobs in background threads only inside a
    #   web server, see {#in_webserver?}.
    # - :async_all runs jobs in background threads in any process.
    #
    # Default: DECENT_JOB_EXECUTION_MODE, then :async in Rails development or
    # :inline in Rails test, then :external.
    #
    # @return [Symbol] one of {EXECUTION_MODES} after validation
    # @note The Rails option wins over the environment value here, but the
    #   environment value wins over the Rails environment default.
    def execution_mode
      return @execution_mode unless @execution_mode.nil?
      @rails_execution_mode || @environment_execution_mode || @rails_default_execution_mode || :external
    end

    # Returns the reporter for operational errors in owned threads.
    #
    # The default logs the error class and message to {#logger}. An explicit
    # nil disables reporting and wins over Rails options.
    #
    # @return [#call, nil] a callable that receives the exception, or nil
    def on_thread_error
      return @on_thread_error if defined?(@on_thread_error)
      ->(error) { logger.error("#{error.class}: #{error.message}") }
    end

    # @return [Logger] the explicit logger, else the Rails logger, else a
    #   tagged logger on $stdout
    def logger
      @logger || @rails_logger || @default_logger
    end

    # Applies Rails `config.decent_job` options as the middle source.
    #
    # Settings with an explicit assignment keep it. An unchanged Rails value
    # is not applied again, so edits made through mutable getters survive. A
    # removed option restores the construction baseline. A blank Rails cron
    # value counts as absent.
    #
    # @param options [#each] option names and values, with Symbol or String keys
    # @param environment [String] the Rails environment name
    # @param logger [Logger] the Rails logger, used unless options has :logger
    # @return [Configuration] self
    # @raise [ArgumentError] for an unknown option name or an invalid value
    # @note Mutates self. Callers pass a candidate copy.
    # @api private
    def apply_rails_config(options, environment:, logger:)
      # Use the public iterator: host options can be lazy application objects.
      normalized = {}

      options.each do |name, value|
        key = name.to_sym if name.is_a?(String) || name.is_a?(Symbol)

        unless GENERIC_OPTIONS.include?(key) || SPECIAL_OPTIONS.include?(key)
          raise ArgumentError, "unknown configuration option: #{name.inspect}"
        end

        normalized[key] = value unless key == :cron && Support.blank?(value)
      end

      options = normalized
      @rails_environment = environment
      @rails_poll_interval = options[:poll_interval]
      @rails_lock_strategy = normalize_lock_strategy(options[:lock_strategy])
      @rails_dequeue_query_sort = normalize_dequeue_query_sort(options[:dequeue_query_sort])
      @rails_queue_select_limit = normalize_queue_select_limit(options[:queue_select_limit])
      @rails_default_execution_mode = {"development" => :async, "test" => :inline}.fetch(environment, :external)
      @rails_execution_mode = options[:execution_mode]&.to_sym
      @rails_logger = options[:logger] || logger
      previous = @rails_options || {}
      @applying_rails_options = true

      begin
        GENERIC_OPTIONS.each do |name|
          next if @explicit_options.key?(name)

          if options.key?(name)
            # Preserve supported edits through mutable getters until the host value changes.
            next if previous.key?(name) && previous[name] == options[name]
            public_send("#{name}=", copy_containers(options[name]))
          elsif previous.key?(name)
            if @baseline_options.key?(name)
              public_send("#{name}=", copy_containers(@baseline_options[name]))
            elsif instance_variable_defined?("@#{name}")
              remove_instance_variable("@#{name}")
            end
          end
        end

        @rails_options = copy_containers(options)
      ensure
        @applying_rails_options = false
      end

      self
    end

    # Returns how workers claim jobs.
    #
    # - :advisory takes a session advisory lock and holds a connection for
    #   the whole attempt.
    # - :skiplocked claims a row with FOR UPDATE SKIP LOCKED.
    # - :hybrid combines both.
    #
    # Default: DECENT_JOB_LOCK_STRATEGY, then :advisory. Read on each claim.
    #
    # @return [Symbol]
    def lock_strategy
      @lock_strategy || @rails_lock_strategy || @environment_lock_strategy || :advisory
    end

    # @param value [Symbol, String, nil] the strategy, or nil to clear it
    # @raise [ArgumentError] for an unknown strategy
    def lock_strategy=(value)
      @lock_strategy = normalize_lock_strategy(value)
    end

    # Returns the column that orders claim candidates within one priority.
    #
    # Default: DECENT_JOB_DEQUEUE_QUERY_SORT, then :created_at.
    #
    # @return [Symbol] :created_at or :scheduled_at
    def dequeue_query_sort
      @dequeue_query_sort || @rails_dequeue_query_sort || @environment_dequeue_query_sort || :created_at
    end

    # @param value [Symbol, String, nil] the column, or nil to clear it
    # @raise [ArgumentError] for any other column
    def dequeue_query_sort=(value)
      @dequeue_query_sort = normalize_dequeue_query_sort(value)
    end

    # Returns the maximum number of candidates an advisory claim reads.
    #
    # Locked candidates count against the limit, so a claim can find no job
    # even when an unlocked job exists beyond it. SKIP LOCKED and hybrid
    # claims ignore it. Default: DECENT_JOB_QUEUE_SELECT_LIMIT, then 1000.
    #
    # @return [Integer]
    def queue_select_limit
      @queue_select_limit || @rails_queue_select_limit || @environment_queue_select_limit || 1000
    end

    # @param value [Integer, String, nil] a nonnegative limit, or nil to clear it
    # @raise [ArgumentError] for a negative or non-integer value
    def queue_select_limit=(value)
      @queue_select_limit = normalize_queue_select_limit(value)
    end

    # Returns the seconds between worker polls for due jobs.
    #
    # A value of zero or less disables polling; workers then depend on
    # LISTEN/NOTIFY and scheduled-time caches. Default:
    # DECENT_JOB_POLL_INTERVAL, then -1 in Rails development with an async
    # mode, otherwise 10.
    #
    # @return [Numeric]
    def poll_interval
      return @poll_interval unless @poll_interval.nil?
      return @rails_poll_interval unless @rails_poll_interval.nil?
      return @environment_poll_interval unless @environment_poll_interval.nil?
      (@rails_environment == "development" && ASYNC_MODES.include?(execution_mode)) ? -1 : 10
    end

    # @param value [Numeric] finite seconds; nil is invalid
    # @raise [ArgumentError] when the value is not a finite real number
    def poll_interval=(value)
      self.class.validate_poll_interval!(value)
      @poll_interval = value
    end

    # @param value [Object] the value to check
    # @return [nil]
    # @raise [ArgumentError] when the value is not a finite real number
    def self.validate_poll_interval!(value)
      unless value.is_a?(Numeric) && value.real? && value.finite?
        raise ArgumentError, "poll_interval must be a finite real number"
      end
    end

    # @param value [Integer] a nonnegative limit
    # @raise [ArgumentError] for any other value
    def max_cache=(value)
      raise ArgumentError, "max_cache must be a nonnegative integer" unless value.is_a?(Integer) && value >= 0
      @max_cache = value
    end

    # @param value [Object] the value to check
    # @return [nil]
    # @raise [ArgumentError] unless the value is true or false
    def self.validate_lower_thread_priority!(value)
      raise ArgumentError, "lower_thread_priority must be true or false" unless [true, false].include?(value)
    end

    # @param value [Boolean] the new setting
    # @raise [ArgumentError] unless the value is true or false
    def lower_thread_priority=(value)
      self.class.validate_lower_thread_priority!(value)
      @lower_thread_priority = value
    end

    # @param value [Boolean, String] true, false, "true", or "false"
    # @raise [ArgumentError] for any other value
    def daemonize=(value)
      value = true if value == "true"
      value = false if value == "false"
      raise ArgumentError, "daemonize must be true or false" unless [true, false].include?(value)
      @daemonize = value
    end

    # @param value [String, #to_path] the path; relative paths expand against
    #   the current working directory at assignment
    # @raise [ArgumentError] for a blank path or a path with a NUL byte
    def pidfile=(value)
      value = value.to_path if value.respond_to?(:to_path)

      unless value.is_a?(String) && !value.strip.empty? && !value.include?("\0")
        raise ArgumentError, "pidfile must be a nonempty path"
      end

      @pidfile = File.expand_path(value)
    end

    # @param value [Object] the value to check
    # @return [nil]
    # @raise [ArgumentError] unless the value is nil or finite and nonnegative
    def self.validate_idle_timeout!(value)
      if !value.nil? && !(value.is_a?(Numeric) && value.real? && value.finite? && value >= 0)
        raise ArgumentError, "idle_timeout must be nil or a finite nonnegative number"
      end
    end

    # @param value [Numeric, nil] seconds, or nil to disable idle exit
    # @raise [ArgumentError] for a negative or non-finite value
    def idle_timeout=(value)
      self.class.validate_idle_timeout!(value)
      @idle_timeout = value
    end

    # @param value [Integer] the requested number of CLI child processes
    # @raise [ArgumentError] for a negative or non-integer value
    def subprocesses=(value)
      raise ArgumentError, "subprocesses must be a nonnegative integer" unless value.is_a?(Integer) && value >= 0
      @subprocesses = value
    end

    # Returns the number of child processes the CLI forks.
    #
    # Two or more pipe-separated queue groups override the assigned count with
    # the group count. 0 runs workers in the CLI process. Default:
    # DECENT_JOB_SUBPROCESSES, then 0.
    #
    # @return [Integer]
    # @raise [ArgumentError] when {#queues} is invalid
    def subprocesses
      groups = queue_groups
      (groups.size > 1) ? groups.size : @subprocesses
    end

    # Builds one configuration per CLI child process.
    #
    # Each copy has one queue group and subprocesses set to 0. Without queue
    # groups, every copy uses the full queue expression.
    #
    # @return [Array<Configuration>] the child recipes
    # @note Logs a warning when queue groups override an assigned count.
    # @api private
    def subprocess_configs
      groups = queue_groups

      if groups.size > 1 && @subprocesses.positive?
        logger.warn("Queue groups override subprocesses=#{@subprocesses} with #{groups.size}")
      end

      recipes = (groups.size > 1) ? groups : Array.new(subprocesses, groups.first)
      recipes.map { |queues| child_configuration(queues) }
    end

    # Builds a copy that runs all queue groups in one process.
    #
    # The CLI uses it when fork is not available. Pipe separators become
    # semicolons, so the pools of all groups run in one worker.
    #
    # @return [Configuration] the copy, with subprocesses set to 0
    # @api private
    def single_process_config
      child_configuration(queue_groups.join(";"))
    end

    # Checks the two process heartbeat settings together.
    #
    # @param advisory [Object] the advisory_lock_heartbeat value
    # @param interval [Object] the process_heartbeat_interval value in seconds
    # @return [nil]
    # @raise [ArgumentError] when advisory is not a Boolean, or interval is not
    #   finite, positive, and less than 300
    def self.validate_process_options!(advisory, interval)
      raise ArgumentError, "advisory_lock_heartbeat must be true or false" unless [true, false].include?(advisory)

      unless interval.is_a?(Numeric) && interval.real? && interval.finite? && interval.positive? && interval < 300
        raise ArgumentError, "process_heartbeat_interval must be finite, positive, and less than 300"
      end
    end

    # @param value [Integer, nil] a port from 0 to 65535, or nil
    # @raise [ArgumentError] for any other value
    def probe_port=(value)
      if !value.nil? && (!value.is_a?(Integer) || !(0..65_535).cover?(value))
        raise ArgumentError, "probe_port must be nil or an integer between 0 and 65535"
      end

      @probe_port = value
    end

    # @param value [String, Symbol, nil] the handler name, stored as a Symbol
    # @raise [ArgumentError] for any other type
    def probe_handler=(value)
      unless value.nil? || value.is_a?(String) || value.is_a?(Symbol)
        raise ArgumentError, "probe_handler must be nil, a string, or a symbol"
      end

      @probe_handler = value&.to_sym
    end

    # @param value [#call, nil] the probe application
    # @raise [ArgumentError] when the value is not callable
    def probe_app=(value)
      raise ArgumentError, "probe_app must be callable or nil" unless value.nil? || value.respond_to?(:call)
      @probe_app = value
    end

    # @param value [Boolean, String] true, false, "true", or "false"
    # @raise [ArgumentError] for any other value
    def advisory_lock_heartbeat=(value)
      value = true if value == "true"
      value = false if value == "false"
      self.class.validate_process_options!(value, @process_heartbeat_interval || 30)
      @advisory_lock_heartbeat = value
    end

    # @param value [Numeric] seconds, finite, positive, and less than 300
    # @raise [ArgumentError] for any other value
    def process_heartbeat_interval=(value)
      self.class.validate_process_options!(advisory_lock_heartbeat, value)
      @process_heartbeat_interval = value
    end

    # @param value [Boolean, String] true, false, "true", or "false"
    # @raise [ArgumentError] for any other value
    def cleanup_discarded_jobs=(value)
      value = true if value == "true"
      value = false if value == "false"
      raise ArgumentError, "cleanup_discarded_jobs must be true or false" unless [true, false].include?(value)
      @cleanup_discarded_jobs = value
    end

    # @param value [Numeric] finite, nonnegative seconds
    # @raise [ArgumentError] for any other value
    def cleanup_preserved_jobs_before_seconds_ago=(value)
      unless value.is_a?(Numeric) && value.real? && value.finite? && value >= 0
        raise ArgumentError, "cleanup_preserved_jobs_before_seconds_ago must be a finite nonnegative number"
      end

      @cleanup_preserved_jobs_before_seconds_ago = value
    end

    # @param value [Integer, String, Boolean, nil] a positive count or -1;
    #   false, 0, "false", or "0" disables; nil, true, or "" restores 1000
    # @raise [ArgumentError] for any other value
    def cleanup_interval_jobs=(value)
      @cleanup_interval_jobs = cleanup_interval(value, default: 1000, integer: true)
    end

    # @param value [Numeric, String, Boolean, nil] positive seconds or -1;
    #   false, 0, "false", or "0" disables; nil, true, or "" restores 600
    # @raise [ArgumentError] for any other value
    def cleanup_interval_seconds=(value)
      @cleanup_interval_seconds = cleanup_interval(value, default: 600)
    end

    # Resolves {#handled_exceptions} to classes.
    #
    # Names resolve on each call, so application classes can load after
    # configuration. {Performer} calls this when it rescues an attempt error.
    #
    # @return [Array<Class>] the exception classes
    # @raise [ArgumentError] when an entry is invalid or names an unknown class
    # @note Can load application code through the host autoloader.
    def handled_exception_classes
      validate_handled_exceptions!

      @handled_exceptions.map do |value|
        klass = value.is_a?(String) ? Support.constantize(value) : value
        raise ArgumentError, "handled_exceptions must contain exception classes or class names" unless klass.is_a?(Class) && klass <= Exception
        klass
      rescue NameError
        raise ArgumentError, "handled_exceptions contains an unknown exception class: #{value.inspect}"
      end
    end

    # Validates the settings and replaces {#errors}.
    #
    # Opens no database and runs no application callback. Resolving a cron job
    # class can load application code.
    #
    # @return [Boolean] true when no setting has an error
    def valid?
      @errors = Errors.new
      Validator.new(self, @errors).validate
      @errors.empty?
    end

    # @return [Configuration] self, when the settings are valid
    # @raise [ArgumentError] with all messages joined by "; " when they are not
    def validate!
      raise ArgumentError, errors.full_messages.join("; ") unless valid?
      self
    end

    # Builds validated cron entries from {#cron}.
    #
    # @param database [Database, nil] the pool the entries enqueue into, or nil
    #   for the global database
    # @return [Array<CronEntry>] one entry per definition
    # @raise [ArgumentError] when {#cron} or an entry definition is invalid
    def cron_entries(database: nil)
      raise ArgumentError, "cron must be a Hash" unless @cron.is_a?(Hash)

      @cron.map do |key, attributes|
        raise ArgumentError, "cron entry #{key} must be a Hash" unless attributes.is_a?(Hash)
        entry = CronEntry.new(attributes.merge(key: key), database: database)
        raise ArgumentError, "invalid cron entry #{key}: #{entry.errors.inspect}" unless entry.valid?
        entry
      end
    end

    # Returns the pooled connection count for the global {Database}.
    #
    # The count is the sum of all worker pool threads across every queue
    # group, plus 2, plus cron connections when cron is enabled. The 2 extra
    # connections let threads that are not execution threads (application
    # enqueues, lifecycle and management calls) check out a connection while
    # every execution thread holds one. Without them, such a call waits up to
    # the checkout timeout. Each cron entry gets one connection per cron
    # thread: a timer and two delivery threads, plus a catch-up thread with a
    # graceful restart period. The listener and the process heartbeat use
    # dedicated sessions outside this count. See docs/runtime-resources.md.
    #
    # @return [Integer] the pool size
    # @raise [ArgumentError] when {#queues} is invalid
    def pool_size
      cron_connections = if enable_cron
        @cron.size * (@cron_graceful_restart_period ? 4 : 3)
      else
        0
      end

      QueueSelection.pools(queue_groups.join(";"), default_threads: @max_threads).sum { |pool| pool.fetch(:threads) } + 2 + cron_connections
    end

    # @return [Hash{Symbol => Object}] keyword arguments for {CronManager#initialize}
    # @api private
    def cron_manager_options
      {graceful_restart_period: @cron_graceful_restart_period, on_thread_error: on_thread_error}
    end

    # Returns the keyword arguments for {Worker#initialize}.
    #
    # A worker copies these values when it is built. Later configuration
    # changes do not reach it, except lower_thread_priority through
    # {DecentJob.lower_thread_priority=}.
    #
    # @return [Hash{Symbol => Object}] the worker settings
    # @api private
    def worker_options
      {advisory_lock_heartbeat: advisory_lock_heartbeat, process_heartbeat_interval: @process_heartbeat_interval,
       queues: @queues, max_threads: @max_threads, poll_interval: poll_interval, max_cache: @max_cache,
       enable_listen_notify: @enable_listen_notify, lower_thread_priority: @lower_thread_priority, on_thread_error: on_thread_error,
       cleanup_interval_jobs: @cleanup_interval_jobs, cleanup_interval_seconds: @cleanup_interval_seconds,
       cleanup_preserved_jobs_before_seconds_ago: @cleanup_preserved_jobs_before_seconds_ago,
       cleanup_discarded_jobs: @cleanup_discarded_jobs}
    end

    private

    # Reads a Boolean environment variable. An absent variable returns the
    # default. "", "0", "f", "F", "false", "FALSE", "off", and "OFF" are false.
    # Any other value is true.
    def environment_boolean(name, default:)
      value = ENV[name]
      return default if value.nil?
      !["", "0", "f", "F", "false", "FALSE", "off", "OFF"].include?(value)
    end

    # Application objects (classes, loggers, callable services) remain shared.
    # Copy only configuration containers, including cycles/shared nested values.
    def copy_containers(value, copies = {}.compare_by_identity)
      return value if value.respond_to?(:call)
      return copies[value] if copies.key?(value)

      case value
      when Hash
        copy = copies[value] = value.dup
        copy.clear
        copy.default = copy_containers(value.default, copies) unless value.default_proc
        value.each { |key, item| copy[copy_containers(key, copies)] = copy_containers(item, copies) }
        copy
      when Array
        copy = copies[value] = value.dup
        copy.map! { |item| copy_containers(item, copies) }
      when String
        copies[value] = value.dup
      else
        value
      end
    end

    def normalize_lock_strategy(value)
      return if value.nil?
      value = value.to_sym if value.is_a?(String)

      unless %i[advisory skiplocked hybrid].include?(value)
        raise ArgumentError, "lock_strategy must be advisory, skiplocked, or hybrid"
      end

      value
    end

    def normalize_dequeue_query_sort(value)
      return if value.nil?
      value = value.to_sym if value.is_a?(String)

      unless %i[created_at scheduled_at].include?(value)
        raise ArgumentError, "dequeue_query_sort must be created_at or scheduled_at"
      end

      value
    end

    def normalize_queue_select_limit(value)
      return if value.nil?
      value = Integer(value, 10) if value.is_a?(String) && value.match?(/\A[0-9]+\z/)

      unless value.is_a?(Integer) && value >= 0
        raise ArgumentError, "queue_select_limit must be a nonnegative integer"
      end

      value
    end

    # Splits {#queues} on pipes into subprocess queue groups.
    def queue_groups
      raise ArgumentError, "queues must be a nonempty string" unless @queues.is_a?(String) && !@queues.strip.empty?
      groups = @queues.split("|", -1).map(&:strip)
      raise ArgumentError, "queue groups must not be empty" if groups.any?(&:empty?)
      groups
    end

    # A copy for one process. subprocesses is 0, so a child never forks again.
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

    # Normalizes a cleanup interval and validates it through {CleanupTracker},
    # so configuration and workers accept the same values.
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
