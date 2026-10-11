# frozen_string_literal: true

require "json"
require "time"
require "decent_job/support"
require "decent_job/isolated_state"
require "decent_job/notifications"
require "decent_job/logging"
require "decent_job/adapter"
require "decent_job/base"
require "decent_job/batch"
require "decent_job/batch_store"
require "decent_job/bulk"
require "decent_job/claim"
require "decent_job/cleaner"
require "decent_job/cleanup_tracker"
require "decent_job/concurrency_controls"
require "decent_job/concurrency_rule"
require "decent_job/configuration"
require "decent_job/cron_entry"
require "decent_job/cron_manager"
require "decent_job/cron_store"
require "decent_job/current"
require "decent_job/daemon"
require "decent_job/dashboard"
require "decent_job/database"
require "decent_job/execution"
require "decent_job/execution_metrics"
require "decent_job/execution_state"
require "decent_job/fork_safety"
require "decent_job/host_integration"
require "decent_job/inline_buffer"
require "decent_job/instrumentation"
require "decent_job/integrations"
require "decent_job/interrupt_error"
require "decent_job/interrupted_error"
require "decent_job/job"
require "decent_job/job_retry"
require "decent_job/lifecycle_events"
require "decent_job/lifecycle_hooks"
require "decent_job/log_subscriber"
require "decent_job/notifier"
require "decent_job/pauses"
require "decent_job/performer"
require "decent_job/probe_server"
require "decent_job/process_record"
require "decent_job/process_tracker"
require "decent_job/queue_selection"
require "decent_job/schema"
require "decent_job/stats"
require "decent_job/store"
require "decent_job/subprocess"
require "decent_job/supervisor"
require "decent_job/systemd_service"
require "decent_job/thread_status"
require "decent_job/worker"

# A PostgreSQL job runner with a native job API and an optional Active Job
# adapter.
#
# The DecentJob module is the global runtime. It owns the active
# {Configuration}, the lazily opened global {Database} pool, and a registry of
# the resources it starts: the default {Worker}, workers of async {Adapter}s,
# and one {CronManager} per database. Resources that application code builds
# directly belong to that code and are not in the registry.
#
# Locks and flags:
#
# - {ForkSafety#runtime_mutex} guards the registry, the configuration, and the
#   global database. Application code, such as configure blocks, cron class
#   loading, and lifecycle observers, runs outside it.
# - @lifecycle_revision increments on every configure, start, stop, and
#   restart. A start or restart that read an older revision gives up, so a
#   newer operation always wins.
# - @stopping is the global stop gate. It stays true after shutdown drains,
#   until an explicit start or restart. Automatic starts do not open it.
# - @cron_stopping and @cron_stopped_databases record cron stops, for all
#   databases or for one.
#
# The module extends {LifecycleHooks}, {ForkSafety}, {HostIntegration}, and
# {ThreadStatus}; those modules share the same module instance variables.
#
# See docs/lifecycle.md, docs/configuration.md, and docs/runtime-resources.md.
module DecentJob
  extend LifecycleHooks
  extend ForkSafety
  extend HostIntegration
  extend ThreadStatus

  # The gem version.
  VERSION = "0.1.0"
  # The initial registry lock. Use {ForkSafety#runtime_mutex} instead, because
  # a forked child replaces the lock with a new one.
  MUTEX = Mutex.new
  # Serializes {.lower_thread_priority=} calls, so workers apply the values in
  # the order the configuration stored them.
  @thread_priority_mutex = Mutex.new
  @configuration = Configuration.new
  @workers = []
  @cron_managers = {}
  @cron_stopped_databases = {}
  @cron_stopping = false
  @stopping = false

  class << self
    # @return [Configuration] the active configuration. Replace it with
    #   {.configure}, not by mutating it while workers run.
    attr_reader :configuration

    # @return [Logger] {Configuration#logger} of the active configuration
    def logger
      configuration.logger
    end

    # Sets an explicit logger on the active configuration.
    #
    # @param logger [Logger, nil] the logger, or nil to fall back to the Rails
    #   or default logger
    # @return [Logger, nil] the argument
    # @note Changes the active configuration in place, without validation.
    def logger=(logger)
      configuration.logger = logger
    end

    # Changes the configuration through a validated copy.
    #
    # The block runs without the registry lock, so it can inspect the runtime.
    # The copy is published only when the block returns and validation passes.
    #
    # @yieldparam config [Configuration] a copy of the active configuration
    # @return [Configuration] the published configuration
    # @raise [ArgumentError] when the global database is already open, or the
    #   copy is invalid
    # @raise [LifecycleInProgress] when another transition is active
    # @raise [ForkInProgress] while the runtime prepares to fork
    # @note Increments the lifecycle revision. Running workers keep the
    #   settings they copied at construction.
    def configure
      with_runtime_transition(:configuring) do
        candidate = runtime_mutex.synchronize do
          raise ArgumentError, "configure before opening the database; call reset! to restart" if @database

          @configuration.dup
        end

        yield candidate
        candidate.validate!

        runtime_mutex.synchronize do
          @configuration = candidate
          @lifecycle_revision = (@lifecycle_revision || 0) + 1
        end

        candidate
      end
    end

    # Returns the global connection pool, and opens it on first use.
    #
    # The pool size comes from {Configuration#pool_size}. Each call validates
    # the configuration outside the lock, because cron validation can load
    # application classes. When the configuration changes during validation,
    # the call starts over with the new one.
    #
    # In :active_record mode ({Configuration#connection}) the pool connects
    # with the Active Record settings and lends the Active Record connection
    # to enqueues. See {ActiveRecordConnection}.
    #
    # @param automatic [Boolean] true for automatic starts: return nil instead
    #   of raising during a transition, stop, or fork, or when
    #   {Configuration#database_configured?} is false
    # @return [Database, nil] the pool, or nil for a skipped automatic call
    # @raise [ArgumentError] when no database is configured or the
    #   configuration is invalid
    # @raise [LifecycleInProgress] when a transition is active and no pool is
    #   open yet
    def database(automatic: false)
      loop do
        candidate = runtime_mutex.synchronize do
          return if automatic && (@runtime_transition || @stopping || @forking)
          raise LifecycleInProgress, "runtime is #{@runtime_transition}" if @runtime_transition && !@database

          @configuration
        end

        # Cron validation can autoload application job classes.
        candidate.validate!

        pool = runtime_mutex.synchronize do
          return if automatic && (@runtime_transition || @stopping || @forking)
          next unless candidate.equal?(@configuration)
          return if automatic && !candidate.database_configured?
          raise LifecycleInProgress, "runtime is #{@runtime_transition}" if @runtime_transition && !@database
          raise ArgumentError, "set DECENT_JOB_DATABASE_URL or configure database_url" unless candidate.database_configured?

          @database ||= open_database(candidate)
        end

        return pool if pool
      end
    end

    # Applies all pending schema migrations to the global database.
    #
    # @return [Schema] the schema object
    # @raise [PG::Error] when a migration fails
    def install
      Schema.new(database).migrate
    end

    # Tells whether every packaged migration is installed.
    #
    # @param database [Database, nil] the pool to check, or nil for the global
    #   database
    # @return [Boolean] false when the metadata table or a version is missing
    def migrated?(database: nil)
      Schema.new(database || self.database).migrated?
    end

    # Pauses one queue, job class, or label. See {Pauses#pause}.
    #
    # @param target [Hash] exactly one of queue:, job_class:, or label:
    # @return [true]
    # @note Claims honor pauses only when {Configuration#enable_pauses} is true.
    def pause(**target)
      Pauses.new(database).pause(**target)
    end

    # Removes a pause and wakes workers. See {Pauses#unpause}.
    #
    # @param target [Hash] exactly one of queue:, job_class:, or label:
    # @return [true]
    def unpause(**target)
      Pauses.new(database).unpause(**target)
    end

    # @return [Hash{Symbol => Array<String>}] the paused :queues,
    #   :job_classes, and :labels; see {Pauses#paused}
    def paused
      Pauses.new(database).paused
    end

    # @param target [Hash] at most one of queue:, job_class:, label:, or
    #   job_instance:; see {Pauses#paused?}
    # @return [Boolean, Symbol] whether the target, or anything, is paused
    def paused?(**target)
      Pauses.new(database).paused?(**target)
    end

    # Starts the default worker and, when enabled, configured cron.
    #
    # The default worker uses {Configuration#worker_options} and the global
    # database. Calling start again reuses the same worker. An explicit start
    # opens the global stop gate, clears the all-database cron stop, and
    # clears the cron stop of the global database.
    #
    # @return [Worker, false] the worker, or false while an earlier shutdown
    #   still drains or when a newer lifecycle operation won
    # @raise [ArgumentError] when no database_url is set, the configuration is
    #   invalid, or a cron job class does not use a DecentJob adapter on the
    #   global database
    # @raise [ForkInProgress] while the runtime prepares to fork
    # @raise [LifecycleInProgress] when a transition is active
    # @note Registers an at_exit hook that calls {.shutdown} with
    #   {Configuration#shutdown_timeout}.
    def start
      revision = synchronize_resource_start do
        return false if @stopping && (@workers + @cron_managers.values).any? { |resource| !resource.shutdown? }

        @lifecycle_revision ||= 0
      end

      connection_pool = database

      worker, started_revision = synchronize_resource_start do
        return false unless revision == @lifecycle_revision
        raise ForkInProgress, "runtime is preparing to fork" if @forking

        @lifecycle_revision = (@lifecycle_revision || 0) + 1
        @stopping = false
        @cron_stopping = false
        @cron_stopped_databases.delete(connection_pool)
        register_exit_hook
        @worker ||= Worker.new(database: connection_pool, **@configuration.worker_options)
        @workers << @worker unless @workers.include?(@worker)
        [@worker.start, @lifecycle_revision]
      end

      start_cron(database: connection_pool, expected_revision: started_revision) if @configuration.enable_cron
      worker
    end

    # @return [Boolean] true when at least one worker is registered, every
    #   registered worker is started, and no global stop is in effect
    def started?
      workers = runtime_mutex.synchronize { @stopping ? [] : @workers.dup }
      !workers.empty? && workers.all?(&:started?)
    end

    # @return [Boolean] true when at least one worker is registered, every
    #   registered worker is connected, and no global stop is in effect
    def connected?
      workers = runtime_mutex.synchronize { @stopping ? [] : @workers.dup }
      !workers.empty? && workers.all?(&:connected?)
    end

    # @return [Hash{Symbol => Object}] scheduler statistics summed over all
    #   registered workers; see {Stats.aggregate}
    def stats
      workers = runtime_mutex.synchronize { @workers.dup }
      Stats.aggregate(workers.flat_map { |worker| worker.stats.fetch(:schedulers) })
    end

    # Tells whether every registered worker is idle.
    #
    # @param duration [Numeric, nil] also require this many seconds without
    #   execution activity, or nil to skip the time check
    # @return [Boolean] true also when no worker is registered
    # @raise [ArgumentError] when duration is negative or not finite
    def idle?(duration = nil)
      Configuration.validate_idle_timeout!(duration)
      workers = runtime_mutex.synchronize { @workers.dup }
      workers.all? { |worker| worker.idle?(duration) }
    end

    # Sets {Configuration#lower_thread_priority} and applies it to every
    # registered worker.
    #
    # A job that already runs keeps its priority; the change applies from the
    # next attempt.
    #
    # @param value [Boolean] true to run attempts at priority -3
    # @return [Boolean] the argument
    # @raise [ArgumentError] unless the value is true or false
    def lower_thread_priority=(value)
      @thread_priority_mutex.synchronize do
        workers = runtime_mutex.synchronize do
          @configuration.lower_thread_priority = value
          @workers.dup
        end

        workers.each { |worker| worker.lower_thread_priority = value }
      end

      value
    end

    # @return [Array<CronManager>] the registered cron managers, one per
    #   database
    def cron_managers
      runtime_mutex.synchronize { @cron_managers.values.dup }
    end

    # @return [Boolean] true when any registered cron manager runs
    def cron_running?
      cron_managers.any?(&:running?)
    end

    # Starts the configured cron entries for one database.
    #
    # Repeated calls reuse the manager registered for the database. An
    # explicit call works also when {Configuration#enable_cron} is false, and
    # clears earlier cron stops for that database.
    #
    # @param database [Database, nil] the pool, or nil for the global database
    # @param automatic [Boolean] true for adapter wakeups: skip silently
    #   during a transition or after a stop
    # @param expected_revision [Integer, nil] give up unless the lifecycle
    #   revision still equals this value; {.start} passes its own revision
    # @return [CronManager, false, nil] the result of {CronManager#start},
    #   false when a newer lifecycle operation won, or nil when an automatic
    #   start was skipped
    # @raise [ArgumentError] when a cron entry is invalid, or a cron job class
    #   does not use a DecentJob adapter on the same database
    # @raise [ForkInProgress] while the runtime prepares to fork
    # @raise [LifecycleInProgress] when a transition is active, for explicit
    #   calls
    def start_cron(database: nil, automatic: false, expected_revision: nil)
      revision = synchronize_resource_start(automatic: automatic || !expected_revision.nil?) do
        return false if expected_revision && expected_revision != (@lifecycle_revision || 0)
        return if automatic && (@stopping || @cron_stopping || @cron_stopped_databases[database])

        @lifecycle_revision ||= 0
      end

      return if revision.nil?

      connection_pool = database || self.database
      # Resolve configuration outside the registry lock: constant loading and
      # adapter lookup may call back into DecentJob.
      entries = @configuration.cron_entries(database: connection_pool)

      entries.each do |entry|
        klass = entry.job_class
        klass = Support.constantize(klass) if klass.is_a?(String)
        next unless Integrations.job_class?(klass)

        adapter = klass.queue_adapter

        unless adapter.is_a?(Adapter) && adapter.database.equal?(connection_pool)
          raise ArgumentError, "Cron jobs must use a DecentJob adapter with the cron entry database"
        end
      end

      synchronize_resource_start(automatic: automatic || !expected_revision.nil?) do
        return false unless revision == @lifecycle_revision
        raise ForkInProgress, "runtime is preparing to fork" if @forking
        return if automatic && (@stopping || @cron_stopping || @cron_stopped_databases[connection_pool])

        unless automatic
          @cron_stopping = false
          @cron_stopped_databases.delete(connection_pool)
        end

        register_exit_hook
        manager = @cron_managers[connection_pool] ||= CronManager.new(entries, **@configuration.cron_manager_options)
        # start only allocates owned threads; application callbacks run later on
        # those threads, never synchronously while the registry lock is held.
        manager.start(automatic: automatic)
      end
    end

    # Stops registered cron managers and waits for them to drain.
    #
    # Automatic starts do not reopen stopped cron. An explicit {.start_cron}
    # or {.start} does. {Adapter#restart} clears only the stop of its own
    # database.
    #
    # @param timeout [Numeric, nil] seconds for the whole drain; nil waits
    #   indefinitely, 0 signals without waiting
    # @param database [Database, nil] stop only this database's manager, or
    #   nil for all of them
    # @param force [Boolean] true interrupts callbacks still running at the
    #   deadline
    # @return [Boolean] true when every selected manager drained
    # @raise [ArgumentError] when timeout is negative or not finite
    # @raise [Exception] the first error from a resource thread, after every
    #   resource finished its wait; see {ResourceShutdown.wait_all}
    def stop_cron(timeout: nil, database: nil, force: false)
      deadline = ResourceShutdown.deadline(timeout)
      ResourceShutdown.wait_all(begin_cron_shutdown(database: database), deadline: deadline, force: force, timeout: timeout)
    end

    # Marks cron as stopped and signals the selected managers without waiting.
    #
    # @param database [Database, nil] one database, or nil for all
    # @return [Array<ResourceShutdown>] one handle per signaled manager
    # @note Runs under the registry lock. {Adapter#begin_shutdown} calls it
    #   with the lock already held.
    # @api private
    def begin_cron_shutdown(database: nil)
      operation = lambda do
        @lifecycle_revision = (@lifecycle_revision || 0) + 1

        if database
          @cron_stopped_databases[database] = true
          managers = [@cron_managers[database]].compact
        else
          @cron_stopping = true
          managers = @cron_managers.values
        end

        ResourceShutdown.capture_all(managers)
      end

      runtime_mutex.owned? ? operation.call : runtime_mutex.synchronize(&operation)
    end

    # @return [Boolean] the global stop gate; it stays true after the drain
    #   finishes, so it does not mean resources still run
    def stopping?
      runtime_mutex.owned? ? @stopping : runtime_mutex.synchronize { @stopping }
    end

    # Tells whether every registered worker and cron manager has drained.
    #
    # The registry is copied under the lock and inspected outside it, so a
    # concurrent start can change the answer at once. Resources that the
    # application built directly do not count.
    #
    # @return [Boolean] true also before any resource started
    def shutdown?
      resources = runtime_mutex.synchronize { @cron_managers.values + @workers }
      resources.all?(&:shutdown?)
    end

    # Stops all registered workers and cron managers and waits for them.
    #
    # Every resource gets its stop signal before the call waits on any of
    # them. The stop gate stays closed after the call returns, also when the
    # drain is incomplete.
    #
    # @param timeout [Numeric, nil] seconds for the whole drain; nil waits
    #   indefinitely, 0 signals without waiting
    # @param force [Boolean] true interrupts jobs and callbacks still running at
    #   the deadline
    # @return [Boolean] true when every resource drained, false when work
    #   still runs; a later call can wait again
    # @raise [ArgumentError] when timeout is negative or not finite
    # @raise [Exception] the first error from a resource thread, after every
    #   resource finished its wait; see {ResourceShutdown.wait_all}
    def shutdown(timeout: nil, force: false)
      deadline = ResourceShutdown.deadline(timeout)
      _revision, handles = begin_shutdown
      ResourceShutdown.wait_all(handles, deadline: deadline, force: force, timeout: timeout)
    end

    # Stops, drains, and restarts all registered workers and cron managers.
    #
    # Workers that an adapter stopped on its own, and cron stopped for one
    # database, stay stopped. They are still drained.
    #
    # @param timeout [Numeric, nil] seconds for the whole drain; nil waits
    #   indefinitely, 0 signals without waiting
    # @param force [Boolean] true interrupts work still running at the deadline
    # @return [DecentJob, false] the module on success, or false when the
    #   drain was incomplete, a newer lifecycle operation won, or a resource
    #   refused to restart
    # @raise [ArgumentError] when timeout is negative or not finite
    # @raise [Exception] the first error from a resource thread, after every
    #   resource finished its wait; see {ResourceShutdown.wait_all}
    # @raise [ForkInProgress] while the runtime prepares to fork
    # @raise [LifecycleInProgress] when a configure or reset transition is
    #   active
    def restart(timeout: nil, force: false)
      deadline = ResourceShutdown.deadline(timeout)
      revision, handles, restart_handles = begin_shutdown(restarting: true)
      return false unless ResourceShutdown.prepare_restart(handles, restart_handles: restart_handles,
        deadline: deadline, force: force, timeout: timeout)

      synchronize_resource_start do
        unless revision == @lifecycle_revision
          restart_handles.each { |handle| handle.restart_failed(:refused) }
          return false
        end

        @lifecycle_revision += 1
        @stopping = false
        @cron_stopping = false
        locally_stopped_cron = @cron_managers.filter_map { |pool, manager| manager if @cron_stopped_databases[pool] }

        restarted = restart_handles.map do |handle|
          next true if @locally_stopped_workers&.include?(handle.resource) || locally_stopped_cron.include?(handle.resource)

          handle.restart
        end.all?

        restarted ? self : false
      end
    end

    # Closes the stop gate and signals every registered resource.
    #
    # @param restarting [Boolean] true also returns the handles to restart,
    #   which leave out locally stopped workers and cron managers
    # @return [Array(Integer, Array<ResourceShutdown>, Array<ResourceShutdown>)]
    #   the new lifecycle revision, all handles, and the restart handles (nil
    #   unless restarting)
    # @api private
    def begin_shutdown(restarting: false)
      runtime_mutex.synchronize do
        @lifecycle_revision = (@lifecycle_revision || 0) + 1
        @stopping = true
        @cron_stopping = true
        handles = ResourceShutdown.capture_all(@cron_managers.values + @workers)

        if restarting
          locally_stopped_cron = @cron_managers.filter_map { |pool, manager| manager if @cron_stopped_databases[pool] }

          restart_handles = handles.reject do |handle|
            @locally_stopped_workers&.include?(handle.resource) || locally_stopped_cron.include?(handle.resource)
          end
        end

        [@lifecycle_revision, handles, restart_handles]
      end
    end

    # Records that an adapter stopped its own worker, so global restart
    # leaves it stopped.
    #
    # @param worker [Worker, nil] the worker; nil is ignored
    # @return [Array<Worker>, nil]
    # @note The caller must hold the registry lock.
    # @api private
    def locally_stop_worker(worker)
      (@locally_stopped_workers ||= []).push(worker) if worker && !@locally_stopped_workers&.include?(worker)
    end

    # Clears the local stop of an adapter's worker and its database's cron.
    #
    # @param worker [Worker, nil] the worker to resume
    # @param database [Database, nil] the database whose cron stop to clear
    # @return [Object, nil]
    # @note The caller must hold the registry lock.
    # @api private
    def locally_resume_worker(worker, database: nil)
      @locally_stopped_workers&.delete(worker)
      @cron_stopped_databases.delete(database) if database
    end

    # Registers and starts an adapter's worker as an automatic start.
    #
    # @param worker [Worker] the adapter's worker
    # @return [Worker, false, nil] the {Worker#start} result, or nil when the
    #   global stop gate is closed or a transition is active
    # @raise [ForkInProgress] while the runtime prepares to fork
    # @api private
    def start_worker(worker)
      synchronize_resource_start(automatic: true) do
        return if @stopping

        register_exit_hook
        @workers << worker unless @workers.include?(worker)
        worker.start(automatic: true)
      end
    end

    # Drains the runtime and restores a fresh configuration.
    #
    # Steps: shut down all registered resources with no deadline, retire
    # every registered adapter, close the global pool, clear the registry,
    # and build a new {Configuration} from the current environment.
    #
    # @return [Boolean] true on success; false when resources did not drain
    #   (for example when called from inside an owned callback), or when a
    #   worker, cron manager, process tracker, or standalone execution that the
    #   application owns still uses the global pool
    # @raise [LifecycleInProgress] when another transition is active
    # @raise [ForkInProgress] while the runtime prepares to fork
    # @raise [Exception] a shutdown or adapter failure. When the resources
    #   still drained, the reset finishes before the error is raised.
    # @note Retired adapters cannot restart. Build new adapters after reset.
    def reset!
      with_runtime_transition(:resetting) do
        failure = nil
        stopped = false

        begin
          stopped = shutdown
        rescue Exception => error # standard:disable Lint/RescueException
          failure = error
          stopped = runtime_mutex.synchronize { (@workers + @cron_managers.values).all?(&:shutdown?) }
        end

        unless stopped
          raise failure if failure

          return false
        end

        pool = runtime_mutex.synchronize { @database }

        if pool && manually_owned_database?(pool)
          raise failure if failure

          return false
        end

        adapters = runtime_mutex.synchronize { @adapters&.keys || [] }

        adapters.each do |adapter|
          adapter.shutdown(timeout: 0)
        rescue Exception => error # standard:disable Lint/RescueException
          failure ||= error
        ensure
          adapter.retire
        end

        pool&.close

        runtime_mutex.synchronize do
          @adapters = ObjectSpace::WeakMap.new
          @rails_ready = nil unless defined?(DecentJob::Railtie)
          @worker = nil
          @workers = []
          @locally_stopped_workers = []
          @cron_managers = {}
          @cron_stopped_databases = {}
          @cron_stopping = false
          @stopping = false
          @database = nil
          @configuration = Configuration.new
        end

        # Adapter.new takes the runtime lock, so clear outside it.
        @default_adapter_mutex.synchronize { @default_adapter = nil }
        raise failure if failure

        true
      end
    end

    # Deletes preserved jobs, attempts, and batches that finished long ago.
    #
    # @param older_than [Numeric, nil] age in seconds from finished_at, or nil
    #   for {Configuration#cleanup_preserved_jobs_before_seconds_ago}
    # @param in_batches_of [Integer] rows per delete page
    # @param include_discarded [Boolean] false keeps discarded jobs and batches
    # @param database [Database, nil] the pool, or nil for the global database
    # @param stopping [#call, nil] returns true to stop between pages; nil uses
    #   the stop check of the current job, if any
    # @return [Integer] the number of deleted jobs, attempts, and batches
    # @raise [ArgumentError] for invalid options
    def cleanup_preserved_jobs(older_than: nil, in_batches_of: 1000, include_discarded: configuration.cleanup_discarded_jobs?, database: nil, stopping: nil)
      stopping = Current.execution&.fetch(:stopping, nil) if stopping.nil?
      Cleaner.new(database: database || self.database).cleanup(
        older_than: older_than.nil? ? configuration.cleanup_preserved_jobs_before_seconds_ago : older_than,
        in_batches_of: in_batches_of, include_discarded: include_discarded, stopping: stopping
      )
    end

    # Runs due jobs on the calling thread until none is left.
    #
    # Immediate retries run in the same call. Future jobs stay pending.
    # PostgreSQL decides which jobs are due.
    #
    # @param queues [String] the queue selection, "*" for all queues
    # @param limit [Integer, nil] the maximum number of attempts, or nil for
    #   no limit; 0 returns without opening the database
    # @return [Integer] the number of attempts run
    # @raise [ArgumentError] when limit is negative or not an Integer
    # @raise [Exception] the unhandled error of a job, which propagates
    def perform_inline(queues: "*", limit: nil)
      if !limit.nil? && (!limit.is_a?(Integer) || limit.negative?)
        raise ArgumentError, "limit must be a nonnegative integer or nil"
      end

      return 0 if limit == 0

      performer = Performer.new(database: database)
      count = 0
      count += 1 while (limit.nil? || count < limit) && performer.perform_one(queues: queues, raise_errors: true)
      count
    end

    private

    # Active Record mode connects the pool with the Active Record settings and
    # lends the Active Record connection to enqueues. Callers hold the
    # registry lock.
    def open_database(configuration)
      return Database.new(url: configuration.database_url, pool_size: configuration.pool_size) unless configuration.connection == :active_record

      require "decent_job/active_record_connection"
      Database.new(url: ActiveRecordConnection.connection_options, pool_size: configuration.pool_size, enqueue_connection: ActiveRecordConnection)
    end

    # Scans ObjectSpace because application-owned resources are not in the
    # registry. reset! must not close a pool that one of them still uses.
    def manually_owned_database?(pool)
      ObjectSpace.each_object(Worker).any? { |worker| worker.database.equal?(pool) && !worker.shutdown? } ||
        ObjectSpace.each_object(ProcessTracker).any? { |tracker| tracker.database.equal?(pool) && !tracker.shutdown? } ||
        ObjectSpace.each_object(Performer).any? { |performer| performer.database.equal?(pool) && performer.active? } ||
        ObjectSpace.each_object(CronManager).any? do |manager|
          !manager.shutdown? && manager.cron_entries.any? { |entry| entry.respond_to?(:database) && entry.database.equal?(pool) }
        end
    end

    # Registers at most one at_exit shutdown per process. Callers hold the
    # registry lock.
    def register_exit_hook
      return if @exit_hook_registered

      at_exit { DecentJob.shutdown(timeout: DecentJob.configuration.shutdown_timeout) }
      @exit_hook_registered = true
    end
  end
end

# Log lifecycle and job events through the configured logger.
DecentJob::LogSubscriber.attach_to(:decent_job)

# When Active Job is already loaded, register the adapter name. The adapter
# file loads the Active Job bridge on first lookup.
if defined?(ActiveJob::QueueAdapters)
  ActiveJob::QueueAdapters.autoload :DecentJobAdapter, "active_job/queue_adapters/decent_job_adapter"
end

# Load the Railtie now, or when a Rails application boots later.
DecentJob.load_railtie_with_rails
