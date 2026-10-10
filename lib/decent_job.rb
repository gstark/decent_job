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

module DecentJob
  extend LifecycleHooks
  extend ForkSafety
  extend HostIntegration
  extend ThreadStatus

  VERSION = "0.1.0"
  MUTEX = Mutex.new
  @thread_priority_mutex = Mutex.new
  @configuration = Configuration.new
  @workers = []
  @cron_managers = {}
  @cron_stopped_databases = {}
  @cron_stopping = false
  @stopping = false

  class << self
    attr_reader :configuration

    def logger
      configuration.logger
    end

    def logger=(logger)
      configuration.logger = logger
    end

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
          return if automatic && !candidate.database_url
          raise LifecycleInProgress, "runtime is #{@runtime_transition}" if @runtime_transition && !@database
          raise ArgumentError, "set DECENT_JOB_DATABASE_URL or configure database_url" unless candidate.database_url
          @database ||= Database.new(url: candidate.database_url, pool_size: candidate.pool_size)
        end

        return pool if pool
      end
    end

    def install
      Schema.new(database).migrate
    end

    def migrated?(database: nil)
      Schema.new(database || self.database).migrated?
    end

    def pause(**target)
      Pauses.new(database).pause(**target)
    end

    def unpause(**target)
      Pauses.new(database).unpause(**target)
    end

    def paused
      Pauses.new(database).paused
    end

    def paused?(**target)
      Pauses.new(database).paused?(**target)
    end

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

    def started?
      workers = runtime_mutex.synchronize { @stopping ? [] : @workers.dup }
      !workers.empty? && workers.all?(&:started?)
    end

    def connected?
      workers = runtime_mutex.synchronize { @stopping ? [] : @workers.dup }
      !workers.empty? && workers.all?(&:connected?)
    end

    def stats
      workers = runtime_mutex.synchronize { @workers.dup }
      Stats.aggregate(workers.flat_map { |worker| worker.stats.fetch(:schedulers) })
    end

    def idle?(duration = nil)
      Configuration.validate_idle_timeout!(duration)
      workers = runtime_mutex.synchronize { @workers.dup }
      workers.all? { |worker| worker.idle?(duration) }
    end

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

    def cron_managers
      runtime_mutex.synchronize { @cron_managers.values.dup }
    end

    def cron_running?
      cron_managers.any?(&:running?)
    end

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

    def stop_cron(timeout: nil, database: nil, force: false)
      deadline = ResourceShutdown.deadline(timeout)
      ResourceShutdown.wait_all(begin_cron_shutdown(database: database), deadline: deadline, force: force, timeout: timeout)
    end

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

    def stopping?
      runtime_mutex.owned? ? @stopping : runtime_mutex.synchronize { @stopping }
    end

    def shutdown?
      resources = runtime_mutex.synchronize { @cron_managers.values + @workers }
      resources.all?(&:shutdown?)
    end

    def shutdown(timeout: nil, force: false)
      deadline = ResourceShutdown.deadline(timeout)
      _revision, handles = begin_shutdown
      ResourceShutdown.wait_all(handles, deadline: deadline, force: force, timeout: timeout)
    end

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

    def locally_stop_worker(worker)
      (@locally_stopped_workers ||= []).push(worker) if worker && !@locally_stopped_workers&.include?(worker)
    end

    def locally_resume_worker(worker, database: nil)
      @locally_stopped_workers&.delete(worker)
      @cron_stopped_databases.delete(database) if database
    end

    def start_worker(worker)
      synchronize_resource_start(automatic: true) do
        return if @stopping
        register_exit_hook
        @workers << worker unless @workers.include?(worker)
        worker.start(automatic: true)
      end
    end

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

    def cleanup_preserved_jobs(older_than: nil, in_batches_of: 1000, include_discarded: configuration.cleanup_discarded_jobs?, database: nil, stopping: nil)
      stopping = Current.execution&.fetch(:stopping, nil) if stopping.nil?
      Cleaner.new(database: database || self.database).cleanup(
        older_than: older_than.nil? ? configuration.cleanup_preserved_jobs_before_seconds_ago : older_than,
        in_batches_of: in_batches_of, include_discarded: include_discarded, stopping: stopping
      )
    end

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

    def manually_owned_database?(pool)
      ObjectSpace.each_object(Worker).any? { |worker| worker.database.equal?(pool) && !worker.shutdown? } ||
        ObjectSpace.each_object(ProcessTracker).any? { |tracker| tracker.database.equal?(pool) && !tracker.shutdown? } ||
        ObjectSpace.each_object(Performer).any? { |performer| performer.database.equal?(pool) && performer.active? } ||
        ObjectSpace.each_object(CronManager).any? do |manager|
          !manager.shutdown? && manager.cron_entries.any? { |entry| entry.respond_to?(:database) && entry.database.equal?(pool) }
        end
    end

    def register_exit_hook
      return if @exit_hook_registered
      at_exit { DecentJob.shutdown(timeout: DecentJob.configuration.shutdown_timeout) }
      @exit_hook_registered = true
    end
  end
end

DecentJob::LogSubscriber.attach_to(:decent_job)

# When Active Job is already loaded, register the adapter name. The adapter
# file loads the Active Job bridge on first lookup.
if defined?(ActiveJob::QueueAdapters)
  ActiveJob::QueueAdapters.autoload :DecentJobAdapter, "active_job/queue_adapters/decent_job_adapter"
end

DecentJob.load_railtie_with_rails
