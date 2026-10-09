# frozen_string_literal: true

require "active_job"
require "decent_job/active_job_extensions/batches"
require "decent_job/active_job_extensions/concurrency"
require "decent_job/active_job_extensions/cron"
require "decent_job/active_job_extensions/labels"
require "decent_job/adapter"
require "decent_job/batch"
require "decent_job/batch_store"
require "decent_job/bulk"
require "decent_job/claim"
require "decent_job/concurrency_controls"
require "decent_job/concurrency_rule"
require "decent_job/configuration"
require "decent_job/cron_entry"
require "decent_job/cron_manager"
require "decent_job/cron_store"
require "decent_job/current"
require "decent_job/database"
require "decent_job/inline_buffer"
require "decent_job/pauses"
require "decent_job/performer"
require "decent_job/queue_selection"
require "decent_job/schema"
require "decent_job/store"
require "decent_job/worker"

ActiveJob::QueueAdapters.autoload :DecentJobAdapter, "active_job/queue_adapters/decent_job_adapter"
ActiveJob::Base.include DecentJob::ActiveJobExtensions::Batches
ActiveJob::Base.include DecentJob::ActiveJobExtensions::Cron
ActiveJob.singleton_class.prepend DecentJob::ActiveJobExtensions::CronBulk

module DecentJob
  VERSION = "0.1.0"
  MUTEX = Mutex.new
  @configuration = Configuration.new
  @workers = []
  @cron_managers = {}
  @cron_stopped_databases = {}
  @cron_stopping = false
  @stopping = false

  class << self
    attr_reader :configuration

    def configure
      MUTEX.synchronize do
        raise ArgumentError, "configure before opening the database; call reset! to restart" if @database
        yield @configuration
        @configuration.validate!
      end
    end

    def database
      MUTEX.synchronize do
        @configuration.validate!
        raise ArgumentError, "set DECENT_JOB_DATABASE_URL or configure database_url" unless @configuration.database_url
        @database ||= Database.new(url: @configuration.database_url, pool_size: @configuration.pool_size)
      end
    end

    def install
      Schema.new(database).migrate
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
      connection_pool = database
      worker = MUTEX.synchronize do
        @stopping = false
        @cron_stopping = false
        @cron_stopped_databases.delete(connection_pool)
        register_exit_hook
        @worker ||= Worker.new(database: connection_pool, **@configuration.worker_options)
        @workers << @worker unless @workers.include?(@worker)
        @worker.start
      end
      start_cron(database: connection_pool, automatic: true) if @configuration.enable_cron
      worker
    end

    def cron_managers
      MUTEX.synchronize { @cron_managers.values.dup }
    end

    def cron_running?
      cron_managers.any?(&:running?)
    end

    def start_cron(database: nil, automatic: false)
      connection_pool = database || self.database
      # Resolve configuration outside the registry lock: constant loading and
      # adapter lookup may call back into DecentJob.
      entries = @configuration.cron_entries(database: connection_pool)
      entries.each do |entry|
        klass = entry.job_class
        klass = klass.constantize if klass.is_a?(String)
        next unless klass.is_a?(Class) && klass <= ActiveJob::Base
        adapter = klass.queue_adapter
        unless adapter.is_a?(Adapter) && adapter.database.equal?(connection_pool)
          raise ArgumentError, "Cron jobs must use a DecentJob adapter with the cron entry database"
        end
      end
      MUTEX.synchronize do
        return if automatic && (@stopping || @cron_stopping || @cron_stopped_databases[connection_pool])
        unless automatic
          @cron_stopping = false
          @cron_stopped_databases.delete(connection_pool)
        end
        register_exit_hook
        manager = @cron_managers[connection_pool] ||= CronManager.new(entries, **@configuration.cron_manager_options)
        # start only allocates owned threads; application callbacks run later on
        # those threads, never synchronously while the registry lock is held.
        manager.start
      end
    end

    def stop_cron(timeout: nil, database: nil)
      validate_timeout!(timeout)
      managers = MUTEX.synchronize do
        if database
          @cron_stopped_databases[database] = true
          [@cron_managers[database]].compact
        else
          @cron_stopping = true
          @cron_managers.values.dup
        end
      end
      stop_resources(managers, timeout: timeout)
    end

    def shutdown(timeout: nil)
      validate_timeout!(timeout)
      resources = MUTEX.synchronize do
        @stopping = true
        @cron_stopping = true
        @cron_managers.values + @workers
      end
      stop_resources(resources, timeout: timeout)
    end

    def start_worker(worker)
      MUTEX.synchronize do
        return if @stopping
        register_exit_hook
        @workers << worker unless @workers.include?(worker)
        worker.start
      end
    end

    def reset!
      shutdown
      MUTEX.synchronize do
        @worker = nil
        @workers = []
        @cron_managers = {}
        @cron_stopped_databases = {}
        @cron_stopping = false
        @stopping = false
        @database&.close
        @database = nil
        @configuration = Configuration.new
      end
    end

    def perform_inline(queues: "*", limit: 1000)
      raise ArgumentError, "limit must be positive" unless limit.is_a?(Integer) && limit.positive?
      performer = Performer.new(database: database)
      count = 0
      count += 1 while count < limit && performer.perform_one(queues: queues, raise_errors: true)
      count
    end

    private

    def validate_timeout!(timeout)
      if !timeout.nil? && (!timeout.is_a?(Numeric) || !timeout.finite? || timeout.negative?)
        raise ArgumentError, "timeout must be a finite nonnegative number"
      end
    end

    def stop_resources(resources, timeout:)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout if timeout
      resources.each { |resource| resource.shutdown(timeout: 0) }
      resources.map do |resource|
        remaining = deadline && [deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC), 0].max
        resource.shutdown(timeout: remaining)
      end.all?
    end

    def register_exit_hook
      return if @exit_hook_registered
      at_exit { DecentJob.shutdown(timeout: DecentJob.configuration.shutdown_timeout) }
      @exit_hook_registered = true
    end
  end
end
