# frozen_string_literal: true

require "active_job"
require "decent_job/active_job_extensions/concurrency"
require "decent_job/active_job_extensions/labels"
require "decent_job/adapter"
require "decent_job/bulk"
require "decent_job/claim"
require "decent_job/concurrency_controls"
require "decent_job/concurrency_rule"
require "decent_job/configuration"
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

module DecentJob
  VERSION = "0.1.0"
  MUTEX = Mutex.new
  @configuration = Configuration.new
  @workers = []
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
      MUTEX.synchronize do
        @stopping = false
        register_exit_hook
        @worker ||= Worker.new(database: connection_pool, **@configuration.worker_options)
        @workers << @worker unless @workers.include?(@worker)
        @worker.start
      end
    end

    def shutdown(timeout: nil)
      workers = MUTEX.synchronize do
        @stopping = true
        @workers.dup
      end
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout if timeout
      workers.each { |worker| worker.shutdown(timeout: 0) }
      results = workers.map do |worker|
        remaining = deadline && [deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC), 0].max
        worker.shutdown(timeout: remaining)
      end
      results.all?
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

    def register_exit_hook
      return if @exit_hook_registered
      at_exit { DecentJob.shutdown(timeout: DecentJob.configuration.shutdown_timeout) }
      @exit_hook_registered = true
    end
  end
end
