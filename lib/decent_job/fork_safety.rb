# frozen_string_literal: true

module DecentJob
  module ForkSafety
    def runtime_mutex
      @runtime_mutex ||= MUTEX
    end

    def synchronize_resource_start(skip_during_fork: false, &block)
      operation = -> {
        next nil if @forking && skip_during_fork
        raise ForkInProgress, "runtime is preparing to fork" if @forking
        block.call
      }
      runtime_mutex.owned? ? operation.call : runtime_mutex.synchronize(&operation)
    end

    def with_prepared_fork
      owner = Process.pid
      previous = runtime_mutex.synchronize do
        raise ForkInProgress, "runtime is already preparing to fork" if @forking
        @forking = true
        [@stopping, @cron_stopping]
      end
      raise ForkInProgress, "workers or cron did not drain before fork" unless shutdown(timeout: configuration.shutdown_timeout)
      run_lifecycle_hooks(:before_supervisor_fork)
      live_worker = ObjectSpace.each_object(Worker).any? { |worker| !worker.shutdown? }
      live_cron = ObjectSpace.each_object(CronManager).any? { |manager| !manager.shutdown? }
      live_tracker = ObjectSpace.each_object(ProcessTracker).any? { |tracker| !tracker.shutdown? }
      raise ForkInProgress, "manually owned workers, cron, or process trackers must drain before fork" if live_worker || live_cron || live_tracker
      Database.with_prepared_fork { yield }
    ensure
      if previous && Process.pid == owner
        runtime_mutex.synchronize do
          @forking = false
          @stopping, @cron_stopping = previous
        end
      end
    end

    def after_supervisor_fork(configuration)
      @runtime_mutex = Mutex.new
      Database.reset_after_fork
      configuration.execution_mode = :external if configuration.execution_mode == :async
      @configuration = configuration
      @database&.resize(configuration.pool_size)
      @worker = nil
      @workers = []
      @cron_managers = {}
      @cron_stopped_databases = {}
      @cron_stopping = false
      @stopping = false
      @forking = false
      ObjectSpace.each_object(Adapter, &:reset_after_fork)
      Thread.current[:decent_job_execution] = nil
      Thread.current[:decent_job_inline_buffer] = nil
      %i[decent_job_batch_context decent_job_cron_context decent_job_bulk_buffer].each do |key|
        Thread.current.thread_variable_set(key, nil)
      end
    end

    # CLI overrides preserve application-owned adapters and cached Database
    # references. Only a drained runtime can change its worker recipe.
    def apply_cli_configuration(options)
      candidate = configuration.dup
      options.each { |name, value| candidate.public_send("#{name}=", value) }
      candidate.validate!
      resources = runtime_mutex.synchronize { !@workers.empty? || !@cron_managers.empty? }
      if resources && !shutdown(timeout: candidate.shutdown_timeout)
        raise ArgumentError, "workers or cron did not drain before CLI configuration"
      end
      runtime_mutex.synchronize do
        @configuration = candidate
        @database&.resize(candidate.pool_size)
        @worker = nil
        @workers = []
        @cron_managers = {}
        @cron_stopped_databases = {}
        @stopping = @cron_stopping = false
      end
      candidate
    end
  end
end
