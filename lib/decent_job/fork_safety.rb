# frozen_string_literal: true

module DecentJob
  module ForkSafety
    def runtime_mutex
      @runtime_mutex ||= MUTEX
    end

    def synchronize_resource_start(skip_during_fork: false, automatic: false, &block)
      operation = -> {
        next nil if @forking && skip_during_fork
        raise ForkInProgress, "runtime is preparing to fork" if @forking
        next nil if @runtime_transition && (automatic || skip_during_fork)
        raise LifecycleInProgress, "runtime is #{@runtime_transition}" if @runtime_transition
        block.call
      }

      LifecycleEvents.defer do
        runtime_mutex.owned? ? operation.call : runtime_mutex.synchronize(&operation)
      end
    end

    def with_runtime_transition(name)
      acquired = false

      Thread.handle_interrupt(Object => :never) do
        runtime_mutex.synchronize do
          raise LifecycleInProgress, "runtime is #{@runtime_transition}" if @runtime_transition
          raise ForkInProgress, "runtime is preparing to fork" if @forking
          @runtime_transition = name
          acquired = true
        end
      end

      yield
    ensure
      if acquired
        Thread.handle_interrupt(Object => :never) do
          runtime_mutex.synchronize { @runtime_transition = nil }
        end
      end
    end

    def with_prepared_fork
      owner = Process.pid
      previous = nil

      Thread.handle_interrupt(Object => :never) do
        previous = runtime_mutex.synchronize do
          raise ForkInProgress, "runtime is already preparing to fork" if @forking
          raise LifecycleInProgress, "runtime is #{@runtime_transition}" if @runtime_transition
          @forking = true
          [@stopping, @cron_stopping]
        end
      end

      raise ForkInProgress, "workers or cron did not drain before fork" unless shutdown(timeout: configuration.shutdown_timeout)
      run_lifecycle_hooks(:before_supervisor_fork)
      live_worker = ObjectSpace.each_object(Worker).any? { |worker| !worker.shutdown? }
      live_cron = ObjectSpace.each_object(CronManager).any? { |manager| !manager.shutdown? }
      live_tracker = ObjectSpace.each_object(ProcessTracker).any? { |tracker| !tracker.shutdown? }
      live_performer = ObjectSpace.each_object(Performer).any?(&:active?)
      raise ForkInProgress, "manually owned workers, cron, process trackers, or executions must drain before fork" if live_worker || live_cron || live_tracker || live_performer
      Database.with_prepared_fork { yield }
    ensure
      if previous && Process.pid == owner
        Thread.handle_interrupt(Object => :never) do
          runtime_mutex.synchronize do
            @forking = false
            @stopping, @cron_stopping = previous
          end
        end
      end
    end

    def after_supervisor_fork(configuration)
      Daemon.after_fork!
      @runtime_mutex = Mutex.new
      @thread_priority_mutex = Mutex.new
      Database.reset_after_fork
      configuration.execution_mode = :external if Configuration::ASYNC_MODES.include?(configuration.execution_mode)
      @configuration = configuration
      @database&.resize(configuration.pool_size)
      @worker = nil
      @workers = []
      @cron_managers = {}
      @cron_stopped_databases = {}
      @cron_stopping = false
      @stopping = false
      @forking = false
      @runtime_transition = nil
      @lifecycle_revision = 0
      @locally_stopped_workers = []
      # Preserve registered adapters in the child's lifecycle so reset! retires
      # them, without adopting ObjectSpace's allocated or failed adapters.
      ObjectSpace.each_object(Adapter, &:reset_after_fork)
      ExecutionState.reset_after_fork
    end

    # CLI overrides preserve application-owned adapters and cached Database
    # references. Only a drained runtime can change its worker recipe.
    def apply_cli_configuration(options)
      with_runtime_transition(:configuring) do
        candidate = configuration.dup
        options.each { |name, value| candidate.public_send("#{name}=", value) }
        candidate.validate!
        resources = runtime_mutex.synchronize { !@workers.empty? || !@cron_managers.empty? }

        if resources && !shutdown(timeout: candidate.shutdown_timeout)
          raise ArgumentError, "workers or cron did not drain before CLI configuration"
        end

        if ObjectSpace.each_object(Performer).any?(&:active?)
          raise ArgumentError, "executions must drain before CLI configuration"
        end

        adapters = runtime_mutex.synchronize { @adapters&.keys || [] }
        adapters.each(&:reset_worker)

        runtime_mutex.synchronize do
          @configuration = candidate
          @database&.resize(candidate.pool_size)
          @worker = nil
          @workers = []
          @locally_stopped_workers = []
          @cron_managers = {}
          @stopping = @cron_stopping = false
          @lifecycle_revision = (@lifecycle_revision || 0) + 1
        end

        candidate
      end
    end
  end
end
