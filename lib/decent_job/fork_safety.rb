# frozen_string_literal: true

module DecentJob
  # Guards runtime lifecycle changes and process forks.
  #
  # {DecentJob} extends this module. Its methods change DecentJob's module
  # instance variables under {#runtime_mutex}, the registry lock. The exception
  # is {#after_supervisor_fork}, which runs in a new child before any other
  # thread starts. Two
  # flags block lifecycle work while they are set:
  #
  # - @runtime_transition (:configuring or :resetting) is set by
  #   {#with_runtime_transition}. Explicit starts raise
  #   {LifecycleInProgress}. Automatic starts skip.
  # - @forking is set by {#with_prepared_fork}. Starts raise {ForkInProgress},
  #   except callers that pass skip_during_fork: true.
  #
  # Only one transition or fork can be active at a time. Flag changes run with
  # interrupts deferred, so Thread#raise or Thread#kill cannot leave a flag set.
  #
  # See docs/lifecycle.md and docs/supervisor.md.
  module ForkSafety
    # Returns the registry lock for workers, cron managers, adapters,
    # configuration, and the global database.
    #
    # @return [Thread::Mutex] the lock; {DecentJob::MUTEX} until a forked child
    #   replaces it in {#after_supervisor_fork}
    # @note Not reentrant. Code that can run with the lock held checks
    #   Mutex#owned? before it synchronizes.
    # @api private
    def runtime_mutex
      @runtime_mutex ||= MUTEX
    end

    # Runs a resource start under the registry lock, if starts are allowed now.
    #
    # Lifecycle notifications recorded inside the block are delivered after
    # the outermost {LifecycleEvents.defer} scope ends, outside all lifecycle
    # locks. The block runs directly when the caller already holds the lock.
    #
    # @param skip_during_fork [Boolean] true returns nil instead of raising
    #   during a fork or a transition; for standalone performers and process
    #   trackers
    # @param automatic [Boolean] true returns nil instead of raising during a
    #   transition; for enqueue wakeups and readiness callbacks
    # @yield the start operation, with the registry lock held
    # @return [Object, nil] the block result, or nil when the start was skipped
    # @raise [ForkInProgress] during a fork, unless skip_during_fork is true
    # @raise [LifecycleInProgress] during a transition, for explicit starts
    # @api private
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

    # Marks a runtime transition for the duration of the block.
    #
    # The registry lock is released while the block runs, so the block can run
    # application code, such as a configure block, that inspects the runtime.
    # The flag keeps starts and other transitions out in the meantime.
    #
    # @param name [Symbol] :configuring or :resetting, shown in error messages
    # @yield the transition work, without the registry lock
    # @return [Object] the block result
    # @raise [LifecycleInProgress] when another transition is active
    # @raise [ForkInProgress] while the runtime prepares to fork
    # @api private
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

    # Drains the runtime and prepares the process for fork(2).
    #
    # Steps, in order:
    #
    # 1. Set @forking, so no new resource can start.
    # 2. Shut down registered workers and cron managers within
    #    {Configuration#shutdown_timeout} seconds.
    # 3. Run the :before_supervisor_fork hooks.
    # 4. Refuse to fork while any live {Worker}, {CronManager},
    #    {ProcessTracker}, or active {Performer} exists in the process. This
    #    includes resources that the application owns.
    # 5. Close idle pooled connections through {Database.with_prepared_fork}
    #    and yield. The block calls fork.
    #
    # In the parent, the ensure block clears @forking and restores the stop
    # flags that existed before the call. The child skips that restore because
    # its pid differs; it must call {#after_supervisor_fork}.
    #
    # @yield the code that forks
    # @return [Object] the block result
    # @raise [ForkInProgress] when a fork is already in progress, when
    #   resources do not drain in time, or when any resource is still live
    # @raise [LifecycleInProgress] when a transition is active
    # @note Stops all registered workers and cron. The parent does not restart
    #   them after the fork.
    # @api private
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

    # Rebuilds runtime state in a forked child.
    #
    # The child gets fresh mutexes and fresh {Database} pools through
    # {Database.reset_after_fork}. Inherited workers, cron managers, and
    # transition flags are dropped. Registered adapters and the open global
    # database object stay, so reset! can still retire and close them. Async execution modes become :external, because the child
    # runs workers that the CLI starts explicitly.
    #
    # @param configuration [Configuration] the child's configuration, for
    #   example one subprocess recipe; it replaces the active configuration
    # @return [void]
    # @note Call this first in the child, before any other DecentJob call.
    #   It mutates the configuration argument.
    # @api private
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

    # Applies CLI options on top of the loaded application configuration.
    #
    # CLI overrides preserve application-owned adapters and cached Database
    # references. Only a drained runtime can change its worker recipe.
    # Registered workers and cron managers are shut down and dropped, and each
    # registered adapter forgets its worker, so the next start uses the new
    # recipe. Adapter stop flags and database-specific cron stops stay.
    #
    # @param options [Hash{Symbol => Object}] setter names and values
    # @return [Configuration] the published configuration
    # @raise [ArgumentError] for invalid values, when resources do not drain
    #   within the new shutdown_timeout, or while a standalone execution runs
    # @raise [LifecycleInProgress] when another transition is active
    # @raise [ForkInProgress] while the runtime prepares to fork
    # @note Each assignment is explicit, so CLI values win over Rails options.
    #   Resizes the open global {Database} pool.
    # @api private
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
