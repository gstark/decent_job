# frozen_string_literal: true

module DecentJob
  # Connects the runtime to a Rails host and to the CLI.
  #
  # {DecentJob} extends this module, so its methods are module methods on
  # DecentJob and its state lives in DecentJob's module instance variables.
  # The registry state (@adapters, @rails_ready, @configuration, @database)
  # changes under {ForkSafety#runtime_mutex}. The exceptions are the boot-time
  # flags that {#prepare_rails!} and {#with_cli_application} set without it.
  #
  # Rails readiness has three values. nil means no Railtie loaded, so async
  # adapters can start at once. false means Rails is booting, so adapters
  # wait. true means Rails finished boot.
  #
  # Do not start adapters while the runtime lock is held: starting a worker
  # takes that lock again through {ForkSafety#synchronize_resource_start}.
  #
  # See docs/rails-integration.md.
  module HostIntegration
    # Loads the Railtie when Rails is present, now or later.
    #
    # Load the Railtie now under Rails, or before a Rails application that
    # boots later reads its configuration. Without ActiveSupport there is no
    # Rails application to configure.
    #
    # @return [void]
    # @note Called once at the end of lib/decent_job.rb. The before_configuration
    #   hook is registered at most once per process.
    # @api private
    def load_railtie_with_rails
      if defined?(Rails::Railtie)
        require "decent_job/railtie"
      elsif defined?(ActiveSupport.on_load) && !@railtie_hook_registered
        @railtie_hook_registered = true
        ActiveSupport.on_load(:before_configuration) do
          require "decent_job/railtie" if defined?(Rails::Railtie)
        end
      end
    end

    # Marks Rails as booting, so adapters wait for
    # {#finish_rails_initialization!} before they start async workers.
    #
    # @return [false]
    # @api private
    def prepare_rails!
      @rails_ready = false
    end

    # Tells whether adapters can start async workers now.
    #
    # @return [Boolean] false while Rails boots or while the CLI loads the
    #   application; the CLI starts its own workers after the load
    # @note Reads state without the runtime lock.
    # @api private
    def async_ready?
      @rails_ready != false && !@loading_cli_application
    end

    # Records a new adapter in the registry.
    #
    # The registry holds adapters weakly, so it does not keep unused adapters
    # alive. {DecentJob.reset!}, {ForkSafety#apply_cli_configuration}, and
    # {#finish_rails_initialization!} walk it. An adapter created during
    # {DecentJob.reset!} is retired at once, because reset is about to discard
    # the runtime it would join.
    #
    # @param adapter [Adapter] the adapter, called from {Adapter#initialize}
    # @return [void]
    # @raise [ForkInProgress] while the runtime prepares to fork
    # @note Starts the adapter's async worker when Rails already finished boot
    #   and the caller does not hold the runtime lock.
    # @api private
    def register_adapter(adapter)
      registration = lambda do
        raise ForkInProgress, "runtime is preparing to fork" if @forking
        (@adapters ||= ObjectSpace::WeakMap.new)[adapter] = true
        @runtime_transition == :resetting
      end
      retire = runtime_mutex.owned? ? registration.call : runtime_mutex.synchronize(&registration)
      adapter.retire if retire
      adapter.start_async if @rails_ready && async_ready? && !runtime_mutex.owned?
    end

    # Applies `config.decent_job` from a Rails application.
    #
    # The change is made on a copy and published only when it passes, so a
    # failed change leaves the active configuration as it was. The Railtie
    # calls this twice: once early without validation, and once after
    # initialization with validation.
    #
    # @param application [Rails::Application] the host application
    # @param validate [Boolean] false skips {Configuration#validate!}, for the
    #   early pass before autoloaders exist
    # @return [Integer] the new lifecycle revision
    # @raise [ArgumentError] for unknown options, invalid settings, or a
    #   database_url change after the global database opened
    # @raise [LifecycleInProgress] when another transition is active
    # @raise [ForkInProgress] while the runtime prepares to fork
    # @note Resizes the open global {Database} pool to the new
    #   {Configuration#pool_size} and increments the lifecycle revision, so
    #   start operations that began before this call give up.
    # @api private
    def configure_rails(application, validate: true)
      with_runtime_transition(:configuring) do
        candidate = runtime_mutex.synchronize { @configuration.dup }
        candidate.apply_rails_config(application.config.decent_job, environment: Rails.env.to_s, logger: Rails.logger)
        # Cron validation can autoload jobs and create adapters. Run application
        # code outside the registry lock and only after Rails installs autoloaders.
        candidate.validate! if validate
        runtime_mutex.synchronize do
          if @database && candidate.database_url != @configuration.database_url
            raise ArgumentError, "configure the Rails database_url before opening the database"
          end
          @configuration = candidate
          @database&.resize(candidate.pool_size)
          @lifecycle_revision = (@lifecycle_revision || 0) + 1
        end
      end
    end

    # Marks Rails as ready and starts every registered adapter that is eligible.
    #
    # Each adapter decides on its own whether to start. An adapter that was
    # stopped explicitly stays stopped.
    #
    # @return [Array<Adapter>] the registered adapters
    # @note Starts adapters after it releases the runtime lock.
    # @api private
    def finish_rails_initialization!
      adapters = runtime_mutex.synchronize do
        @rails_ready = true
        @adapters&.keys || []
      end
      adapters.each(&:start_async)
    end

    # Runs a block with automatic adapter startup suppressed.
    #
    # The CLI wraps its whole run in this block, so application files it loads
    # do not start embedded workers. The CLI starts its configured workers
    # itself.
    #
    # @yield the CLI command
    # @return [Object] the block result
    # @note The flag is process-wide, not per thread. Nested calls restore the
    #   outer value.
    # @api private
    def with_cli_application
      previous = @loading_cli_application
      @loading_cli_application = true
      yield
    ensure
      @loading_cli_application = previous
    end

    # Runs a block inside the Rails reloader or executor when one exists.
    #
    # {Performer} and {CronManager} wrap application code with the reloader, so
    # code reloading and executor callbacks apply to each job. {ProcessTracker}
    # passes reload: false, so its heartbeat callbacks use only the executor.
    #
    # @param reload [Boolean] true to prefer the reloader, false to use only
    #   the executor
    # @yield the application code
    # @return [Object] the block result
    def wrap_execution(reload: true, &block)
      application = Rails.application if defined?(Rails) && Rails.respond_to?(:application)
      wrapper = if reload && application&.respond_to?(:reloader)
        application.reloader
      elsif application&.respond_to?(:executor)
        application.executor
      end
      wrapper ? wrapper.wrap(&block) : yield
    end
  end
end
