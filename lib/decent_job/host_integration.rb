# frozen_string_literal: true

module DecentJob
  module HostIntegration
    # Load the Railtie now under Rails, or before a Rails application that
    # boots later reads its configuration. Without ActiveSupport there is no
    # Rails application to configure.
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

    def prepare_rails!
      @rails_ready = false
    end

    def async_ready?
      @rails_ready != false && !@loading_cli_application
    end

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

    def finish_rails_initialization!
      adapters = runtime_mutex.synchronize do
        @rails_ready = true
        @adapters&.keys || []
      end

      adapters.each(&:start_async)
    end

    def with_cli_application
      previous = @loading_cli_application
      @loading_cli_application = true
      yield
    ensure
      @loading_cli_application = previous
    end

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
