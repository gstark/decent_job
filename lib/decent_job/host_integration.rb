# frozen_string_literal: true

module DecentJob
  module HostIntegration
    def prepare_rails!
      @rails_ready = false
    end

    def async_ready?
      @rails_ready != false && !@loading_cli_application
    end

    def register_adapter(adapter)
      synchronize_resource_start { (@adapters ||= ObjectSpace::WeakMap.new)[adapter] = true }
      adapter.start_async if @rails_ready && async_ready? && !runtime_mutex.owned?
    end

    def configure_rails(application, validate: true)
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

    def wrap_execution(&block)
      application = Rails.application if defined?(Rails) && Rails.respond_to?(:application)
      wrapper = if application&.respond_to?(:reloader)
        application.reloader
      elsif application&.respond_to?(:executor)
        application.executor
      end
      wrapper ? wrapper.wrap(&block) : yield
    end
  end
end
