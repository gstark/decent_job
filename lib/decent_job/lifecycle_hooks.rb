# frozen_string_literal: true

module DecentJob
  module LifecycleHooks
    EVENTS = %i[before_supervisor_fork before_subprocess_boot].freeze

    EVENTS.each do |event|
      define_method(event) do |&block|
        raise ArgumentError, "a lifecycle hook block is required" unless block
        lifecycle_hooks.fetch(event) << block
      end
    end

    def clear_lifecycle_hooks
      @lifecycle_hooks = EVENTS.to_h { |event| [event, []] }
    end

    def run_lifecycle_hooks(event)
      lifecycle_hooks.fetch(event).dup.each do |hook|
        ActiveSupport::Notifications.instrument("#{event}.decent_job") { hook.call }
      rescue => error
        report_thread_error(error)
      end
    end

    def report_thread_error(error)
      configuration.on_thread_error.call(error)
    rescue Exception # standard:disable Lint/RescueException
      nil
    end

    private

    def lifecycle_hooks
      @lifecycle_hooks ||= EVENTS.to_h { |event| [event, []] }
    end
  end
end
