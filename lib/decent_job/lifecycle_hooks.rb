# frozen_string_literal: true

module DecentJob
  # Registers and runs application callbacks around process forks.
  #
  # {DecentJob} extends this module. {ForkSafety#with_prepared_fork} runs
  # :before_supervisor_fork in the parent after the runtime drains. {Supervisor}
  # and {Daemon} call it before they fork. Each {Subprocess} runs
  # :before_subprocess_boot
  # in the child before it starts its worker. Hooks run in registration order
  # on the calling thread. Hook storage belongs to the process; a forked child
  # inherits a copy of the hooks.
  #
  # Registration is not synchronized. Register hooks at boot, before the
  # supervisor starts.
  #
  # See docs/supervisor.md.
  module LifecycleHooks
    # The supported hook names.
    EVENTS = %i[before_supervisor_fork before_subprocess_boot].freeze

    # @!method before_supervisor_fork(&block)
    #   Registers a hook that runs in the supervisor before it forks children.
    #   @yield the hook, called with no arguments
    #   @return [Array<Proc>] the registered hooks for this event
    #   @raise [ArgumentError] when no block is given
    # @!method before_subprocess_boot(&block)
    #   Registers a hook that runs in each child before its worker starts.
    #   @yield the hook, called with no arguments
    #   @return [Array<Proc>] the registered hooks for this event
    #   @raise [ArgumentError] when no block is given
    EVENTS.each do |event|
      define_method(event) do |&block|
        raise ArgumentError, "a lifecycle hook block is required" unless block
        lifecycle_hooks.fetch(event) << block
      end
    end

    # Removes all registered hooks for every event.
    #
    # @return [Hash{Symbol => Array}] the new empty hook lists
    def clear_lifecycle_hooks
      @lifecycle_hooks = EVENTS.to_h { |event| [event, []] }
    end

    # Runs the hooks for one event in registration order.
    #
    # A hook that raises a StandardError goes to {#report_thread_error}, and
    # the next hook still runs. Other exceptions propagate.
    #
    # @param event [Symbol] one of {EVENTS}
    # @return [Array<Proc>] the hooks that were run
    # @raise [KeyError] when the event is unknown
    # @note Each hook is instrumented as "<event>.decent_job".
    # @api private
    def run_lifecycle_hooks(event)
      lifecycle_hooks.fetch(event).dup.each do |hook|
        Notifications.instrument("#{event}.decent_job") { hook.call }
      rescue => error
        report_thread_error(error)
      end
    end

    # Sends an operational error to {Configuration#on_thread_error}.
    #
    # @param error [Exception] the error to report
    # @return [Object, nil] the reporter result, or nil when the reporter is
    #   nil or raises
    # @note Never raises. A failing reporter must not break the shutdown or
    #   fork path that reports the error, so all exceptions are swallowed.
    # @api private
    def report_thread_error(error)
      configuration.on_thread_error&.call(error)
    rescue Exception # standard:disable Lint/RescueException
      nil
    end

    private

    def lifecycle_hooks
      @lifecycle_hooks ||= EVENTS.to_h { |event| [event, []] }
    end
  end
end
