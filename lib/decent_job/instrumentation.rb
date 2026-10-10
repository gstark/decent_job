# frozen_string_literal: true

module DecentJob
  # Wraps an application operation in a notification without letting
  # observers change that operation.
  #
  # Observer failures must never change application execution or its outcome.
  # An observer that raises cannot skip, repeat, or replace the block, its
  # return value, or its exception. {Notifications.instrument} has no such
  # boundary, so use this module for events that surround job code or
  # admission decisions.
  #
  # Callers: {Performer} (perform_job), {Adapter} (enqueue_job),
  # {ConcurrencyControls} (enqueue_concurrency_limit_exceeded and
  # enqueue_concurrency_throttle_exceeded), {Worker} (task, timer, and listener
  # events), and {LifecycleEvents} (pool and cron lifecycle events).
  #
  # Ownership: everything runs on the calling thread. When the calling context
  # has a borrowed connection with an open transaction, each observer call runs
  # inside a savepoint on that connection. A failed observer query then cannot
  # abort the application transaction.
  #
  # Invariants:
  # - Observers get a copy of the payload. The caller keeps the original hash
  #   as its own outcome state. The copy is synchronized after the block.
  # - Interrupts (Thread#raise, Thread#kill, Timeout) are deferred outside the
  #   block and the observer calls, so start and finish stay paired.
  # - An attempt that did not complete, or a finish that was interrupted, marks
  #   the payload with interrupted: true and removes :value, :handled_error, and
  #   :retried, so no observer sees a false success.
  #
  # See docs/instrumentation.md.
  module Instrumentation
    # Runs the block inside start and finish notifications for +name+.
    #
    # @param name [String] full event name, for example "perform_job.decent_job"
    # @param payload [Hash{Symbol => Object}] the caller's outcome hash. The
    #   block can add fields. On an exception this method adds :exception
    #   ([class name, message]) and :exception_object. On an interrupt it adds
    #   interrupted: true.
    # @param database [Database, nil] used to put observer calls in a
    #   savepoint when the current context is in a transaction; nil skips that
    # @param on_error [#call, nil] receives each observer or subscriber error.
    #   An explicit nil drops those errors silently.
    # @yieldparam payload [Hash{Symbol => Object}] the caller's hash, not the
    #   observer copy
    # @return [Object] the block result
    # @raise [Exception] whatever the block raises. Observer errors never raise.
    # @note Observers are synchronous and can delay the caller. Recursive
    #   observer-error reporting is suppressed per execution context.
    def self.instrument(name, payload, database:, on_error: DecentJob.configuration.on_thread_error)
      Thread.handle_interrupt(Object => :never) do
        notification_payload = payload.dup
        handle = observe(database, on_error) { Notifications.build_handle(name, notification_payload) }
        completed = false
        begin
          observe(database, on_error) { handle.start } if handle
          value = Thread.handle_interrupt(Object => :immediate) { yield payload }
          completed = true
          value
        rescue Exception => error # standard:disable Lint/RescueException
          completed = true
          payload[:exception] = [error.class.name, error.message]
          payload[:exception_object] = error
          raise
        ensure
          interrupt(payload) unless completed
          begin
            synchronize_payload(notification_payload, payload, database, on_error)
          ensure
            finished = false
            begin
              observe(database, on_error) { handle.finish } if handle
              finished = true
            ensure
              if !finished || Thread.current.status == "aborting"
                interrupt(payload)
                synchronize_payload(notification_payload, payload, database, on_error)
              end
            end
          end
        end
      end
    end

    # Removes success fields so that an interrupted attempt does not look
    # like a successful return to a later observer.
    def self.interrupt(payload)
      payload[:interrupted] = true
      payload.delete(:value)
      payload.delete(:handled_error)
      payload.delete(:retried)
    end
    private_class_method :interrupt

    # An observer may freeze or mutate its hash. The application outcome remains
    # separately owned; a frozen observer hash cannot receive subsequent fields.
    def self.synchronize_payload(notification_payload, payload, database, on_error)
      notification_payload.replace(payload)
    rescue Exception => error # standard:disable Lint/RescueException
      report(error, database, on_error)
    end
    private_class_method :synchronize_payload

    # Runs one observer call with interrupts allowed. The savepoint keeps a
    # failed observer query from poisoning the caller's transaction.
    def self.observe(database, on_error, &block)
      Thread.handle_interrupt(Object => :immediate) do
        database&.in_transaction? ? database.transaction(&block) : block.call
      end
    rescue Exception => error # standard:disable Lint/RescueException
      report(error, database, on_error)
      nil
    end
    private_class_method :observe

    # Calls on_error once per error. The IsolatedState flag stops recursion
    # when the reporter itself triggers another instrumented event.
    def self.report(error, database, on_error)
      # Do not start another application reporter while killed cleanup drains.
      return if Thread.current.status == "aborting" || IsolatedState[:decent_job_reporting_observer_error]

      begin
        IsolatedState[:decent_job_reporting_observer_error] = true
        callback = ->(*) { on_error&.call(error) }
        Thread.handle_interrupt(Object => :immediate) do
          database&.in_transaction? ? database.transaction(&callback) : callback.call
        end
      rescue Exception # standard:disable Lint/RescueException
        nil
      ensure
        IsolatedState.delete(:decent_job_reporting_observer_error)
      end
    end
    private_class_method :report
  end
end
