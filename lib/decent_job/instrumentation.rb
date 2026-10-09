# frozen_string_literal: true

module DecentJob
  # Observer failures must never change application execution or its outcome.
  module Instrumentation
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

    def self.observe(database, on_error, &block)
      Thread.handle_interrupt(Object => :immediate) do
        database&.in_transaction? ? database.transaction(&block) : block.call
      end
    rescue Exception => error # standard:disable Lint/RescueException
      report(error, database, on_error)
      nil
    end
    private_class_method :observe

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
