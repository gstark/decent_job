# frozen_string_literal: true

module DecentJob
  # Per-context storage. Delegate to ActiveSupport when the host loads it so the
  # configured thread/fiber isolation level and share_with keep working.
  module IsolatedState
    KEY = :decent_job_isolated_state
    private_constant :KEY

    def self.context
      external ? external.context : Thread.current
    end

    def self.[](key)
      external ? external[key] : storage[key]
    end

    def self.[]=(key, value)
      external ? (external[key] = value) : (storage[key] = value)
    end

    def self.delete(key)
      external ? external.delete(key) : storage.delete(key)
    end

    def self.external
      ActiveSupport::IsolatedExecutionState if defined?(ActiveSupport::IsolatedExecutionState)
    end
    private_class_method :external

    def self.storage
      Thread.current.thread_variable_get(KEY) || Thread.current.thread_variable_set(KEY, {})
    end
    private_class_method :storage
  end
end
