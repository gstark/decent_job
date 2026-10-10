# frozen_string_literal: true

module DecentJob
  # Per-context key-value storage for DecentJob metadata.
  #
  # Delegate to ActiveSupport when the host loads it so the configured
  # thread/fiber isolation level and share_with keep working. Without Active
  # Support, the storage is one Hash per thread, kept in a thread variable, so
  # fibers on one thread share it.
  #
  # {ExecutionState}, {LifecycleEvents}, {Logging}, and {Instrumentation} build
  # on this module. Callers own the keys they write. Values are not copied, so
  # callers store immutable values when a host can copy the context with
  # share_with.
  #
  # The delegation choice is made on each call. The host must choose its
  # isolation level at boot, before DecentJob stores any state.
  #
  # See docs/runtime-resources.md.
  module IsolatedState
    KEY = :decent_job_isolated_state
    private_constant :KEY

    # Returns the object that identifies the current isolation context.
    #
    # @return [Thread, Fiber] the current thread, or the current fiber under
    #   Active Support fiber isolation
    def self.context
      external ? external.context : Thread.current
    end

    # @param key [Symbol] the storage key
    # @return [Object, nil] the stored value, or nil when the key is absent
    def self.[](key)
      external ? external[key] : storage[key]
    end

    # @param key [Symbol] the storage key
    # @param value [Object] the value to store in the current context only
    # @return [Object] the value
    def self.[]=(key, value)
      external ? (external[key] = value) : (storage[key] = value)
    end

    # @param key [Symbol] the storage key
    # @return [Object, nil] the removed value, or nil when the key was absent
    def self.delete(key)
      external ? external.delete(key) : storage.delete(key)
    end

    def self.external
      ActiveSupport::IsolatedExecutionState if defined?(ActiveSupport::IsolatedExecutionState)
    end
    private_class_method :external

    # A thread variable, not a fiber-local, so fibers on one thread share it
    # like Active Support's default :thread isolation.
    def self.storage
      Thread.current.thread_variable_get(KEY) || Thread.current.thread_variable_set(KEY, {})
    end
    private_class_method :storage
  end
end
