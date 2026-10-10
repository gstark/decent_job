# frozen_string_literal: true

module DecentJob
  # Per-context key-value storage for DecentJob metadata.
  #
  # {Current}, {ThreadStatus}, {Bulk}, {CronEntry}, {Batch},
  # {LifecycleEvents}, and {InlineBuffer} store their scoped values here.
  # Storage goes through {IsolatedState}, so it follows the host's boot-time
  # thread or fiber isolation setting. Keep our state outside
  # CurrentAttributes, which ActiveJob and Rails reset around execution.
  #
  # Invariants:
  # - The stored State is immutable. Every write replaces it with a new
  #   frozen snapshot.
  # - State belongs to one process. A State with another pid is ignored, and
  #   a fresh State with a new generation replaces it.
  #
  # See docs/runtime-resources.md.
  module ExecutionState
    # pid: the owning process. generation: a unique object per reset, which
    # {.with} compares by identity. values: a frozen Hash of the stored keys.
    State = Data.define(:pid, :generation, :values)
    private_constant :State

    # @param key [Symbol]
    # @return [Object, nil] the value in the current context, or nil when unset
    def self.[](key)
      state.values[key]
    end

    # Sets a value in the current context without a scope. Nothing restores
    # the previous value.
    #
    # @param key [Symbol]
    # @param value [Object]
    # @return [Object] value
    def self.[]=(key, value)
      store(state, key, value)
    end

    # Sets a value for the duration of the block, then restores the previous
    # value.
    #
    # @param key [Symbol]
    # @param value [Object]
    # @yield runs with the value set
    # @return [Object] the block result
    # @note The restore also runs on exceptions and nonlocal exits. Interrupts
    #   are deferred while the value is set and restored.
    def self.with(key, value)
      scope = previous = nil
      Thread.handle_interrupt(Object => :never) do
        scope = state
        previous = scope.values[key]
        store(scope, key, value)
      end
      yield
    ensure
      Thread.handle_interrupt(Object => :never) do
        current = IsolatedState[:decent_job]
        # A reset or fork starts a new generation. Never publish an old scope's
        # snapshot again, even if its ensure runs before the next state read.
        if scope && current && current.pid == Process.pid && current.generation.equal?(scope.generation)
          store(current, key, previous)
        end
      end
    end

    # Drops inherited state in a forked child. Only the DecentJob key is
    # deleted, so unrelated host keys stay.
    #
    # @return [void]
    # @note {ForkSafety#after_supervisor_fork} calls this.
    def self.reset_after_fork
      IsolatedState.delete(:decent_job)
    end

    def self.state
      current = IsolatedState[:decent_job]
      unless current && current.pid == Process.pid
        current = State.new(pid: Process.pid, generation: Object.new, values: {}.freeze)
        IsolatedState[:decent_job] = current
      end
      current
    end

    def self.store(current, key, value)
      # Host streaming copies the top-level execution state with share_with.
      # Replace snapshots so neither context can mutate the other's values.
      IsolatedState[:decent_job] = current.with(values: current.values.merge(key => value).freeze)
    end
    private_class_method :state, :store
  end
end
