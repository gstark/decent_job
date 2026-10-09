# frozen_string_literal: true

module DecentJob
  # Follow the host's boot-time thread/fiber isolation setting. Keep our state
  # outside CurrentAttributes, which ActiveJob and Rails reset around execution.
  module ExecutionState
    State = Data.define(:pid, :generation, :values)
    private_constant :State

    def self.[](key)
      state.values[key]
    end

    def self.[]=(key, value)
      store(state, key, value)
    end

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
