# frozen_string_literal: true

module DecentJob
  module Current
    def self.execution
      ExecutionState[:execution]
    end

    def self.with(execution)
      ExecutionState.with(:execution, execution) { yield }
    end
  end
end
