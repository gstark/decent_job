# frozen_string_literal: true

module DecentJob
  # Holds the execution context of the job that runs in the current context.
  #
  # {Performer} and {JobRetry} set the context around a job body. Enqueue,
  # batch, concurrency, cleanup, and metrics code read it to find the owning
  # database, job id, and stop check. The value lives in {ExecutionState}, so
  # it follows the host's thread or fiber isolation and survives the reset of
  # application CurrentAttributes.
  #
  # See docs/runtime-resources.md.
  module Current
    # Returns the context of the current job.
    #
    # @return [Hash{Symbol => Object}, nil] the execution context, with keys
    #   such as :database, :job_id, and :stopping, or nil outside a job
    def self.execution
      ExecutionState[:execution]
    end

    # Runs a block with an execution context and restores the outer one after.
    #
    # @param execution [Hash{Symbol => Object}, nil] the context, or nil to
    #   hide the outer job from the block
    # @yield the code that runs in the context
    # @return [Object] the block result
    # @note The outer value comes back on normal return, exceptions, and
    #   nonlocal exits.
    def self.with(execution)
      ExecutionState.with(:execution, execution) { yield }
    end
  end
end
