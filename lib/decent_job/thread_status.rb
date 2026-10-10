# frozen_string_literal: true

module DecentJob
  # Lets job code ask whether its worker is still running.
  #
  # {DecentJob} extends this module, so the predicates are module methods on
  # DecentJob. {Worker} execution threads mark themselves with {.with_worker}.
  # The worker reference lives in {ExecutionState}, so it follows the host's
  # thread or fiber isolation.
  #
  # See docs/worker-observability.md.
  module ThreadStatus
    # Marks the current context as owned by a worker for the block.
    #
    # @param worker [Worker] the worker that owns the execution thread
    # @yield the worker loop
    # @return [Object] the block result
    # @api private
    def self.with_worker(worker)
      ExecutionState.with(:worker, worker) { yield }
    end

    # Tells whether the current worker still accepts work.
    #
    # @return [Boolean] false when the owning worker is stopping, true when it
    #   runs or when no worker owns the current context
    def current_thread_running?
      worker = ExecutionState[:worker]
      worker ? worker.running? : true
    end

    # Tells whether the current worker is stopping.
    #
    # @return [Boolean, nil] true when the owning worker is stopping, false when
    #   it runs, or nil when no worker owns the current context
    def current_thread_shutting_down?
      worker = ExecutionState[:worker]
      worker && !worker.running?
    end
  end
end
