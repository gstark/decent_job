# frozen_string_literal: true

module DecentJob
  module ThreadStatus
    def self.with_worker(worker)
      ExecutionState.with(:worker, worker) { yield }
    end

    def current_thread_running?
      worker = ExecutionState[:worker]
      worker ? worker.running? : true
    end

    def current_thread_shutting_down?
      worker = ExecutionState[:worker]
      worker && !worker.running?
    end
  end
end
