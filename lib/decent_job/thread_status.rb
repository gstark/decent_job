# frozen_string_literal: true

module DecentJob
  module ThreadStatus
    def self.with_worker(worker)
      previous = ActiveSupport::IsolatedExecutionState[:decent_job_worker]
      ActiveSupport::IsolatedExecutionState[:decent_job_worker] = worker
      yield
    ensure
      ActiveSupport::IsolatedExecutionState[:decent_job_worker] = previous
    end

    def current_thread_running?
      worker = ActiveSupport::IsolatedExecutionState[:decent_job_worker]
      worker ? worker.running? : true
    end

    def current_thread_shutting_down?
      worker = ActiveSupport::IsolatedExecutionState[:decent_job_worker]
      worker && !worker.running?
    end
  end
end
