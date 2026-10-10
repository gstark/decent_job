# frozen_string_literal: true

require "decent_job/current"
require "decent_job/interrupt_error"

module DecentJob
  module JobExtensions
    # Fail a job whose previous execution was interrupted instead of running
    # it again.
    #
    # Opt-in for native and Active Job classes, because it changes how an
    # interrupted job ends. An execution is interrupted when its worker lost
    # the claim (for example the process died) before the attempt finished.
    # {Claim} marks the old attempt interrupted when a new claim starts and
    # passes execution_interrupted in {Current.execution}.
    #
    # The around_perform callback raises {InterruptError} before perform runs.
    # Retry handlers can still handle it. When it is not handled, the attempt
    # error_event is "interrupted". The check applies only when this job is
    # the one the current execution runs, on the same {Adapter} database.
    #
    # See docs/execution-recovery.md.
    module InterruptErrors
      def self.included(base)
        base.around_perform do |job, block|
          context = Current.execution
          adapter = job.queue_adapter

          if context && context[:job_id] == job.job_id && adapter.is_a?(Adapter) && adapter.database.equal?(context[:database]) && context[:execution_interrupted]
            raise InterruptError, "Interrupted after starting perform at '#{context[:execution_interrupted]}'"
          end

          block.call
        end
      end
    end
  end
end
