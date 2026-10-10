# frozen_string_literal: true

require "decent_job/current"
require "decent_job/interrupt_error"

module DecentJob
  module JobExtensions
    # Fail a job whose previous execution was interrupted instead of running
    # it again.
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
