# frozen_string_literal: true

require "decent_job/interrupt_error"

module DecentJob
  module ActiveJobExtensions
    module InterruptErrors
      extend ActiveSupport::Concern

      included do
        around_perform do |job, block|
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
