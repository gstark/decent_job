# frozen_string_literal: true

require "active_support/core_ext/object/deep_dup"

module DecentJob
  module Stats
    def self.aggregate(schedulers)
      schedulers = schedulers.deep_dup
      result = {schedulers: schedulers}
      %i[empty_executions_count errored_executions_count succeeded_executions_count total_executions_count].each do |key|
        result[key] = schedulers.sum { |pool| pool.fetch(key) }
      end
      %i[execution_at check_queue_at].each do |key|
        result[key] = schedulers.filter_map { |pool| pool[key] }.max_by { |value| Time.iso8601(value) }
      end
      result[:active_execution_thread_count] = schedulers.sum { |pool| pool.fetch(:active_threads) }
      result[:total_succeeded_executions_count] = result[:succeeded_executions_count]
      result[:total_errored_executions_count] = result[:errored_executions_count]
      result
    end
  end
end
