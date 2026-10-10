# frozen_string_literal: true

module DecentJob
  # Combines per-pool scheduler counters into one worker statistics hash.
  #
  # {Worker#stats} calls it for one worker. {DecentJob.stats} calls it for the
  # schedulers of every registered worker. The global result is not an atomic
  # snapshot, because each worker is read separately.
  #
  # See docs/worker-observability.md.
  module Stats
    # Builds aggregate statistics from per-pool counter hashes.
    #
    # @param schedulers [Array<Hash{Symbol => Object}>] per-pool hashes as
    #   built by {Worker#reset_stats} and merged with {SchedulerCache#stats}.
    #   Time fields are ISO 8601 Strings or nil.
    # @return [Hash{Symbol => Object}] a deep copy of the input under
    #   +:schedulers+, summed execution counts, the latest +execution_at+ and
    #   +check_queue_at+ (nil when no pool has one), and
    #   +active_execution_thread_count+. The +total_+ count keys are aliases.
    # @note Compares times as instants, so different UTC offsets sort correctly.
    def self.aggregate(schedulers)
      schedulers = Support.deep_dup(schedulers)
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
