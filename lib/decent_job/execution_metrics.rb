# frozen_string_literal: true

require "time"

module DecentJob
  # Aggregates retained attempt snapshots. All latency values are seconds.
  class ExecutionMetrics
    AGGREGATES_SQL = <<~SQL
      COUNT(DISTINCT job_id) AS jobs_count,
      COUNT(*) AS executions_count,
      AVG(EXTRACT(EPOCH FROM started_at - scheduled_at)) AS avg_queue,
      MIN(EXTRACT(EPOCH FROM started_at - scheduled_at)) AS min_queue,
      MAX(EXTRACT(EPOCH FROM started_at - scheduled_at)) AS max_queue,
      AVG(duration) AS avg_execution,
      MIN(duration) AS min_execution,
      MAX(duration) AS max_execution,
      AVG(EXTRACT(EPOCH FROM started_at - scheduled_at) + duration) AS avg_total,
      MIN(EXTRACT(EPOCH FROM started_at - scheduled_at) + duration) AS min_total,
      MAX(EXTRACT(EPOCH FROM started_at - scheduled_at) + duration) AS max_total
    SQL
    private_constant :AGGREGATES_SQL

    def initialize(database: nil)
      @database = database || Current.execution&.fetch(:database, nil) || DecentJob.database
    end

    def summary(from: nil, to: nil, job_class: nil, queue_name: nil)
      aggregate(from: from, to: to, job_class: job_class, queue_name: queue_name).first
    end

    def grouped_by(group, from: nil, to: nil, job_class: nil, queue_name: nil)
      unless group.is_a?(String) || group.is_a?(Symbol)
        raise ArgumentError, "group must be job_class or queue_name"
      end

      column = case group
      when :job_class, "job_class" then "job_class"
      when :queue_name, "queue_name" then "queue_name"
      else raise ArgumentError, "group must be job_class or queue_name"
      end

      aggregate(group: column, from: from, to: to, job_class: job_class, queue_name: queue_name)
    end

    private

    def aggregate(from:, to:, job_class:, queue_name:, group: nil)
      validate_filters!(from: from, to: to, job_class: job_class, queue_name: queue_name)
      conditions = []
      values = []

      [["scheduled_at >=", from&.iso8601(6)], ["scheduled_at <", to&.iso8601(6)],
        ["job_class =", job_class], ["queue_name =", queue_name]].each do |predicate, value|
        next if value.nil?
        values << value
        conditions << "#{predicate} $#{values.length}"
      end

      sql = "SELECT #{"#{group}, " if group}#{AGGREGATES_SQL} FROM decent_job_executions"
      sql += " WHERE #{conditions.join(" AND ")}" unless conditions.empty?
      sql += " GROUP BY #{group} ORDER BY #{group} ASC NULLS LAST" if group

      @database.with_connection do |connection|
        connection.exec_params(sql, values).map do |row|
          row.to_h do |key, value|
            decoded = if key == group
              value
            elsif key.end_with?("_count")
              value.to_i
            else
              value&.to_f
            end

            [key.to_sym, decoded]
          end
        end
      end
    end

    def validate_filters!(from:, to:, job_class:, queue_name:)
      {from: from, to: to}.each do |name, value|
        raise ArgumentError, "#{name} must be a Time or nil" unless value.nil? || value.is_a?(Time)
      end

      raise ArgumentError, "from must not be after to" if from && to && from > to

      {job_class: job_class, queue_name: queue_name}.each do |name, value|
        raise ArgumentError, "#{name} must be a String or nil" unless value.nil? || value.is_a?(String)
      end
    end
  end
end
