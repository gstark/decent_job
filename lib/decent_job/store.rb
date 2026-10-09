# frozen_string_literal: true

require "json"
require "securerandom"
require "time"

module DecentJob
  # SQL persistence only. Active Job execution belongs to Performer.
  class Store
    def initialize(database)
      @database = database
    end

    def enqueue_all(jobs)
      return [] if jobs.empty?

      ids = jobs.map { SecureRandom.uuid }
      @database.transaction do |connection|
        jobs.zip(ids).each_slice(500) do |entries|
          params = []
          values = entries.map do |job, id|
            offset = params.size
            payload = job.serialize.merge("provider_job_id" => id)
            params.concat([id, job.job_id, job.queue_name, job.class.name, job.priority || 0,
              JSON.generate(payload), (job.scheduled_at || Time.now).utc.iso8601(6)])
            "(#{(1..7).map { |index| "$#{offset + index}" }.join(", ")})"
          end
          connection.exec_params(<<~SQL, params)
            INSERT INTO decent_jobs (id, active_job_id, queue_name, job_class, priority, payload, scheduled_at)
            VALUES #{values.join(", ")}
          SQL
        end
        notify(connection)
      end
      ids
    end

    def find(id)
      @database.with_connection do |connection|
        row = connection.exec_params("SELECT * FROM decent_jobs WHERE id = $1", [id]).first
        decode(row) if row
      end
    end

    def reschedule(job, context)
      @database.transaction do |connection|
        params = [context.fetch(:id), context.fetch(:execution_id), JSON.generate(job.serialize),
          (job.scheduled_at || Time.now).utc.iso8601(6), job.queue_name, job.priority || 0]
        result = connection.exec_params(<<~SQL, params)
          UPDATE decent_jobs SET state = 'ready', payload = $3, scheduled_at = $4,
            queue_name = $5, priority = $6, updated_at = clock_timestamp(), finished_at = NULL, error = NULL
          WHERE id = $1 AND execution_id = $2
        SQL
        raise ActiveJob::EnqueueError, "execution ownership lost" unless result.cmd_tuples == 1
        notify(connection)
      end
      job.successfully_enqueued = true
    end

    def executions(id)
      @database.with_connection do |connection|
        connection.exec_params("SELECT * FROM decent_job_executions WHERE job_id = $1 ORDER BY started_at, id", [id]).map { |row| decode(row) }
      end
    end

    def notify(connection)
      connection.exec("SELECT pg_notify('decent_job', '')")
    end

    def decode(row)
      row.to_h do |key, value|
        decoded = if value.nil?
          nil
        elsif %w[payload error].include?(key)
          JSON.parse(value)
        elsif key.end_with?("_at")
          Time.parse(value)
        elsif %w[priority executions_count process_id].include?(key)
          Integer(value)
        else
          value
        end
        [key, decoded]
      end
    end
  end
end
