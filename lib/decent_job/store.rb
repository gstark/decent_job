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
      accepted_ids = {}
      @database.transaction do |connection|
        jobs.zip(ids).each_slice(500) do |entries|
          params = []
          values = entries.map do |job, id|
            cron = CronEntry.context_for(@database) || {}
            batch = Batch.context_for(@database, job: job) || {}
            offset = params.size
            payload = job.serialize.merge("provider_job_id" => id)
            params.concat([id, job.job_id, job.queue_name, job.class.name, job.priority || 0,
              JSON.generate(payload), (job.scheduled_at || Time.now).utc.iso8601(6),
              PG::TextEncoder::Array.new.encode(payload.fetch("decent_job_labels", [])),
              PG::TextEncoder::Array.new.encode(payload.fetch("decent_job_concurrency_keys", [])),
              batch[:batch_id], batch[:batch_callback_id], cron[:key]&.to_s, cron[:at]&.getutc&.iso8601(6)])
            "(#{(1..13).map { |index| "$#{offset + index}" }.join(", ")})"
          end
          result = connection.exec_params(<<~SQL, params)
            INSERT INTO decent_jobs (id, active_job_id, queue_name, job_class, priority, payload, scheduled_at, labels, concurrency_keys, batch_id, batch_callback_id, cron_key, cron_at)
            VALUES #{values.join(", ")}
            ON CONFLICT (cron_key, cron_at) WHERE cron_key IS NOT NULL AND cron_at IS NOT NULL DO NOTHING
            RETURNING id
          SQL
          result.each { |row| accepted_ids[row.fetch("id")] = true }
        end
        notify(connection)
      end
      ids.map { |id| id if accepted_ids[id] }
    end

    def find(id)
      @database.with_connection do |connection|
        row = connection.exec_params("SELECT * FROM decent_jobs WHERE id = $1", [id]).first
        decode(row) if row
      end
    end

    def reschedule(job, context, payload: nil)
      @database.transaction do |connection|
        payload ||= job.serialize
        params = [context.fetch(:id), context.fetch(:execution_id), JSON.generate(payload),
          (job.scheduled_at || Time.now).utc.iso8601(6), job.queue_name, job.priority || 0,
          PG::TextEncoder::Array.new.encode(payload.fetch("decent_job_labels", [])),
          PG::TextEncoder::Array.new.encode(payload.fetch("decent_job_concurrency_keys", []))]
        result = connection.exec_params(<<~SQL, params)
          UPDATE decent_jobs SET state = 'ready', payload = $3, scheduled_at = $4,
            queue_name = $5, priority = $6, labels = $7, concurrency_keys = $8,
            updated_at = clock_timestamp(), finished_at = NULL, performed_at = NULL,
            error = #{context[:manual_retry] ? "error" : "NULL"}, error_event = 'retried'
          WHERE id = $1 AND execution_id IS NOT DISTINCT FROM $2::uuid
            AND #{context[:manual_retry] ? "state = 'discarded'" : "finished_at IS NULL AND state IN ('ready', 'running')"}
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
        elsif %w[labels concurrency_keys].include?(key)
          PG::TextDecoder::Array.new.decode(value)
        elsif key.end_with?("_at")
          Time.parse(value)
        elsif key == "duration"
          Float(value)
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
