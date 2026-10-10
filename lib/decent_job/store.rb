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

    def enqueue_attributes(job, id: SecureRandom.uuid, payload: nil)
      cron = CronEntry.context_for(@database) || {}
      batch = Batch.context_for(@database, job: job) || {}
      payload = (payload || job.serialize).merge("provider_job_id" => id)
      Job::ATTRIBUTES.to_h { |key| [key, nil] }.merge(
        "id" => id, "active_job_id" => job.job_id, "queue_name" => job.queue_name,
        "job_class" => job.class.name, "priority" => job.priority || 0, "payload" => payload,
        "scheduled_at" => (job.scheduled_at || Time.now).utc,
        "labels" => payload.fetch("decent_job_labels", []),
        "concurrency_keys" => payload.fetch("decent_job_concurrency_keys", []),
        "batch_id" => batch[:batch_id], "batch_callback_id" => batch[:batch_callback_id],
        "cron_key" => cron[:key]&.to_s, "cron_at" => cron[:at]&.getutc,
        "state" => "ready", "executions_count" => 0
      )
    end

    def enqueue_all(jobs, attributes: nil, skip_duplicates: false)
      return [] if jobs.empty?

      ids = nil
      accepted_ids = {}

      @database.transaction do |connection|
        attributes ||= jobs.map { |job| enqueue_attributes(job) }
        ids = attributes.map { |row| row.fetch("id") }

        attributes.each_slice(500) do |entries|
          params = []

          values = entries.map do |row|
            offset = params.size
            params.concat([row.fetch("id"), row.fetch("active_job_id"), row.fetch("queue_name"), row.fetch("job_class"), row.fetch("priority"),
              JSON.generate(row.fetch("payload")), row.fetch("scheduled_at").iso8601(6),
              PG::TextEncoder::Array.new.encode(row.fetch("labels")),
              PG::TextEncoder::Array.new.encode(row.fetch("concurrency_keys")),
              row["batch_id"], row["batch_callback_id"], row["cron_key"], row["cron_at"]&.iso8601(6)])
            "(#{(1..13).map { |index| "$#{offset + index}" }.join(", ")})"
          end

          result = connection.exec_params(<<~SQL, params)
            INSERT INTO decent_jobs (id, active_job_id, queue_name, job_class, priority, payload, scheduled_at, labels, concurrency_keys, batch_id, batch_callback_id, cron_key, cron_at)
            VALUES #{values.join(", ")}
            ON CONFLICT #{"(cron_key, cron_at) WHERE cron_key IS NOT NULL AND cron_at IS NOT NULL" unless skip_duplicates} DO NOTHING
            RETURNING #{block_given? ? "*" : "id"}
          SQL

          result.each do |row|
            accepted_ids[row.fetch("id")] = true
            yield decode(row) if block_given?
          end
        end

        Notifier.notify_jobs(connection, jobs.zip(ids).filter_map { |job, id| job if accepted_ids[id] })
      end

      ids.map { |id| id if accepted_ids[id] }
    end

    def scheduling_hints(ids)
      @database.with_connection do |connection|
        connection.exec_params(<<~SQL, [PG::TextEncoder::Array.new.encode(ids)]).map do |row|
          SELECT queue_name,
            CASE WHEN scheduled_at > statement_timestamp() THEN scheduled_at ELSE NULL END AS deadline,
            count(*) AS count
          FROM decent_jobs WHERE id = ANY($1::uuid[]) AND state = 'ready'
          GROUP BY queue_name, deadline
        SQL
          state = {queue_name: row.fetch("queue_name"), count: row.fetch("count").to_i}
          state[:scheduled_at] = Time.parse(row.fetch("deadline")) if row["deadline"]
          state
        end
      end
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
            created_at = #{(DecentJob.configuration.dequeue_query_sort == :created_at) ? "clock_timestamp()" : "created_at"},
            updated_at = clock_timestamp(), finished_at = NULL, performed_at = NULL,
            error = #{context[:manual_retry] ? "error" : "NULL"}, error_event = 'retried'
          WHERE id = $1 AND execution_id IS NOT DISTINCT FROM $2::uuid
            AND #{context[:manual_retry] ? "state = 'discarded'" : "finished_at IS NULL AND state IN ('ready', 'running')"}
          RETURNING *
        SQL
        raise EnqueueError, "execution ownership lost" unless result.cmd_tuples == 1
        yield decode(result.first) if block_given?
        Notifier.notify_jobs(connection, [job])

        unless context[:manual_retry]
          context[:reenqueues] = context.fetch(:reenqueues, 0) + 1
          @database.after_rollback { context[:reenqueues] -= 1 }
        end
      end

      job.successfully_enqueued = true
    end

    def executions(id)
      @database.with_connection do |connection|
        connection.exec_params("SELECT * FROM decent_job_executions WHERE job_id = $1 ORDER BY started_at, id", [id]).map { |row| decode(row) }
      end
    end

    def notify(connection)
      Notifier.notify(connection)
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
