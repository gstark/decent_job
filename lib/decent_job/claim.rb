# frozen_string_literal: true

require "json"
require "securerandom"

module DecentJob
  class Claim
    def initialize(database, enable_pauses: false, process_tracker: nil)
      @process_tracker = process_tracker || ProcessTracker.new(database: database)
      @database = database
      @store = Store.new(database)
      @enable_pauses = enable_pauses
    end

    def with_next(queues: "*", job_id: nil, stopping: -> { false })
      selection = queues.is_a?(QueueSelection) ? queues : QueueSelection.new(queues)
      @database.with_connection do |connection|
        candidates(connection, selection, job_id).each do |candidate|
          return nil if stopping.call
          key = candidate.fetch("lock_key")
          locked = false
          begin
            locked = connection.exec_params("SELECT pg_try_advisory_lock($1::bigint)", [key]).getvalue(0, 0) == "t"
            next unless locked
            acquired = false
            result = @process_tracker.register(stopping: stopping) do |process_id|
              row = start(candidate.fetch("id"), process_id)
              if row
                acquired = true
                yield(row)
              end
            end
            return result if acquired
          ensure
            connection.exec_params("SELECT pg_advisory_unlock($1::bigint)", [key]) if locked && Thread.current.status != "aborting" && !connection.finished? && connection.status == PG::CONNECTION_OK
          end
        end
      end
      nil
    end

    def next_scheduled_at(queues: "*", limit: 100, now_limit: 0)
      selection = queues.is_a?(QueueSelection) ? queues : QueueSelection.new(queues)
      filter = if selection.queues.empty?
        "TRUE"
      elsif selection.exclude?
        "NOT (queue_name = ANY($1::text[]))"
      else
        "queue_name = ANY($1::text[])"
      end
      unless [limit, now_limit].all? { |value| value.is_a?(Integer) && value >= 0 }
        raise ArgumentError, "discovery limits must be nonnegative integers"
      end
      scope = <<~SQL
        SELECT scheduled_at FROM decent_jobs
        WHERE state = 'ready' AND finished_at IS NULL AND locked_by_id IS NULL
          AND #{filter} AND $1::text[] IS NOT NULL
          AND #{@enable_pauses ? Pauses::EXCLUDED_SQL : "TRUE"}
      SQL
      @database.with_connection do |connection|
        connection.exec_params(<<~SQL, [PG::TextEncoder::Array.new.encode(selection.queues), limit, now_limit]).map { |row| Time.parse(row.fetch("scheduled_at")) }
          (#{scope} AND scheduled_at <= statement_timestamp()
            AND NOT EXISTS (
              SELECT 1 FROM pg_locks
              WHERE locktype = 'advisory' AND granted AND objsubid = 1
                AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
                AND ((classid::bigint << 32) | objid::bigint) = hashtextextended(current_schema() || '.decent_jobs.' || decent_jobs.id::text, 0)
            )
            ORDER BY scheduled_at, id LIMIT $3)
          UNION ALL
          (#{scope} AND scheduled_at > statement_timestamp() ORDER BY scheduled_at, id LIMIT $2)
          ORDER BY scheduled_at
        SQL
      end
    end

    private

    def candidates(connection, selection, job_id)
      filter = if selection.queues.empty?
        "TRUE"
      elsif selection.exclude?
        "NOT (queue_name = ANY($1::text[]))"
      else
        "queue_name = ANY($1::text[])"
      end
      order = selection.ordered? ? "array_position($1::text[], queue_name), " : ""
      encoded = PG::TextEncoder::Array.new.encode(selection.queues)
      connection.exec_params(<<~SQL, [encoded, job_id])
        SELECT id, hashtextextended(current_schema() || '.decent_jobs.' || id::text, 0) AS lock_key
        FROM decent_jobs
        WHERE state IN ('ready', 'running') AND scheduled_at <= clock_timestamp()
          AND #{filter} AND ($2::uuid IS NULL OR id = $2::uuid)
          AND $1::text[] IS NOT NULL
          AND #{@enable_pauses ? Pauses::EXCLUDED_SQL : "TRUE"}
        ORDER BY #{order}priority, scheduled_at, id
        LIMIT 1000
      SQL
    end

    def start(id, process_id)
      @database.transaction do |connection|
        row = connection.exec_params(<<~SQL, [id]).first
          SELECT * FROM decent_jobs WHERE id = $1
            AND state IN ('ready', 'running') AND scheduled_at <= clock_timestamp()
            AND #{@enable_pauses ? Pauses::EXCLUDED_SQL : "TRUE"}
          FOR UPDATE
        SQL
        if row
          interrupted_at = interrupt_previous(connection, row)
          execution_id = SecureRandom.uuid
          execution = connection.exec_params(<<~SQL, [execution_id, id, Process.pid, process_id]).first
            INSERT INTO decent_job_executions (id, job_id, process_id, process_uuid, payload, scheduled_at, job_class, queue_name)
            SELECT $1, id, $3, $4, payload, scheduled_at, job_class, queue_name FROM decent_jobs WHERE id = $2
            RETURNING started_at
          SQL
          started = @store.decode(connection.exec_params(<<~SQL, [id, execution_id, execution.fetch("started_at"), process_id]).first)
            UPDATE decent_jobs SET state = 'running', execution_id = $2, performed_at = $3,
              locked_by_id = $4, locked_at = clock_timestamp(),
              executions_count = executions_count + 1, updated_at = clock_timestamp()
            WHERE id = $1 RETURNING *
          SQL
          started.merge("execution_interrupted" => interrupted_at)
        end
      end
    end

    def interrupt_previous(connection, row)
      return unless row["execution_id"]

      previous = connection.exec_params(<<~SQL, [row.fetch("execution_id"), row.fetch("id")]).first
        SELECT started_at FROM decent_job_executions
        WHERE id = $1 AND job_id = $2 AND state = 'running' AND finished_at IS NULL
        FOR UPDATE
      SQL
      return unless previous

      started_at = Time.parse(previous.fetch("started_at"))
      error = JSON.generate("class" => InterruptedError.name, "message" => "Interrupted after starting perform at '#{started_at}'", "backtrace" => [])
      connection.exec_params(<<~SQL, [row.fetch("execution_id"), error])
        UPDATE decent_job_executions SET state = 'interrupted', error_event = 'interrupted',
          finished_at = clock_timestamp(), error = $2 WHERE id = $1
      SQL
      connection.exec_params("UPDATE decent_jobs SET error = $2, error_event = 'interrupted' WHERE id = $1", [row.fetch("id"), error])
      started_at
    end
  end
end
