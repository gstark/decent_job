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
      strategy = DecentJob.configuration.lock_strategy
      migrated = @database.with_connection { |connection| ConcurrencyClaims.lock_type_column?(connection) }
      strategy = :advisory unless migrated
      claimed = nil
      original_error = nil
      capture = ->(row) { claimed = row }
      if strategy == :advisory
        with_advisory(queues: queues, job_id: job_id, stopping: stopping, migrated: migrated, capture: capture) { |row| yield row }
      else
        with_row_claim(queues: queues, job_id: job_id, stopping: stopping, strategy: strategy, capture: capture) { |row| yield row }
      end
    rescue Exception => original_error # standard:disable Lint/RescueException
      raise
    ensure
      release_ownership(claimed, migrated: migrated, original_error: original_error) if claimed
    end

    def with_advisory(queues:, job_id:, stopping:, migrated:, capture:)
      selection = queues.is_a?(QueueSelection) ? queues : QueueSelection.new(queues)
      @database.with_connection do |connection|
        candidates(connection, selection, job_id).each do |candidate|
          return nil if stopping.call
          key = candidate.fetch("lock_key")
          # A session lock is reentrant. Skip keys already owned by this
          # checkout so nested performers cannot interrupt an active attempt.
          Thread.handle_interrupt(Object => :on_blocking) do
            locked = false
            ownership_known = false
            original_error = nil
            begin
              locked = connection.exec_params(<<~SQL, [key]).getvalue(0, 0) == "t"
                SELECT CASE WHEN EXISTS (
                  SELECT 1 FROM pg_locks
                  WHERE locktype = 'advisory' AND granted AND objsubid = 1
                    AND pid = pg_backend_pid()
                    AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
                    AND ((classid::bigint << 32) | objid::bigint) = $1::bigint
                ) THEN false ELSE pg_try_advisory_lock($1::bigint) END
              SQL
              ownership_known = true
              next unless locked

              acquired = false
              result = Thread.handle_interrupt(Object => :immediate) do
                @process_tracker.register(stopping: stopping) do |process_id|
                  row = start(candidate.fetch("id"), process_id, strategy: :advisory, migrated: migrated, capture: capture)
                  if row
                    acquired = true
                    yield(row)
                  end
                end
              end
              return result if acquired
            rescue Exception => original_error # standard:disable Lint/RescueException
              raise
            ensure
              unless Thread.current.status == "aborting" || connection.finished?
                if !ownership_known
                  # An interrupted query may have acquired the lock without
                  # returning its result. Never return that session to the pool.
                  connection.finish
                elsif locked && connection.status == PG::CONNECTION_OK
                  released = false
                  begin
                    connection.exec_params("SELECT pg_advisory_unlock($1::bigint)", [key])
                    released = true
                  rescue PG::Error
                    raise unless original_error
                  ensure
                    connection.finish unless released || connection.finished?
                  end
                end
              end
            end
          end
        end
      end
      nil
    end

    private :with_advisory

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
        WHERE state = 'ready' AND finished_at IS NULL AND NOT (#{ConcurrencyClaims::EXECUTION_LOCKED_SQL})
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

    def with_row_claim(queues:, job_id:, stopping:, strategy:, capture:)
      selection = queues.is_a?(QueueSelection) ? queues : QueueSelection.new(queues)
      @process_tracker.register(stopping: stopping) do |process_id|
        next if stopping.call
        if strategy == :hybrid
          @database.with_connection do |connection|
            Thread.handle_interrupt(Object => :on_blocking) do
              key = nil
              row = nil
              ownership_known = false
              original_error = nil
              begin
                @database.transaction do
                  candidate = candidates(connection, selection, job_id, row_lock: true).first
                  if candidate
                    key = candidate.fetch("lock_key")
                    locked = connection.exec_params("SELECT pg_try_advisory_lock($1::bigint)", [key]).getvalue(0, 0) == "t"
                    ownership_known = true
                    if locked
                      row = start(candidate.fetch("id"), process_id, strategy: strategy, migrated: true, capture: capture)
                    else
                      key = nil
                    end
                  end
                end
                Thread.handle_interrupt(Object => :immediate) { yield row } if row
              rescue Exception => original_error # standard:disable Lint/RescueException
                raise
              ensure
                unless key.nil? || Thread.current.status == "aborting" || connection.finished?
                  if !ownership_known
                    connection.finish
                  elsif connection.status == PG::CONNECTION_OK
                    released = false
                    begin
                      connection.exec_params("SELECT pg_advisory_unlock($1::bigint)", [key])
                      released = true
                    rescue PG::Error
                      raise unless original_error
                    ensure
                      connection.finish unless released || connection.finished?
                    end
                  end
                end
              end
            end
          end
        else
          row = nil
          @database.transaction do |connection|
            candidate = candidates(connection, selection, job_id, row_lock: true).first
            row = start(candidate.fetch("id"), process_id, strategy: strategy, migrated: true, capture: capture) if candidate
          end
          yield row if row
        end
      end
    end

    def release_ownership(row, migrated:, original_error:)
      Thread.handle_interrupt(Object => :never) do
        release = lambda do |connection|
          connection.exec_params(<<~SQL, row.values_at("id", "execution_id", "locked_by_id"))
            UPDATE decent_jobs SET locked_by_id = NULL, locked_at = NULL #{", lock_type = NULL" if migrated}
            WHERE id = $1 AND execution_id = $2 AND locked_by_id = $3
          SQL
        end
        released = false
        delegated = false
        begin
          if Thread.current.status == "aborting"
            delegated = true
            release_after_checkout(release, original_error: original_error)
          else
            begin
              @database.with_connection { |connection| release.call(connection) }
              released = true
            rescue ConnectionTimeout, ClosedDatabase, PG::Error
              # Cleanup must not depend on a spare pool slot or an application
              # transaction that has already failed. The callback owns its handoff.
              delegated = true
              release_after_checkout(release, original_error: original_error)
            end
          end
        ensure
          release_after_checkout(release, original_error: original_error) if !released && !delegated && Thread.current.status == "aborting"
        end
      end
    end

    def release_after_checkout(release, original_error: nil)
      # A caller can hold an outer checkout with an interrupted transaction.
      # Wait for all application ensures and session retirement before cleanup.
      @database.after_connection_release do
        aborting = Thread.current.status == "aborting"
        released = false
        connection = nil
        begin
          Thread.handle_interrupt(Object => (aborting ? :never : :on_blocking)) do
            connection = @database.connect
            release.call(connection)
            released = true
          end
        rescue ConnectionTimeout, ClosedDatabase, PG::Error
          raise unless original_error || Thread.current.status == "aborting"
        ensure
          connection&.finish unless connection&.finished?
          # A live fallback remains cancellable. If it is killed, close its
          # interrupted session before handing cleanup to the aborting path.
          if !released && !aborting && Thread.current.status == "aborting"
            release_after_checkout(release, original_error: original_error)
          end
        end
      end
    end

    def candidates(connection, selection, job_id, row_lock: false)
      filter = if selection.queues.empty?
        "TRUE"
      elsif selection.exclude?
        "NOT (queue_name = ANY($1::text[]))"
      else
        "queue_name = ANY($1::text[])"
      end
      order = selection.ordered? ? "array_position($1::text[], queue_name), " : ""
      encoded = PG::TextEncoder::Array.new.encode(selection.queues)
      configuration = DecentJob.configuration
      timestamp = {created_at: "created_at", scheduled_at: "scheduled_at"}.fetch(configuration.dequeue_query_sort)
      limit = (job_id || row_lock) ? 1 : configuration.queue_select_limit
      connection.exec_params(<<~SQL, [encoded, job_id, limit])
        SELECT id, hashtextextended(current_schema() || '.decent_jobs.' || id::text, 0) AS lock_key
        FROM decent_jobs
        WHERE state IN ('ready', 'running') AND finished_at IS NULL AND scheduled_at <= clock_timestamp()
          AND #{filter} AND ($2::uuid IS NULL OR id = $2::uuid)
          AND $1::text[] IS NOT NULL
          AND #{row_lock ? "NOT (#{ConcurrencyClaims::EXECUTION_LOCKED_SQL})" : "TRUE"}
          AND #{@enable_pauses ? Pauses::EXCLUDED_SQL : "TRUE"}
        ORDER BY #{order}priority, #{timestamp}, id
        LIMIT $3
        #{"FOR NO KEY UPDATE SKIP LOCKED" if row_lock}
      SQL
    end

    def start(id, process_id, strategy:, migrated:, capture:)
      @database.transaction do |connection|
        row = connection.exec_params(<<~SQL, [id]).first
          SELECT * FROM decent_jobs WHERE id = $1
            AND state IN ('ready', 'running') AND finished_at IS NULL AND scheduled_at <= clock_timestamp()
            AND #{@enable_pauses ? Pauses::EXCLUDED_SQL : "TRUE"}
          AND NOT #{ConcurrencyClaims::PERSISTED_LOCKED_SQL}
          #{(strategy == :advisory) ? "FOR UPDATE" : "FOR NO KEY UPDATE SKIP LOCKED"}
        SQL
        if row
          interrupted_at = interrupt_previous(connection, row)
          execution_id = SecureRandom.uuid
          execution = connection.exec_params(<<~SQL, [execution_id, id, Process.pid, process_id]).first
            INSERT INTO decent_job_executions (id, job_id, process_id, process_uuid, payload, scheduled_at, job_class, queue_name)
            SELECT $1, id, $3, $4, payload, scheduled_at, job_class, queue_name FROM decent_jobs WHERE id = $2
            RETURNING *
          SQL
          started = @store.decode(connection.exec_params(<<~SQL, [id, execution_id, execution.fetch("started_at"), process_id]).first)
            UPDATE decent_jobs SET state = 'running', execution_id = $2, performed_at = $3,
              locked_by_id = $4, locked_at = clock_timestamp(), #{"lock_type = '#{strategy}'," if migrated}
              executions_count = executions_count + 1, updated_at = clock_timestamp()
            WHERE id = $1 RETURNING *
          SQL
          started["execution_interrupted"] = interrupted_at
          started["execution_attributes"] = @store.decode(execution)
          capture.call(started)
          started
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
