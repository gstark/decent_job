# frozen_string_literal: true

module DecentJob
  # All claim mutations take sorted scope locks before touching job rows.
  class ConcurrencyClaims
    JOB_KEY_SQL = "hashtextextended(current_schema() || '.decent_jobs.' || decent_jobs.id::text, 0)"
    JOB_LOCKED_SQL = <<~SQL
      EXISTS (
        SELECT 1 FROM pg_locks
        WHERE locktype = 'advisory' AND granted AND objsubid = 1
          AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
          AND classid = ((#{JOB_KEY_SQL} >> 32) & 4294967295)::oid
          AND objid = (#{JOB_KEY_SQL} & 4294967295)::oid
      )
    SQL

    def initialize(database)
      @database = database
    end

    def synchronize(job_id, scopes: [])
      @database.transaction do |connection|
        existing = connection.exec_params("SELECT scope FROM decent_job_concurrency_claims WHERE job_id = $1", [job_id]).column_values(0)
        (scopes + existing).uniq.sort.each { |scope| lock(connection, scope) }
        yield connection
      end
    end

    def grant(connection, scope, context)
      upsert(connection, scope, context, "granted")
    end

    def wait(connection, scope, context)
      release(connection, context)
      connection.exec_params("DELETE FROM decent_job_concurrency_claims WHERE job_id = $1 AND scope <> $2", [context.fetch(:id), scope])
      upsert(connection, scope, context, "waiting")
    end

    def release(connection, context)
      rows = connection.exec_params("DELETE FROM decent_job_concurrency_claims WHERE job_id = $1 AND execution_id = $2 AND state = 'granted' RETURNING scope", [context.fetch(:id), context.fetch(:execution_id)])
      rows.each { |row| promote(connection, row.fetch("scope")) }
    end

    def finish(context)
      synchronize(context.fetch(:id)) do |connection|
        yield connection
        release(connection, context)
        # Lock before filtering so an in-flight promotion cannot be missed.
        rows = connection.exec_params("SELECT * FROM decent_job_concurrency_claims WHERE job_id = $1 FOR UPDATE", [context.fetch(:id)])
        state = connection.exec_params("SELECT state FROM decent_jobs WHERE id = $1", [context.fetch(:id)]).getvalue(0, 0)
        if %w[succeeded discarded].include?(state)
          # The finisher has verified execution ownership. Completed jobs can
          # also retain waiting claims from an earlier attempt's rule set.
          connection.exec_params("DELETE FROM decent_job_concurrency_claims WHERE job_id = $1", [context.fetch(:id)])
        elsif rows.any? { |row| row["state"] == "promotion_pending" && row["execution_id"] == context.fetch(:execution_id) }
          schedule(connection, context.fetch(:id))
          connection.exec_params("UPDATE decent_job_concurrency_claims SET state = 'promoted', updated_at = clock_timestamp() WHERE job_id = $1 AND execution_id = $2 AND state = 'promotion_pending'", [context.fetch(:id), context.fetch(:execution_id)])
        end
      end
    end

    # Called even on idle polls, so a crash does not leave waiters sleeping until
    # their retry delay expires. Recheck ownership after acquiring the scope lock.
    def recover
      @database.with_connection do |connection|
        scopes = connection.exec(<<~SQL).column_values(0)
          SELECT DISTINCT scope FROM decent_job_concurrency_claims
          JOIN decent_jobs ON decent_jobs.id = job_id
          WHERE decent_jobs.state IN ('succeeded', 'discarded')
            OR (decent_job_concurrency_claims.state IN ('granted', 'promotion_pending')
              AND NOT (#{JOB_LOCKED_SQL}))
        SQL
        scopes.sort.each do |scope|
          @database.transaction do
            lock(connection, scope)
            rows = connection.exec_params(<<~SQL, [scope])
              DELETE FROM decent_job_concurrency_claims USING decent_jobs
              WHERE job_id = decent_jobs.id AND scope = $1 AND decent_job_concurrency_claims.state = 'granted'
                AND (decent_jobs.state IN ('succeeded', 'discarded') OR NOT (#{JOB_LOCKED_SQL}))
              RETURNING job_id
            SQL
            rows.ntuples.times { promote(connection, scope) }
            pending = connection.exec_params(<<~SQL, [scope])
              SELECT job_id FROM decent_job_concurrency_claims
              JOIN decent_jobs ON decent_jobs.id = job_id
              WHERE scope = $1 AND decent_job_concurrency_claims.state = 'promotion_pending'
                AND decent_jobs.state = 'ready' AND NOT (#{JOB_LOCKED_SQL})
              FOR UPDATE OF decent_job_concurrency_claims
            SQL
            pending.each do |row|
              schedule(connection, row.fetch("job_id"))
              connection.exec_params("UPDATE decent_job_concurrency_claims SET state = 'promoted', updated_at = clock_timestamp() WHERE scope = $1 AND job_id = $2", [scope, row.fetch("job_id")])
            end
            connection.exec_params(<<~SQL, [scope])
              DELETE FROM decent_job_concurrency_claims USING decent_jobs
              WHERE job_id = decent_jobs.id AND scope = $1 AND decent_jobs.state IN ('succeeded', 'discarded')
            SQL
          end
        end
      end
    end

    private

    def lock(connection, scope)
      connection.exec_params("SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || '.decent_job.concurrency.' || $1, 0))", [scope])
    end

    def upsert(connection, scope, context, state)
      connection.exec_params(<<~SQL, [scope, context.fetch(:id), context.fetch(:execution_id), state])
        INSERT INTO decent_job_concurrency_claims (scope, job_id, execution_id, state) VALUES ($1, $2, $3, $4)
        ON CONFLICT (scope, job_id) DO UPDATE SET execution_id = EXCLUDED.execution_id,
          state = EXCLUDED.state, updated_at = clock_timestamp()
      SQL
    end

    def promote(connection, scope)
      loop do
        row = connection.exec_params("SELECT job_id FROM decent_job_concurrency_claims WHERE scope = $1 AND state = 'waiting' ORDER BY created_at, job_id LIMIT 1 FOR UPDATE", [scope]).first
        return unless row
        id = row.fetch("job_id")
        state = connection.exec_params("SELECT state FROM decent_jobs WHERE id = $1 FOR UPDATE", [id]).first&.fetch("state")
        if !state || %w[succeeded discarded].include?(state)
          connection.exec_params("DELETE FROM decent_job_concurrency_claims WHERE scope = $1 AND job_id = $2", [scope, id])
          next
        end
        promoted = state == "ready"
        schedule(connection, id) if promoted
        connection.exec_params("UPDATE decent_job_concurrency_claims SET state = $3, updated_at = clock_timestamp() WHERE scope = $1 AND job_id = $2", [scope, id, promoted ? "promoted" : "promotion_pending"])
        return
      end
    end

    def schedule(connection, id)
      connection.exec_params("UPDATE decent_jobs SET scheduled_at = clock_timestamp(), updated_at = clock_timestamp() WHERE id = $1 AND state = 'ready'", [id])
      Store.new(@database).notify(connection)
    end
  end
end
