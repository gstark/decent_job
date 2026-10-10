# frozen_string_literal: true

module DecentJob
  # All claim mutations take sorted scope locks before touching job rows.
  #
  # ConcurrencyClaims owns the decent_job_concurrency_claims table. A row
  # links one job to one perform-time scope ("key:<value>", "label:<value>",
  # or "all"). Its state is one of:
  #
  # - 'granted': the attempt in execution_id holds a slot in the scope.
  # - 'waiting': the job was over the limit and waits for a slot. created_at
  #   sets the queue order and is kept on update.
  # - 'promotion_pending': a slot opened, but the waiting job was still
  #   executing. {#finish} or {#recover} schedules it later.
  # - 'promoted': the job was made due (scheduled_at = now) and NOTIFY was sent.
  #   This does not reserve a slot. The next perform check decides again.
  #
  # {PerformControls} grants and waits. {Performer} calls {#finish}.
  # {Job} management actions and {Cleaner} call {#synchronize} and
  # {#release_all}. Process cleanup ({ProcessRecord#cleanup}) calls
  # {#synchronize} and {#release}.
  #
  # Lock order: the scope locks first, in sorted order, then the decent_jobs
  # row. A scope lock is pg_advisory_xact_lock on hashtextextended(
  # current_schema() || '.decent_job.concurrency.' || scope, 0), the same key
  # that {ConcurrencyControls} uses. Transaction-scoped locks release at
  # commit or rollback, so they also work behind PgBouncer transaction
  # pooling. Taking a process row lock before scope locks can deadlock.
  #
  # The SQL constants detect whether a job attempt is executing under any lock
  # strategy. They reference the table by its name, decent_jobs. Interpolate
  # them only into queries where that name refers to the jobs table.
  #
  # See docs/locking.md.
  class ConcurrencyClaims
    # Raised inside {#synchronize} to roll back and retry with a larger scope set.
    class ScopeChanged < StandardError; end
    private_constant :ScopeChanged

    # The bigint advisory lock key of a job. It must match the lock_key that
    # {Claim} selects.
    JOB_KEY_SQL = "hashtextextended(current_schema() || '.decent_jobs.' || decent_jobs.id::text, 0)"
    # True when any session in the current database holds the job advisory
    # lock, including the current session. pg_locks splits a bigint key into
    # classid (high 32 bits) and objid (low 32 bits) with objsubid = 1.
    JOB_LOCKED_SQL = <<~SQL
      EXISTS (
        SELECT 1 FROM pg_locks
        WHERE locktype = 'advisory' AND granted AND objsubid = 1
          AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
          AND classid = ((#{JOB_KEY_SQL} >> 32) & 4294967295)::oid
          AND objid = (#{JOB_KEY_SQL} & 4294967295)::oid
      )
    SQL

    # Skiplocked ownership survives the claiming transaction. Advisory and
    # hybrid ownership ends with the session, even if its process still lives.
    # to_jsonb reads lock_type without naming the column, so the expression
    # also works before migration 011 adds it.
    PERSISTED_LOCKED_SQL = "(decent_jobs.locked_by_id IS NOT NULL AND COALESCE(to_jsonb(decent_jobs)->>'lock_type' = 'skiplocked', false))"
    # True when an attempt owns the job under any strategy.
    EXECUTION_LOCKED_SQL = "((#{JOB_LOCKED_SQL}) OR #{PERSISTED_LOCKED_SQL})"

    # Detects migration 011.
    #
    # @param connection [PG::Connection]
    # @return [Boolean] true when decent_jobs has the lock_type column
    # @api private
    def self.lock_type_column?(connection)
      connection.exec("SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'decent_jobs'::regclass AND attname = 'lock_type' AND NOT attisdropped)").getvalue(0, 0) == "t"
    end

    # @param database [Database] the pool for all queries
    def initialize(database)
      @database = database
    end

    # Locks a job and every scope it has a claim in, then yields in that
    # transaction.
    #
    # Locks the union of scopes and the job's existing claim scopes in sorted
    # order, then locks the decent_jobs row with FOR UPDATE. If a claim in a
    # new scope committed during the wait, it rolls back and starts again.
    #
    # @param job_id [String] the decent_jobs id
    # @param scopes [Array<String>] extra scopes to lock
    # @param wait [Boolean] false uses pg_try_advisory_xact_lock and NOWAIT
    # @yieldparam connection [PG::Connection] the connection of the transaction
    # @return [Object] the block result
    # @raise [PG::LockNotAvailable] when wait is false and a lock is held
    # @note The block can run more than once when the scope set changes, but
    #   only after a rollback, so its writes from an earlier run are undone.
    #   Side effects outside the database are not undone.
    def synchronize(job_id, scopes: [], wait: true)
      @database.transaction do |connection|
        existing = connection.exec_params("SELECT scope FROM decent_job_concurrency_claims WHERE job_id = $1", [job_id]).column_values(0)
        locked_scopes = (scopes + existing).uniq.sort
        locked_scopes.each { |scope| lock(connection, scope, wait: wait) }
        connection.exec_params("SELECT id FROM decent_jobs WHERE id = $1 FOR UPDATE #{"NOWAIT" unless wait}", [job_id])
        current_scopes = connection.exec_params("SELECT scope FROM decent_job_concurrency_claims WHERE job_id = $1", [job_id]).column_values(0)
        # A claim may have committed while we waited for the job row. Release
        # this savepoint's locks and reacquire the complete scope set in order.
        raise ScopeChanged unless (current_scopes - locked_scopes).empty?
        yield connection
      end
    rescue ScopeChanged
      retry
    end

    # Records that the attempt holds a slot in scope.
    #
    # @param connection [PG::Connection] a connection inside {#synchronize}
    # @param scope [String]
    # @param context [Hash] execution context with :id and :execution_id
    # @return [void]
    def grant(connection, scope, context)
      upsert(connection, scope, context, "granted")
    end

    # Makes the job wait for a slot in scope.
    #
    # Releases the attempt's granted claims (promoting their waiters) and
    # deletes the job's claims in other scopes. A job waits in at most one
    # scope.
    #
    # @param connection [PG::Connection] a connection inside {#synchronize}
    # @param scope [String]
    # @param context [Hash] execution context with :id and :execution_id
    # @return [void]
    def wait(connection, scope, context)
      release(connection, context)
      connection.exec_params("DELETE FROM decent_job_concurrency_claims WHERE job_id = $1 AND scope <> $2", [context.fetch(:id), scope])
      upsert(connection, scope, context, "waiting")
    end

    # Deletes the attempt's granted claims and promotes one waiter in each
    # released scope.
    #
    # @param connection [PG::Connection] a connection inside {#synchronize}
    # @param context [Hash] execution context with :id and :execution_id
    # @return [void]
    # @note Promotion updates scheduled_at and sends NOTIFY.
    def release(connection, context)
      rows = connection.exec_params("DELETE FROM decent_job_concurrency_claims WHERE job_id = $1 AND execution_id = $2 AND state = 'granted' RETURNING scope", [context.fetch(:id), context.fetch(:execution_id)])
      rows.each { |row| promote(connection, row.fetch("scope")) }
    end

    # Deletes every claim of the job, of any attempt and state, and promotes a
    # waiter in each scope where the job had a granted claim.
    #
    # @param connection [PG::Connection] a connection inside {#synchronize}
    # @param id [String] the decent_jobs id
    # @return [void]
    # @note Used when a job is discarded or destroyed, and by {Cleaner}.
    def release_all(connection, id)
      rows = connection.exec_params("DELETE FROM decent_job_concurrency_claims WHERE job_id = $1 RETURNING scope, state", [id])
      rows.each { |row| promote(connection, row.fetch("scope")) if row["state"] == "granted" }
    end

    # Finishes an attempt and its claims in one transaction.
    #
    # Yields so {Performer} can write the outcome, then releases the attempt's
    # granted claims. A succeeded or discarded job loses all its claims. A job
    # with a 'promotion_pending' claim from this attempt is scheduled now and
    # marked 'promoted'.
    #
    # @param context [Hash] execution context with :id and :execution_id
    # @param after_release [#call] called with the connection after the claim
    #   updates, in the same transaction
    # @yieldparam connection [PG::Connection] the connection of the transaction
    # @return [Object] the result of after_release
    # @note The block can run more than once; see {#synchronize}.
    def finish(context, after_release: ->(_connection) {})
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

        # Terminal cleanup must follow release and its state read, and commit
        # atomically with finishing the attempt and promoting its waiters.
        after_release.call(connection)
      end
    end

    # Called even on idle polls, so a crash does not leave waiters sleeping until
    # their retry delay expires. Recheck ownership after acquiring the scope lock.
    #
    # Repairs claims left by finished jobs and by attempts that lost their
    # owner. Each scope is handled in its own transaction, so recovery holds
    # at most one scope lock at a time.
    #
    # @return [void]
    # @note {Performer#perform_one} calls this before every claim. It scans
    #   pg_locks and can update scheduled_at and send NOTIFY.
    def recover
      @database.with_connection do |connection|
        scopes = connection.exec(<<~SQL).column_values(0)
          SELECT DISTINCT scope FROM decent_job_concurrency_claims
          JOIN decent_jobs ON decent_jobs.id = job_id
          WHERE decent_jobs.state IN ('succeeded', 'discarded')
            OR (decent_job_concurrency_claims.state IN ('granted', 'promotion_pending')
              AND NOT (#{EXECUTION_LOCKED_SQL}))
        SQL

        scopes.sort.each do |scope|
          @database.transaction do
            lock(connection, scope)
            rows = connection.exec_params(<<~SQL, [scope])
              DELETE FROM decent_job_concurrency_claims USING decent_jobs
              WHERE job_id = decent_jobs.id AND scope = $1 AND decent_job_concurrency_claims.state = 'granted'
                AND (decent_jobs.state IN ('succeeded', 'discarded') OR NOT (#{EXECUTION_LOCKED_SQL}))
              RETURNING job_id
            SQL
            rows.ntuples.times { promote(connection, scope) }
            pending = connection.exec_params(<<~SQL, [scope])
              SELECT job_id FROM decent_job_concurrency_claims
              JOIN decent_jobs ON decent_jobs.id = job_id
              WHERE scope = $1 AND decent_job_concurrency_claims.state = 'promotion_pending'
                AND decent_jobs.state = 'ready' AND NOT (#{EXECUTION_LOCKED_SQL})
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

    # Takes the transaction-scoped scope lock. Outside a transaction the lock
    # releases at once, so callers must be inside one.
    def lock(connection, scope, wait: true)
      function = wait ? "pg_advisory_xact_lock" : "pg_try_advisory_xact_lock"
      result = connection.exec_params("SELECT #{function}(hashtextextended(current_schema() || '.decent_job.concurrency.' || $1, 0))", [scope])
      raise PG::LockNotAvailable, "Concurrency scope is locked" if !wait && result.getvalue(0, 0) != "t"
    end

    def upsert(connection, scope, context, state)
      connection.exec_params(<<~SQL, [scope, context.fetch(:id), context.fetch(:execution_id), state])
        INSERT INTO decent_job_concurrency_claims (scope, job_id, execution_id, state) VALUES ($1, $2, $3, $4)
        ON CONFLICT (scope, job_id) DO UPDATE SET execution_id = EXCLUDED.execution_id,
          state = EXCLUDED.state, updated_at = clock_timestamp()
      SQL
    end

    # Wakes the oldest waiter in scope. Deletes waiters whose job is gone or
    # finished and tries the next one. A ready, unowned job is scheduled now;
    # a job that still executes becomes 'promotion_pending'.
    def promote(connection, scope)
      loop do
        row = connection.exec_params("SELECT job_id FROM decent_job_concurrency_claims WHERE scope = $1 AND state = 'waiting' ORDER BY created_at, job_id LIMIT 1 FOR UPDATE", [scope]).first
        return unless row
        id = row.fetch("job_id")
        job = connection.exec_params("SELECT state, #{EXECUTION_LOCKED_SQL} AS locked FROM decent_jobs WHERE id = $1 FOR UPDATE", [id]).first
        state = job&.fetch("state")

        if !state || %w[succeeded discarded].include?(state)
          connection.exec_params("DELETE FROM decent_job_concurrency_claims WHERE scope = $1 AND job_id = $2", [scope, id])
          next
        end

        promoted = state == "ready" && job.fetch("locked") != "t"
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
