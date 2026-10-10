# frozen_string_literal: true

require "json"
require "securerandom"

module DecentJob
  # Claims one runnable job, starts its attempt, and holds the claim until the
  # attempt ends.
  #
  # {Performer#perform_one} calls {#with_next} once for each attempt. {Worker}
  # also creates a short-lived instance to call {#next_scheduled_at} when it
  # refreshes its deadline cache. An instance has no per-thread state, so
  # threads can share it. The claim state lives in local variables, in the
  # caller's pool checkout, and in the decent_jobs row.
  #
  # Each call to {#with_next} reads {Configuration#lock_strategy} again:
  #
  # - +:advisory+ tries a session advisory lock on each row of a bounded
  #   candidate list, then rechecks the row with FOR UPDATE. The calling
  #   context keeps one pool checkout for the whole attempt, because a session
  #   advisory lock belongs to the PostgreSQL session.
  # - +:skiplocked+ selects and starts one row in a single transaction with
  #   FOR NO KEY UPDATE SKIP LOCKED. Ownership is persisted in locked_by_id and
  #   lock_type = 'skiplocked'. No connection is held while the job runs, so
  #   this strategy works behind PgBouncer transaction pooling.
  # - +:hybrid+ selects one row with SKIP LOCKED, then takes the session
  #   advisory lock in the same transaction. It also keeps its checkout for the
  #   whole attempt.
  #
  # Every strategy writes locked_by_id (the process UUID from
  # {ProcessTracker#register}), locked_at, and, after migration 011, lock_type.
  # Before migration 011 every strategy falls back to +:advisory+.
  #
  # Invariants:
  #
  # - The job advisory key is hashtextextended(current_schema() ||
  #   '.decent_jobs.' || id, 0). It must match
  #   {ConcurrencyClaims::JOB_KEY_SQL}.
  # - An advisory or hybrid lock is held from claim until the attempt block
  #   returns. It is released with pg_advisory_unlock, or by closing the
  #   session when the unlock is not safe.
  # - If a query that can take an advisory lock is interrupted before its
  #   result is read, the lock state is unknown. The session must then be
  #   closed. It must never go back to the pool with a lock that nobody owns.
  # - Persisted ownership is cleared only when id, execution_id, and
  #   locked_by_id all still match. A stale cleanup can never clear the
  #   ownership of a newer attempt.
  #
  # Interrupt handling uses Thread.handle_interrupt in three modes:
  #
  # - +:on_blocking+ around lock bookkeeping. Thread#raise and Thread#kill
  #   land only inside blocking calls such as a PG query, never between a
  #   query result and the local variable that records it.
  # - +:immediate+ around the job body, so shutdown can interrupt it.
  # - +:never+ around ownership release, so a second interrupt cannot stop
  #   the cleanup.
  #
  # When the thread is killed (Thread#status is "aborting"), the code does not
  # send more SQL on the claim connection. {Database#with_connection} then
  # cancels any running query and closes the session, which drops its session
  # advisory locks.
  #
  # See docs/locking.md, docs/execution-recovery.md, and docs/pgbouncer.md.
  class Claim
    # @param database [Database] the pool that claims, starts, and releases jobs
    # @param enable_pauses [Boolean] true to exclude jobs that match a row in
    #   decent_job_pauses (see {Pauses::EXCLUDED_SQL})
    # @param process_tracker [ProcessTracker, nil] the tracker that supplies the
    #   process UUID stored in locked_by_id, or nil to create one for database
    def initialize(database, enable_pauses: false, process_tracker: nil)
      @process_tracker = process_tracker || ProcessTracker.new(database: database)
      @database = database
      @store = Store.new(database)
      @enable_pauses = enable_pauses
    end

    # Claims the next runnable job, starts an attempt, and yields the job row.
    #
    # Eligible jobs are in state 'ready' or 'running', have no finished_at, are
    # due (scheduled_at <= clock_timestamp()), match the queue selection, and
    # are not paused when pauses are enabled. A 'running' job is eligible so
    # that a replacement can claim an attempt whose owner lost its lock. The
    # replacement marks the previous execution 'interrupted'.
    #
    # @param queues [String, QueueSelection] queue selection, "*" for all queues
    # @param job_id [String, nil] claim only this job id, or any job when nil
    # @param stopping [#call] returns true when the caller must stop claiming
    # @yieldparam row [Hash{String => Object}] the decoded decent_jobs row after
    #   the start update. It also has "execution_attributes" (the decoded
    #   decent_job_executions row of this attempt) and "execution_interrupted"
    #   (the start Time of the attempt that this claim interrupted, or nil).
    # @return [Object, nil] the block result, or nil when no job was claimed
    # @note Each call runs one query to detect the lock_type column, so a
    #   running worker picks up migration 011 on its next claim.
    # @note The ensure block clears locked_by_id, locked_at, and lock_type for
    #   this execution, also when the block raises or the thread is killed.
    #   Normal completion in {Performer} has already cleared them, so this
    #   update then matches no row. For SKIP LOCKED claims this cleanup makes
    #   an abandoned attempt claimable again without process cleanup.
    def with_next(queues: "*", job_id: nil, stopping: -> { false })
      strategy = DecentJob.configuration.lock_strategy
      migrated = @database.with_connection { |connection| ConcurrencyClaims.lock_type_column?(connection) }
      strategy = :advisory unless migrated
      claimed = nil
      original_error = nil
      # #start calls capture inside its transaction, before the block runs.
      # The ensure below then knows which execution to release, even when an
      # interrupt lands before the strategy method returns.
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

    # Advisory strategy. Holds one checkout for the candidate loop and the
    # whole attempt. Tries each candidate in order and stops at the first job
    # that it locks and starts. #start rechecks the row with FOR UPDATE, so it
    # waits for row locks held by management actions and skips a job that
    # another strategy owns. If #start returns nil, the ensure unlocks the key
    # and the loop tries the next candidate.
    def with_advisory(queues:, job_id:, stopping:, migrated:, capture:)
      selection = queues.is_a?(QueueSelection) ? queues : QueueSelection.new(queues)

      @database.with_connection do |connection|
        candidates(connection, selection, job_id).each do |candidate|
          return nil if stopping.call
          key = candidate.fetch("lock_key")

          # A session lock is reentrant. Skip keys already owned by this
          # checkout so nested performers cannot interrupt an active attempt.
          # A nested with_next in the same context reuses this checkout, so
          # pg_try_advisory_lock alone would succeed and start a second attempt.
          #
          # ownership_known turns true only after the lock query returns. If an
          # interrupt lands inside that query, the server can hold the lock
          # while this code cannot know it, so the ensure closes the session.
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
              # A killed thread sends no more SQL here. Database#with_connection
              # cancels and closes this session, which drops the lock. A bad
              # connection status is left to checkin, which closes it too.
              unless Thread.current.status == "aborting" || connection.finished?
                if !ownership_known
                  # An interrupted query may have acquired the lock without
                  # returning its result. Never return that session to the pool.
                  connection.finish
                elsif locked && connection.status == PG::CONNECTION_OK
                  # If the unlock fails, close the session so the lock cannot
                  # leak into the pool. Raise the unlock error only when it is
                  # the first error, so it never hides the original one.
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

    # Returns scheduled_at times of ready jobs for the {Worker} deadline cache.
    #
    # The result has up to now_limit due jobs that no worker owns, plus up to
    # limit future jobs, sorted by scheduled_at. It returns times only, never
    # job rows. Jobs in state 'running' are not included, so recovery of an
    # abandoned attempt depends on polling or a wakeup, not on this cache.
    #
    # @param queues [String, QueueSelection] queue selection, "*" for all queues
    # @param limit [Integer] maximum number of future deadlines
    # @param now_limit [Integer] maximum number of due, unowned jobs, usually the
    #   pool thread count
    # @return [Array<Time>] scheduled times in ascending order
    # @raise [ArgumentError] when limit or now_limit is not a nonnegative Integer
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
            ORDER BY scheduled_at, id LIMIT $3)
          UNION ALL
          (#{scope} AND scheduled_at > statement_timestamp() ORDER BY scheduled_at, id LIMIT $2)
          ORDER BY scheduled_at
        SQL
      end
    end

    private

    # SKIP LOCKED and hybrid strategies. Both register the process first,
    # because #start writes the process UUID to locked_by_id. Both select one
    # row and do not try a second candidate.
    #
    # Hybrid: the row lock and pg_try_advisory_lock happen in one transaction.
    # The session advisory lock survives COMMIT and ROLLBACK, so the ensure
    # must unlock it. key is set before the lock query, so an interrupt inside
    # that query leaves key set and ownership_known false, and the ensure
    # closes the session. If the advisory lock is taken by another session,
    # key is reset to nil and the call claims nothing.
    #
    # SKIP LOCKED: the transaction commits before the yield, so the job body
    # runs without a checkout. Ownership is only the persisted row state, which
    # #with_next clears in its ensure.
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

    # Clears the persisted claim of this execution. Runs with interrupts
    # deferred. A killed thread, or a failed pooled update, hands the work to
    # #release_after_checkout, which uses a fresh connection after the outer
    # checkout ends. The final ensure covers an unexpected error while the
    # thread is aborting.
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

    # Runs release on a new unpooled connection from Database#connect, so it
    # does not need a free pool slot. It runs at once when this context has no
    # checkout. Errors are raised only when there is no original error and the
    # thread is not aborting.
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

    # Selects eligible job ids and their advisory keys in dequeue order.
    #
    # Advisory mode (row_lock false) reads up to queue_select_limit rows with
    # no row lock and leaves owner checks to the advisory try-lock and #start.
    # Row mode reads one row with FOR NO KEY UPDATE SKIP LOCKED and excludes
    # rows that any strategy owns. NO KEY UPDATE does not block the KEY SHARE
    # locks that foreign keys take when other transactions insert rows that
    # reference the job. "$1::text[] IS NOT NULL" references $1 in every
    # filter form, so PostgreSQL can infer its type.
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

    # Rechecks eligibility, then creates the attempt in one transaction (a
    # savepoint when the caller is in a transaction). Advisory mode uses FOR
    # UPDATE and waits for other row locks. Row modes already hold the row
    # lock in the outer transaction, so SKIP LOCKED still returns the row.
    # Returns the started row, or nil when the job is no longer eligible.
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

    # Marks the unfinished previous execution of a reclaimed job as
    # 'interrupted' and returns its start Time. Returns nil when there is no
    # previous running execution. This is the only place that records an
    # abandoned attempt; there is no separate recovery sweep.
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
