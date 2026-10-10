# frozen_string_literal: true

require "json"
require "time"

module DecentJob
  # A read-only snapshot of one decent_job_processes row, plus cleanup of
  # inactive process records.
  #
  # {ProcessTracker} writes the rows. This class reads them and deletes rows
  # whose process is no longer active. {Worker} calls {.cleanup}
  # periodically on its execution threads.
  #
  # Liveness: a row with heartbeat_mode 'advisory' is active while some
  # session holds the advisory lock keyed by KEY_SQL. A row with
  # heartbeat_mode 'heartbeat' is active while updated_at is less than 300
  # seconds old. KEY_SQL must match the key {ProcessTracker} locks.
  #
  # decent_jobs.locked_by_id and decent_job_executions.process_uuid
  # deliberately have no foreign key to this table, so attempt history keeps
  # the process UUID after the row is deleted.
  #
  # Lock order: never take a process row lock before concurrency scope locks.
  # {#cleanup} takes the {ConcurrencyClaims} scope locks first and checks the
  # owner and session-lock predicates again after it holds all locks.
  #
  # See docs/process-tracking.md. Schema: db/migrations/009_add_processes.sql.
  class ProcessRecord
    # Seconds after the last heartbeat before {#stale?} is true.
    STALE_INTERVAL = 30
    # Seconds after the last heartbeat before {#expired?} is true. ACTIVE_SQL
    # uses the same 300 second limit.
    EXPIRED_INTERVAL = 300
    # Advisory lock key of a process row, split into classid and objid in LOCKED_SQL.
    KEY_SQL = "hashtextextended(current_schema() || '.decent_job_processes.' || decent_job_processes.id::text, 0)"
    LOCKED_SQL = <<~SQL
      EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND granted AND objsubid = 1
        AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
        AND classid = ((#{KEY_SQL} >> 32) & 4294967295)::oid
        AND objid = (#{KEY_SQL} & 4294967295)::oid)
    SQL
    ACTIVE_SQL = "((heartbeat_mode = 'advisory' AND (#{LOCKED_SQL})) OR (heartbeat_mode = 'heartbeat' AND updated_at > clock_timestamp() - interval '300 seconds'))"
    # Every query that builds a record must start with this. {#initialize}
    # reads the database_now column, which decent_job_processes does not have.
    SELECT_SQL = "SELECT *, clock_timestamp() AS database_now FROM decent_job_processes"
    private_constant :SELECT_SQL

    # @return [String] the process UUID
    # @return [Hash{String => Object}] the parsed state JSON (string keys)
    # @return [String] "advisory" or "heartbeat"
    # @return [Time] row creation time
    # @return [Time] time of the last heartbeat write
    attr_reader :id, :state, :heartbeat_mode, :created_at, :updated_at

    # Loads one process record.
    #
    # @param id [String] the process UUID
    # @param database [Database, nil] the database, or nil for the current
    #   execution's database, then {DecentJob.database}
    # @return [ProcessRecord]
    # @raise [KeyError] when no row has this id
    def self.find(id, database: nil)
      records(database, "id = $1", [id]).first || raise(KeyError, "Process #{id} was not found")
    end

    # @param database [Database, nil] see {.find}
    # @return [Array<ProcessRecord>] all rows, ordered by created_at and id
    def self.all(database: nil)
      records(database, "TRUE")
    end

    # @param database [Database, nil] see {.find}
    # @return [Array<ProcessRecord>] rows whose process is active (see class header)
    def self.active(database: nil)
      records(database, ACTIVE_SQL)
    end

    # @param database [Database, nil] see {.find}
    # @return [Array<ProcessRecord>] rows whose process is not active
    def self.inactive(database: nil)
      records(database, "NOT (#{ACTIVE_SQL})")
    end

    # Deletes inactive process records and releases what they still own.
    #
    # Scans inactive rows in pages ordered by id. For each row it runs
    # {#cleanup}. Rows that are live or contended at lock time are skipped and
    # can be retried in a later sweep.
    #
    # @param database [Database, nil] see {.find}
    # @param in_batches_of [Integer] page size for process and owned-job scans
    # @param stopping [#call, nil] returns true to stop between records or
    #   owned jobs
    # @return [Integer] the number of deleted process rows
    # @raise [ArgumentError] when in_batches_of is not a positive Integer or
    #   stopping is not callable
    # @note Writes SQL: clears decent_jobs.locked_by_id and locked_at,
    #   releases granted concurrency claims (which can promote waiting jobs),
    #   and deletes decent_job_processes rows. Keeps jobs and attempt history.
    def self.cleanup(database: nil, in_batches_of: 1000, stopping: nil)
      raise ArgumentError, "in_batches_of must be a positive integer" unless in_batches_of.is_a?(Integer) && in_batches_of.positive?
      raise ArgumentError, "stopping must be callable" unless stopping.nil? || stopping.respond_to?(:call)

      database = resolve_database(database)
      deleted = 0
      cursor = nil

      until stopping&.call
        rows = database.with_connection do |connection|
          connection.exec_params(<<~SQL, [cursor, in_batches_of]).to_a
            #{SELECT_SQL} WHERE NOT (#{ACTIVE_SQL})
              AND ($1::uuid IS NULL OR id > $1::uuid) ORDER BY id LIMIT $2
          SQL
        end

        break if rows.empty?

        rows.each do |row|
          return deleted if stopping&.call
          record = new(row)
          deleted += 1 if record.cleanup(database, in_batches_of: in_batches_of, stopping: stopping)
        end

        cursor = rows.last.fetch("id")
        break if rows.size < in_batches_of
      end

      deleted
    end

    def self.resolve_database(database)
      database || Current.execution&.fetch(:database, nil) || DecentJob.database
    end
    private_class_method :resolve_database

    def self.records(database, predicate, parameters = [])
      resolve_database(database).with_connection do |connection|
        connection.exec_params("#{SELECT_SQL} WHERE #{predicate} ORDER BY created_at, id", parameters).map { |row| new(row) }
      end
    end
    private_class_method :records

    # @param row [Hash{String => String}] a decent_job_processes row from pg,
    #   selected with {SELECT_SQL} so it has the database_now column
    # @api private
    def initialize(row)
      @id = row.fetch("id").dup.freeze
      @state = JSON.parse(row.fetch("state"))
      @heartbeat_mode = row.fetch("heartbeat_mode")
      @created_at = Time.parse(row.fetch("created_at"))
      @updated_at = Time.parse(row.fetch("updated_at"))
      # Heartbeat age uses the database clock, like ACTIVE_SQL. The offset
      # keeps that true as local time moves on after the load.
      @clock_offset = Time.parse(row.fetch("database_now")) - Time.now
    end

    # @return [Boolean] true when the last heartbeat is at least 30 seconds
    #   old. An advisory-locked process can be stale and still active.
    # @note Uses the database clock, read when the record loaded.
    def stale?
      updated_at <= database_now - STALE_INTERVAL
    end

    # @return [Boolean] true when the last heartbeat is at least 300 seconds old
    # @note Uses the database clock, read when the record loaded.
    def expired?
      updated_at <= database_now - EXPIRED_INTERVAL
    end

    private def database_now
      Time.now + @clock_offset
    end

    # @return [String] the basename of the recorded process title, or ""
    def basename
      File.basename(state.fetch("proctitle", ""))
    end

    # @return [Array<Hash>] per-pool scheduler statistics from the last
    #   heartbeat, or an empty array
    def schedulers
      state.fetch("schedulers", [])
    end

    # Releases the jobs this inactive process owns, then deletes its row.
    #
    # Each owned job is released in its own short transaction. A job is
    # skipped when its session lock is still held or a lock is contended. The
    # row is deleted only when no decent_jobs row still names it.
    #
    # @param database [Database] the database to clean
    # @param in_batches_of [Integer] page size for the owned-job scan
    # @param stopping [#call, nil] returns true to stop between jobs
    # @return [Boolean] true when the row was deleted. False when stopped,
    #   when the process is active again, when a lock was contended, or when
    #   jobs still name the process.
    # @note Never take a process row lock before concurrency scope locks. The
    #   owner and session-lock predicates are checked again after all locks
    #   are held.
    # @api private
    def cleanup(database, in_batches_of: 1000, stopping: nil)
      claims = ConcurrencyClaims.new(database)
      cursor = nil

      until stopping&.call
        jobs = database.with_connection do |connection|
          connection.exec_params(<<~SQL, [id, cursor, in_batches_of]).column_values(0)
            SELECT id FROM decent_jobs WHERE locked_by_id = $1
              AND ($2::uuid IS NULL OR id > $2::uuid) ORDER BY id LIMIT $3
          SQL
        end

        break if jobs.empty?

        jobs.each do |job_id|
          return false if stopping&.call
          release_job(database, claims, job_id)
        end

        cursor = jobs.last
        break if jobs.size < in_batches_of
      end

      return false if stopping&.call

      without_waiting(database) do |connection|
        next false unless lock_inactive(connection)
        connection.exec_params(<<~SQL, [id]).ntuples.positive?
          DELETE FROM decent_job_processes WHERE id = $1
            AND NOT EXISTS (SELECT 1 FROM decent_jobs WHERE locked_by_id = $1) RETURNING id
        SQL
      end
    end

    private

    def release_job(database, claims, job_id)
      without_waiting(database) do |connection|
        claims.synchronize(job_id, wait: false) do
          next unless lock_inactive(connection)
          row = connection.exec_params(<<~SQL, [job_id, id]).first
            SELECT execution_id FROM decent_jobs WHERE id = $1 AND locked_by_id = $2
              AND NOT (#{ConcurrencyClaims::JOB_LOCKED_SQL})
          SQL
          next unless row
          claims.release(connection, {id: job_id, execution_id: row.fetch("execution_id")}) if row["execution_id"]
          connection.exec_params("UPDATE decent_jobs SET locked_by_id = NULL, locked_at = NULL #{", lock_type = NULL" if ConcurrencyClaims.lock_type_column?(connection)} WHERE id = $1 AND locked_by_id = $2", [job_id, id])
        end
      end
    end

    # Runs the block in a transaction with lock_timeout at most 100 ms. NOWAIT
    # covers explicit row locks; the timeout covers locks taken by claim
    # promotion. The previous value is restored before the block's
    # transaction ends, because this transaction can be a savepoint inside a
    # caller's transaction. A lock failure returns false.
    def without_waiting(database)
      database.transaction do |connection|
        # Promotions can lock a waiting job in addition to the explicit NOWAIT
        # locks. Bound those waits, retaining a caller's stricter timeout.
        timeout = connection.exec("SELECT current_setting('lock_timeout') AS previous, setting::int AS milliseconds FROM pg_settings WHERE name = 'lock_timeout'").first
        previous = timeout.fetch("previous")
        milliseconds = timeout.fetch("milliseconds").to_i
        connection.exec("SET LOCAL lock_timeout = '100ms'") if milliseconds.zero? || milliseconds > 100

        begin
          yield connection
        ensure
          connection.exec_params("SELECT set_config('lock_timeout', $1, true)", [previous]) unless connection.transaction_status == PG::PQTRANS_INERROR
        end
      end
    rescue PG::LockNotAvailable
      false
    end

    # Locks the process row with NOWAIT, then checks again that it is inactive.
    # The second check closes the race with a process that renewed its
    # heartbeat or advisory lock after the scan.
    def lock_inactive(connection)
      row = connection.exec_params("SELECT id FROM decent_job_processes WHERE id = $1 FOR UPDATE NOWAIT", [id]).first
      row && connection.exec_params("SELECT id FROM decent_job_processes WHERE id = $1 AND NOT (#{ACTIVE_SQL})", [id]).ntuples.positive?
    end
  end
end
