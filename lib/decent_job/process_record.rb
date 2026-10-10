# frozen_string_literal: true

require "json"
require "time"

module DecentJob
  # Current worker presence. Attempt UUIDs deliberately have no foreign key here.
  class ProcessRecord
    STALE_INTERVAL = 30
    EXPIRED_INTERVAL = 300
    KEY_SQL = "hashtextextended(current_schema() || '.decent_job_processes.' || decent_job_processes.id::text, 0)"
    LOCKED_SQL = <<~SQL
      EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND granted AND objsubid = 1
        AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
        AND classid = ((#{KEY_SQL} >> 32) & 4294967295)::oid
        AND objid = (#{KEY_SQL} & 4294967295)::oid)
    SQL
    ACTIVE_SQL = "((heartbeat_mode = 'advisory' AND (#{LOCKED_SQL})) OR (heartbeat_mode = 'heartbeat' AND updated_at > clock_timestamp() - interval '300 seconds'))"

    attr_reader :id, :state, :heartbeat_mode, :created_at, :updated_at

    def self.find(id, database: nil)
      records(database, "id = $1", [id]).first || raise(KeyError, "Process #{id} was not found")
    end

    def self.all(database: nil)
      records(database, "TRUE")
    end

    def self.active(database: nil)
      records(database, ACTIVE_SQL)
    end

    def self.inactive(database: nil)
      records(database, "NOT (#{ACTIVE_SQL})")
    end

    def self.cleanup(database: nil, in_batches_of: 1000, stopping: nil)
      raise ArgumentError, "in_batches_of must be a positive integer" unless in_batches_of.is_a?(Integer) && in_batches_of.positive?
      raise ArgumentError, "stopping must be callable" unless stopping.nil? || stopping.respond_to?(:call)

      database = resolve_database(database)
      deleted = 0
      cursor = nil

      until stopping&.call
        rows = database.with_connection do |connection|
          connection.exec_params(<<~SQL, [cursor, in_batches_of]).to_a
            SELECT * FROM decent_job_processes WHERE NOT (#{ACTIVE_SQL})
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
        connection.exec_params("SELECT * FROM decent_job_processes WHERE #{predicate} ORDER BY created_at, id", parameters).map { |row| new(row) }
      end
    end
    private_class_method :records

    def initialize(row)
      @id = row.fetch("id").dup.freeze
      @state = JSON.parse(row.fetch("state"))
      @heartbeat_mode = row.fetch("heartbeat_mode")
      @created_at = Time.parse(row.fetch("created_at"))
      @updated_at = Time.parse(row.fetch("updated_at"))
    end

    def stale?
      updated_at <= Time.now - STALE_INTERVAL
    end

    def expired?
      updated_at <= Time.now - EXPIRED_INTERVAL
    end

    def basename
      File.basename(state.fetch("proctitle", ""))
    end

    def schedulers
      state.fetch("schedulers", [])
    end

    # Never take a process row lock before concurrency scope locks. The owner
    # and session-lock predicates are checked again after acquiring all locks.
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

    def lock_inactive(connection)
      row = connection.exec_params("SELECT id FROM decent_job_processes WHERE id = $1 FOR UPDATE NOWAIT", [id]).first
      row && connection.exec_params("SELECT id FROM decent_job_processes WHERE id = $1 AND NOT (#{ACTIVE_SQL})", [id]).ntuples.positive?
    end
  end
end
