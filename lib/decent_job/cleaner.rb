# frozen_string_literal: true

require "decent_job/notifications"
require "time"

module DecentJob
  # Each candidate is rechecked under the same locks used by job management and
  # batch generation changes. Busy candidates are left for the next sweep.
  class Cleaner
    def initialize(database:)
      @database = database
      @claims = ConcurrencyClaims.new(database)
    end

    def self.validate_options!(older_than:, include_discarded:, in_batches_of: 1000, stopping: nil)
      unless older_than.is_a?(Numeric) && older_than.real? && older_than.finite? && older_than >= 0
        raise ArgumentError, "older_than must be a finite nonnegative number"
      end

      raise ArgumentError, "in_batches_of must be a positive integer" unless in_batches_of.is_a?(Integer) && in_batches_of.positive?
      raise ArgumentError, "include_discarded must be true or false" unless [true, false].include?(include_discarded)
      raise ArgumentError, "stopping must be callable" unless stopping.nil? || stopping.respond_to?(:call)
    end

    def cleanup(older_than: DecentJob.configuration.cleanup_preserved_jobs_before_seconds_ago, in_batches_of: 1000,
      include_discarded: DecentJob.configuration.cleanup_discarded_jobs?, stopping: nil)
      self.class.validate_options!(older_than: older_than, in_batches_of: in_batches_of, include_discarded: include_discarded, stopping: stopping)

      cutoff = Time.now - older_than
      timestamp = cutoff.utc.iso8601(6)
      payload = {older_than: older_than, timestamp: cutoff, destroyed_jobs_count: 0,
                 destroyed_executions_count: 0, destroyed_batches_count: 0, destroyed_records_count: 0}

      Notifications.instrument("cleanup_preserved_jobs.decent_job", payload) do
        pages("decent_jobs", timestamp, in_batches_of, stopping, include_discarded) do |id|
          counts = delete_job(id, timestamp, include_discarded)
          payload[:destroyed_jobs_count] += counts.fetch(:jobs)
          payload[:destroyed_executions_count] += counts.fetch(:executions)
        end

        pages("decent_job_batches", timestamp, in_batches_of, stopping, include_discarded) do |id|
          payload[:destroyed_batches_count] += delete_batch(id, timestamp, include_discarded)
        end

        delete_orphan_updates(in_batches_of, stopping)
      ensure
        payload[:destroyed_records_count] = payload.values_at(:destroyed_jobs_count, :destroyed_executions_count, :destroyed_batches_count).sum
      end

      payload.fetch(:destroyed_records_count)
    end

    private

    def pages(table, cutoff, size, stopping, include_discarded)
      eligibility = if table == "decent_jobs"
        include_discarded ? "state IN ('succeeded', 'discarded')" : "state = 'succeeded'"
      else
        include_discarded ? "TRUE" : "discarded_at IS NULL"
      end

      cursor_time = cursor_id = nil

      until stopping&.call
        rows = @database.with_connection do |connection|
          connection.exec_params(<<~SQL, [cutoff, cursor_time, cursor_id, size]).to_a
            SELECT id, finished_at FROM #{table} WHERE finished_at <= $1::timestamptz AND #{eligibility}
              AND ($2::timestamptz IS NULL OR (finished_at, id) > ($2::timestamptz, $3::uuid))
            ORDER BY finished_at, id LIMIT $4
          SQL
        end

        break if rows.empty?

        rows.each do |row|
          return nil if stopping&.call
          yield row.fetch("id")
        end

        cursor_time, cursor_id = rows.last.values_at("finished_at", "id")
        break if rows.size < size
      end
    end

    def delete_job(id, cutoff, include_discarded)
      result = {jobs: 0, executions: 0}

      completed = without_waiting do |connection|
        row = connection.exec_params("SELECT #{ConcurrencyClaims::JOB_KEY_SQL} AS lock_key, #{ConcurrencyClaims::EXECUTION_LOCKED_SQL} AS locked FROM decent_jobs WHERE id = $1", [id]).first
        next unless row && row.fetch("locked") == "f"
        next unless connection.exec_params("SELECT pg_try_advisory_xact_lock($1::bigint)", [row.fetch("lock_key")]).getvalue(0, 0) == "t"

        @claims.synchronize(id, wait: false) do
          eligible = connection.exec_params(<<~SQL, [id, cutoff, include_discarded]).ntuples.positive?
            SELECT id FROM decent_jobs WHERE id = $1 AND finished_at <= $2::timestamptz
              AND (state = 'succeeded' OR ($3 AND state = 'discarded'))
              AND NOT #{ConcurrencyClaims::PERSISTED_LOCKED_SQL}
          SQL
          next unless eligible
          @claims.release_all(connection, id)
          executions = connection.exec_params("SELECT count(*) FROM decent_job_executions WHERE job_id = $1", [id]).getvalue(0, 0).to_i
          jobs = connection.exec_params("DELETE FROM decent_jobs WHERE id = $1", [id]).cmd_tuples
          result = {jobs: jobs, executions: executions}
        end
      end

      completed ? result : {jobs: 0, executions: 0}
    end

    def delete_batch(id, cutoff, include_discarded)
      count = 0

      completed = without_waiting do |connection|
        locked = connection.exec_params("SELECT pg_try_advisory_xact_lock(hashtextextended(current_schema() || '.decent_job.batch.' || $1, 0))", [id]).getvalue(0, 0) == "t"
        next unless locked
        row = connection.exec_params(<<~SQL, [id, cutoff, include_discarded]).first
          SELECT id FROM decent_job_batches WHERE id = $1 AND finished_at <= $2::timestamptz
            AND ($3 OR discarded_at IS NULL) FOR UPDATE NOWAIT
        SQL
        next unless row
        # Retry of an individual member does not open a new batch generation.
        # Lock all members before deciding that it is safe to detach them.
        jobs = connection.exec_params(<<~SQL, [id])
          SELECT state, finished_at, #{ConcurrencyClaims::EXECUTION_LOCKED_SQL} AS locked FROM decent_jobs
          WHERE batch_id = $1 OR batch_callback_id = $1 ORDER BY id FOR UPDATE NOWAIT
        SQL
        next if jobs.any? { |job| job["finished_at"].nil? || !%w[succeeded discarded].include?(job["state"]) || job["locked"] == "t" }
        next if connection.exec_params("SELECT 1 FROM decent_job_batch_events WHERE batch_id = $1 AND dispatched_at IS NULL LIMIT 1", [id]).ntuples.positive?
        connection.exec_params("DELETE FROM decent_job_batch_updates WHERE batch_id = $1", [id])
        count = connection.exec_params("DELETE FROM decent_job_batches WHERE id = $1", [id]).cmd_tuples
      end

      completed ? count : 0
    end

    def delete_orphan_updates(size, stopping)
      cursor = 0

      until stopping&.call
        ids = @database.with_connection do |connection|
          connection.exec_params(<<~SQL, [cursor, size]).column_values(0).map(&:to_i)
            WITH candidates AS (
              SELECT id FROM decent_job_batch_updates
              WHERE id > $1 AND NOT EXISTS (SELECT 1 FROM decent_job_batches WHERE id = batch_id)
              ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED
            )
            DELETE FROM decent_job_batch_updates USING candidates
            WHERE decent_job_batch_updates.id = candidates.id RETURNING decent_job_batch_updates.id
          SQL
        end

        break if ids.size < size
        cursor = ids.max
      end
    end

    def without_waiting
      @database.transaction do |connection|
        # Try-locks cover our explicit lock ordering. Foreign-key actions and
        # waiter promotion can touch other rows; bound those waits as well.
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

      true
    rescue PG::LockNotAvailable
      false
    end
  end
end
