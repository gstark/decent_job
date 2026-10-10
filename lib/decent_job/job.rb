# frozen_string_literal: true

module DecentJob
  # A persisted job and its execution history, without an ORM dependency.
  #
  # One Job wraps one decent_jobs row. It is the operator API: status
  # queries, retry, reschedule, discard and destroy. {Job.find} loads one.
  # {Adapter}, {Performer} and {BatchStore} build them with
  # {Job.from_attributes} for instrumentation events and batch queries. The job instance (the {Base} or Active Job object)
  # is a different thing; {#job_instance} builds it from the payload.
  #
  # The attribute readers return a cached snapshot. They do not query the
  # database. Call {#reload} to refresh it. Management actions reload it.
  # {#id} is the provider ID (decent_jobs.id) and never changes.
  #
  # Management actions take the same PostgreSQL advisory lock as workers,
  # then lock the row and check the state again. A held lock raises
  # {LockedError}. A state that does not allow the action raises
  # {StateMismatchError}. A force discard skips the advisory lock and waits
  # for the row lock.
  #
  # A Job is not thread safe. Use one per thread.
  #
  # See docs/job-management.md.
  class Job
    # The persisted state does not allow the action, for example a retry of
    # a job that is not discarded.
    class StateMismatchError < StandardError; end
    # A worker or another management action holds the job's lock.
    class LockedError < StandardError; end
    # {#retry_job} cannot build the job instance, for example because the job
    # class no longer exists. Not the same class as DecentJob::DeserializationError.
    class DeserializationError < StandardError; end
    # {#retry_job} found a job that does not use a DecentJob {Adapter} for
    # this database.
    class AdapterMismatchError < StandardError; end
    # The error stored on a job that an operator discarded. It is never raised.
    class DiscardJobError < StandardError; end

    # The decent_jobs columns that have attribute readers. lock_type has its
    # own reader, because rows from before migration 011 do not have it.
    ATTRIBUTES = %w[id active_job_id queue_name job_class priority payload scheduled_at labels concurrency_keys batch_id batch_callback_id cron_key cron_at state execution_id locked_by_id locked_at lock_type executions_count created_at updated_at performed_at finished_at error error_event].freeze
    attr_reader :database, :id
    (ATTRIBUTES - ["id", "lock_type"]).each { |name| define_method(name) { @attributes.fetch(name) } }

    # Loads a job row.
    #
    # @param id [String] the provider ID (decent_jobs.id)
    # @param database [Database, nil] the pool to use. Nil means the database
    #   of the job that runs in this thread, or {DecentJob.database} outside
    #   execution.
    # @return [Job]
    # @raise [KeyError] when no row has this ID
    def self.find(id, database: nil)
      database ||= Current.execution&.fetch(:database, nil) || DecentJob.database
      new(id, database: database).reload
    end

    # Builds a Job from a decoded row without a query.
    #
    # @param attributes [Hash{String => Object}] a decoded decent_jobs row;
    #   it is deep copied
    # @param database [Database] the pool for later queries
    # @return [Job]
    def self.from_attributes(attributes, database:)
      new(attributes.fetch("id"), database: database, attributes: attributes)
    end

    # @param id [String] the provider ID
    # @param database [Database] the pool for queries
    # @param attributes [Hash, nil] a decoded row, or nil to load it later
    #   with {#reload}. The attribute readers fail until attributes exist.
    def initialize(id, database:, attributes: nil)
      @id = id.to_s.dup.freeze
      @database = database
      @store = Store.new(database)
      @attributes = Support.deep_dup(attributes) if attributes
    end

    # @return [String, nil] "advisory", "skiplocked" or "hybrid" while a
    #   claim holds the job, or nil
    def lock_type
      @attributes["lock_type"]
    end

    # @return [Hash{String => Object}] a deep copy of the cached row
    def attributes
      Support.deep_dup(@attributes)
    end

    # Reads the row again.
    #
    # @return [self]
    # @raise [KeyError] when the row no longer exists
    def reload
      @attributes = @store.find(@id) || raise(KeyError, "Job #{@id} was not found")
      self
    end

    # Queries the attempts of this job.
    #
    # @return [Array<Hash{String => Object}>] decoded decent_job_executions
    #   rows, oldest first
    def executions
      @store.executions(id)
    end

    # The deserialized job object, native or Active Job.
    #
    # The payload goes through a JSON round trip first, so the job instance
    # does not share objects with the cached attributes. Arguments are
    # loaded now, not on first use.
    #
    # @param ignore_deserialization_errors [Boolean] when true, a job whose
    #   arguments do not load is returned with its arguments still
    #   serialized, and a missing or unsupported class returns nil
    # @return [Object, nil] the job instance, or nil (see above)
    # @raise [UnknownJobClassError, ArgumentError, DecentJob::DeserializationError]
    #   when the class or the arguments do not load and errors are not ignored
    def job_instance(ignore_deserialization_errors: false)
      Current.with(database: database) do
        Integrations.deserialize(JSON.parse(JSON.generate(payload))).tap do |job|
          job.send(:deserialize_arguments_if_needed)
        rescue *Integrations.deserialization_errors
          raise unless ignore_deserialization_errors
        end
      end
    rescue NameError, ArgumentError
      raise unless ignore_deserialization_errors
      nil
    end

    # @return [Float, nil] seconds from the due time to the start of the
    #   first attempt (or to finish, or to now when it did not start). Nil
    #   while the job is not due yet.
    def queue_latency
      expected = scheduled_at || created_at
      now = Time.now
      return if expected >= now
      (performed_at || finished_at || now) - expected
    end

    # @return [Float, nil] seconds from performed_at to finished_at, or to
    #   now while it runs. Nil when it never started.
    def runtime_latency
      (finished_at || Time.now) - performed_at if performed_at
    end

    # @return [Float, nil] queue_latency plus runtime_latency, or nil when
    #   one of them is nil
    def total_latency
      queue = queue_latency
      runtime = runtime_latency
      queue + runtime if queue && runtime
    end

    # Returns the status from the cached attributes and one live lock query.
    #
    # @return [Symbol] :succeeded or :discarded when finished, :running when
    #   a claim holds it, :queued when it is due, :retried when it is in the
    #   future and has an error, else :scheduled
    # @note A force-discarded job reports :discarded while its execution can
    #   still run.
    def status
      return state.to_sym if finished?
      return :running if running?
      return :queued if scheduled_at <= Time.now
      error ? :retried : :scheduled
    end

    def finished?
      !finished_at.nil?
    end

    def discarded?
      state == "discarded"
    end

    def succeeded?
      state == "succeeded"
    end

    # Checks the live lock state in the database, not the cached state.
    #
    # @return [Boolean] true when a session advisory lock or persisted SKIP
    #   LOCKED ownership holds the job. An abandoned "running" row whose
    #   worker died returns false.
    def running?
      database.with_connection do |connection|
        connection.exec_params("SELECT (locked_by_id IS NOT NULL OR (#{ConcurrencyClaims::JOB_LOCKED_SQL})) FROM decent_jobs WHERE id = $1", [id]).first&.values&.first == "t"
      end
    end

    def retried?
      status == :retried
    end

    def scheduled?
      status == :scheduled
    end

    def queued?
      status == :queued
    end

    # Retries a discarded job now.
    #
    # It runs the job class's retry_job and enqueue callbacks, then updates
    # the existing row. Both IDs, the attempt history, cron metadata and
    # batch membership stay. Jobs that callbacks enqueue are inserted in the
    # same transaction.
    #
    # @return [Object, false] the accepted job instance, or false when a
    #   callback aborted the enqueue
    # @raise [LockedError] when another process holds the job
    # @raise [StateMismatchError] when the job is not discarded
    # @raise [DeserializationError] when the job class does not exist
    # @raise [AdapterMismatchError] when the job does not use an {Adapter}
    #   for this database
    # @note The SQL writes happen in one transaction. NOTIFY follows the
    #   enqueue notify settings. Inline execution and async wakeups run after
    #   commit.
    def retry_job
      with_job_lock do
        database.transaction do
          reload
          raise StateMismatchError, "Only discarded jobs can be retried" unless discarded?
          prepared = JobRetry.new(self).prepare
          scopes = ConcurrencyControls.new(database).scopes_for(prepared.fetch(:buffer).jobs.reject { |job| job.job_id == active_job_id })
          ConcurrencyClaims.new(database).synchronize(id, scopes: scopes, wait: false) do |connection|
            lock_row(connection)
            raise StateMismatchError, "Only discarded jobs can be retried" unless discarded?
            job = prepared[:job]
            if job
              job.queue_adapter.reschedule(job, prepared.fetch(:context), payload: prepared.fetch(:payload), attributes: @attributes, scheduled_at: job.scheduled_at&.to_f)
              job.queue_adapter.after_enqueue([id])
            end
            isolated_context do
              Bulk.with_buffer(nil) { prepared.fetch(:buffer).enqueue }
            end
            reload
            job || false
          end
        end
      end
    rescue PG::LockNotAvailable
      raise LockedError, "Job #{id} is locked"
    end

    # Moves the scheduled time of an unfinished job. It updates the column
    # and the payload's scheduled_at key.
    #
    # @param time [Time, #to_time] the new due time. Default: now.
    # @return [self]
    # @raise [LockedError] when a worker or another action holds the job
    # @raise [StateMismatchError] when the job has finished
    # @note Sends a broad NOTIFY that ignores the job notify settings.
    def reschedule_job(time = Time.now)
      manage do |connection|
        assert_unfinished!
        timestamp = time.to_time.utc.iso8601(6)
        connection.exec_params(<<~SQL, [id, timestamp, JSON.generate(timestamp)])
          UPDATE decent_jobs SET scheduled_at = $2::timestamptz, payload = jsonb_set(payload, '{scheduled_at}', $3::jsonb),
            updated_at = clock_timestamp() WHERE id = $1
        SQL
        @store.notify(connection)
        reload
      end
    end

    # Discards an unfinished job that no worker holds.
    #
    # @param message [String] the message of the stored {DiscardJobError}
    # @return [self]
    # @raise [LockedError] when a worker or another action holds the job
    # @raise [StateMismatchError] when the job has finished
    # @note Marks the job and its unfinished attempts discarded, releases
    #   its concurrency claims, and requests batch processing. Batch
    #   reconciliation runs after commit.
    def discard_job(message)
      discard(message, force: false)
    end

    # Discards an unfinished job, also when a worker runs it.
    #
    # It waits for the row lock instead of failing. The running Ruby code
    # does not stop; it can see the discard through queue_adapter.stopping?.
    # Its later result cannot overwrite the discard. The stored error gets
    # "force_discarded" => true.
    #
    # @param message [String] the message of the stored {DiscardJobError}
    # @return [self]
    # @raise [StateMismatchError] when the job has finished
    def force_discard_job(message)
      discard(message, force: true)
    end

    # Deletes a finished job. Its attempts and concurrency claims go with it
    # (ON DELETE CASCADE).
    #
    # @return [self]
    # @raise [LockedError] while a force-discarded execution still holds the
    #   job
    # @raise [StateMismatchError] when the job has not finished
    def destroy_job
      manage do |connection|
        raise StateMismatchError, "Only finished jobs can be destroyed" unless finished?
        ConcurrencyClaims.new(database).release_all(connection, id)
        connection.exec_params("DELETE FROM decent_jobs WHERE id = $1", [id])
        self
      end
    end

    private

    # Clears the execution, cron and batch context, so callback-created jobs
    # do not inherit the operator caller's context.
    def isolated_context(&block)
      Current.with(nil) do
        CronEntry.with_context(nil) { Batch.with_context(nil, &block) }
      end
    end

    def assert_unfinished!
      raise StateMismatchError, "Job has already finished" if finished?
    end

    # The integration wraps the update, so Active Job sends its discard
    # instrumentation event. A job whose instance does not load still gets
    # discarded.
    def discard(message, force:)
      manage(force: force) do |connection|
        assert_unfinished!
        exception = DiscardJobError.new(message)
        error_details = {"class" => exception.class.name, "message" => exception.message, "backtrace" => []}
        error_details["force_discarded"] = true if force
        details = JSON.generate(error_details)
        update = proc do
          connection.exec_params(<<~SQL, [id, details])
            WITH finished AS (
              UPDATE decent_jobs SET state = 'discarded', error = $2, error_event = 'discarded',
                finished_at = clock_timestamp(), updated_at = clock_timestamp() WHERE id = $1 RETURNING finished_at
            )
            UPDATE decent_job_executions SET state = 'discarded', error = $2, error_event = 'discarded', finished_at = finished.finished_at
            FROM finished WHERE job_id = $1 AND decent_job_executions.finished_at IS NULL
          SQL
          ConcurrencyClaims.new(database).release_all(connection, id)
          batch_ids = [batch_id, batch_callback_id].compact.uniq
          BatchStore.new(database).request(connection, batch_ids, job_id: id, discarded: true)
          database.after_commit do
            isolated_context { batch_ids.sort.each { |batch_id| BatchStore.new(database).reconcile(batch_id) } }
          end
        end
        job = job_instance(ignore_deserialization_errors: true)
        job ? Integrations.for_job(job).discarding(job, exception, &update) : update.call
        reload
      end
    end

    # Runs block under the concurrency claim lock and a row lock. Without
    # force, it also takes the job's advisory lock first and fails fast.
    def manage(force: false, &block)
      operation = proc do
        ConcurrencyClaims.new(database).synchronize(id, wait: force) do |connection|
          lock_row(connection, wait: force)
          block.call(connection)
        end
      end
      force ? operation.call : with_job_lock(&operation)
    rescue PG::LockNotAvailable
      raise LockedError, "Job #{id} is locked"
    end

    # Locks the row with FOR UPDATE and refreshes the cache. Without wait,
    # NOWAIT fails fast, and persisted SKIP LOCKED ownership counts as locked.
    def lock_row(connection, wait: false)
      row = connection.exec_params("SELECT * FROM decent_jobs WHERE id = $1 FOR UPDATE #{"NOWAIT" unless wait}", [id]).first
      raise KeyError, "Job #{id} was not found" unless row
      raise LockedError, "Job #{id} is locked" if !wait && row["locked_by_id"] && row["lock_type"] == "skiplocked"
      @attributes = @store.decode(row)
    rescue PG::LockNotAvailable
      raise LockedError, "Job #{id} is locked"
    end

    # Takes the job's advisory lock for one transaction, the same key that
    # workers use. pg_try_advisory_xact_lock fails fast and releases at
    # commit or rollback.
    def with_job_lock
      database.transaction do |connection|
        row = connection.exec_params(<<~SQL, [id]).first
          SELECT #{ConcurrencyClaims::JOB_KEY_SQL} AS lock_key,
            #{ConcurrencyClaims::EXECUTION_LOCKED_SQL} AS locked
          FROM decent_jobs WHERE id = $1
        SQL
        raise KeyError, "Job #{id} was not found" unless row
        raise LockedError, "Job #{id} is locked" if row.fetch("locked") == "t"
        locked = connection.exec_params("SELECT pg_try_advisory_xact_lock($1::bigint)", [row.fetch("lock_key")]).getvalue(0, 0) == "t"
        raise LockedError, "Job #{id} is locked" unless locked
        yield
      end
    end
  end
end
