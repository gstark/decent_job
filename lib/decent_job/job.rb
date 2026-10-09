# frozen_string_literal: true

module DecentJob
  # A persisted job and its execution history, without an ORM dependency.
  class Job
    class StateMismatchError < StandardError; end
    class LockedError < StandardError; end
    class DeserializationError < StandardError; end
    class AdapterMismatchError < StandardError; end
    class DiscardJobError < StandardError; end

    ATTRIBUTES = %w[id active_job_id queue_name job_class priority payload scheduled_at labels concurrency_keys batch_id batch_callback_id cron_key cron_at state execution_id locked_by_id locked_at lock_type executions_count created_at updated_at performed_at finished_at error error_event].freeze
    attr_reader :database, :id
    (ATTRIBUTES - ["id", "lock_type"]).each { |name| define_method(name) { @attributes.fetch(name) } }

    def self.find(id, database: nil)
      database ||= Current.execution&.fetch(:database, nil) || DecentJob.database
      new(id, database: database).reload
    end

    def self.from_attributes(attributes, database:)
      new(attributes.fetch("id"), database: database, attributes: attributes)
    end

    def initialize(id, database:, attributes: nil)
      @id = id.to_s.dup.freeze
      @database = database
      @store = Store.new(database)
      @attributes = Support.deep_dup(attributes) if attributes
    end

    def lock_type
      @attributes["lock_type"]
    end

    def attributes
      Support.deep_dup(@attributes)
    end

    def reload
      @attributes = @store.find(@id) || raise(KeyError, "Job #{@id} was not found")
      self
    end

    def executions
      @store.executions(id)
    end

    # The deserialized job object, native or Active Job.
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

    def queue_latency
      expected = scheduled_at || created_at
      now = Time.now
      return if expected >= now
      (performed_at || finished_at || now) - expected
    end

    def runtime_latency
      (finished_at || Time.now) - performed_at if performed_at
    end

    def total_latency
      queue = queue_latency
      runtime = runtime_latency
      queue + runtime if queue && runtime
    end

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

    def discard_job(message)
      discard(message, force: false)
    end

    def force_discard_job(message)
      discard(message, force: true)
    end

    def destroy_job
      manage do |connection|
        raise StateMismatchError, "Only finished jobs can be destroyed" unless finished?
        ConcurrencyClaims.new(database).release_all(connection, id)
        connection.exec_params("DELETE FROM decent_jobs WHERE id = $1", [id])
        self
      end
    end

    private

    def isolated_context(&block)
      Current.with(nil) do
        CronEntry.with_context(nil) { Batch.with_context(nil, &block) }
      end
    end

    def assert_unfinished!
      raise StateMismatchError, "Job has already finished" if finished?
    end

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

    def lock_row(connection, wait: false)
      row = connection.exec_params("SELECT * FROM decent_jobs WHERE id = $1 FOR UPDATE #{"NOWAIT" unless wait}", [id]).first
      raise KeyError, "Job #{id} was not found" unless row
      raise LockedError, "Job #{id} is locked" if !wait && row["locked_by_id"] && row["lock_type"] == "skiplocked"
      @attributes = @store.decode(row)
    rescue PG::LockNotAvailable
      raise LockedError, "Job #{id} is locked"
    end

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
