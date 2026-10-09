# frozen_string_literal: true

require "json"
require "securerandom"
require "time"

module DecentJob
  # Owns batch transitions and their durable callback deliveries. Completion
  # requests have no foreign key or unique batch key, so finishing a job never
  # waits on a batch lock or on another completion request.
  class BatchStore
    # A normal failed attempt can retain its session lock while reconciling a
    # batch. A force-discarded attempt may still have application code running.
    FORCE_DISCARD_LOCKED_SQL = "COALESCE(decent_jobs.error ->> 'force_discarded', 'false') = 'true' AND (#{ConcurrencyClaims::EXECUTION_LOCKED_SQL})"
    private_constant :FORCE_DISCARD_LOCKED_SQL

    ATTRIBUTES = %w[description on_finish on_success on_discard callback_queue_name callback_priority properties].freeze

    def initialize(database)
      @database = database
    end

    def find(id)
      @database.with_connection do |connection|
        decode(connection.exec_params("SELECT * FROM decent_job_batches WHERE id = $1", [id]).first)
      end
    end

    def save(id:, attributes:)
      return save_all([attributes]).first unless id

      values = normalize_attributes(attributes)
      @database.with_connection do |connection|
        return find(id) if values.empty?
        assignments = values.keys.each_with_index.map { |key, index| "#{key} = $#{index + 2}" }
        row = connection.exec_params("UPDATE decent_job_batches SET #{assignments.join(", ")}, updated_at = clock_timestamp() WHERE id = $1 RETURNING *", [id, *values.values]).first
        raise KeyError, "Batch not found: #{id}" unless row

        decode(row)
      end
    end

    def save_all(attributes)
      return [] if attributes.empty?

      values = attributes.map { |entry| normalize_attributes(entry) }
      ids = values.map { SecureRandom.uuid }
      columns = ["id", *ATTRIBUTES]
      params = []
      placeholders = values.zip(ids).map do |entry, id|
        offset = params.size
        params.concat([id, *ATTRIBUTES.map { |name| entry.fetch(name, (name == "properties") ? "{}" : nil) }])
        "(#{columns.each_index.map { |index| "$#{offset + index + 1}" }.join(", ")})"
      end
      @database.with_connection do |connection|
        rows = connection.exec_params("INSERT INTO decent_job_batches (#{columns.join(", ")}) VALUES #{placeholders.join(", ")} RETURNING *", params)
        by_id = rows.to_h { |row| [row.fetch("id"), decode(row)] }
        ids.map { |id| by_id.fetch(id) }
      end
    end

    def locked(ids)
      @database.transaction do |connection|
        Array(ids).compact.uniq.sort.each do |id|
          connection.exec_params("SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || '.decent_job.batch.' || $1, 0))", [id])
        end
        yield connection
      end
    end

    def lock_scopes(jobs)
      ConcurrencyControls.new(@database).lock_scopes(jobs)
    end

    def open(id)
      @database.with_connection do |connection|
        connection.exec_params("DELETE FROM decent_job_batch_pending_jobs WHERE batch_id = $1", [id])
        # Capture unfinished jobs in the same snapshot as the generation reset.
        # Their completion may commit before this new generation is visible.
        connection.exec_params(<<~SQL, [id])
          WITH opened AS (
            UPDATE decent_job_batches SET generation = generation + 1,
              enqueued_at = NULL, jobs_finished_at = NULL, finished_at = NULL,
              discarded_at = NULL, updated_at = clock_timestamp() WHERE id = $1
            RETURNING id, generation
          )
          INSERT INTO decent_job_batch_pending_jobs (batch_id, generation, job_id)
          SELECT opened.id, opened.generation, jobs.id FROM opened
          JOIN decent_jobs jobs ON jobs.batch_id = opened.id OR jobs.batch_callback_id = opened.id
          WHERE jobs.state IN ('ready', 'running')
        SQL
      end
    end

    def mark_enqueued(id)
      @database.with_connection do |connection|
        connection.exec_params("UPDATE decent_job_batches SET enqueued_at = clock_timestamp(), updated_at = clock_timestamp() WHERE id = $1", [id])
      end
    end

    def attach(id, jobs)
      ids = jobs.filter_map(&:provider_job_id).uniq
      return if ids.empty?
      @database.transaction do |connection|
        encoded = PG::TextEncoder::Array.new.encode(ids)
        previous = connection.exec_params("SELECT batch_id FROM decent_jobs WHERE id = ANY($1::uuid[]) ORDER BY id FOR UPDATE", [encoded]).column_values(0)
        result = connection.exec_params("UPDATE decent_jobs SET batch_id = $2, updated_at = clock_timestamp() WHERE id = ANY($1::uuid[])", [encoded, id])
        raise KeyError, "Some batch jobs are missing" unless result.cmd_tuples == ids.size
        request(connection, [id, *previous])
      end
    end

    def members(id, callbacks: false, ids: nil, deserialize_arguments: true)
      return [] if ids && ids.empty?

      column = callbacks ? "batch_callback_id" : "batch_id"
      params = [id]
      params << PG::TextEncoder::Array.new.encode(ids) if ids
      filter = ids ? "AND id = ANY($2::uuid[])" : ""
      @database.with_connection do |connection|
        connection.exec_params("SELECT payload FROM decent_jobs WHERE #{column} = $1 #{filter} ORDER BY created_at, id", params).map do |row|
          Current.with(database: @database) do
            ActiveJob::Base.deserialize(JSON.parse(row.fetch("payload"))).tap do |job|
              job.send(:deserialize_arguments_if_needed) if deserialize_arguments
            end
          end
        end
      end
    end

    def retry_members(id)
      @database.transaction do |connection|
        rows = connection.exec_params("SELECT * FROM decent_jobs WHERE batch_id = $1 AND state = 'discarded' AND NOT (#{FORCE_DISCARD_LOCKED_SQL}) ORDER BY id", [id]).to_a
        retries = rows.map { |row| prepare_retry(row) }
        jobs = retries.flat_map do |prepared|
          prepared.fetch(:buffer).active_jobs.reject { |job| job.job_id == prepared.fetch(:context).fetch(:job_id) }
        end
        jobs.each { |job| validate_adapter(job.class) }
        lock_scopes(jobs)
        encoded_ids = PG::TextEncoder::Array.new.encode(rows.map { |row| row.fetch("id") })
        locked = connection.exec_params("SELECT id, state, execution_id, #{FORCE_DISCARD_LOCKED_SQL} AS force_discard_locked FROM decent_jobs WHERE id = ANY($1::uuid[]) ORDER BY id FOR UPDATE", [encoded_ids]).to_h { |row| [row.fetch("id"), row] }
        ids = retries.filter_map do |prepared|
          previous = prepared.fetch(:row)
          current = locked[previous.fetch("id")]
          next unless current && current["state"] == "discarded" && current["execution_id"] == previous["execution_id"] && current["force_discard_locked"] == "f"

          job = prepared[:job]
          if job
            Store.new(@database).reschedule(job, prepared.fetch(:context), payload: prepared.fetch(:payload))
            # The discarded attempt is now deliberately retried. Its completion
            # request may have arrived while this generation was being opened.
            connection.exec_params("DELETE FROM decent_job_batch_updates WHERE batch_id = $1 AND job_id = $2 AND discarded", [id, previous.fetch("id")])
          else
            request(connection, [id], job_id: previous.fetch("id"), discarded: true)
          end
          Bulk.with_buffer(nil) do
            Batch.within_thread(database: @database) { prepared.fetch(:buffer).enqueue }
          end
          previous.fetch("id") if job&.successfully_enqueued?
        end
        request(connection, [id])
        ids
      end
    end

    def request(connection, ids, job_id: nil, discarded: false)
      ids = Array(ids).compact.uniq
      return if ids.empty?
      connection.exec_params(<<~SQL, [PG::TextEncoder::Array.new.encode(ids), job_id, discarded])
        INSERT INTO decent_job_batch_updates (batch_id, generation, job_id, discarded)
        SELECT id, generation, $2, $3 FROM decent_job_batches WHERE id = ANY($1::uuid[])
      SQL
      Store.new(@database).notify(connection)
    end

    def reconcile(id)
      locked([id]) do |connection|
        snapshot = completion_snapshot(connection, id)
        unless snapshot
          connection.exec_params("DELETE FROM decent_job_batch_updates WHERE batch_id = $1", [id])
          next
        end
        updates = JSON.parse(snapshot.delete("pending_updates"))
        members_pending = snapshot.delete("members_pending") == "t"
        callbacks_pending = snapshot.delete("callbacks_pending") == "t"
        row = decode(snapshot)
        discarded = updates.any? { |update| update["discarded"] && update["current_generation"] }
        if discarded && !row["discarded_at"]
          timestamp(connection, id, "discarded_at")
          row = find(id)
          create_event(connection, row, "discard")
        end
        if row["enqueued_at"] && !row["jobs_finished_at"] && !members_pending
          timestamp(connection, id, "jobs_finished_at")
          create_event(connection, row, "success") unless row["discarded_at"]
          create_event(connection, row, "finish")
        end
        dispatch(connection, id)
        row = find(id)
        pending = connection.exec_params("SELECT 1 FROM decent_job_batch_events WHERE batch_id = $1 AND dispatched_at IS NULL LIMIT 1", [id]).ntuples.positive?
        if row["jobs_finished_at"] && !row["finished_at"] && !callbacks_pending && !pending && !unfinished?(connection, id, "batch_callback_id")
          timestamp(connection, id, "finished_at")
        end
        delete_updates(connection, updates)
      end
    end

    def reconcile_pending(limit: 100)
      ids = @database.with_connection do |connection|
        connection.exec_params(<<~SQL, [limit]).column_values(0)
          SELECT id FROM decent_job_batches
          WHERE EXISTS (SELECT 1 FROM decent_job_batch_updates WHERE batch_id = decent_job_batches.id)
             OR EXISTS (SELECT 1 FROM decent_job_batch_events WHERE batch_id = decent_job_batches.id AND dispatched_at IS NULL)
          ORDER BY reconciled_at NULLS FIRST, id LIMIT $1
        SQL
      end
      ids.each do |id|
        @database.with_connection { |connection| connection.exec_params("UPDATE decent_job_batches SET reconciled_at = clock_timestamp() WHERE id = $1", [id]) }
        reconcile(id)
      rescue => error
        begin
          DecentJob.configuration.on_thread_error&.call(error)
        rescue
          nil
        end
      end
      ids.size
    end

    private

    def completion_snapshot(connection, id)
      # Finish records the terminal state and its update in one transaction.
      # Read both from one MVCC snapshot so a concurrent discard cannot appear
      # terminal without also contributing its failure. Later completions leave
      # an update for the next reconciliation.
      connection.exec_params(<<~SQL, [id]).first
        SELECT batches.*,
          (SELECT COALESCE(jsonb_agg(jsonb_build_object(
             'id', updates.id, 'discarded', updates.discarded,
             'current_generation', updates.generation = batches.generation OR EXISTS (
               SELECT 1 FROM decent_job_batch_pending_jobs pending
               WHERE pending.batch_id = batches.id AND pending.generation = batches.generation AND pending.job_id = updates.job_id
             )) ORDER BY updates.id), '[]'::jsonb)
           FROM decent_job_batch_updates updates WHERE updates.batch_id = batches.id) AS pending_updates,
          EXISTS (SELECT 1 FROM decent_jobs WHERE batch_id = batches.id AND state IN ('ready', 'running')) AS members_pending,
          EXISTS (SELECT 1 FROM decent_jobs WHERE batch_callback_id = batches.id AND state IN ('ready', 'running')) AS callbacks_pending
        FROM decent_job_batches batches WHERE batches.id = $1
      SQL
    end

    def prepare_retry(row)
      record = Job.from_attributes(Store.new(@database).decode(row), database: @database)
      JobRetry.new(record).prepare.merge(row: row)
    rescue Job::AdapterMismatchError => error
      raise ArgumentError, error.message
    end

    def normalize_attributes(attributes)
      values = attributes.to_h { |key, value| [key.to_s, value] }
      raise ArgumentError, "Unknown batch attributes" unless (values.keys - ATTRIBUTES).empty?
      values.each do |key, value|
        if key.start_with?("on_") && value
          klass = value.is_a?(Class) ? value : value.to_s.constantize
          validate_adapter(klass)
          values[key] = klass.name
        end
      end
      values["properties"] = JSON.generate(values["properties"]) if values.key?("properties")
      values
    end

    def validate_adapter(klass)
      unless klass <= ActiveJob::Base && klass.queue_adapter.is_a?(Adapter) && klass.queue_adapter.database.equal?(@database)
        raise ArgumentError, "Batch jobs must use a DecentJob adapter with the batch's database"
      end
    end

    def unfinished?(connection, id, column)
      connection.exec_params("SELECT 1 FROM decent_jobs WHERE #{column} = $1 AND state IN ('ready', 'running') LIMIT 1", [id]).ntuples.positive?
    end

    def timestamp(connection, id, column)
      connection.exec_params("UPDATE decent_job_batches SET #{column} = clock_timestamp(), updated_at = clock_timestamp() WHERE id = $1", [id])
    end

    def create_event(connection, row, event)
      klass = row["on_#{event}"]
      return unless klass
      connection.exec_params(<<~SQL, [row.fetch("id"), row.fetch("generation"), event, klass, row["callback_queue_name"], row["callback_priority"]])
        INSERT INTO decent_job_batch_events (batch_id, generation, event, job_class, queue_name, priority)
        VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING
      SQL
    end

    def dispatch(connection, id)
      events = connection.exec_params("SELECT * FROM decent_job_batch_events WHERE batch_id = $1 AND dispatched_at IS NULL ORDER BY generation, event", [id]).to_a
      return if events.empty?
      batch = Batch.find(id, database: @database)
      pairs = events.map do |event|
        klass = event.fetch("job_class").constantize
        validate_adapter(klass)
        options = {queue: event["queue_name"], priority: event["priority"]&.to_i}.compact
        [event, klass.new(batch, {event: event.fetch("event").to_sym}).tap { |job| job.set(**options) }]
      end
      Batch.within_thread(batch_callback_id: id, database: @database) do
        # Enqueue callbacks can create additional jobs. Persist the whole capture
        # and acquire its scope locks before inserting any of its jobs.
        buffer = Bulk::Buffer.new
        buffer.capture do
          pairs.each { |_, job| job.enqueue }
        end
        buffer.prepare
        buffer.active_jobs.each do |job|
          unless job.queue_adapter.is_a?(Adapter) && job.queue_adapter.database.equal?(@database)
            raise ArgumentError, "Batch jobs must use a DecentJob adapter with the batch's database"
          end
        end
        lock_scopes(buffer.active_jobs)
        Bulk.with_buffer(nil) do
          buffer.enqueue
          pairs.each do |event, job|
            next unless buffer.active_jobs.include?(job)
            next unless job.provider_job_id && job.successfully_enqueued?
            connection.exec_params(<<~SQL, [id, event.fetch("generation"), event.fetch("event"), job.provider_job_id])
              UPDATE decent_job_batch_events SET job_id = $4, dispatched_at = clock_timestamp()
              WHERE batch_id = $1 AND generation = $2 AND event = $3
            SQL
          end
        end
      end
    end

    def delete_updates(connection, updates)
      return if updates.empty?
      connection.exec_params("DELETE FROM decent_job_batch_updates WHERE id = ANY($1::bigint[])", [PG::TextEncoder::Array.new.encode(updates.map { |update| update.fetch("id") })])
    end

    def decode(row)
      return unless row
      row.to_h do |key, value|
        value = if value.nil?
          nil
        elsif key == "properties"
          JSON.parse(value)
        elsif key.end_with?("_at")
          Time.parse(value)
        elsif %w[generation callback_priority].include?(key)
          value.to_i
        else
          value
        end
        [key, value]
      end
    end
  end
end
