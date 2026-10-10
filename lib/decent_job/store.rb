# frozen_string_literal: true

require "json"
require "securerandom"
require "time"

module DecentJob
  # SQL persistence only. Active Job execution belongs to Performer.
  #
  # Store inserts, reschedules, and reads rows in decent_jobs and
  # decent_job_executions, and decodes PostgreSQL text values into Ruby
  # values. {Adapter}, {Claim}, {Job}, {BatchStore}, and {CronStore} create
  # instances as needed. An instance holds only the {Database}, so threads can
  # share it. Every method uses the calling context's checkout, so a call
  # inside an open {Database#transaction} joins that transaction.
  class Store
    # @param database [Database] the pool for all queries
    def initialize(database)
      @database = database
    end

    # Builds the decent_jobs attributes for a new job. Does not write to the
    # database.
    #
    # Reads the current cron context ({CronEntry.context_for}) and batch
    # context ({Batch.context_for}) for this database.
    #
    # @param job [Object] an Active Job or native job instance
    # @param id [String] the decent_jobs id (UUID). It is also stored as
    #   provider_job_id in the payload.
    # @param payload [Hash, nil] the serialized job, or nil to call job.serialize
    # @return [Hash{String => Object}] every {Job::ATTRIBUTES} key. state is
    #   'ready' and scheduled_at is now (UTC) when the job has no schedule.
    def enqueue_attributes(job, id: SecureRandom.uuid, payload: nil)
      cron = CronEntry.context_for(@database) || {}
      batch = Batch.context_for(@database, job: job) || {}
      payload = (payload || job.serialize).merge("provider_job_id" => id)
      Job::ATTRIBUTES.to_h { |key| [key, nil] }.merge(
        "id" => id, "active_job_id" => job.job_id, "queue_name" => job.queue_name,
        "job_class" => job.class.name, "priority" => job.priority || 0, "payload" => payload,
        "scheduled_at" => (job.scheduled_at || Time.now).utc,
        "labels" => payload.fetch("decent_job_labels", []),
        "concurrency_keys" => payload.fetch("decent_job_concurrency_keys", []),
        "batch_id" => batch[:batch_id], "batch_callback_id" => batch[:batch_callback_id],
        "cron_key" => cron[:key]&.to_s, "cron_at" => cron[:at]&.getutc,
        "state" => "ready", "executions_count" => 0
      )
    end

    # Inserts jobs in one transaction and sends NOTIFY for the accepted jobs.
    #
    # Rows go in batches of 500, which keeps each INSERT under the PostgreSQL
    # limit of 65535 bind parameters (13 for each row).
    #
    # @param jobs [Array<Object>] the job instances, in the same order as attributes
    # @param attributes [Array<Hash>, nil] rows from {#enqueue_attributes}, or
    #   nil to build them
    # @param skip_duplicates [Boolean] false ignores only a conflict on
    #   (cron_key, cron_at) and raises on other conflicts. True ignores every
    #   unique conflict, including a duplicate id.
    # @yieldparam row [Hash{String => Object}] each inserted row, decoded. The
    #   block runs inside the transaction.
    # @return [Array<String, nil>] one entry for each job: its id, or nil when
    #   the insert was skipped as a duplicate
    # @raise [PG::UniqueViolation] when skip_duplicates is false and a row
    #   conflicts on a unique key other than (cron_key, cron_at)
    # @note NOTIFY is transactional, so workers receive it only after the
    #   outermost transaction commits. {Notifier.notify_jobs} sends nothing
    #   when LISTEN/NOTIFY is disabled.
    def enqueue_all(jobs, attributes: nil, skip_duplicates: false)
      return [] if jobs.empty?

      ids = nil
      accepted_ids = {}

      @database.transaction do |connection|
        attributes ||= jobs.map { |job| enqueue_attributes(job) }
        ids = attributes.map { |row| row.fetch("id") }

        attributes.each_slice(500) do |entries|
          params = []

          values = entries.map do |row|
            offset = params.size
            params.concat([row.fetch("id"), row.fetch("active_job_id"), row.fetch("queue_name"), row.fetch("job_class"), row.fetch("priority"),
              JSON.generate(row.fetch("payload")), row.fetch("scheduled_at").iso8601(6),
              PG::TextEncoder::Array.new.encode(row.fetch("labels")),
              PG::TextEncoder::Array.new.encode(row.fetch("concurrency_keys")),
              row["batch_id"], row["batch_callback_id"], row["cron_key"], row["cron_at"]&.iso8601(6)])
            "(#{(1..13).map { |index| "$#{offset + index}" }.join(", ")})"
          end

          result = connection.exec_params(<<~SQL, params)
            INSERT INTO decent_jobs (id, active_job_id, queue_name, job_class, priority, payload, scheduled_at, labels, concurrency_keys, batch_id, batch_callback_id, cron_key, cron_at)
            VALUES #{values.join(", ")}
            ON CONFLICT #{"(cron_key, cron_at) WHERE cron_key IS NOT NULL AND cron_at IS NOT NULL" unless skip_duplicates} DO NOTHING
            RETURNING #{block_given? ? "*" : "id"}
          SQL

          result.each do |row|
            accepted_ids[row.fetch("id")] = true
            yield decode(row) if block_given?
          end
        end

        Notifier.notify_jobs(connection, jobs.zip(ids).filter_map { |job, id| job if accepted_ids[id] })
      end

      ids.map { |id| id if accepted_ids[id] }
    end

    # Summarizes ready jobs by queue so an in-process {Worker} can wake up
    # without NOTIFY.
    #
    # @param ids [Array<String>] decent_jobs ids
    # @return [Array<Hash{Symbol => Object}>] one entry for each queue and
    #   deadline, with :queue_name and :count. :scheduled_at is present only
    #   for jobs scheduled in the future. Due jobs have no :scheduled_at.
    def scheduling_hints(ids)
      @database.with_connection do |connection|
        connection.exec_params(<<~SQL, [PG::TextEncoder::Array.new.encode(ids)]).map do |row|
          SELECT queue_name,
            CASE WHEN scheduled_at > statement_timestamp() THEN scheduled_at ELSE NULL END AS deadline,
            count(*) AS count
          FROM decent_jobs WHERE id = ANY($1::uuid[]) AND state = 'ready'
          GROUP BY queue_name, deadline
        SQL
          state = {queue_name: row.fetch("queue_name"), count: row.fetch("count").to_i}
          state[:scheduled_at] = Time.parse(row.fetch("deadline")) if row["deadline"]
          state
        end
      end
    end

    # @param id [String] the decent_jobs id
    # @return [Hash{String => Object}, nil] the decoded row, or nil when no job
    #   has this id
    def find(id)
      @database.with_connection do |connection|
        row = connection.exec_params("SELECT * FROM decent_jobs WHERE id = $1", [id]).first
        decode(row) if row
      end
    end

    # Makes an existing job row ready again with a new payload and schedule.
    #
    # The row id stays the same. It is used for a retry or self-reenqueue from
    # a running attempt, and for an operator retry of a discarded job
    # ({JobRetry}). The update requires that execution_id still matches
    # context[:execution_id], so a stale attempt cannot reschedule a job that
    # a newer attempt owns. It does not clear locked_by_id: the running
    # attempt still owns the job until {Performer} finishes it.
    #
    # @param job [Object] the job instance to persist
    # @param context [Hash] execution context with :id and :execution_id.
    #   :manual_retry true requires state 'discarded' and keeps the stored
    #   error. Otherwise the job must be unfinished and in state 'ready' or
    #   'running', and the error is cleared.
    # @param payload [Hash, nil] the serialized job, or nil to call job.serialize
    # @yieldparam row [Hash{String => Object}] the updated row, decoded
    # @return [true] after the job is marked successfully enqueued
    # @raise [EnqueueError] when the row no longer matches, which means the
    #   attempt lost ownership
    # @note Sends NOTIFY. Refreshes created_at when dequeue_query_sort is
    #   :created_at. For a non-manual retry it increments context[:reenqueues]
    #   and registers a rollback callback that decrements it. {Performer} reads
    #   this count to report a retry.
    def reschedule(job, context, payload: nil)
      @database.transaction do |connection|
        payload ||= job.serialize
        params = [context.fetch(:id), context.fetch(:execution_id), JSON.generate(payload),
          (job.scheduled_at || Time.now).utc.iso8601(6), job.queue_name, job.priority || 0,
          PG::TextEncoder::Array.new.encode(payload.fetch("decent_job_labels", [])),
          PG::TextEncoder::Array.new.encode(payload.fetch("decent_job_concurrency_keys", []))]
        result = connection.exec_params(<<~SQL, params)
          UPDATE decent_jobs SET state = 'ready', payload = $3, scheduled_at = $4,
            queue_name = $5, priority = $6, labels = $7, concurrency_keys = $8,
            created_at = #{(DecentJob.configuration.dequeue_query_sort == :created_at) ? "clock_timestamp()" : "created_at"},
            updated_at = clock_timestamp(), finished_at = NULL, performed_at = NULL,
            error = #{context[:manual_retry] ? "error" : "NULL"}, error_event = 'retried'
          WHERE id = $1 AND execution_id IS NOT DISTINCT FROM $2::uuid
            AND #{context[:manual_retry] ? "state = 'discarded'" : "finished_at IS NULL AND state IN ('ready', 'running')"}
          RETURNING *
        SQL
        raise EnqueueError, "execution ownership lost" unless result.cmd_tuples == 1
        yield decode(result.first) if block_given?
        Notifier.notify_jobs(connection, [job])

        unless context[:manual_retry]
          context[:reenqueues] = context.fetch(:reenqueues, 0) + 1
          @database.after_rollback { context[:reenqueues] -= 1 }
        end
      end

      job.successfully_enqueued = true
    end

    # @param id [String] the decent_jobs id
    # @return [Array<Hash{String => Object}>] the decoded executions of the job,
    #   oldest first
    def executions(id)
      @database.with_connection do |connection|
        connection.exec_params("SELECT * FROM decent_job_executions WHERE job_id = $1 ORDER BY started_at, id", [id]).map { |row| decode(row) }
      end
    end

    # Sends a NOTIFY with an empty payload, which wakes workers for all queues.
    #
    # @param connection [PG::Connection] the connection of the current
    #   transaction. The notification is delivered when it commits.
    # @return [void]
    def notify(connection)
      Notifier.notify(connection)
    end

    # Converts a PostgreSQL text row into Ruby values.
    #
    # payload and error become parsed JSON. labels and concurrency_keys become
    # arrays. Columns that end in _at become Time. duration becomes Float.
    # priority, executions_count, and process_id become Integer. Other values,
    # such as UUIDs, states, and booleans ("t" or "f"), stay strings. NULL
    # stays nil.
    #
    # @param row [Hash{String => String, nil}] a row from PG::Result
    # @return [Hash{String => Object}] the decoded row
    def decode(row)
      row.to_h do |key, value|
        decoded = if value.nil?
          nil
        elsif %w[payload error].include?(key)
          JSON.parse(value)
        elsif %w[labels concurrency_keys].include?(key)
          PG::TextDecoder::Array.new.decode(value)
        elsif key.end_with?("_at")
          Time.parse(value)
        elsif key == "duration"
          Float(value)
        elsif %w[priority executions_count process_id].include?(key)
          Integer(value)
        else
          value
        end

        [key, decoded]
      end
    end
  end
end
