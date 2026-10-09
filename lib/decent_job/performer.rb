# frozen_string_literal: true

module DecentJob
  class Performer
    Result = Data.define(:job_id, :state, :error, :unhandled_error, :error_event)

    def initialize(database: DecentJob.database, enable_pauses: DecentJob.configuration.enable_pauses, on_unhandled_exception: nil, process_tracker: nil, on_execution_start: nil)
      @on_execution_start = on_execution_start
      @database = database
      @on_unhandled_exception = on_unhandled_exception
      @store = Store.new(database)
      @claim = Claim.new(database, enable_pauses: enable_pauses, process_tracker: process_tracker)
    end

    def perform_one(queues: "*", job_id: nil, raise_errors: false, stopping: -> { false })
      BatchStore.new(@database).reconcile_pending
      ConcurrencyClaims.new(@database).recover
      @claim.with_next(queues: queues, job_id: job_id, stopping: stopping) do |row|
        execute(row, raise_errors: raise_errors, stopping: stopping)
      end
    end

    private

    def execute(row, raise_errors:, stopping:)
      if @on_execution_start.respond_to?(:arity) && @on_execution_start.arity == 1
        @on_execution_start.call(row.fetch("active_job_id"))
      else
        @on_execution_start&.call
      end
      active_job = nil
      error = nil
      error_event = nil
      state = "succeeded"
      unhandled = false
      current_thread = Thread.current
      subscriber = lambda do |name, _start, _finish, _id, payload|
        next unless Thread.current == current_thread && payload[:job]&.job_id == row.fetch("active_job_id")

        active_job = payload[:job]
        next if name == "perform.active_job"

        state = name.start_with?("enqueue_retry") ? "retried" : "discarded"
        error = payload[:error]
        error_event = {"enqueue_retry" => "retried", "discard" => "discarded", "retry_stopped" => "retry_stopped"}.fetch(name.split(".").first)
      end
      context = {database: @database, job_id: row.fetch("active_job_id"), id: row.fetch("id"), execution_id: row.fetch("execution_id"),
                 execution_interrupted: row["execution_interrupted"], batch_id: row["batch_id"], batch_callback_id: row["batch_callback_id"], stopping: stopping}
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      begin
        Current.with(context) do
          ActiveSupport::Notifications.subscribed(subscriber, /\A(?:perform|enqueue_retry|discard|retry_stopped)\.active_job\z/) do
            value = wrap_execution { ActiveJob::Base.execute(row.fetch("payload")) }
            if value.is_a?(Exception) && error.nil?
              error = value
              error_event = "handled"
              state = "discarded"
            end
          end
        end
      rescue *DecentJob.configuration.handled_exception_classes => exception
        error_event = if exception.is_a?(InterruptError)
          "interrupted"
        elsif error_event == "retry_stopped" && error.equal?(exception)
          "retry_stopped"
        else
          "unhandled"
        end
        error = exception
        state = "discarded"
        unhandled = true
      rescue Exception => exception # standard:disable Lint/RescueException
        # The worker distinguishes application exceptions deliberately excluded
        # from handling from recoverable infrastructure failures.
        @on_unhandled_exception&.call(exception)
        raise
      end
      # User ensure blocks and Active Job handlers can swallow termination by
      # raising/rescuing another error. It remains an unfinished killed attempt.
      return if Thread.current.status == "aborting"

      duration = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
      unhandled_error = error if unhandled
      state, error, error_event, forced = finish(row, state, error, error_event, active_job, unhandled, duration)
      raise error if raise_errors && unhandled && !forced

      Result.new(job_id: row.fetch("id"), state: state, error: error, unhandled_error: unhandled_error, error_event: error_event)
    end

    def wrap_execution(&block)
      DecentJob.wrap_execution(&block)
    end

    def preserve_record?(active_job, error, error_event, unhandled)
      policy = DecentJob.configuration.preserve_job_records
      if policy.respond_to?(:call)
        # Isolate application SQL failures so retaining a record remains possible.
        return @database.transaction { policy.call(active_job, error, error_event&.to_sym) }
      end

      policy == true || (policy == :on_unhandled_error && unhandled)
    rescue => exception
      begin
        @database.transaction { DecentJob.configuration.on_thread_error.call(exception) }
      rescue
        nil
      end
      true
    end

    def finish(row, state, error, error_event, active_job, unhandled, duration)
      details = error && JSON.generate("class" => error.class.name, "message" => error.message, "backtrace" => error.backtrace || [])
      batch_ids = []
      forced = false
      cleanup = lambda do |connection|
        next if forced || state == "retried" || row["cron_key"]
        next if preserve_record?(active_job, error, error_event, unhandled)

        connection.exec_params(<<~SQL, [row.fetch("id"), row.fetch("execution_id")])
          DELETE FROM decent_jobs WHERE id = $1 AND execution_id = $2
            AND state IN ('succeeded', 'discarded') AND finished_at IS NOT NULL AND cron_key IS NULL
        SQL
      end
      context = {id: row.fetch("id"), execution_id: row.fetch("execution_id")}
      ConcurrencyClaims.new(@database).finish(context, after_release: cleanup) do |connection|
        current = connection.exec_params("SELECT state, execution_id, batch_id, batch_callback_id, finished_at, error, error_event FROM decent_jobs WHERE id = $1 FOR UPDATE", [row.fetch("id")]).first
        raise "Execution ownership lost" unless current && current["execution_id"] == row.fetch("execution_id")

        if current["finished_at"]
          state = current.fetch("state")
          error_event = current["error_event"]
          stored_error = JSON.parse(current["error"] || "{}")
          error = Job::DiscardJobError.new(stored_error["message"]) if stored_error["class"] == "DecentJob::Job::DiscardJobError"
          forced = true
          connection.exec_params("UPDATE decent_jobs SET locked_by_id = NULL, locked_at = NULL WHERE id = $1 AND execution_id = $2 AND locked_by_id = $3", [row.fetch("id"), row.fetch("execution_id"), row["locked_by_id"]])
          next
        end

        state = "retried" if current["state"] == "ready" || (unhandled && DecentJob.configuration.retry_on_unhandled_error)
        if state == "retried"
          connection.exec_params(<<~SQL, [row.fetch("id"), details, error_event])
            UPDATE decent_jobs SET state = 'ready', error = $2, error_event = $3, performed_at = NULL, locked_by_id = NULL, locked_at = NULL,
              updated_at = clock_timestamp() WHERE id = $1
          SQL
        else
          connection.exec_params(<<~SQL, [row.fetch("id"), state, details, error_event])
            UPDATE decent_jobs SET state = $2, finished_at = clock_timestamp(), locked_by_id = NULL, locked_at = NULL,
              updated_at = clock_timestamp(), error = $3, error_event = $4 WHERE id = $1
          SQL
        end
        connection.exec_params(<<~SQL, [row.fetch("execution_id"), state, details, current["batch_id"], error_event, duration])
          UPDATE decent_job_executions SET state = $2, finished_at = clock_timestamp(), error = $3,
            batch_id = $4, error_event = $5, duration = $6 WHERE id = $1
        SQL
        batch_ids = current.values_at("batch_id", "batch_callback_id").compact.uniq
        BatchStore.new(@database).request(connection, batch_ids, job_id: row.fetch("id"), discarded: state == "discarded")
      end
      batch_ids.sort.each { |id| BatchStore.new(@database).reconcile(id) }
      [state, error, error_event, forced]
    end
  end
end
