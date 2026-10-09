# frozen_string_literal: true

module DecentJob
  class Performer
    Result = Data.define(:job_id, :state, :error)

    def initialize(database: DecentJob.database, enable_pauses: DecentJob.configuration.enable_pauses)
      @database = database
      @store = Store.new(database)
      @claim = Claim.new(database, enable_pauses: enable_pauses)
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
      error = nil
      error_event = nil
      state = "succeeded"
      unhandled = false
      current_thread = Thread.current
      subscriber = lambda do |name, _start, _finish, _id, payload|
        next unless Thread.current == current_thread && payload[:job]&.job_id == row.fetch("active_job_id")

        state = name.start_with?("enqueue_retry") ? "retried" : "discarded"
        error = payload[:error]
        error_event = {"enqueue_retry" => "retried", "discard" => "discarded", "retry_stopped" => "retry_stopped"}.fetch(name.split(".").first)
      end
      context = {database: @database, job_id: row.fetch("active_job_id"), id: row.fetch("id"), execution_id: row.fetch("execution_id"),
                 batch_id: row["batch_id"], batch_callback_id: row["batch_callback_id"], stopping: stopping}
      begin
        Current.with(context) do
          ActiveSupport::Notifications.subscribed(subscriber, /\A(?:enqueue_retry|discard|retry_stopped)\.active_job\z/) do
            wrap_execution { ActiveJob::Base.execute(row.fetch("payload")) }
          end
        end
      rescue => exception
        error_event = "unhandled" unless error_event == "retry_stopped" && error.equal?(exception)
        error = exception
        state = "discarded"
        unhandled = true
      end
      state, error, forced = finish(row, state, error, error_event)
      raise error if raise_errors && unhandled && !forced

      Result.new(job_id: row.fetch("id"), state: state, error: error)
    end

    def wrap_execution(&block)
      if defined?(Rails.application) && Rails.application
        Rails.application.executor.wrap(&block)
      else
        yield
      end
    end

    def finish(row, state, error, error_event)
      details = error && JSON.generate("class" => error.class.name, "message" => error.message, "backtrace" => error.backtrace || [])
      batch_ids = []
      forced = false
      ConcurrencyClaims.new(@database).finish(id: row.fetch("id"), execution_id: row.fetch("execution_id")) do |connection|
        current = connection.exec_params("SELECT state, execution_id, batch_id, batch_callback_id, finished_at, error FROM decent_jobs WHERE id = $1 FOR UPDATE", [row.fetch("id")]).first
        raise "Execution ownership lost" unless current && current["execution_id"] == row.fetch("execution_id")

        if current["finished_at"]
          state = current.fetch("state")
          stored_error = JSON.parse(current["error"] || "{}")
          error = Job::DiscardJobError.new(stored_error["message"]) if stored_error["class"] == "DecentJob::Job::DiscardJobError"
          forced = true
          next
        end

        state = "retried" if current["state"] == "ready"
        if state == "retried"
          connection.exec_params("UPDATE decent_jobs SET error = $2, error_event = $3 WHERE id = $1", [row.fetch("id"), details, error_event])
        else
          connection.exec_params(<<~SQL, [row.fetch("id"), state, details, error_event])
            UPDATE decent_jobs SET state = $2, finished_at = clock_timestamp(),
              updated_at = clock_timestamp(), error = $3, error_event = $4 WHERE id = $1
          SQL
        end
        connection.exec_params(<<~SQL, [row.fetch("execution_id"), state, details, current["batch_id"], error_event])
          UPDATE decent_job_executions SET state = $2, finished_at = clock_timestamp(), error = $3, batch_id = $4, error_event = $5 WHERE id = $1
        SQL
        batch_ids = current.values_at("batch_id", "batch_callback_id").compact.uniq
        BatchStore.new(@database).request(connection, batch_ids, job_id: row.fetch("id"), discarded: state == "discarded")
      end
      batch_ids.sort.each { |id| BatchStore.new(@database).reconcile(id) }
      [state, error, forced]
    end
  end
end
