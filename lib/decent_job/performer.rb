# frozen_string_literal: true

module DecentJob
  class Performer
    Result = Data.define(:job_id, :state, :error)

    def initialize(database: DecentJob.database)
      @database = database
      @store = Store.new(database)
      @claim = Claim.new(database)
    end

    def perform_one(queues: "*", job_id: nil, raise_errors: false, stopping: -> { false })
      @claim.with_next(queues: queues, job_id: job_id, stopping: stopping) do |row|
        execute(row, raise_errors: raise_errors)
      end
    end

    private

    def execute(row, raise_errors:)
      error = nil
      state = "succeeded"
      unhandled = false
      current_thread = Thread.current
      subscriber = lambda do |name, _start, _finish, _id, payload|
        next unless Thread.current == current_thread && payload[:job]&.job_id == row.fetch("active_job_id")

        state = name.start_with?("enqueue_retry") ? "retried" : "discarded"
        error = payload[:error]
      end
      context = {database: @database, job_id: row.fetch("active_job_id"), id: row.fetch("id"), execution_id: row.fetch("execution_id")}
      begin
        Current.with(context) do
          ActiveSupport::Notifications.subscribed(subscriber, /\A(?:enqueue_retry|discard|retry_stopped)\.active_job\z/) do
            wrap_execution { ActiveJob::Base.execute(row.fetch("payload")) }
          end
        end
      rescue => exception
        error = exception
        state = "discarded"
        unhandled = true
      end
      state = finish(row, state, error)
      raise error if raise_errors && unhandled

      Result.new(job_id: row.fetch("id"), state: state, error: error)
    end

    def wrap_execution(&block)
      if defined?(Rails.application) && Rails.application
        Rails.application.executor.wrap(&block)
      else
        yield
      end
    end

    def finish(row, state, error)
      details = error && JSON.generate("class" => error.class.name, "message" => error.message, "backtrace" => error.backtrace || [])
      @database.transaction do |connection|
        current = connection.exec_params("SELECT state, execution_id FROM decent_jobs WHERE id = $1 FOR UPDATE", [row.fetch("id")]).first
        raise "Execution ownership lost" unless current && current["execution_id"] == row.fetch("execution_id")

        state = "retried" if current["state"] == "ready"
        unless state == "retried"
          connection.exec_params(<<~SQL, [row.fetch("id"), state, details])
            UPDATE decent_jobs SET state = $2, finished_at = clock_timestamp(),
              updated_at = clock_timestamp(), error = $3 WHERE id = $1
          SQL
        end
        connection.exec_params(<<~SQL, [row.fetch("execution_id"), state, details])
          UPDATE decent_job_executions SET state = $2, finished_at = clock_timestamp(), error = $3 WHERE id = $1
        SQL
      end
      state
    end
  end
end
