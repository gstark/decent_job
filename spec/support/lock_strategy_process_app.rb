# frozen_string_literal: true

require "decent_job"
require "json"

module LockStrategyProcessLog
  def self.write(details)
    File.open(ENV.fetch("LOCK_STRATEGY_PROCESS_LOG"), "a") do |file|
      file.flock(File::LOCK_EX)
      file.write(JSON.generate(details.merge(role: ENV.fetch("LOCK_STRATEGY_PROCESS_ROLE"), pid: Process.pid)) + "\n")
    end
  end
end

class LockStrategyProcessJob < ActiveJob::Base
  def perform(value, gate = nil)
    database = self.class.queue_adapter.database
    record = DecentJob::Store.new(database).find(provider_job_id)
    advisory = database.with_connection do |connection|
      connection.exec_params(<<~SQL, [provider_job_id]).getvalue(0, 0) == "t"
        SELECT EXISTS (
          SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND granted AND objsubid = 1
            AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
            AND ((classid::bigint << 32) | objid::bigint) = hashtextextended(current_schema() || '.decent_jobs.' || $1::text, 0)
        )
      SQL
    end
    LockStrategyProcessLog.write(type: "started", value: value, job_id: provider_job_id,
      lock_type: record.fetch("lock_type"), owner: record.fetch("locked_by_id"), advisory: advisory)
    sleep 0.01 while gate && !File.exist?(gate)
    LockStrategyProcessLog.write(type: "body_finished", value: value, job_id: provider_job_id)
  end
end

class LockStrategyLimitedProcessJob < LockStrategyProcessJob
  include DecentJob::ActiveJobExtensions::Concurrency

  decent_job_control_concurrency_with(key: "lock-strategy-process-limit", perform_limit: 1)
end

if ENV["LOCK_STRATEGY_PROCESS_CONNECTION"]
  DecentJob.configure do |config|
    config.database_url = JSON.parse(ENV.fetch("LOCK_STRATEGY_PROCESS_CONNECTION"), symbolize_names: true)
    config.enable_listen_notify = false
    config.cleanup_interval_jobs = false
    config.cleanup_interval_seconds = false
    config.logger = Logger.new(File::NULL)
    config.on_thread_error = ->(error) { LockStrategyProcessLog.write(type: "error", error: error.class.name, message: error.message) }
  end
  ActiveJob::Base.logger = Logger.new(File::NULL)
  adapter = DecentJob::Adapter.new(execution_mode: :external)
  LockStrategyProcessJob.queue_adapter = adapter
  LockStrategyLimitedProcessJob.queue_adapter = adapter
  ActiveSupport::Notifications.subscribe("finished_job_task.decent_job") do |event|
    result = event.payload[:result]
    LockStrategyProcessLog.write(type: result ? "completed" : "empty", job_id: result&.job_id, state: result&.state)
  end
end
