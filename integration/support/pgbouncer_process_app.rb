# frozen_string_literal: true

require "decent_job/active_job"
require "json"

module PgBouncerProcessLog
  def self.write(details)
    File.open(ENV.fetch("PGBOUNCER_PROCESS_LOG"), "a") do |file|
      file.flock(File::LOCK_EX)
      file.puts(JSON.generate(details.merge(role: ENV.fetch("PGBOUNCER_PROCESS_ROLE"))))
    end
  end
end

class PgBouncerProcessJob < ActiveJob::Base
  def perform(value, gate = nil)
    row = DecentJob::Store.new(self.class.queue_adapter.database).find(provider_job_id)
    PgBouncerProcessLog.write(type: "started", value: value, job_id: provider_job_id,
      owner: row.fetch("locked_by_id"), lock_type: row.fetch("lock_type"))
    sleep 0.01 while gate && !File.exist?(gate)
    PgBouncerProcessLog.write(type: "body_finished", job_id: provider_job_id)
  end
end

if ENV["PGBOUNCER_PROCESS_CONNECTION"]
  DecentJob.configure do |config|
    config.database_url = JSON.parse(ENV.fetch("PGBOUNCER_PROCESS_CONNECTION"), symbolize_names: true)
    config.lock_strategy = :skiplocked
    config.enable_listen_notify = false
    config.advisory_lock_heartbeat = false
    config.process_heartbeat_interval = 0.05
    config.poll_interval = 0.02
    config.cleanup_interval_jobs = false
    config.cleanup_interval_seconds = false
    config.logger = Logger.new(File::NULL)
    config.on_thread_error = ->(error) { PgBouncerProcessLog.write(type: "error", error: error.class.name, message: error.message) }
  end
  ActiveJob::Base.logger = Logger.new(File::NULL)
  PgBouncerProcessJob.queue_adapter = DecentJob::Adapter.new(execution_mode: :external)
  ActiveSupport::Notifications.subscribe("finished_job_task.decent_job") do |event|
    result = event.payload[:result]
    PgBouncerProcessLog.write(type: result ? "completed" : "empty", job_id: result&.job_id, state: result&.state)
  end
end
