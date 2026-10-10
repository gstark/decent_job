# frozen_string_literal: true

# Shared configuration for the example worker, dashboard, and seeder.
require "decent_job"

DecentJob.configure do |config|
  config.database_url = ENV.fetch("DECENT_JOB_DATABASE_URL", "postgres:///decent_job_example")
  config.execution_mode = :external
  config.max_threads = 5
  config.poll_interval = 5
  config.enable_listen_notify = true
  config.enable_pauses = true
  config.preserve_job_records = true
  config.cleanup_preserved_jobs_before_seconds_ago = 6 * 3600
  config.cron = {
    heartbeat: {
      class: "HeartbeatJob",
      cron: "every minute",
      description: "Proves the scheduler is alive"
    },
    hourly_digest: {
      class: "DigestJob",
      cron: "0 * * * *",
      args: -> { [Time.now.utc.strftime("%Y-%m-%dT%H")] },
      set: {queue: "reports", priority: 5},
      description: "Hourly activity digest"
    },
    nightly_cleanup: {
      class: "CleanupJob",
      cron: "every day at 3:30am",
      kwargs: {older_than_hours: 6},
      description: "Deletes preserved records older than six hours",
      enabled_by_default: false
    }
  }
end
