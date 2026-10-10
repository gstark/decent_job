# frozen_string_literal: true

require "decent_job/active_job"
require "json"

class CronProcessJob < ActiveJob::Base
  def perform(marker)
    File.open(marker, "a") do |file|
      file.flock(File::LOCK_EX)
      file.puts JSON.generate(job_id: job_id, provider_job_id: provider_job_id)
    end
  end
end

if ENV["DECENT_JOB_CRON_SPEC_CONNECTION"]
  ActiveJob::Base.logger = Logger.new(File::NULL)
  CronProcessJob.queue_adapter = :decent_job

  DecentJob.configure do |configuration|
    configuration.database_url = JSON.parse(ENV.fetch("DECENT_JOB_CRON_SPEC_CONNECTION"))
    configuration.queues = "cron"
    configuration.max_threads = 1
    configuration.poll_interval = 0.02
    configuration.enable_listen_notify = false
    configuration.enable_cron = ENV["DECENT_JOB_CRON_SPEC_ENABLED"] == "true"
    configuration.cron = {
      pulse: {
        cron: ENV.fetch("DECENT_JOB_CRON_SPEC_EXPRESSION", "* * * * * * UTC"),
        class: if ENV["DECENT_JOB_CRON_SPEC_BLOCK"] == "true"
                 -> {
                   File.write(ENV.fetch("DECENT_JOB_CRON_SPEC_STARTED"), "entered")
                   sleep 60
                   CronProcessJob
                 }
               else
                 CronProcessJob
               end,
        args: [ENV.fetch("DECENT_JOB_CRON_SPEC_MARKER")],
        set: {queue: "cron"}
      }
    }
  end
end
