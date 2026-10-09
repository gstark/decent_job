# frozen_string_literal: true

require "json"

class CliRecordingJob < ActiveJob::Base
  def perform(marker, duration = 0)
    configuration = DecentJob.configuration
    File.write(marker, JSON.generate({queues: configuration.queues,
      max_threads: configuration.max_threads, poll_interval: configuration.poll_interval,
      shutdown_timeout: configuration.shutdown_timeout,
      listen_notify: configuration.enable_listen_notify}))
    sleep duration
    File.write("#{marker}.finished", "done")
  end
end

if ENV["DECENT_JOB_CLI_SPEC_CONNECTION"]
  DecentJob.configure do |configuration|
    configuration.database_url = JSON.parse(ENV.fetch("DECENT_JOB_CLI_SPEC_CONNECTION"))
    configuration.queues = "application-default"
    configuration.max_threads = 4
    configuration.poll_interval = 30
  end
  ActiveJob::Base.queue_adapter = :decent_job
end
