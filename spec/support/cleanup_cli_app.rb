# frozen_string_literal: true

require "json"

class CleanupProcessJob < ActiveJob::Base
  def perform(marker = nil, fail_execution = false)
    File.write(marker, "performed") if marker
    raise "cleanup failure" if fail_execution
  end
end

if ENV["DECENT_JOB_CLEANUP_SPEC_CONNECTION"]
  ActiveJob::Base.logger = Logger.new(File::NULL)
  CleanupProcessJob.queue_adapter = :decent_job
  DecentJob.configure do |config|
    config.database_url = JSON.parse(ENV.fetch("DECENT_JOB_CLEANUP_SPEC_CONNECTION"))
    config.max_threads = 1
    config.poll_interval = 0.02
    config.enable_listen_notify = false
    config.cleanup_preserved_jobs_before_seconds_ago = Float(ENV["DECENT_JOB_CLEANUP_SPEC_APP_AGE"]) if ENV["DECENT_JOB_CLEANUP_SPEC_APP_AGE"]
    config.cleanup_discarded_jobs = false if ENV["DECENT_JOB_CLEANUP_SPEC_APP_KEEP_DISCARDED"]
    config.on_thread_error = ->(error) { warn error.full_message }
  end
  if ENV["DECENT_JOB_CLEANUP_SPEC_EVENT"]
    ActiveSupport::Notifications.subscribe("cleanup_preserved_jobs.decent_job") do |_name, _start, _finish, _id, payload|
      path = ENV.fetch("DECENT_JOB_CLEANUP_SPEC_EVENT")
      temporary = "#{path}.#{Process.pid}.tmp"
      File.write(temporary, JSON.generate(payload))
      File.rename(temporary, path)
    end
  end
end
