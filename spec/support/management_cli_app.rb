# frozen_string_literal: true

require "json"

class ManagementProcessJob < ActiveJob::Base
  def perform(mode, started, release, result)
    File.write(started, provider_job_id)
    if mode == "shutdown"
      sleep 0.01 until (reason = queue_adapter.stopping?)
      File.write(result, JSON.generate(reason: reason))
      return
    end
    sleep 0.01 until File.exist?(release)
    reason = queue_adapter.stopping?
    retry_job(wait: 0) if mode == "retry"
    File.write(result, JSON.generate(reason: reason, successfully_enqueued: successfully_enqueued?))
  end
end

if ENV["DECENT_JOB_MANAGEMENT_SPEC_CONNECTION"]
  ActiveJob::Base.logger = Logger.new(File::NULL)
  ManagementProcessJob.queue_adapter = :decent_job
  DecentJob.configure do |config|
    config.database_url = JSON.parse(ENV.fetch("DECENT_JOB_MANAGEMENT_SPEC_CONNECTION"))
    config.max_threads = 1
    config.poll_interval = 0.02
    config.enable_listen_notify = false
    config.on_thread_error = ->(error) { File.write(ENV.fetch("DECENT_JOB_MANAGEMENT_SPEC_ERROR"), error.full_message) }
  end
end
