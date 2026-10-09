# frozen_string_literal: true

require "json"

class ProcessTrackingJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::Concurrency

  decent_job_control_concurrency_with(perform_limit: 1)

  def perform(mode, started, release, result)
    holders = []
    if mode == "pool"
      entered = Queue.new
      holders = 2.times.map do
        Thread.new do
          queue_adapter.database.with_connection do
            entered << true
            sleep 0.01 until File.exist?(release)
          end
        end
      end
      2.times { entered.pop }
    end
    File.write(started, provider_job_id)
    unless mode == "finish"
      sleep 0.01 until File.exist?(release)
    end
    holders.each(&:value)
    File.write(result, JSON.generate(provider_job_id: provider_job_id, pid: Process.pid))
  ensure
    holders&.each(&:join)
  end
end

if ENV["DECENT_JOB_PROCESS_SPEC_CONNECTION"]
  ActiveJob::Base.logger = Logger.new(File::NULL)
  ProcessTrackingJob.queue_adapter = :decent_job
  DecentJob.configure do |config|
    config.database_url = JSON.parse(ENV.fetch("DECENT_JOB_PROCESS_SPEC_CONNECTION"))
    config.max_threads = 1
    config.poll_interval = 0.02
    config.enable_listen_notify = false
    config.cleanup_interval_jobs = false
    config.cleanup_interval_seconds = false
    config.on_thread_error = lambda do |error|
      File.open(ENV.fetch("DECENT_JOB_PROCESS_SPEC_ERRORS"), "a") do |file|
        file.puts JSON.generate(class: error.class.name, message: error.message)
      end
    end
  end
end
