# frozen_string_literal: true

require "decent_job/active_job"
require "json"

module DequeueProcessLog
  def self.write(details)
    File.open(ENV.fetch("DEQUEUE_PROCESS_LOG"), "a") do |file|
      file.flock(File::LOCK_EX)
      file.write(JSON.generate(details) + "\n")
    end
  end
end

class DequeueProcessJob < ActiveJob::Base
  def perform(value)
    DequeueProcessLog.write(type: "performed", value: value)
  end
end

if ENV["DEQUEUE_PROCESS_DATABASE"]
  DecentJob.configure do |config|
    config.database_url = JSON.parse(ENV.fetch("DEQUEUE_PROCESS_DATABASE"), symbolize_names: true)
    config.enable_listen_notify = false
    config.cleanup_interval_jobs = false
    config.cleanup_interval_seconds = false
    config.dequeue_query_sort = ENV["DEQUEUE_APP_SORT"] if ENV["DEQUEUE_APP_SORT"]
    config.queue_select_limit = ENV["DEQUEUE_APP_LIMIT"] if ENV["DEQUEUE_APP_LIMIT"]
    config.on_thread_error = ->(error) { DequeueProcessLog.write(type: "error", message: error.message) }
  end

  DequeueProcessJob.queue_adapter = :decent_job
  ActiveJob::Base.logger = Logger.new(File::NULL)

  ActiveSupport::Notifications.subscribe("finished_job_task.decent_job") do |event|
    DequeueProcessLog.write(type: "empty") unless event.payload[:result]
  end
end
