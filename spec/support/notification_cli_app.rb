# frozen_string_literal: true

require "decent_job/active_job"
require "json"

class NotificationProcessJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::NotifyOptions

  def perform(value)
    File.open(ENV.fetch("NOTIFICATION_RESULTS"), "a") { |file| file.puts(value) }
  end
end

class NotificationRetryProcessJob < NotificationProcessJob
  retry_on RuntimeError, wait: 0, attempts: 2

  def perform(value)
    raise "retry once" if executions == 1

    super
  end
end

if ENV["NOTIFICATION_DATABASE"]
  DecentJob.configure do |config|
    config.database_url = JSON.parse(ENV.fetch("NOTIFICATION_DATABASE"), symbolize_names: true)
    config.max_threads = 1
    config.poll_interval = 60
    config.enable_listen_notify = ENV.fetch("NOTIFICATION_ENABLED", "true") == "true"
    config.process_heartbeat_interval = 0.1
  end

  NotificationProcessJob.queue_adapter = :decent_job
  ActiveJob::Base.logger = Logger.new(File::NULL)
end

if $PROGRAM_NAME == __FILE__
  jobs = JSON.parse(ENV.fetch("NOTIFICATION_JOBS")).map do |attributes|
    klass = attributes.fetch("retry", false) ? NotificationRetryProcessJob : NotificationProcessJob
    job = klass.new(attributes.fetch("value"))
    job.queue_name = attributes.fetch("queue", "default")
    job.decent_job_notify = attributes["notify"] if attributes.key?("notify")
    job
  end

  case ENV.fetch("NOTIFICATION_ACTION", "bulk")
  when "bulk"
    ActiveJob.perform_all_later(jobs)
  when "single"
    jobs.each(&:enqueue)
  when "rollback"
    begin
      DecentJob.database.transaction do
        ActiveJob.perform_all_later(jobs)
        raise "roll back enqueue"
      end
    rescue RuntimeError => error
      raise unless error.message == "roll back enqueue"
    end
  end

  puts JSON.generate(ids: jobs.map(&:provider_job_id))
end
