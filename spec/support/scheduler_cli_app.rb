# frozen_string_literal: true

require "decent_job/active_job"
require "json"

class SchedulerProcessJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::NotifyOptions

  def perform(value, release: nil)
    append(ENV.fetch("SCHEDULER_STARTED"), value)
    sleep 0.01 while release && !File.exist?(release)
    append(ENV.fetch("SCHEDULER_FINISHED"), value)
  end

  private

  def append(path, value)
    File.open(path, "a") { |file| file.write(JSON.generate(value: value, at: Time.now.to_f, pid: Process.pid) + "\n") }
  end
end

if ENV["SCHEDULER_DATABASE"]
  DecentJob.configure do |config|
    config.database_url = JSON.parse(ENV.fetch("SCHEDULER_DATABASE"), symbolize_names: true)
    config.max_threads = 1
    config.process_heartbeat_interval = 0.05
    config.on_thread_error = ->(error) { File.open(ENV.fetch("SCHEDULER_ERRORS"), "a") { |file| file.puts("#{error.class}: #{error.message}") } }
  end

  SchedulerProcessJob.queue_adapter = :decent_job
  ActiveJob::Base.logger = Logger.new(File::NULL)
end
