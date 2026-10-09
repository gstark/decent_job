# frozen_string_literal: true

require "decent_job/active_job"
require "json"

class ObservabilityProcessJob < ActiveJob::Base
  def perform
    File.write(ENV.fetch("OBSERVABILITY_STARTED"), JSON.generate(snapshot))
    sleep 0.01 until DecentJob.current_thread_shutting_down?
    File.write(ENV.fetch("OBSERVABILITY_STOPPING"), JSON.generate(snapshot))
  end

  def snapshot
    {priority: Thread.current.priority,
     running: DecentJob.current_thread_running?,
     shutting_down: DecentJob.current_thread_shutting_down?,
     stats: DecentJob.stats}
  end
end

if ENV["OBSERVABILITY_DATABASE"]
  DecentJob.configure do |config|
    config.database_url = JSON.parse(ENV.fetch("OBSERVABILITY_DATABASE"), symbolize_names: true)
    config.max_threads = 1
    config.lower_thread_priority = true
    config.process_heartbeat_interval = 0.05
  end
  ObservabilityProcessJob.queue_adapter = :decent_job
  ActiveJob::Base.logger = Logger.new(File::NULL)
end
