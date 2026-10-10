# frozen_string_literal: true

require "decent_job/active_job"
require "json"

class RuntimeEventsProcessJob < ActiveJob::Base
  def perform(action)
    raise "runtime worker failure" if action == "fail"
    action
  end
end

if ENV["RUNTIME_EVENTS_DATABASE"]
  append = lambda do |details|
    File.open(ENV.fetch("RUNTIME_EVENTS_LOG"), "a") do |file|
      file.flock(File::LOCK_EX)
      file.write(JSON.generate(details) + "\n")
    end
  end

  DecentJob.configure do |config|
    config.database_url = JSON.parse(ENV.fetch("RUNTIME_EVENTS_DATABASE"), symbolize_names: true)
    config.on_thread_error = ->(error) { append.call(name: "error_callback", error: error.class.name, message: error.message) }
  end

  RuntimeEventsProcessJob.queue_adapter = :decent_job
  ActiveJob::Base.logger = Logger.new(File::NULL)

  ActiveSupport::Notifications.subscribe(/\A(?:finished_job_task|finished_timer_task|notifier_\w+)\.decent_job\z/) do |event|
    payload = event.payload
    result = payload[:result]
    details = {name: event.name, pid: payload[:process_id], thread: payload[:thread_name],
               queues: payload[:queues], pool_index: payload[:pool_index], time_class: payload[:time]&.class&.name,
               error: payload[:error]&.class&.name, raw: payload[:payload]}

    if event.name == "finished_job_task.decent_job"
      details.merge!(job_id: result&.job_id, state: result&.state,
        unhandled_error: result&.unhandled_error&.class&.name,
        has_result: payload.key?(:result), scheduler: payload[:scheduler].is_a?(DecentJob::Worker))
    elsif event.name == "finished_timer_task.decent_job"
      details[:result] = result
    end

    append.call(details)
  end
end
