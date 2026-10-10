# frozen_string_literal: true

require "decent_job/active_job"
require "json"

class JobEventsProcessJob < ActiveJob::Base
  retry_on RuntimeError, wait: 0, attempts: 2

  def perform(action)
    raise "retry once" if action == "retry" && executions == 1

    false
  end
end

if ENV["JOB_EVENTS_DATABASE"]
  ActiveSupport::IsolatedExecutionState.isolation_level = ENV.fetch("JOB_EVENTS_ISOLATION").to_sym

  DecentJob.configure do |config|
    config.database_url = JSON.parse(ENV.fetch("JOB_EVENTS_DATABASE"), symbolize_names: true)
    config.preserve_job_records = false
    config.on_thread_error = ->(error) { File.open(ENV.fetch("JOB_EVENTS_ERRORS"), "a") { |file| file.puts("#{error.class}: #{error.message}") } }
  end

  JobEventsProcessJob.queue_adapter = :decent_job
  ActiveJob::Base.logger = Logger.new(File::NULL)
  snapshots = []

  ActiveSupport::Notifications.subscribe(/\A(?:enqueue_job|perform_job)\.decent_job\z/) do |event|
    payload = event.payload
    job = payload.fetch(:job)
    execution = payload[:execution]
    snapshots << [job, execution] if execution

    persisted = execution && DecentJob.database.with_connection do |connection|
      connection.exec_params("SELECT state, finished_at FROM decent_job_executions WHERE id = $1", [execution.id]).first
    end

    details = {name: event.name, id: job.id, active_job_id: job.active_job_id, execution_id: execution&.id,
               pid: payload[:process_id], thread: payload[:thread_name], value: payload[:value],
               has_value: payload.key?(:value), error: payload[:error]&.class&.name,
               error_event: payload[:error_event], retried: payload[:retried], accepted: payload[:accepted],
               persisted: persisted, action: job.payload.fetch("arguments").first}
    File.open(ENV.fetch("JOB_EVENTS_LOG"), "a") { |file| file.puts(JSON.generate(details)) }
  end

  at_exit do
    retained = snapshots.map do |job, execution|
      {id: job.id, job_class: job.job_class, arguments: job.payload.fetch("arguments"),
       execution_id: execution.id, job_id: execution.job_id, number: execution.number,
       started: execution.started_at.iso8601(6), state: execution.state}
    end

    File.write(ENV.fetch("JOB_EVENTS_SNAPSHOTS"), JSON.generate(retained))
  end
end
