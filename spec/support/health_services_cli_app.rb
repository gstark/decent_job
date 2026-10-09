# frozen_string_literal: true

require "decent_job"
require "json"

class HealthServicesJob < ActiveJob::Base
  def perform(started, release, finished)
    File.write(started, Process.pid.to_s)
    sleep 0.01 until File.exist?(release)
    File.write(finished, "finished")
  end
end

if ENV["DECENT_JOB_HEALTH_SPEC_CONNECTION"]
  DecentJob.configure do |configuration|
    configuration.database_url = JSON.parse(ENV.fetch("DECENT_JOB_HEALTH_SPEC_CONNECTION"), symbolize_names: true)
    configuration.max_threads = 1
    configuration.poll_interval = 0.02
    configuration.process_heartbeat_interval = 0.03
    configuration.cleanup_interval_jobs = false
    configuration.cleanup_interval_seconds = false
    configuration.probe_port = Integer(ENV.fetch("DECENT_JOB_HEALTH_CONFIG_PORT")) if ENV["DECENT_JOB_HEALTH_CONFIG_PORT"]
    configuration.on_thread_error = ->(error) { File.open(ENV.fetch("DECENT_JOB_HEALTH_ERRORS"), "a") { |file| file.puts "#{error.class}: #{error.message}" } }
    if ENV["DECENT_JOB_HEALTH_CUSTOM_APP"]
      configuration.probe_app = ->(env) { [207, {"Content-Type" => "text/plain"}, ["#{env.fetch("REQUEST_METHOD")} #{env.fetch("PATH_INFO")}?#{env.fetch("QUERY_STRING")} café"]] }
    end
    if ENV["DECENT_JOB_HEALTH_BLOCKING_APP"]
      configuration.probe_app = lambda do |env|
        if env.fetch("PATH_INFO") == "/hold"
          directory = ENV.fetch("DECENT_JOB_HEALTH_BLOCKING_APP")
          File.write(File.join(directory, "probe-started"), "started")
          sleep 0.01 until File.exist?(File.join(directory, "probe-release"))
          [200, {}, ["drained"]]
        else
          DecentJob::ProbeServer.default_app.call(env)
        end
      end
    end
  end
  HealthServicesJob.queue_adapter = :decent_job
end
