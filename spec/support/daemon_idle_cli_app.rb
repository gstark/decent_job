# frozen_string_literal: true

require "decent_job"
require "json"

class DaemonIdleProcessJob < ActiveJob::Base
  def perform(started, release = nil, finished = nil)
    backend = self.class.queue_adapter.database.with_connection { |connection| connection.backend_pid }
    File.write(started, JSON.generate(pid: Process.pid, cwd: Dir.pwd, backend: backend))
    if release
      sleep 0.01 until File.exist?(release)
    end
    File.write(finished, "finished") if finished
  end
end

if ENV["DECENT_JOB_IDLE_SPEC_CONNECTION"]
  directory = ENV.fetch("DECENT_JOB_IDLE_SPEC_DIRECTORY")
  DecentJob.configure do |configuration|
    configuration.database_url = JSON.parse(ENV.fetch("DECENT_JOB_IDLE_SPEC_CONNECTION"), symbolize_names: true)
    configuration.max_threads = 1
    configuration.poll_interval = 0.02
    configuration.process_heartbeat_interval = 0.05
    configuration.advisory_lock_heartbeat = true
    configuration.cleanup_interval_jobs = false
    configuration.cleanup_interval_seconds = false
    configuration.idle_timeout = Float(ENV.fetch("DECENT_JOB_IDLE_SPEC_CONFIG_TIMEOUT")) if ENV["DECENT_JOB_IDLE_SPEC_CONFIG_TIMEOUT"]
    configuration.on_thread_error = ->(error) { File.open(File.join(directory, "errors"), "a") { |file| file.puts "#{error.class}: #{error.message}" } }
  end
  DaemonIdleProcessJob.queue_adapter = :decent_job
  ActiveJob::Base.logger = Logger.new(File::NULL)
  if ENV["DECENT_JOB_IDLE_SPEC_PRELOAD"]
    backend = DaemonIdleProcessJob.queue_adapter.database.with_connection { |connection| connection.backend_pid }
    File.write(File.join(directory, "preloaded"), backend.to_s)
  end
  at_exit { File.write(File.join(directory, "exit-#{Process.pid}"), "exited") }
  DecentJob.before_subprocess_boot do
    File.write(File.join(directory, "child-#{Process.pid}"), Process.ppid.to_s)
  end
end
