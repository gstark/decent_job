# frozen_string_literal: true

require "decent_job/active_job"
require "json"

class SupervisorProcessJob < ActiveJob::Base
  def perform(started, release = nil, finished = nil)
    global_pid = DecentJob.database.with_connection { |connection| connection.backend_pid }

    explicit_pid = if defined?(SUPERVISOR_CACHED_DATABASE)
      SUPERVISOR_CACHED_DATABASE.with_connection { |connection| connection.backend_pid }
    end

    File.write(started, JSON.generate(pid: Process.pid, queue: queue_name, global_backend: global_pid, explicit_backend: explicit_pid))

    if release
      sleep 0.01 until File.exist?(release)
    end

    File.write(finished, "finished") if finished
  end
end

if ENV["DECENT_JOB_SUPERVISOR_SPEC_CONNECTION"]
  directory = ENV.fetch("DECENT_JOB_SUPERVISOR_SPEC_DIRECTORY")
  connection_options = JSON.parse(ENV.fetch("DECENT_JOB_SUPERVISOR_SPEC_CONNECTION"), symbolize_names: true)

  DecentJob.configure do |configuration|
    configuration.database_url = connection_options
    configuration.max_threads = 1
    configuration.poll_interval = 0.02
    configuration.process_heartbeat_interval = 0.05
    configuration.advisory_lock_heartbeat = true
    configuration.cleanup_interval_jobs = false
    configuration.cleanup_interval_seconds = false
    configuration.on_thread_error = ->(error) { File.open(File.join(directory, "errors"), "a") { |file| file.puts "#{error.class}: #{error.message}" } }
  end

  SupervisorProcessJob.queue_adapter = :decent_job

  if ENV["DECENT_JOB_SUPERVISOR_PRELOAD"]
    SUPERVISOR_CACHED_DATABASE = DecentJob::Database.new(url: connection_options)
    global_backend = SupervisorProcessJob.queue_adapter.database.with_connection { |connection| connection.backend_pid }
    explicit_backend = SUPERVISOR_CACHED_DATABASE.with_connection { |connection| connection.backend_pid }
    File.write(File.join(directory, "preloaded"), JSON.generate(global: global_backend, explicit: explicit_backend))
  end

  DecentJob.before_supervisor_fork do
    File.open(File.join(directory, "forks"), "a") { |file| file.puts Process.pid }
    SUPERVISOR_CACHED_DATABASE.with_connection { |connection| connection.exec("SELECT 1") } if defined?(SUPERVISOR_CACHED_DATABASE)
  end

  DecentJob.before_subprocess_boot do
    File.write(File.join(directory, "child-#{Process.pid}"), Process.ppid.to_s)
    sleep 15 if ENV["DECENT_JOB_SUPERVISOR_BLOCK_BOOT"]
  end
end
