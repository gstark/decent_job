# frozen_string_literal: true

require "decent_job"
require "json"
require "timeout"

class ForcedShutdownProcessJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::InterruptErrors

  discard_on DecentJob::InterruptError

  def perform(directory, mode)
    database = DecentJob::Current.execution.fetch(:database)
    database.transaction do |connection|
      connection.exec("INSERT INTO forced_shutdown_probe DEFAULT VALUES")
      File.write(File.join(directory, "started.json"), JSON.generate(backend_pid: connection.backend_pid))
      begin
        if mode == "postgres"
          connection.exec("SELECT pg_sleep(60)")
        else
          sleep 60
        end
      ensure
        File.write(File.join(directory, "cleanup"), "started")
        sleep 0.01 until File.exist?(File.join(directory, "release"))
      end
    end
    File.write(File.join(directory, "completed"), "unexpected completion")
  end
end

if $PROGRAM_NAME == __FILE__
  ActiveJob::Base.logger = Logger.new(File::NULL)
  directory = ENV.fetch("FORCED_SHUTDOWN_DIRECTORY")
  errors = []
  database = DecentJob::Database.new(url: JSON.parse(ENV.fetch("FORCED_SHUTDOWN_DATABASE")), pool_size: 1)
  worker = DecentJob::Worker.new(database: database, max_threads: 1, poll_interval: 0.02,
    enable_listen_notify: false, advisory_lock_heartbeat: true,
    cleanup_interval_jobs: false, cleanup_interval_seconds: false,
    on_thread_error: ->(error) { errors << error.full_message })
  events = []
  subscriber = ActiveSupport::Notifications.subscribe("scheduler_shutdown_kill.decent_job") do |event|
    events << event.payload.fetch(:active_job_ids)
  end
  worker.start
  Timeout.timeout(10) { sleep 0.01 until File.exist?(File.join(directory, "force")) }
  drained = worker.shutdown(timeout: 0, force: true)
  File.write(File.join(directory, "forced.json"), JSON.generate(drained: drained, ids: worker.interrupted_job_ids, events: events))
  Timeout.timeout(10) { sleep 0.01 until File.exist?(File.join(directory, "release")) }
  drained = worker.shutdown(timeout: 5)
  backend_pid = database.with_connection do |connection|
    connection.exec("SELECT 1")
    connection.backend_pid
  end
  File.write(File.join(directory, "drained.json"), JSON.generate(drained: drained, pool: database.stats,
    backend_pid: backend_pid, errors: errors))
  ActiveSupport::Notifications.unsubscribe(subscriber)
  database.close
end
