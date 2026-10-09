# frozen_string_literal: true

require "decent_job"
require "json"
require "timeout"

class LifecycleProcessJob < ActiveJob::Base
  def perform(directory, stage)
    File.write(File.join(directory, stage), provider_job_id)
    if stage == "started"
      sleep 0.01 until File.exist?(File.join(directory, "release"))
    end
  end
end

if $PROGRAM_NAME == __FILE__
  ActiveJob::Base.logger = Logger.new(File::NULL)
  directory = ENV.fetch("LIFECYCLE_DIRECTORY")
  DecentJob.configure do |config|
    config.database_url = JSON.parse(ENV.fetch("LIFECYCLE_DATABASE"))
    config.execution_mode = :async_all
    config.max_threads = 1
    config.poll_interval = 0.02
    config.enable_listen_notify = false
    config.enable_cron = false
    config.cleanup_interval_jobs = false
    config.cleanup_interval_seconds = false
  end
  database = DecentJob.database
  adapter = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
  LifecycleProcessJob.queue_adapter = adapter
  adapter.start_async
  target = (ENV.fetch("LIFECYCLE_SCOPE") == "global") ? DecentJob : adapter
  wait_for = ->(name) { Timeout.timeout(12) { sleep 0.01 until File.exist?(File.join(directory, name)) } }
  write = ->(name, data) { File.write(File.join(directory, "#{name}.json"), JSON.generate(data)) }
  wait_for.call("stop")
  stopped = target.shutdown(timeout: 0)
  restarted = target.restart(timeout: 0)
  followup = LifecycleProcessJob.perform_later(directory, "followup")
  write.call("stopping", stopped: stopped, restarted: !!restarted, followup_id: followup.provider_job_id,
    process_ids: DecentJob::ProcessRecord.active(database: database).map(&:id))
  wait_for.call("release")
  write.call("drained", stopped: target.shutdown(timeout: 5), active: DecentJob::ProcessRecord.active(database: database).map(&:id))
  wait_for.call("restart")
  restarted = target.restart(timeout: 5)
  Timeout.timeout(10) do
    sleep 0.01 until DecentJob::Store.new(database).find(followup.provider_job_id).fetch("finished_at") &&
        DecentJob.stats.fetch(:total_executions_count).positive?
  end
  write.call("resumed", restarted: !!restarted, process_ids: DecentJob::ProcessRecord.active(database: database).map(&:id),
    stats: DecentJob.stats)
  wait_for.call("finish")
  write.call("finished", global: DecentJob.shutdown(timeout: 5), adapter: adapter.shutdown(timeout: 5), pool: database.stats)
  DecentJob.reset!
end
