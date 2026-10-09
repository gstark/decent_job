# frozen_string_literal: true

require "decent_job/active_job"
require "json"
require "timeout"

class RuntimeUtilitiesJob < ActiveJob::Base
  def perform(directory)
    File.write(File.join(directory, "job.json"), JSON.generate(
      id: job_id,
      current: DecentJob::Current.execution.fetch(:job_id),
      nested: Fiber.new { DecentJob::Current.execution&.fetch(:job_id) }.resume
    ))
    sleep 0.01 until File.exist?(File.join(directory, "release_job"))
    File.write(File.join(directory, "job_stopping"), DecentJob.current_thread_shutting_down?.to_s)
  end
end

if $PROGRAM_NAME == __FILE__
  ActiveJob::Base.logger = Logger.new(File::NULL)
  ActiveSupport::IsolatedExecutionState.isolation_level = ENV.fetch("UTILITY_ISOLATION").to_sym
  directory = ENV.fetch("UTILITY_DIRECTORY")
  write = ->(name, data) { File.write(File.join(directory, "#{name}.json"), JSON.generate(data)) }
  DecentJob.configure do |config|
    config.database_url = JSON.parse(ENV.fetch("UTILITY_DATABASE"))
    config.execution_mode = :async_all
    config.max_threads = 1
    config.poll_interval = -1
    config.process_heartbeat_interval = 0.03
    config.enable_cron = true
    config.cleanup_interval_jobs = false
    config.cleanup_interval_seconds = false
    config.cron = {hold: {
      cron: ->(previous) { previous ? previous + 3600 : Time.now },
      class: -> {
        write.call("cron", current: DecentJob::CronEntry.current_context.fetch(:key),
          nested: Fiber.new { DecentJob::CronEntry.current_context&.fetch(:key) }.resume)
        sleep 0.01 until File.exist?(File.join(directory, "release_cron"))
        File.write(File.join(directory, "cron_finished"), "true")
        nil
      }
    }}
  end
  pool = DecentJob.database
  pool.resize(2)
  adapter = DecentJob::Adapter.new(database: pool, execution_mode: :async_all)
  RuntimeUtilitiesJob.queue_adapter = adapter
  worker = adapter.start_async
  wake = worker.method(:wake)
  worker.define_singleton_method(:wake) do |state = nil|
    if state.is_a?(Hash) && state["queue_name"] == "utility-probe"
      File.write(File.join(directory, "notified"), "true")
    end
    wake.call(state)
  end
  Timeout.timeout(12) { sleep 0.01 until worker.connected? && pool.stats.fetch(:active) == 2 }
  write.call("ready", process_id: worker.process_id, pool: pool.stats)
  Timeout.timeout(12) { sleep 0.01 until File.exist?(File.join(directory, "stop")) }
  write.call("stopping", stopped: DecentJob.shutdown(timeout: 0), started: DecentJob.started?,
    pool: pool.stats)
  drained = DecentJob.shutdown(timeout: 10)
  write.call("finished", drained: drained, pool: pool.stats)
  DecentJob.reset!
end
