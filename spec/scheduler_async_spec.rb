# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

class SchedulerLocalJob < RecordingJob
  include DecentJob::ActiveJobExtensions::NotifyOptions

  self.decent_job_notify = false
end

class SchedulerFutureRetryJob < SchedulerLocalJob
  retry_on RuntimeError, wait: ->(_executions) { 0.15 }, attempts: 2

  def perform(value)
    raise "retry later" if executions == 1
    super
  end
end

describe "local async scheduling" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    DecentJob.configuration.poll_interval = -1
    DecentJob.configuration.max_cache = 0
    DecentJob.configuration.enable_listen_notify = false
    DecentJob.configuration.max_threads = 1
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    SchedulerLocalJob.queue_adapter = @adapter
    SchedulerFutureRetryJob.queue_adapter = @adapter
    SchedulerLocalJob.events = Queue.new
    @adapter.start_async
  end

  after do
    @adapter.shutdown(timeout: 5)
    DecentJob.reset!
  end

  it "retains local future hints when both PostgreSQL notification switches are off" do
    sleep 0.1
    SchedulerLocalJob.set(wait: 0.2).perform_later("local future")
    _(Timeout.timeout(3) { SchedulerLocalJob.events.pop }).must_equal "local future"
  end

  it "waits for outer commit and drops hints for rolled-back jobs" do
    database.transaction do
      SchedulerLocalJob.set(wait: 0.05).perform_later("committed")
      sleep 0.15
      _(SchedulerLocalJob.events.size).must_equal 0
    end
    _(Timeout.timeout(3) { SchedulerLocalJob.events.pop }).must_equal "committed"
    _ {
      database.transaction do
        SchedulerLocalJob.set(wait: 0.05).perform_later("rolled back")
        raise "rollback"
      end
    }.must_raise RuntimeError
    sleep 0.1
    _(SchedulerLocalJob.events.size).must_equal 0
  end
  it "rearms a future retry without PostgreSQL notifications or startup discovery" do
    SchedulerFutureRetryJob.perform_later("retried locally")
    _(Timeout.timeout(3) { SchedulerLocalJob.events.pop }).must_equal "retried locally"
  end
end
