# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"
require "decent_job/worker"

class PausedLabelRetryJob < RecordingJob
  include DecentJob::ActiveJobExtensions::Labels

  retry_on RuntimeError, wait: 0, attempts: 2

  def perform(value)
    if executions == 1
      self.decent_job_labels = ["after-retry"]
      raise "retry with changed labels"
    end
    super
  end
end

describe "persistent pauses" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @pauses = DecentJob::Pauses.new(database)
    RecordingJob.queue_adapter = DecentJob::Adapter.new(database: database)
    PausedLabelRetryJob.queue_adapter = RecordingJob.queue_adapter
    RecordingJob.events = Queue.new
    @performer = DecentJob::Performer.new(database: database, enable_pauses: true)
  end

  it "persists independent queue, class and label pauses" do
    @pauses.pause(queue: :mailers)
    @pauses.pause(job_class: RecordingJob)
    @pauses.pause(label: " email ")
    @pauses.pause(queue: :mailers)
    _(DecentJob::Pauses.new(database).paused).must_equal({queues: ["mailers"], job_classes: ["RecordingJob"], labels: ["email"]})
    _(@pauses.paused?).must_equal true
    @pauses.unpause(queue: "mailers")
    _(@pauses.paused?(queue: "mailers")).must_equal false
    _(@pauses.paused?(job_class: RecordingJob)).must_equal true
  end

  it "requires exactly one nonblank target for mutation" do
    _ { @pauses.pause }.must_raise ArgumentError
    _ { @pauses.pause(queue: " ") }.must_raise ArgumentError
    _ { @pauses.pause(queue: "x", label: "y") }.must_raise ArgumentError
    _ { @pauses.unpause }.must_raise ArgumentError
    _ { @pauses.paused?(queue: "x", job_class: "Y") }.must_raise ArgumentError
  end

  it "leaves paused jobs queued until unpaused" do
    job = RecordingJob.set(queue: "mailers").perform_later("paused")
    @pauses.pause(queue: "mailers")
    _(@performer.perform_one).must_be_nil
    _(DecentJob::Store.new(database).find(job.provider_job_id).fetch("state")).must_equal "ready"
    @pauses.unpause(queue: "mailers")
    _(@performer.perform_one.state).must_equal "succeeded"
    _(RecordingJob.events.pop).must_equal "paused"
  end

  it "notifies a sleeping worker when a queue is unpaused" do
    previous = DecentJob.configuration.enable_pauses
    DecentJob.configuration.enable_pauses = true
    @pauses.pause(queue: "default")
    RecordingJob.perform_later("unpaused notification")
    connections = Queue.new
    database.define_singleton_method(:connect) do
      super().tap { |connection| connections << connection.backend_pid }
    end
    worker = DecentJob::Worker.new(database: database, max_threads: 1, poll_interval: 60).start
    listener_pid = Timeout.timeout(5) { connections.pop }
    Timeout.timeout(5) do
      until @admin.exec_params("SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND query = 'LISTEN decent_job'", [listener_pid]).ntuples.positive?
        sleep 0.01
      end
    end
    sleep 0.1
    _(RecordingJob.events.size).must_equal 0
    @pauses.unpause(queue: "default")
    _(Timeout.timeout(5) { RecordingJob.events.pop }).must_equal "unpaused notification"
  ensure
    worker&.shutdown(timeout: 5)
    DecentJob.configuration.enable_pauses = previous
  end

  it "matches a paused class or any stored label" do
    job = RecordingJob.perform_later("labels")
    @pauses.pause(job_class: RecordingJob)
    _(@performer.perform_one).must_be_nil
    @pauses.unpause(job_class: RecordingJob)
    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET labels = ARRAY['email', 'account'] WHERE id = $1", [job.provider_job_id])
    end
    @pauses.pause(label: "account")
    _(@performer.perform_one).must_be_nil
    @pauses.unpause(label: "account")
    _(@performer.perform_one.state).must_equal "succeeded"
  end

  it "ignores pauses when support is disabled" do
    RecordingJob.perform_later("unpaused")
    @pauses.pause(job_class: RecordingJob)
    _(DecentJob::Performer.new(database: database, enable_pauses: false).perform_one.state).must_equal "succeeded"
  end

  it "returns the reason that an Active Job is paused" do
    job = RecordingJob.new
    @pauses.pause(queue: "default")
    _(@pauses.paused?(job_instance: job)).must_equal :queue_name_paused
    @pauses.unpause(queue: "default")
    @pauses.pause(job_class: RecordingJob)
    _(@pauses.paused?(job_instance: job)).must_equal :job_class_paused
  end

  it "filters a retry using its updated labels" do
    @pauses.pause(label: "after-retry")
    job = PausedLabelRetryJob.set(decent_job_labels: ["before-retry"]).perform_later("resumed")
    _(@performer.perform_one.state).must_equal "retried"
    _(@performer.perform_one).must_be_nil
    _(DecentJob::Store.new(database).find(job.provider_job_id).fetch("labels")).must_equal ["after-retry"]
    @pauses.unpause(label: "after-retry")
    _(@performer.perform_one.state).must_equal "succeeded"
    _(RecordingJob.events.pop).must_equal "resumed"
  end
end
