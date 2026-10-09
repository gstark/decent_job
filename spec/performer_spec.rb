# frozen_string_literal: true

require "spec_helper"
require "support/jobs"

describe "job execution" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)
    [RecordingJob, RetryingJob, DiscardingJob, FailingJob, BlockingJob].each { |job| job.queue_adapter = @adapter }
    RecordingJob.events = Queue.new
    BlockingJob.started = Queue.new
    BlockingJob.release = Queue.new
    @store = DecentJob::Store.new(database)
    @performer = DecentJob::Performer.new(database: database)
  end

  it "executes through Active Job and records a successful attempt" do
    job = RecordingJob.perform_later("hello")
    result = @performer.perform_one
    _(result.state).must_equal "succeeded"
    _(RecordingJob.events.pop).must_equal "hello"
    _(@store.find(job.provider_job_id).fetch("state")).must_equal "succeeded"
    _(@store.executions(job.provider_job_id).map { |row| row.fetch("state") }).must_equal ["succeeded"]
    _(@performer.perform_one).must_be_nil
  end

  it "respects due times and ascending priority" do
    RecordingJob.set(wait: 3600).perform_later("future")
    RecordingJob.set(priority: 10).perform_later("low")
    RecordingJob.set(priority: -10).perform_later("high")
    2.times { @performer.perform_one }
    _(2.times.map { RecordingJob.events.pop }).must_equal ["high", "low"]
    _(@performer.perform_one).must_be_nil
  end

  it "filters queues and supports strict queue order" do
    RecordingJob.set(queue: "slow", priority: -10).perform_later("slow")
    RecordingJob.set(queue: "fast", priority: 10).perform_later("fast")
    @performer.perform_one(queues: "+fast,slow")
    _(RecordingJob.events.pop).must_equal "fast"
    _(@performer.perform_one(queues: "-slow")).must_be_nil
    @performer.perform_one(queues: "slow")
    _(RecordingJob.events.pop).must_equal "slow"
  end

  it "persists retry state and preserves the job identifier" do
    job = RetryingJob.perform_later("retried")
    _(@performer.perform_one.state).must_equal "retried"
    _(@store.find(job.provider_job_id).fetch("state")).must_equal "ready"
    _(@performer.perform_one.state).must_equal "succeeded"
    _(RecordingJob.events.pop).must_equal "retried"
    _(@store.executions(job.provider_job_id).map { |row| row.fetch("state") }).must_equal ["retried", "succeeded"]
  end

  it "distinguishes handled discards from success" do
    job = DiscardingJob.perform_later
    _(@performer.perform_one.state).must_equal "discarded"
    _(@store.find(job.provider_job_id).fetch("error").fetch("class")).must_equal "ArgumentError"
  end

  it "records unhandled errors without losing the worker" do
    job = FailingJob.perform_later
    result = @performer.perform_one
    _(result.error.message).must_equal "unhandled error"
    _(@store.find(job.provider_job_id).fetch("state")).must_equal "discarded"
    _(@store.executions(job.provider_job_id).first.fetch("error").fetch("backtrace")).wont_be_empty
  end

  it "does not execute a job held by another worker" do
    BlockingJob.perform_later
    worker = Thread.new { @performer.perform_one }
    BlockingJob.started.pop
    _(DecentJob::Performer.new(database: database).perform_one).must_be_nil
  ensure
    BlockingJob.release << true
    worker&.value
  end

  it "recovers a running record after its owning connection disappears" do
    job = RecordingJob.perform_later("recovered")
    previous = SecureRandom.uuid
    database.transaction do |connection|
      connection.exec_params("UPDATE decent_jobs SET state = 'running', execution_id = $2 WHERE id = $1", [job.provider_job_id, previous])
      connection.exec_params("INSERT INTO decent_job_executions (id, job_id, process_id) VALUES ($1, $2, 999)", [previous, job.provider_job_id])
    end
    _(@performer.perform_one.state).must_equal "succeeded"
    _(RecordingJob.events.pop).must_equal "recovered"
    _(@store.executions(job.provider_job_id).map { |row| row.fetch("state") }).must_equal ["interrupted", "succeeded"]
  end
end
