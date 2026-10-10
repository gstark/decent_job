# frozen_string_literal: true

require "spec_helper"
require "support/jobs"

class ManagementDiscardHandlerFailureJob < ActiveJob::Base
  discard_on ArgumentError do
    raise TypeError, "discard handler failed"
  end

  def perform
    raise ArgumentError, "original failure"
  end
end

class ManagementRetryHandlerFailureJob < ActiveJob::Base
  retry_on ArgumentError, attempts: 1 do
    raise TypeError, "retry handler failed"
  end

  def perform
    raise ArgumentError, "original failure"
  end
end

class ManagementExhaustedRetryJob < ActiveJob::Base
  retry_on ArgumentError, attempts: 1

  def perform
    raise ArgumentError, "original failure"
  end
end

describe DecentJob::Job do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)
    [RecordingJob, RetryingJob, FailingJob, BlockingJob].each { |klass| klass.queue_adapter = @adapter }
    RecordingJob.events = Queue.new
    BlockingJob.started = Queue.new
    BlockingJob.release = Queue.new
    @performer = DecentJob::Performer.new(database: database)
  end

  def managed(job)
    DecentJob::Job.find(job.provider_job_id, database: database)
  end

  it "reads decoded jobs, arguments, statuses and attempt history" do
    aj = RecordingJob.perform_later("hello")
    job = managed(aj)
    _(job.id).must_equal aj.provider_job_id
    _(job.active_job_id).must_equal aj.job_id
    _(job.status).must_equal :queued
    _(job.queued?).must_equal true
    _(job.created_at).must_be_kind_of Time
    _(job.job_instance.arguments).must_equal ["hello"]
    _(job.executions).must_equal []
    @performer.perform_one
    _(job.reload).must_equal job
    _(job.succeeded?).must_equal true
    _(job.finished?).must_equal true
    _(job.executions.first.fetch("state")).must_equal "succeeded"
    _(proc { DecentJob::Job.find(SecureRandom.uuid, database: database) }).must_raise KeyError
  end

  it "uses the current execution database when finding jobs" do
    aj = RecordingJob.perform_later("hello")

    DecentJob::Current.with(database: database) do
      _(DecentJob::Job.find(aj.provider_job_id).database).must_equal database
    end
  end

  it "reschedules unfinished jobs and preserves their payload and identity" do
    job = managed(RecordingJob.perform_later("later"))
    time = Time.now + 3600
    job.reschedule_job(time)
    _(job.scheduled?).must_equal true
    _(job.scheduled_at.to_f).must_be_close_to time.to_f, 0.001
    _(Time.parse(job.payload.fetch("scheduled_at")).to_f).must_be_close_to time.to_f, 0.001
    job.reschedule_job
    result = @performer.perform_one
    _(result.state).must_equal "succeeded", result.error&.full_message
    _(proc { job.reschedule_job }).must_raise DecentJob::Job::StateMismatchError
  end

  it "discards queued jobs without inventing an attempt and destroys terminal jobs" do
    job = managed(RecordingJob.perform_later("unused"))
    _(proc { job.destroy_job }).must_raise DecentJob::Job::StateMismatchError
    job.discard_job("operator request")
    _(job.discarded?).must_equal true
    _(job.error.fetch("class")).must_equal "DecentJob::Job::DiscardJobError"
    _(job.error.fetch("message")).must_equal "operator request"
    _(job.executions).must_equal []
    _(proc { job.discard_job("twice") }).must_raise DecentJob::Job::StateMismatchError
    job.destroy_job
    _(proc { job.reload }).must_raise KeyError
  end

  it "retries discarded jobs in place with their attempt history" do
    aj = FailingJob.perform_later
    job = managed(aj)
    _(proc { job.retry_job }).must_raise DecentJob::Job::StateMismatchError
    @performer.perform_one
    result = job.retry_job
    _(result).must_be_kind_of FailingJob
    _(result.provider_job_id).must_equal aj.provider_job_id
    _(job.reload.status).must_equal :queued
    _(job.executions.size).must_equal 1
    _(job.job_instance.executions).must_equal 1
    @performer.perform_one
    _(job.reload.executions.size).must_equal 2
    job.destroy_job
    _(DecentJob::Store.new(database).executions(aj.provider_job_id)).must_equal []
  end

  it "retries a discarded job that never started" do
    job = managed(RecordingJob.perform_later("retried"))
    job.discard_job("stopped before starting")
    _(job.retry_job.successfully_enqueued?).must_equal true
    _(job.executions).must_equal []
    result = @performer.perform_one
    _(result.state).must_equal "succeeded", result.error&.full_message
  end

  it "rejects ordinary actions while an execution owns the advisory lock" do
    job = managed(BlockingJob.perform_later)
    worker = Thread.new { @performer.perform_one }
    BlockingJob.started.pop
    _(job.reload.running?).must_equal true

    [:reschedule_job, :retry_job, :destroy_job].each do |action|
      _(proc { job.public_send(action) }).must_raise DecentJob::Job::LockedError
    end

    _(proc { job.discard_job("no") }).must_raise DecentJob::Job::LockedError
  ensure
    BlockingJob.release << true
    worker&.value
  end

  it "force discards a running job and its attempt, and wins against successful completion" do
    job = managed(BlockingJob.perform_later)
    worker = Thread.new { @performer.perform_one }
    BlockingJob.started.pop
    job.force_discard_job("stop")
    _(job.discarded?).must_equal true
    _(job.executions.first.fetch("state")).must_equal "discarded"
    _(proc { job.destroy_job }).must_raise DecentJob::Job::LockedError
    BlockingJob.release << true
    result = worker.value
    _(result.state).must_equal "discarded"
    _(result.error).must_be_kind_of DecentJob::Job::DiscardJobError
    _(job.reload.error.fetch("message")).must_equal "stop"
    _(job.executions.first.fetch("error").fetch("message")).must_equal "stop"
  ensure
    BlockingJob.release << true
    worker&.value
  end

  it "retains the original error and marks a manual retry until its next attempt succeeds" do
    job = managed(RecordingJob.perform_later("done"))
    job.discard_job("manual discard")
    _(job.error_event).must_equal "discarded"
    error = job.error
    job.retry_job
    _(job.error).must_equal error
    _(job.error_event).must_equal "retried"
    @performer.perform_one
    _(job.reload.error).must_be_nil
    _(job.error_event).must_be_nil
    _(job.executions.first.fetch("error_event")).must_be_nil
  end

  it "records retry and terminal failure error events on real attempts" do
    job = managed(RetryingJob.perform_later("recovered"))
    @performer.perform_one
    _(job.reload.error_event).must_equal "retried"
    _(job.error.fetch("message")).must_equal "try again"
    _(job.executions.first.fetch("error_event")).must_equal "retried"
    @performer.perform_one
    _(job.reload.error).must_be_nil
    _(job.error_event).must_be_nil
    failure = managed(FailingJob.perform_later)
    @performer.perform_one
    _(failure.reload.error_event).must_equal "unhandled"
    _(failure.executions.first.fetch("error_event")).must_equal "unhandled"
  end

  it "reports actual execution ownership rather than an abandoned running row" do
    job = managed(RecordingJob.perform_later("recovered"))
    database.with_connection { |connection| connection.exec_params("UPDATE decent_jobs SET state = 'running' WHERE id = $1", [job.id]) }
    _(job.reload.running?).must_equal false
    _(job.status).must_equal :queued

    DecentJob::Claim.new(database).with_next do
      database.with_connection { |connection| connection.exec_params("UPDATE decent_jobs SET state = 'ready' WHERE id = $1", [job.id]) }
      _(job.reload.running?).must_equal true
      _(job.status).must_equal :running
    end
  end

  [ManagementDiscardHandlerFailureJob, ManagementRetryHandlerFailureJob].each do |klass|
    it "records a new exception from #{klass} as unhandled on the job and attempt" do
      klass.queue_adapter = @adapter
      job = managed(klass.perform_later)
      result = @performer.perform_one
      _(result.state).must_equal "discarded"
      _(result.error).must_be_kind_of TypeError
      _(job.reload.error_event).must_equal "unhandled"
      _(job.error.fetch("class")).must_equal "TypeError"
      attempt = job.executions.first
      _(attempt.fetch("error_event")).must_equal "unhandled"
      _(attempt.fetch("error").fetch("class")).must_equal "TypeError"
    end
  end

  it "preserves retry_stopped when exhausted retries re-raise the original exception" do
    ManagementExhaustedRetryJob.queue_adapter = @adapter
    job = managed(ManagementExhaustedRetryJob.perform_later)
    result = @performer.perform_one
    _(result.state).must_equal "discarded"
    _(result.error).must_be_kind_of ArgumentError
    _(job.reload.error_event).must_equal "retry_stopped"
    _(job.error.fetch("message")).must_equal "original failure"
    attempt = job.executions.first
    _(attempt.fetch("error_event")).must_equal "retry_stopped"
    _(attempt.fetch("error").fetch("class")).must_equal "ArgumentError"
  end

  it "keeps attributes detached so editing their identity cannot redirect management to another job" do
    first = managed(RecordingJob.perform_later("first"))
    second = managed(RecordingJob.perform_later("second"))
    snapshot = first.attributes
    _(snapshot.fetch("created_at")).must_be_kind_of Time
    snapshot["id"] = second.id
    snapshot.fetch("payload")["arguments"][0].replace("changed")
    _(first.payload.fetch("arguments")).must_equal ["first"]
    first.discard_job("discard original")
    _(first.reload.discarded?).must_equal true
    _(first.job_instance.arguments).must_equal ["first"]
    _(second.reload.queued?).must_equal true
  end

  it "owns an immutable provider ID independent of input and returned attribute strings" do
    aj = RecordingJob.perform_later("original")
    original_id = aj.provider_job_id.dup
    requested_id = original_id.dup
    job = DecentJob::Job.find(requested_id, database: database)
    requested_id.replace(SecureRandom.uuid)
    job.attributes.fetch("id").replace(SecureRandom.uuid)
    _(job.id).must_equal original_id
    _(proc { job.id.replace(SecureRandom.uuid) }).must_raise FrozenError
    _(job.reload.id).must_equal original_id
    job.discard_job("same identity")
    _(DecentJob::Job.find(original_id, database: database).discarded?).must_equal true
  end

  it "cannot bypass the original execution's advisory lock by editing its attribute ID" do
    first = managed(BlockingJob.perform_later)
    second = managed(RecordingJob.perform_later("unrelated"))
    worker = Thread.new { @performer.perform_one(job_id: first.id) }
    BlockingJob.started.pop
    first.force_discard_job("still executing")
    first.attributes["id"] = second.id
    _(proc { first.retry_job }).must_raise DecentJob::Job::LockedError
    _(first.reload.discarded?).must_equal true
    _(second.reload.queued?).must_equal true
  ensure
    BlockingJob.release << true
    worker&.value
  end
end
