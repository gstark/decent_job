# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

class PreservationJob < RecordingJob
  class_attribute :performed
  attr_reader :performed_value

  def perform(value = "performed")
    self.class.performed = self
    @performed_value = value
    super
  end
end

class PreservationReenqueueJob < PreservationJob
  def perform(*)
    enqueue if executions == 1
    super
  end
end

class PreservationExhaustedJob < ActiveJob::Base
  retry_on RuntimeError, attempts: 1

  def perform
    raise "exhausted"
  end
end

class PreservationHandledExhaustedJob < PreservationExhaustedJob
  retry_on(RuntimeError, attempts: 1) { |_job, _error| }
end

class PreservationHandledJob < PreservationExhaustedJob
  rescue_from(RuntimeError) { |error| error }
end

class PreservationBlockingJob < BlockingJob
  include DecentJob::ActiveJobExtensions::Concurrency

  decent_job_control_concurrency_with(key: "preservation", perform_limit: 1)
end

class PreservationCallbackJob < ActiveJob::Base
  class_attribute :events

  def perform(batch, options)
    self.class.events << [batch.id, options.fetch(:event)]
  end
end

describe "job record preservation" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)
    [PreservationJob, PreservationReenqueueJob, PreservationExhaustedJob, PreservationHandledExhaustedJob,
      PreservationHandledJob, PreservationBlockingJob, PreservationCallbackJob, RetryingJob, DiscardingJob, FailingJob].each { |job| job.queue_adapter = @adapter }
    RecordingJob.events = Queue.new
    PreservationJob.performed = nil
    PreservationBlockingJob.started = Queue.new
    PreservationBlockingJob.release = Queue.new
    PreservationCallbackJob.events = []
    @store = DecentJob::Store.new(database)
    @performer = DecentJob::Performer.new(database: database)
    @threads = []
  end

  after do
    10.times { PreservationBlockingJob.release << true }
    @threads.each(&:value)
    DecentJob.reset!
  end

  it "preserves records by default" do
    _(DecentJob.configuration.preserve_job_records).must_equal true
    job = PreservationJob.perform_later
    @performer.perform_one
    _(@store.find(job.provider_job_id).fetch("state")).must_equal "succeeded"
    _(@store.executions(job.provider_job_id).size).must_equal 1
  end

  it "validates the policy" do
    config = DecentJob::Configuration.new

    [true, false, :on_unhandled_error, ->(*) { true }].each do |policy|
      config.preserve_job_records = policy
      _(config.validate!).must_equal config
    end

    [nil, :always, "false", 1].each do |policy|
      config.preserve_job_records = policy
      error = _ { config.validate! }.must_raise ArgumentError
      _(error.message).must_include "preserve_job_records"
    end
  end

  [PreservationJob, DiscardingJob, FailingJob].each do |job_class|
    it "deletes terminal #{job_class} records and their attempts when disabled" do
      DecentJob.configuration.preserve_job_records = false
      job = job_class.perform_later
      result = @performer.perform_one
      _(result.job_id).must_equal job.provider_job_id
      _(result.state).must_equal((job_class == PreservationJob) ? "succeeded" : "discarded")
      _(@store.find(job.provider_job_id)).must_be_nil
      _(@store.executions(job.provider_job_id)).must_be_empty
    end
  end

  {PreservationJob => false, DiscardingJob => false, FailingJob => true,
   PreservationExhaustedJob => true, PreservationHandledExhaustedJob => false, PreservationHandledJob => false}.each do |job_class, retained|
    it "preserves #{job_class} under error-only policy: #{retained}" do
      DecentJob.configuration.preserve_job_records = :on_unhandled_error
      job = job_class.perform_later
      @performer.perform_one
      _(!@store.find(job.provider_job_id).nil?).must_equal retained
    end
  end

  it "passes the actual executed object and success context to the predicate" do
    received = []

    DecentJob.configuration.preserve_job_records = ->(*context) {
      received << context
      false
    }

    job = PreservationJob.perform_later("instance mutation")
    @performer.perform_one
    _(received.size).must_equal 1
    _(received.first.first).must_be_same_as PreservationJob.performed
    _(received.first.first.performed_value).must_equal "instance mutation"
    _(received.first.drop(1)).must_equal [nil, nil]
    _(@store.find(job.provider_job_id)).must_be_nil
  end

  {DiscardingJob => :discarded, FailingJob => :unhandled,
   PreservationExhaustedJob => :retry_stopped, PreservationHandledExhaustedJob => :retry_stopped, PreservationHandledJob => :handled}.each do |job_class, event|
    it "passes #{event} and the actual exception for #{job_class}" do
      received = []

      DecentJob.configuration.preserve_job_records = ->(*context) {
        received << context
        true
      }

      job_class.perform_later
      result = @performer.perform_one
      _(received.first.first).must_be_kind_of job_class
      _(received.first[1]).must_be_same_as result.error
      _(received.first[2]).must_equal event
    end
  end

  it "records returned handled exceptions as discarded without treating them as unhandled" do
    job = PreservationHandledJob.perform_later
    result = @performer.perform_one(raise_errors: true)
    _(result.state).must_equal "discarded"
    _(@store.find(job.provider_job_id).fetch("error_event")).must_equal "handled"
    _(@store.executions(job.provider_job_id).first.fetch("state")).must_equal "discarded"
  end

  it "passes an existing job instance when argument deserialization fails" do
    received = []

    DecentJob.configuration.preserve_job_records = ->(*context) {
      received << context
      false
    }

    job = PreservationJob.perform_later

    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET payload = jsonb_set(payload, '{arguments}', '[{\"_aj_serialized\": \"RemovedArgumentSerializer\"}]') WHERE id = $1", [job.provider_job_id])
    end

    result = @performer.perform_one
    _(received.size).must_equal 1
    _(received.first.first).must_be_kind_of PreservationJob
    _(received.first[1]).must_be_same_as result.error
    _(received.first[2]).must_equal :unhandled
    _(result.error).must_be_kind_of ActiveJob::DeserializationError
    _(@store.find(job.provider_job_id)).must_be_nil
  end

  it "retains records when the predicate and error reporter both raise" do
    received = []
    DecentJob.configuration.preserve_job_records = ->(*) { raise "predicate failed" }

    DecentJob.configuration.on_thread_error = ->(error) {
      received << error
      raise "reporter failed"
    }

    job = PreservationJob.perform_later
    _(@performer.perform_one.state).must_equal "succeeded"
    _(received.map(&:message)).must_equal ["predicate failed"]
    _(@store.find(job.provider_job_id).fetch("state")).must_equal "succeeded"
  end

  it "retains records when predicate and reporter SQL errors abort their transactions" do
    DecentJob.configuration.preserve_job_records = lambda do |*|
      database.with_connection { |connection| connection.exec("SELECT missing_preservation_function()") }
    end

    reported = []

    DecentJob.configuration.on_thread_error = lambda do |error|
      reported << error
      database.with_connection { |connection| connection.exec("SELECT missing_reporter_function()") }
    end

    job = PreservationJob.perform_later
    _(@performer.perform_one.state).must_equal "succeeded"
    _(reported.first).must_be_kind_of PG::UndefinedFunction
    _(@store.find(job.provider_job_id).fetch("state")).must_equal "succeeded"
  end

  [RetryingJob, PreservationReenqueueJob].each do |job_class|
    it "preserves unfinished #{job_class} history without evaluating the predicate" do
      received = []

      DecentJob.configuration.preserve_job_records = ->(*context) {
        received << context
        false
      }

      job = job_class.perform_later("retry")
      _(@performer.perform_one.state).must_equal "retried"
      _(received).must_be_empty
      _(@store.find(job.provider_job_id).fetch("state")).must_equal "ready"
      _(@store.executions(job.provider_job_id).map { |attempt| attempt.fetch("state") }).must_equal ["retried"]
      _(@performer.perform_one.state).must_equal "succeeded"
      _(received.size).must_equal 1
      _(@store.find(job.provider_job_id)).must_be_nil
      _(@store.executions(job.provider_job_id)).must_be_empty
    end
  end

  it "evaluates a missing job class with nil and its unhandled deserialization error" do
    received = []

    DecentJob.configuration.preserve_job_records = ->(*context) {
      received << context
      false
    }

    job = PreservationJob.perform_later

    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET payload = jsonb_set(payload, '{job_class}', '\"RemovedPreservationJob\"') WHERE id = $1", [job.provider_job_id])
    end

    result = @performer.perform_one
    _(received.first).must_equal [nil, result.error, :unhandled]
    _(result.error).must_be_kind_of NameError
    _(@store.find(job.provider_job_id)).must_be_nil
  end

  it "preserves cron keys without evaluating the predicate" do
    DecentJob.configuration.preserve_job_records = ->(*) { flunk "cron predicate evaluated" }
    entry = DecentJob::CronEntry.new(key: "preserved", cron: "@daily", class: PreservationJob, database: database)
    job = entry.enqueue(Time.utc(2026, 1, 1))
    @performer.perform_one
    _(@store.find(job.provider_job_id).fetch("state")).must_equal "succeeded"
    _(entry.enqueue(Time.utc(2026, 1, 1))).must_equal false
  end

  it "deletes successful inline retries after their final execution" do
    DecentJob.configuration.preserve_job_records = false
    RetryingJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :inline)
    job = RetryingJob.perform_later("inline")
    _(RecordingJob.events.pop).must_equal "inline"
    _(@store.find(job.provider_job_id)).must_be_nil
    _(@store.executions(job.provider_job_id)).must_be_empty
  end

  it "still raises unhandled inline errors after deleting their records" do
    DecentJob.configuration.preserve_job_records = false
    FailingJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :inline)
    _ { FailingJob.perform_later }.must_raise RuntimeError
    database.with_connection { |connection| _(connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)).must_equal "0" }
  end

  it "finishes failed batches and deletes callback records without losing failure" do
    DecentJob.configuration.preserve_job_records = false
    job = DiscardingJob.new
    batch = DecentJob::Batch.enqueue([job], database: database, on_finish: PreservationCallbackJob, on_discard: PreservationCallbackJob, on_success: PreservationCallbackJob)
    @performer.perform_one(job_id: job.provider_job_id)
    _(@store.find(job.provider_job_id)).must_be_nil
    _(batch.reload.discarded?).must_equal true
    _(batch.callback_job_instances.size).must_equal 2
    2.times { @performer.perform_one }
    _(PreservationCallbackJob.events.map(&:last).sort).must_equal %i[discard finish]
    _(batch.reload.finished?).must_equal true
    _(batch.callback_job_instances).must_be_empty
    batch.enqueue([PreservationJob.new])
    3.times { @performer.perform_one }
    _(batch.reload.succeeded?).must_equal true
    _(batch.finished?).must_equal true
    _(PreservationCallbackJob.events.map(&:last).sort).must_equal %i[discard finish finish success]
  end

  it "does not delete a batch member retried during its preservation predicate" do
    job = PreservationHandledJob.new
    batch = DecentJob::Batch.enqueue([job], database: database)
    calls = 0

    DecentJob.configuration.preserve_job_records = lambda do |*_context|
      calls += 1
      batch.retry if calls == 1
      false
    end

    @performer.perform_one
    _(@store.find(job.provider_job_id).fetch("state")).must_equal "ready"
    _(@store.executions(job.provider_job_id).map { |attempt| attempt.fetch("state") }).must_equal ["discarded"]
    _(batch.reload.finished?).must_equal false
    @performer.perform_one
    _(@store.find(job.provider_job_id)).must_be_nil
    _(batch.reload.discarded?).must_equal true
  end

  it "rolls back finishing and batch requests if terminal deletion fails" do
    DecentJob.configuration.preserve_job_records = false
    job = DiscardingJob.new
    batch = DecentJob::Batch.enqueue([job], database: database)

    database.with_connection do |connection|
      connection.exec(<<~SQL)
        CREATE FUNCTION reject_preservation_delete() RETURNS trigger LANGUAGE plpgsql AS $$
          BEGIN RAISE EXCEPTION 'delete failed'; END;
        $$;
        CREATE TRIGGER reject_preservation_delete BEFORE DELETE ON decent_jobs
          FOR EACH ROW EXECUTE FUNCTION reject_preservation_delete();
      SQL
    end

    _ { @performer.perform_one }.must_raise PG::RaiseException
    row = @store.find(job.provider_job_id)
    _(row.fetch("state")).must_equal "running"
    _(row.fetch("finished_at")).must_be_nil
    _(@store.executions(job.provider_job_id).first.fetch("state")).must_equal "running"

    database.with_connection do |connection|
      _(connection.exec("SELECT count(*) FROM decent_job_batch_updates").getvalue(0, 0)).must_equal "0"
      connection.exec("DROP TRIGGER reject_preservation_delete ON decent_jobs")
    end

    @performer.perform_one
    _(@store.find(job.provider_job_id)).must_be_nil
    _(batch.reload.discarded?).must_equal true
  end

  it "promotes a concurrency waiter before deleting the finished owner" do
    DecentJob.configuration.preserve_job_records = false
    first = PreservationBlockingJob.perform_later
    second = PreservationBlockingJob.perform_later
    @threads << Thread.new { @performer.perform_one(job_id: first.provider_job_id) }
    Timeout.timeout(5) { PreservationBlockingJob.started.pop }
    _(@performer.perform_one(job_id: second.provider_job_id).state).must_equal "retried"
    PreservationBlockingJob.release << true
    @threads.last.value
    _(@store.find(first.provider_job_id)).must_be_nil
    _(@store.find(second.provider_job_id).fetch("scheduled_at")).must_be :<=, Time.now
    PreservationBlockingJob.release << true
    _(@performer.perform_one(job_id: second.provider_job_id).state).must_equal "succeeded"
    database.with_connection { |connection| _(connection.exec("SELECT count(*) FROM decent_job_concurrency_claims").getvalue(0, 0)).must_equal "0" }
  end

  it "retains authoritative forced discards when their owner finishes" do
    DecentJob.configuration.preserve_job_records = ->(*) { flunk "force-discard predicate evaluated" }
    job = PreservationBlockingJob.perform_later
    @threads << Thread.new { @performer.perform_one(job_id: job.provider_job_id) }
    Timeout.timeout(5) { PreservationBlockingJob.started.pop }
    DecentJob::Job.find(job.provider_job_id, database: database).force_discard_job("operator stopped")
    PreservationBlockingJob.release << true
    _(@threads.last.value.state).must_equal "discarded"
    _(@store.find(job.provider_job_id).fetch("error").fetch("message")).must_equal "operator stopped"
    _(@store.executions(job.provider_job_id).first.fetch("state")).must_equal "discarded"
  end
end
