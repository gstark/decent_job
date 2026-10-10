# frozen_string_literal: true

require "spec_helper"
require "support/jobs"

class InterruptContextJob < ActiveJob::Base
  class_attribute :observations, default: []
  class_attribute :stored_errors, default: []

  def perform
    context = DecentJob::Current.execution
    observations << context&.fetch(:execution_interrupted, nil)

    if context && context[:execution_id]
      row = DecentJob::Store.new(context.fetch(:database)).find(context.fetch(:id))
      stored_errors << row.slice("error", "error_event")
    end
  end
end

class InterruptHookJob < InterruptContextJob
  include DecentJob::ActiveJobExtensions::InterruptErrors
end

class InterruptRetryJob < InterruptHookJob
  retry_on DecentJob::InterruptError, wait: 0, attempts: 2
end

class InterruptDiscardJob < InterruptHookJob
  discard_on DecentJob::InterruptError
end

class InterruptStoppedJob < InterruptHookJob
  retry_on DecentJob::InterruptError, wait: 0, attempts: 1
end

class InterruptManualRetryJob < InterruptHookJob
  rescue_from(DecentJob::InterruptError) { retry_job(wait: 300) }
end

class InterruptRescueJob < InterruptHookJob
  rescue_from(DecentJob::InterruptError) { InterruptHookJob.perform_now }
end

describe "interrupted executions" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)
    [InterruptContextJob, InterruptHookJob, InterruptRetryJob, InterruptDiscardJob, InterruptStoppedJob, InterruptManualRetryJob, InterruptRescueJob].each { |job| job.queue_adapter = @adapter }
    InterruptContextJob.observations = []
    InterruptContextJob.stored_errors = []
    @store = DecentJob::Store.new(database)
    @performer = DecentJob::Performer.new(database: database)
  end

  after { DecentJob.reset! }

  def abandon(job)
    id = SecureRandom.uuid

    database.transaction do |connection|
      connection.exec_params("INSERT INTO decent_job_executions (id, job_id, process_id, started_at) VALUES ($1, $2, 123, clock_timestamp() - interval '1 minute')", [id, job.provider_job_id])
      connection.exec_params("UPDATE decent_jobs SET state = 'running', execution_id = $2, executions_count = executions_count + 1 WHERE id = $1", [job.provider_job_id, id])
    end

    @store.executions(job.provider_job_id).last
  end

  it "defines raised and stored interruption errors with the public inheritance" do
    _(DecentJob::InterruptError < StandardError).must_equal true
    _(DecentJob::InterruptedError < DecentJob::InterruptError).must_equal true
  end

  it "records the abandoned timestamp exactly once and executes ordinary jobs with recovery context" do
    job = InterruptContextJob.perform_later
    prior = abandon(job)
    _(InterruptContextJob.observations).must_be_empty
    _(@performer.perform_one.state).must_equal "succeeded"
    _(InterruptContextJob.observations).must_equal [prior.fetch("started_at")]
    history = @store.executions(job.provider_job_id)
    _(history.map { |attempt| attempt.fetch("state") }).must_equal ["interrupted", "succeeded"]
    _(history.first.fetch("error_event")).must_equal "interrupted"
    _(history.first.fetch("error").fetch("class")).must_equal "DecentJob::InterruptedError"
    _(history.first.fetch("error").fetch("message")).must_include prior.fetch("started_at").to_s
    _(history.first.fetch("finished_at")).must_be :>=, history.first.fetch("started_at")
    _(DecentJob::Current.execution).must_be_nil
    _(@performer.perform_one).must_be_nil
    _(@store.executions(job.provider_job_id)).must_equal history
  end

  it "exposes InterruptedError on the job while the replacement runs and clears it on success" do
    job = InterruptContextJob.perform_later
    prior = abandon(job)
    @performer.perform_one
    during = InterruptContextJob.stored_errors.first
    _(during.fetch("error_event")).must_equal "interrupted"
    _(during.fetch("error").fetch("class")).must_equal "DecentJob::InterruptedError"
    _(during.fetch("error").fetch("message")).must_include prior.fetch("started_at").to_s
    finished = @store.find(job.provider_job_id)
    _(finished.fetch("error")).must_be_nil
    _(finished.fetch("error_event")).must_be_nil
  end

  it "raises before the body and lets Active Job retry exactly one replacement attempt" do
    job = InterruptRetryJob.perform_later
    abandon(job)
    result = @performer.perform_one
    _(result.state).must_equal "retried"
    _(result.error_event).must_equal "retried"
    _(result.unhandled_error).must_be_nil
    _(InterruptContextJob.observations).must_be_empty
    _(@store.executions(job.provider_job_id).size).must_equal 2
    _(@performer.perform_one.state).must_equal "succeeded"
    _(InterruptContextJob.observations).must_equal [nil]
    _(@store.executions(job.provider_job_id).map { |attempt| attempt.fetch("state") }).must_equal ["interrupted", "retried", "succeeded"]
  end

  it "allows discard_on to finish the replacement without running the body" do
    job = InterruptDiscardJob.perform_later
    abandon(job)
    result = @performer.perform_one
    _(result.state).must_equal "discarded"
    _(result.error_event).must_equal "discarded"
    _(result.unhandled_error).must_be_nil
    _(InterruptContextJob.observations).must_be_empty
    _(@store.executions(job.provider_job_id).size).must_equal 2
  end

  it "persists an unhandled interruption and gives interrupted precedence over retry_stopped" do
    [InterruptHookJob, InterruptStoppedJob].each do |klass|
      job = klass.perform_later
      prior = abandon(job)
      result = @performer.perform_one
      _(result.unhandled_error).must_be_kind_of DecentJob::InterruptError
      _(result.error_event).must_equal "interrupted"
      _(result.error.message).must_include prior.fetch("started_at").to_s
      _(@store.find(job.provider_job_id).fetch("error_event")).must_equal "interrupted"
      _(@store.executions(job.provider_job_id).map { |attempt| attempt.fetch("error_event") }).must_equal ["interrupted", "interrupted"]
    end

    _(InterruptContextJob.observations).must_be_empty
  end

  it "retains recovery metadata on backend retry but does not raise the hook again" do
    DecentJob.configuration.retry_on_unhandled_error = true
    job = InterruptHookJob.perform_later
    abandon(job)
    _(@performer.perform_one.state).must_equal "retried"
    _(@store.find(job.provider_job_id).fetch("error_event")).must_equal "interrupted"
    _(@performer.perform_one.state).must_equal "succeeded"
    _(InterruptContextJob.observations).must_equal [nil]
    _(@store.executions(job.provider_job_id).map { |attempt| attempt.fetch("state") }).must_equal ["interrupted", "retried", "succeeded"]
  end

  it "supports a rescue_from handler scheduling a manual retry without an extra attempt" do
    job = InterruptManualRetryJob.perform_later
    abandon(job)
    result = @performer.perform_one
    _(result.state).must_equal "retried"
    _(result.unhandled_error).must_be_nil
    _(@store.find(job.provider_job_id).fetch("scheduled_at")).must_be :>, Time.now + 250
    _(@store.executions(job.provider_job_id).size).must_equal 2
    _(InterruptContextJob.observations).must_be_empty
  end

  it "does not flag fresh jobs or ordinary finished retries" do
    job = InterruptHookJob.perform_later
    _(@performer.perform_one.state).must_equal "succeeded"
    _(InterruptContextJob.observations).must_equal [nil]
    _(DecentJob::Current.execution).must_be_nil
    _(@store.executions(job.provider_job_id).first.fetch("error_event")).must_be_nil
  end

  it "lets a rescue_from handler run an unrelated nested job without raising its interruption hook" do
    job = InterruptRescueJob.perform_later
    abandon(job)
    result = @performer.perform_one
    _(result.state).must_equal "discarded"
    _(result.error_event).must_equal "handled"
    _(result.unhandled_error).must_be_nil
    _(InterruptContextJob.observations.size).must_equal 1
    _(DecentJob::Current.execution).must_be_nil
  end

  it "guards the opt-in hook by job identity, adapter, and database for nested perform_now" do
    job = InterruptHookJob.new
    job.provider_job_id = SecureRandom.uuid
    other = DecentJob::Database.new(url: @connection_options, pool_size: 1)
    context = {job_id: job.job_id, id: job.provider_job_id, database: database, execution_interrupted: Time.now}

    [context.merge(job_id: SecureRandom.uuid), context.merge(database: other)].each do |unrelated|
      DecentJob::Current.with(unrelated) { job.perform_now }
    end

    InterruptHookJob.queue_adapter = :test
    DecentJob::Current.with(context) { job.perform_now }
    _(InterruptContextJob.observations.size).must_equal 3
    _(DecentJob::Current.execution).must_be_nil
  ensure
    other&.close
  end

  it "marks each newly abandoned attempt without rewriting earlier completed histories" do
    DecentJob.configuration.handled_exceptions = []
    job = InterruptHookJob.perform_later
    first = abandon(job)
    _ { @performer.perform_one }.must_raise DecentJob::InterruptError
    completed = @store.executions(job.provider_job_id).first
    _ { @performer.perform_one }.must_raise DecentJob::InterruptError
    history = @store.executions(job.provider_job_id)
    _(history.size).must_equal 3
    _(history.first).must_equal completed
    _(history.map { |attempt| attempt.fetch("state") }).must_equal ["interrupted", "interrupted", "running"]
    _(history[1].fetch("error").fetch("message")).wont_include first.fetch("started_at").to_s
  end
end
