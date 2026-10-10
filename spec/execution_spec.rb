# frozen_string_literal: true

require "minitest/mock"
require "spec_helper"
require "support/jobs"

describe "execution inspection" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)
    [RecordingJob, RetryingJob, FailingJob].each { |klass| klass.queue_adapter = @adapter }
    RecordingJob.events = Queue.new
    @performer = DecentJob::Performer.new(database: database)
  end

  it "finds decoded attempts and ordered history without changing hash history" do
    job = RetryingJob.perform_later("done")
    2.times { @performer.perform_one }
    rows = DecentJob::Store.new(database).executions(job.provider_job_id)
    executions = DecentJob::Execution.for_job(job.provider_job_id, database: database)
    _(executions.map(&:id)).must_equal rows.map { |row| row.fetch("id") }
    _(executions.map(&:number)).must_equal [1, 2]
    _(executions.map(&:status)).must_equal [:retried, :succeeded]
    execution = DecentJob::Execution.find(executions.first.id, database: database)
    _(execution.attributes).must_equal rows.first
    _(execution.job_id).must_equal job.provider_job_id
    _(execution.database).must_equal database
    _(execution.error).must_be_kind_of Hash
    _(execution.process_id).must_equal Process.pid
    _(execution.payload).must_be_kind_of Hash
    _(execution.duration).must_be_kind_of Float
    _(execution.started_at).must_be_kind_of Time
    _(execution.performed_at).must_equal execution.started_at
    _(execution.last_status_at).must_equal execution.finished_at
    _(execution.queue_latency).must_equal execution.started_at - execution.scheduled_at
    _(execution.runtime_latency).must_equal execution.duration
    _(execution.total_latency).must_equal execution.queue_latency + execution.duration
    _(execution.interrupted_duration).must_be_nil
    _(DecentJob::Job.find(job.provider_job_id, database: database).executions).must_equal rows
    _(DecentJob::Execution.for_job(SecureRandom.uuid, database: database)).must_equal []
    _ { DecentJob::Execution.find(SecureRandom.uuid, database: database) }.must_raise KeyError
  end

  it "resolves the current execution database and otherwise the global database" do
    job = RecordingJob.perform_later("done")
    @performer.perform_one
    id = DecentJob::Store.new(database).executions(job.provider_job_id).first.fetch("id")

    DecentJob::Current.with(database: database) do
      _(DecentJob::Execution.find(id).database).must_equal database
      _(DecentJob::Execution.for_job(job.provider_job_id).map(&:id)).must_equal [id]
    end

    DecentJob.stub(:database, database) do
      _(DecentJob::Execution.find(id).database).must_equal database
      _(DecentJob::Execution.for_job(job.provider_job_id).map(&:id)).must_equal [id]
    end
  end

  it "owns its immutable identity and returns detached nested attributes" do
    job = RecordingJob.perform_later("original")
    @performer.perform_one
    id = DecentJob::Store.new(database).executions(job.provider_job_id).first.fetch("id")
    requested_id = id.dup
    execution = DecentJob::Execution.find(requested_id, database: database)
    requested_id.replace(SecureRandom.uuid)
    attributes = execution.attributes
    attributes.fetch("id").replace(SecureRandom.uuid)
    attributes.fetch("payload").fetch("arguments").first.replace("changed")
    _(execution.id).must_equal id
    _(execution.payload.fetch("arguments")).must_equal ["original"]
    _ { execution.id.replace(SecureRandom.uuid) }.must_raise FrozenError
  end

  it "reports in-progress and interrupted wall intervals without inventing measured runtime" do
    RecordingJob.perform_later("done")
    first_id = nil

    DecentJob::Claim.new(database).with_next do |row|
      first_id = row.fetch("execution_id")
      execution = DecentJob::Execution.find(first_id, database: database)
      _(execution.status).must_equal :running
      _(execution.last_status_at).must_equal execution.started_at
      _(execution.queue_latency).must_be_kind_of Float
      _(execution.runtime_latency).must_be_nil
      _(execution.total_latency).must_be_nil
      _(execution.interrupted_duration).must_be_nil
    end

    @performer.perform_one
    execution = DecentJob::Execution.find(first_id, database: database)
    _(execution.status).must_equal :interrupted
    _(execution.runtime_latency).must_be_nil
    _(execution.total_latency).must_be_nil
    _(execution.interrupted_duration).must_equal execution.finished_at - execution.started_at
  end

  it "keeps historical unknown metrics nil and defaults known payload numbering to one" do
    job = RecordingJob.perform_later("done")
    @performer.perform_one

    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_job_executions SET payload = NULL, scheduled_at = NULL, duration = NULL WHERE job_id = $1", [job.provider_job_id])
    end

    execution = DecentJob::Execution.for_job(job.provider_job_id, database: database).first
    _(execution.number).must_be_nil
    _(execution.queue_latency).must_be_nil
    _(execution.runtime_latency).must_be_nil
    _(execution.total_latency).must_be_nil

    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_job_executions SET payload = '{}', scheduled_at = started_at + interval '5 seconds' WHERE job_id = $1", [job.provider_job_id])
    end

    execution = DecentJob::Execution.find(execution.id, database: database)
    _(execution.number).must_equal 1
    _(execution.queue_latency).must_equal(-5.0)
  end
end
