# frozen_string_literal: true

require "spec_helper"
require "support/jobs"

class DatabaseErrorJob < ActiveJob::Base
  def perform
    raise PG::Error, "job database failure"
  end
end

class InlineChainJob < RecordingJob
  def perform(remaining)
    if remaining.positive?
      self.class.perform_later(remaining - 1)
    else
      super("finished chain")
    end
  end
end

describe "inline execution" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :inline)
    [RecordingJob, RetryingJob, FailingJob, DiscardingJob, DatabaseErrorJob, InlineChainJob].each { |job| job.queue_adapter = @adapter }
    RecordingJob.events = Queue.new
  end

  it "executes immediate jobs before returning" do
    job = RecordingJob.perform_later("inline")
    _(RecordingJob.events.pop).must_equal "inline"
    _(DecentJob::Store.new(database).find(job.provider_job_id).fetch("state")).must_equal "succeeded"
  end

  it "leaves future jobs persisted" do
    job = RecordingJob.set(wait: 3600).perform_later("later")
    _(RecordingJob.events).must_be_empty
    _(DecentJob::Store.new(database).find(job.provider_job_id).fetch("state")).must_equal "ready"
  end

  it "waits until the caller transaction commits" do
    database.transaction do
      RecordingJob.perform_later("committed")
      _(RecordingJob.events).must_be_empty
    end
    _(RecordingJob.events.pop).must_equal "committed"
  end

  it "never executes rolled back jobs" do
    _ {
      database.transaction do
        RecordingJob.perform_later("rolled back")
        raise "abort"
      end
    }.must_raise RuntimeError
    _(RecordingJob.events).must_be_empty
  end

  it "raises unhandled job errors after recording the attempt" do
    _ { FailingJob.perform_later }.must_raise RuntimeError
    database.with_connection do |connection|
      _(connection.exec("SELECT state FROM decent_job_executions").column_values(0)).must_equal ["discarded"]
    end
  end

  it "does not raise errors handled by discard_on" do
    job = DiscardingJob.perform_later
    _(DecentJob::Store.new(database).find(job.provider_job_id).fetch("state")).must_equal "discarded"
  end

  it "propagates a job's database exception without relabeling it as an enqueue error" do
    error = _ { DatabaseErrorJob.perform_later }.must_raise PG::Error
    _(error.message).must_equal "job database failure"
  end

  it "drains immediate retries without leaving the job queued" do
    job = RetryingJob.perform_later("retried inline")
    _(DecentJob::Store.new(database).find(job.provider_job_id).fetch("state")).must_equal "succeeded"
    _(RecordingJob.events.pop).must_equal "retried inline"
  end

  it "buffers nested inline jobs without growing the Ruby call stack" do
    InlineChainJob.perform_later(200)
    _(RecordingJob.events.pop).must_equal "finished chain"
    database.with_connection do |connection|
      _(connection.exec("SELECT count(*) FROM decent_jobs WHERE state = 'succeeded'").getvalue(0, 0)).must_equal "201"
    end
  end
end
