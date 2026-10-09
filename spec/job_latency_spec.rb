# frozen_string_literal: true

require "minitest/mock"
require "spec_helper"
require "support/jobs"

describe "job latency" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)
    [RecordingJob, RetryingJob].each { |klass| klass.queue_adapter = @adapter }
    RecordingJob.events = Queue.new
    @performer = DecentJob::Performer.new(database: database)
  end

  it "reports no latency before a scheduled job is due" do
    job = DecentJob::Job.find(RecordingJob.set(wait: 60).perform_later("future").provider_job_id, database: database)
    _(job.performed_at).must_be_nil
    _(job.queue_latency).must_be_nil
    _(job.runtime_latency).must_be_nil
    _(job.total_latency).must_be_nil
    Time.stub(:now, job.scheduled_at) { _(job.queue_latency).must_be_nil }
  end

  it "reports increasing queue time and no runtime before a queued job starts" do
    job = DecentJob::Job.find(RecordingJob.perform_later("queued").provider_job_id, database: database)
    Time.stub(:now, job.scheduled_at + 10) do
      _(job.queue_latency).must_equal 10.0
      _(job.runtime_latency).must_be_nil
      _(job.total_latency).must_be_nil
    end
  end

  it "reports running and completed wall latencies from the shared performed timestamp" do
    job = DecentJob::Job.find(RecordingJob.perform_later("running").provider_job_id, database: database)
    DecentJob::Claim.new(database).with_next do
      job.reload
      Time.stub(:now, job.performed_at + 10) do
        _(job.queue_latency).must_equal job.performed_at - job.scheduled_at
        _(job.runtime_latency).must_equal 10.0
        _(job.total_latency).must_equal job.queue_latency + 10
      end
    end
    @performer.perform_one
    job.reload
    Time.stub(:now, job.finished_at + 100) do
      _(job.queue_latency).must_equal job.performed_at - job.scheduled_at
      _(job.runtime_latency).must_equal job.finished_at - job.performed_at
      _(job.total_latency).must_be_close_to job.finished_at - job.scheduled_at, 0.000001
    end
  end

  it "does not carry an earlier runtime into a queued retry" do
    job = DecentJob::Job.find(RetryingJob.perform_later("retry").provider_job_id, database: database)
    @performer.perform_one
    job.reload
    _(job.performed_at).must_be_nil
    Time.stub(:now, job.scheduled_at + 10) do
      _(job.queue_latency).must_equal 10.0
      _(job.runtime_latency).must_be_nil
      _(job.total_latency).must_be_nil
    end
  end

  it "uses the discard timestamp for a job discarded before it started" do
    job = DecentJob::Job.find(RecordingJob.perform_later("discard").provider_job_id, database: database)
    job.discard_job("cancelled")
    Time.stub(:now, job.finished_at + 100) do
      _(job.queue_latency).must_equal job.finished_at - job.scheduled_at
      _(job.runtime_latency).must_be_nil
      _(job.total_latency).must_be_nil
    end
  end

  it "preserves signed wall clock intervals rather than hiding clock skew" do
    job = DecentJob::Job.find(RecordingJob.perform_later("done").provider_job_id, database: database)
    @performer.perform_one
    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET performed_at = scheduled_at - interval '5 seconds', finished_at = scheduled_at - interval '10 seconds' WHERE id = $1", [job.id])
    end
    job.reload
    Time.stub(:now, job.scheduled_at + 1) do
      _(job.queue_latency).must_equal(-5.0)
      _(job.runtime_latency).must_equal(-5.0)
      _(job.total_latency).must_equal(-10.0)
    end
  end
end
