# frozen_string_literal: true

require "spec_helper"
require "support/jobs"

describe "scheduled job discovery" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    RecordingJob.queue_adapter = DecentJob::Adapter.new(database: database)
    RecordingJob.events = Queue.new
    @claim = DecentJob::Claim.new(database, enable_pauses: true)
  end

  it "finds future eligible dates within queue selections and a limit" do
    now = Time.now
    jobs = %w[urgent ordinary paused].map.with_index do |queue, index|
      RecordingJob.set(queue: queue, wait_until: now + index + 60).perform_later(queue)
    end
    DecentJob::Pauses.new(database).pause(queue: "paused")
    times = @claim.next_scheduled_at(queues: "+ordinary,urgent", limit: 1)
    _(times.size).must_equal 1
    _(times.first.to_f).must_be_close_to jobs.first.scheduled_at.to_f, 0.001
    excluded = @claim.next_scheduled_at(queues: "-urgent", limit: 5)
    _(excluded.size).must_equal 1
    _(excluded.first.to_f).must_be_close_to jobs[1].scheduled_at.to_f, 0.001
    record = DecentJob::Job.find(jobs.first.provider_job_id, database: database)
    record.discard_job("removed")
    record.destroy_job
    _(@claim.next_scheduled_at(queues: "urgent", limit: 5)).must_equal []
  end

  it "can include bounded due work to close the gap between an empty claim and warming" do
    3.times { RecordingJob.perform_later("due") }
    future = RecordingJob.set(wait: 60).perform_later("future")
    times = @claim.next_scheduled_at(limit: 1, now_limit: 2)
    _(times.size).must_equal 3
    _(times.count { |time| time <= Time.now }).must_equal 2
    _(times.last.to_f).must_be_close_to future.scheduled_at.to_f, 0.001
  end
  it "keeps held due work out of discovery so it cannot displace future dates" do
    held = RecordingJob.perform_later("held")
    future = RecordingJob.set(wait: 60).perform_later("future")
    database.with_connection do |connection|
      connection.exec_params("SELECT pg_advisory_lock(hashtextextended(current_schema() || '.decent_jobs.' || $1, 0))", [held.provider_job_id])
      times = @claim.next_scheduled_at(limit: 1, now_limit: 1)
      _(times.size).must_equal 1
      _(times.first.to_f).must_be_close_to future.scheduled_at.to_f, 0.001
    ensure
      connection.exec("SELECT pg_advisory_unlock_all()")
    end
  end
end
