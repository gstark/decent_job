# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

class ShutdownEnqueuerJob < BlockingJob
  def perform
    super
    RecordingJob.perform_later("enqueued during shutdown")
  end
end

describe "async execution" do
  include DatabaseSpec

  before do
    DecentJob.reset!
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :async)
    RecordingJob.queue_adapter = @adapter
    RecordingJob.events = Queue.new
  end

  after do
    @adapter.shutdown
    DecentJob.reset!
  end

  it "executes on a background worker" do
    RecordingJob.perform_later("async")
    _(Timeout.timeout(5) { RecordingJob.events.pop }).must_equal "async"
    _(@adapter.shutdown(timeout: 2)).must_equal true
  end

  it "waits for the outer transaction to commit" do
    database.transaction do
      RecordingJob.perform_later("committed")
      _(RecordingJob.events).must_be_empty
    end
    _(Timeout.timeout(5) { RecordingJob.events.pop }).must_equal "committed"
  end

  it "includes adapter workers in the global shutdown lifecycle" do
    RecordingJob.perform_later("start")
    _(Timeout.timeout(5) { RecordingJob.events.pop }).must_equal "start"
    _(DecentJob.shutdown(timeout: 2)).must_equal true
    _(@adapter.shutdown(timeout: 0)).must_equal true
  end

  it "does not start another adapter's worker while draining a job" do
    parent_adapter = DecentJob::Adapter.new(database: database, execution_mode: :async)
    ShutdownEnqueuerJob.queue_adapter = parent_adapter
    ShutdownEnqueuerJob.started = Queue.new
    ShutdownEnqueuerJob.release = Queue.new
    ShutdownEnqueuerJob.perform_later
    Timeout.timeout(5) { ShutdownEnqueuerJob.started.pop }
    _(DecentJob.shutdown(timeout: 0)).must_equal false
    ShutdownEnqueuerJob.release << true
    _(DecentJob.shutdown(timeout: 5)).must_equal true
    _(@adapter.shutdown(timeout: 0)).must_equal true
    _(RecordingJob.events).must_be_empty
  ensure
    ShutdownEnqueuerJob.release << true
    parent_adapter&.shutdown(timeout: 5)
  end
end
