# frozen_string_literal: true

require "spec_helper"
require "support/jobs"

class StoppingManualRetryJob < ActiveJob::Base
  class_attribute :enqueue_reasons, default: []

  before_enqueue do
    reason = queue_adapter.stopping?
    enqueue_reasons << [reason, queue_adapter.stopping?(self)]
    throw :abort if reason
  end

  def perform
    raise "retry manually"
  end
end

describe "adapter stopping reasons" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)
    RecordingJob.queue_adapter = @adapter
    StoppingManualRetryJob.queue_adapter = @adapter
    StoppingManualRetryJob.enqueue_reasons = []
    @previous_pauses = DecentJob.configuration.enable_pauses
  end

  after do
    DecentJob.configuration.enable_pauses = @previous_pauses
  end

  it "returns false normally and stopping after direct shutdown" do
    _(@adapter.stopping?).must_equal false
    @adapter.shutdown
    _(@adapter.stopping?).must_equal :stopping
  end

  it "observes the executing worker's shutdown signal only in its database" do
    DecentJob::Current.with(database: database, stopping: -> { true }) do
      _(@adapter.stopping?).must_equal :stopping
    end

    _(@adapter.stopping?).must_equal false
    other = DecentJob::Database.new(url: @connection_options)

    DecentJob::Current.with(database: other, stopping: -> { true }) do
      _(@adapter.stopping?).must_equal false
    end
  ensure
    other&.close
  end

  it "observes completed execution rows and current persisted pause targets without a job argument" do
    aj = RecordingJob.perform_later("pause")
    DecentJob.configuration.enable_pauses = true

    DecentJob::Claim.new(database).with_next do |row|
      DecentJob::Current.with(database: database, id: row.fetch("id"), job_id: aj.job_id, execution_id: row.fetch("execution_id")) do
        DecentJob::Pauses.new(database).pause(queue: "default")
        _(@adapter.stopping?).must_equal :queue_name_paused
        DecentJob::Job.find(row.fetch("id"), database: database).force_discard_job("stop")
        _(@adapter.stopping?).must_equal :force_discarded
        _(@adapter.stopping?(RecordingJob.new)).must_equal :queue_name_paused
      end
    end
  end

  it "checks explicit job pause targets only when pauses are enabled" do
    DecentJob::Pauses.new(database).pause(job_class: RecordingJob)
    _(@adapter.stopping?(RecordingJob.new)).must_equal false
    DecentJob.configuration.enable_pauses = true
    _(@adapter.stopping?(RecordingJob.new)).must_equal :job_class_paused
    _(@adapter.stopping?).must_equal false
  end

  [:individual, :batch].each do |kind|
    it "does not treat historical attempts as executing during #{kind} retry callbacks" do
      batch = DecentJob::Batch.enqueue([StoppingManualRetryJob.new], database: database) if kind == :batch
      aj = batch ? batch.job_instances.first : StoppingManualRetryJob.perform_later
      DecentJob::Performer.new(database: database).perform_one
      job = DecentJob::Job.find(aj.provider_job_id, database: database)
      _(job.discarded?).must_equal true
      StoppingManualRetryJob.enqueue_reasons.clear
      DecentJob.configuration.enable_pauses = true
      DecentJob::Pauses.new(database).pause(queue: "default")

      batch ? batch.retry : job.retry_job

      _(StoppingManualRetryJob.enqueue_reasons).must_equal [[false, :queue_name_paused]]
      _(job.reload.finished?).must_equal false
      _(job.executions.size).must_equal 1
    end
  end

  it "still observes direct shutdown during manual retry context" do
    @adapter.shutdown

    DecentJob::Current.with(database: database, manual_retry: true) do
      _(@adapter.stopping?).must_equal :stopping
    end
  end
end
