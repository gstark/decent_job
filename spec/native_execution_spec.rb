# frozen_string_literal: true

require "spec_helper"

module NativeExecution
  EVENTS = []
end

class NativeExecutionJob < DecentJob::Base
  def perform(value, keyword: nil)
    NativeExecution::EVENTS << [:perform, value, keyword, batch&.id]
  end
end

class NativeRetryJob < DecentJob::Base
  retry_on ArgumentError, wait: 60, attempts: 3
  discard_on KeyError

  def perform(error_class)
    raise DecentJob::Support.constantize(error_class), "native failure"
  end
end

class NativeUnhandledJob < DecentJob::Base
  def perform
    raise "unhandled native"
  end
end

class NativeLimitedJob < DecentJob::Base
  decent_job_control_concurrency_with(key: "native-limit", total_limit: 1)
  self.decent_job_labels = ["native"]

  def perform
  end
end

class NativeBatchCallback < DecentJob::Base
  def perform(batch, params)
    NativeExecution::EVENTS << [:callback, batch.id, params.fetch(:event)]
  end
end

describe "native job execution" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    DecentJob::Base.queue_adapter = DecentJob::Adapter.new(database: database)
    @store = DecentJob::Store.new(database)
    @performer = DecentJob::Performer.new(database: database)
    NativeExecution::EVENTS.clear
  end

  after do
    DecentJob::Base.remove_instance_variable(:@queue_adapter)
    DecentJob.reset!
  end

  it "performs a stored native job with its arguments" do
    NativeExecutionJob.perform_later(1, keyword: :k)
    result = @performer.perform_one
    _(result.state).must_equal "succeeded"
    _(NativeExecution::EVENTS).must_equal [[:perform, 1, :k, nil]]
  end

  it "records retries, discards and unhandled errors" do
    retried = NativeRetryJob.perform_later("ArgumentError")
    result = @performer.perform_one
    _(result.state).must_equal "retried"
    row = @store.find(retried.provider_job_id)
    _(row.values_at("state", "error_event")).must_equal ["ready", "retried"]
    _(row.fetch("scheduled_at")).must_be :>, Time.now + 50
    _(row.fetch("payload").fetch("executions")).must_equal 1

    NativeRetryJob.perform_later("KeyError")
    result = @performer.perform_one
    _(result.state).must_equal "discarded"
    _(result.error_event).must_equal "discarded"
    _(result.error).must_be_kind_of KeyError

    NativeUnhandledJob.perform_later
    result = @performer.perform_one
    _(result.state).must_equal "discarded"
    _(result.error_event).must_equal "unhandled"
    _(result.unhandled_error.message).must_equal "unhandled native"
  end

  it "deserializes, discards and retries a native job record" do
    job = NativeExecutionJob.perform_later("managed")
    record = DecentJob::Job.find(job.provider_job_id, database: database)
    _(record.job_instance).must_be_kind_of NativeExecutionJob
    _(record.job_instance.arguments).must_equal ["managed"]

    record.discard_job("stop")
    _(record.reload.state).must_equal "discarded"
    retried = record.retry_job
    _(retried).must_be_kind_of NativeExecutionJob
    _(record.reload.state).must_equal "ready"
    _(@performer.perform_one.state).must_equal "succeeded"
    _(NativeExecution::EVENTS).must_equal [[:perform, "managed", nil, nil]]
  end

  it "applies labels and concurrency limits to native jobs" do
    first = NativeLimitedJob.perform_later
    row = @store.find(first.provider_job_id)
    _(row.fetch("labels")).must_equal ["native"]
    _(row.fetch("concurrency_keys")).must_include "native-limit"

    second = NativeLimitedJob.new
    _(second.enqueue).must_equal false
    _(second.enqueue_error).must_be_kind_of DecentJob::EnqueueError
  end

  it "runs native batch members and their callback" do
    batch = DecentJob::Batch.enqueue(database: database, on_finish: NativeBatchCallback) do
      NativeExecutionJob.perform_later(2)
    end
    2.times { _(@performer.perform_one.state).must_equal "succeeded" }
    _(NativeExecution::EVENTS).must_equal [[:perform, 2, nil, batch.id], [:callback, batch.id, :finish]]
    _(batch.reload.finished?).must_equal true
  end

  it "enqueues native jobs in bulk" do
    captured = DecentJob::Bulk.enqueue do
      NativeExecutionJob.perform_later(3)
      NativeExecutionJob.perform_later(4)
    end
    _(captured.map(&:provider_job_id).compact.size).must_equal 2
    DecentJob.perform_all_later([NativeExecutionJob.new(5)])
    3.times { @performer.perform_one }
    _(NativeExecution::EVENTS.map { |event| event[1] }.sort).must_equal [3, 4, 5]
  end
end
