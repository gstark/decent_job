# frozen_string_literal: true

require "open3"
require "rbconfig"
require "spec_helper"
require "tmpdir"

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

class NativeEnqueueCallbackJob < DecentJob::Base
  before_enqueue { NativeExecution::EVENTS << [:before_enqueue, arguments.first] }

  def perform(value)
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

  it "stores, retries and performs a native job without Active Job, ActiveSupport or GlobalID" do
    script = <<~RUBY
      require "decent_job"

      class FreshProcessJob < DecentJob::Base
        retry_on ArgumentError, attempts: 2, wait: 0
        before_perform { |job| job.decent_job_labels }

        def perform(path, at:)
          raise ArgumentError, "first attempt" if executions == 1
          File.write(path, JSON.generate(executions: executions, year: at.year, queue: queue_name))
        end
      end

      database = DecentJob::Database.new(url: JSON.parse(ENV.fetch("CONNECTION"), symbolize_names: true), pool_size: 2)
      FreshProcessJob.queue_adapter = DecentJob::Adapter.new(database: database)
      FreshProcessJob.set(queue: :fresh).perform_later(ENV.fetch("RESULT"), at: Time.utc(2026))
      performer = DecentJob::Performer.new(database: database)
      states = 2.times.map { performer.perform_one.state }
      raise "states: \#{states}" unless states == ["retried", "succeeded"]
      loaded = {active_job: defined?(ActiveJob), active_support: defined?(ActiveSupport), global_id: defined?(GlobalID)}.compact
      raise "loaded: \#{loaded.keys}" unless loaded.empty?
      database.close
    RUBY
    Dir.mktmpdir do |directory|
      result = File.join(directory, "result.json")
      environment = {"CONNECTION" => JSON.generate(@connection_options), "RESULT" => result}
      output, status = Open3.capture2e(environment, RbConfig.ruby, "-Ilib", "-e", script)
      _(status.success?).must_equal true, output
      _(JSON.parse(File.read(result))).must_equal("executions" => 2, "year" => 2026, "queue" => "fresh")
    end
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

  it "bulk enqueues uncontrolled native jobs without enqueue callbacks" do
    jobs = [NativeEnqueueCallbackJob.new(1), NativeEnqueueCallbackJob.new(2)]
    _(DecentJob.perform_all_later(jobs)).must_be_nil
    _(jobs.map(&:provider_job_id).compact.size).must_equal 2
    _(NativeExecution::EVENTS).must_be_empty
  end

  it "bulk enqueues controlled native jobs through their admission check" do
    jobs = [NativeLimitedJob.new, NativeLimitedJob.new]
    DecentJob.perform_all_later(jobs)
    _(jobs.map(&:successfully_enqueued?)).must_equal [true, false]
    _(jobs.last.enqueue_error).must_be_kind_of DecentJob::EnqueueError
  end
end
