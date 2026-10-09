# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

class GlobalInlineWaitingJob < RecordingJob
  def perform(deadline)
    remaining = deadline - Time.now.to_f
    sleep remaining if remaining.positive?
    super("waited")
  end
end

describe "global shutdown inspection" do
  after { DecentJob.reset! }

  it "observes an unconfigured runtime without validating, allocating, or changing its revision" do
    DecentJob.configuration.database_url = nil
    DecentJob.configuration.max_threads = -1
    revision = DecentJob.instance_variable_get(:@lifecycle_revision)
    3.times { _(DecentJob.shutdown?).must_equal true }
    _(DecentJob.instance_variable_get(:@database)).must_be_nil
    _(DecentJob.instance_variable_get(:@lifecycle_revision)).must_equal revision
    _(DecentJob.stopping?).must_equal false
  end

  it "checks every registered resource outside the registry mutex" do
    worker = DecentJob::Worker.new(database: Object.new, enable_listen_notify: false)
    manager = DecentJob::CronManager.new
    [worker, manager].each do |resource|
      resource.define_singleton_method(:shutdown?) do
        raise "registry mutex held during inspection" if DecentJob.runtime_mutex.owned?
        DecentJob.stopping?
        super()
      end
    end
    manager.start
    DecentJob.runtime_mutex.synchronize do
      DecentJob.instance_variable_get(:@workers) << worker
      DecentJob.instance_variable_get(:@cron_managers)[Object.new] = manager
    end
    inspectors = 5.times.map { Thread.new { 20.times.map { DecentJob.shutdown? } } }
    Timeout.timeout(3) { inspectors.each { |thread| _(thread.value).must_equal [false] * 20 } }
    manager.shutdown
    _(DecentJob.shutdown?).must_equal true
  ensure
    inspectors&.each { |thread| thread.kill.join if thread.alive? }
    manager&.shutdown
  end

  it "excludes directly owned resources outside the global registry" do
    manager = DecentJob::CronManager.new
    manager.start
    _(manager.shutdown?).must_equal false
    _(DecentJob.shutdown?).must_equal true
  ensure
    manager&.shutdown
  end
end

describe "global inline limit validation" do
  after { DecentJob.reset! }

  it "returns zero without opening or validating the database" do
    DecentJob.configuration.database_url = nil
    DecentJob.configuration.max_threads = -1
    _(DecentJob.perform_inline(limit: 0)).must_equal 0
    _(DecentJob.instance_variable_get(:@database)).must_be_nil
  end

  [-1, 1.0, true, false, "1"].each do |limit|
    it "rejects #{limit.inspect} before opening the database" do
      DecentJob.configuration.database_url = nil
      error = _ { DecentJob.perform_inline(limit: limit) }.must_raise ArgumentError
      _(error.message).must_match(/limit/)
      _(DecentJob.instance_variable_get(:@database)).must_be_nil
    end
  end
end

describe "global inline execution" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    DecentJob.configure { |config| config.database_url = @connection_options }
    adapter = DecentJob::Adapter.new(database: DecentJob.database, execution_mode: :external)
    [RecordingJob, RetryingJob, FailingJob, GlobalInlineWaitingJob].each { |job| job.queue_adapter = adapter }
    RecordingJob.events = Queue.new
  end

  after { DecentJob.reset! }

  it "drains more than one thousand real jobs by default" do
    jobs = 1001.times.map { |index| RecordingJob.new(index) }
    _(RecordingJob.queue_adapter.enqueue_all(jobs)).must_equal 1001
    _(DecentJob.perform_inline).must_equal 1001
    _(RecordingJob.events.size).must_equal 1001
    _(DecentJob.perform_inline).must_equal 0
  end

  it "claims future work that becomes due during an ordinary performance" do
    deadline = Time.now + 0.25
    GlobalInlineWaitingJob.perform_later(deadline.to_f + 0.01)
    future = RecordingJob.set(wait_until: deadline).perform_later("became due")
    _(DecentJob.perform_inline).must_equal 2
    _(RecordingJob.events.pop(true)).must_equal "waited"
    _(RecordingJob.events.pop(true)).must_equal "became due"
    _(DecentJob::Store.new(database).find(future.provider_job_id).fetch("state")).must_equal "succeeded"
  end

  it "applies a finite limit to attempts of the same retrying job" do
    job = RetryingJob.perform_later("bounded retry")
    _(DecentJob.perform_inline(limit: 1)).must_equal 1
    _(RecordingJob.events).must_be_empty
    _(DecentJob::Store.new(database).executions(job.provider_job_id).size).must_equal 1
    _(DecentJob.perform_inline(limit: 1)).must_equal 1
    _(RecordingJob.events.pop(true)).must_equal "bounded retry"
    _(DecentJob::Store.new(database).executions(job.provider_job_id).size).must_equal 2
  end

  it "accepts an explicit unlimited drain and counts immediate retry attempts" do
    RetryingJob.perform_later("retried")
    _(DecentJob.perform_inline(limit: nil)).must_equal 2
    _(RecordingJob.events.pop(true)).must_equal "retried"
    _(DecentJob.perform_inline(limit: nil)).must_equal 0
  end

  it "limits attempts while retaining queue selection and future work" do
    first = RecordingJob.set(queue: "selected").perform_later("first")
    second = RecordingJob.set(queue: "selected").perform_later("second")
    other = RecordingJob.set(queue: "other").perform_later("other")
    future = RecordingJob.set(queue: "selected", wait: 3600).perform_later("future")
    _(DecentJob.perform_inline(queues: "selected", limit: 1)).must_equal 1
    _(DecentJob.perform_inline(queues: "selected")).must_equal 1
    store = DecentJob::Store.new(database)
    [first, second].each { |job| _(store.find(job.provider_job_id).fetch("state")).must_equal "succeeded" }
    [other, future].each { |job| _(store.find(job.provider_job_id).fetch("state")).must_equal "ready" }
  end

  it "raises an unhandled job error after recording its attempt" do
    job = FailingJob.perform_later
    error = _ { DecentJob.perform_inline(limit: nil) }.must_raise RuntimeError
    _(error.message).must_equal "unhandled error"
    _(DecentJob::Store.new(database).executions(job.provider_job_id).size).must_equal 1
  end
end
