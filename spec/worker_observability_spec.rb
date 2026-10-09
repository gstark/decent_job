# frozen_string_literal: true

require "spec_helper"
require "minitest/mock"
require "support/jobs"
require "timeout"

class ObservabilityBlockingFailureJob < BlockingJob
  def perform
    super
    raise "execution failed"
  end
end

describe "worker observability" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    adapter = DecentJob::Adapter.new(database: database)
    [BlockingJob, ObservabilityBlockingFailureJob, RecordingJob, RetryingJob, DiscardingJob].each { |job| job.queue_adapter = adapter }
    BlockingJob.started = Queue.new
    BlockingJob.release = Queue.new
    ObservabilityBlockingFailureJob.started = BlockingJob.started
    ObservabilityBlockingFailureJob.release = BlockingJob.release
    RecordingJob.events = Queue.new
    @workers = []
  end

  after do
    10.times { BlockingJob.release << true }
    @workers.each { |worker| worker.shutdown(timeout: 5) }
    DecentJob.reset!
  end

  def worker(**options)
    DecentJob::Worker.new(database: database, max_threads: 1, poll_interval: -1, enable_listen_notify: false, **options).tap { |value| @workers << value }
  end

  def wait_until(&block)
    Timeout.timeout(5) { sleep 0.005 until block.call }
  end

  def pop(queue)
    Timeout.timeout(5) { queue.pop }
  end

  it "reports actual execution starts, completions, and every successful queue check" do
    instance = worker
    _(instance.stats[:execution_at]).must_be_nil
    _(instance.stats[:check_queue_at]).must_be_nil
    2.times { BlockingJob.perform_later }
    instance.start
    pop(BlockingJob.started)
    first = instance.stats
    _(first.fetch(:active_execution_thread_count)).must_equal 1
    _(first.fetch(:total_executions_count)).must_equal 0
    _(first.fetch(:execution_at)).wont_be_nil
    _(first.fetch(:check_queue_at)).wont_be_nil
    _(first[:schedulers].first[:execution_at]).must_equal first[:execution_at]
    BlockingJob.release << true
    pop(BlockingJob.started)
    second = instance.stats
    _(second[:execution_at] > first[:execution_at]).must_equal true
    _(second[:check_queue_at] > first[:check_queue_at]).must_equal true
    _(second[:succeeded_executions_count]).must_equal 1
    BlockingJob.release << true
    wait_until { instance.stats[:empty_executions_count].to_i.positive? }
    final = instance.stats
    _(final[:execution_at] > second[:execution_at]).must_equal true
    _(final[:total_executions_count]).must_equal 2
    _(final[:active_execution_thread_count]).must_equal 0
    _(final[:total_succeeded_executions_count]).must_equal 2
  end

  it "counts handled retries and discards as errors" do
    RetryingJob.perform_later("done")
    DiscardingJob.perform_later
    instance = worker.start
    pop(RecordingJob.events)
    wait_until { instance.stats[:total_executions_count] == 3 }
    stats = instance.stats
    _(stats.fetch(:errored_executions_count)).must_equal 2
    _(stats.fetch(:succeeded_executions_count)).must_equal 1
  end

  it "records completion before a blocking error reporter returns" do
    reports = Queue.new
    reporter_release = Queue.new
    instance = worker(on_thread_error: ->(_error) {
      reports << true
      reporter_release.pop
    })
    ObservabilityBlockingFailureJob.perform_later
    instance.start
    pop(BlockingJob.started)
    started_at = instance.stats[:execution_at]
    BlockingJob.release << true
    pop(reports)
    stats = instance.stats
    _(stats[:errored_executions_count]).must_equal 1
    _(stats[:execution_at] > started_at).must_equal true
  ensure
    reporter_release << true
  end

  it "records activity before reporting an infrastructure failure after execution" do
    reports = Queue.new
    reporter_release = Queue.new
    clock = [100.0]
    instance = worker(on_thread_error: ->(error) {
      reports << error
      reporter_release.pop
    })
    instance.define_singleton_method(:monotonic_time) { clock.first }
    constructor = DecentJob::Performer.method(:new)
    failing_finish = lambda do |**options|
      constructor.call(**options).tap do |performer|
        performer.define_singleton_method(:finish) { |*| raise IOError, "failed to persist completion" }
      end
    end
    BlockingJob.perform_later
    DecentJob::Performer.stub(:new, failing_finish) do
      instance.start
      pop(BlockingJob.started)
      started_at = instance.stats[:execution_at]
      clock[0] = 110.0
      BlockingJob.release << true
      _(pop(reports)).must_be_kind_of IOError
      stats = instance.stats
      _(stats[:total_executions_count]).must_equal 0
      _(stats[:errored_executions_count]).must_equal 0
      _(stats[:succeeded_executions_count]).must_equal 0
      _(stats[:execution_at] > started_at).must_equal true
      instance.shutdown(timeout: 0)
      clock[0] = 120.0
      reporter_release << true
      _(instance.shutdown(timeout: 5)).must_equal true
      _(instance.stats[:execution_at]).must_equal stats[:execution_at]
      _(instance.idle?(5)).must_equal true
    end
  ensure
    reporter_release << true
  end

  it "does not count infrastructure errors as execution outcomes" do
    errors = Queue.new
    instance = worker(on_thread_error: ->(error) { errors << error })
    performer = DecentJob::Performer.new(database: database)
    performer.define_singleton_method(:perform_one) { |**| raise IOError, "database unavailable" }
    DecentJob::Performer.stub(:new, performer) do
      instance.start
      _(pop(errors)).must_be_kind_of IOError
      stats = instance.stats
      _(stats[:total_executions_count]).must_equal 0
      _(stats[:errored_executions_count]).must_equal 0
      _(stats[:succeeded_executions_count]).must_equal 0
      _(stats[:empty_executions_count]).must_equal 0
      _(stats[:execution_at]).must_be_nil
      _(stats[:check_queue_at]).wont_be_nil
      instance.shutdown(timeout: 5)
    end
  end

  it "returns independent strings and nested snapshots" do
    instance = worker
    first = instance.stats
    first[:schedulers].first[:queues].replace("changed")
    first[:schedulers].first[:started_at].replace("changed")
    second = instance.stats
    _(second[:schedulers].first[:queues]).must_equal "*"
    _(second[:schedulers].first[:started_at]).wont_equal "changed"
    BlockingJob.perform_later
    instance.start
    pop(BlockingJob.started)
    instance.stats[:execution_at].replace("changed")
    _(instance.stats[:execution_at]).wont_equal "changed"
  end

  it "resets activity timestamps and outcomes on restart" do
    instance = worker.start
    wait_until { instance.stats[:empty_executions_count].to_i.positive? }
    instance.shutdown(timeout: 5)
    BlockingJob.perform_later
    instance.start
    pop(BlockingJob.started)
    _(instance.stats[:empty_executions_count]).must_equal 0
    _(instance.stats[:total_executions_count]).must_equal 0
  end

  it "aggregates registered workers without holding the registry lock" do
    fast = worker(queues: "fast")
    slow = worker(queues: "slow")
    BlockingJob.set(queue: "fast").perform_later
    DecentJob.start_worker(fast)
    DecentJob.start_worker(slow)
    pop(BlockingJob.started)
    wait_until { slow.stats[:empty_executions_count].to_i.positive? }
    original = fast.method(:stats)
    fast.define_singleton_method(:stats) do
      DecentJob.stopping?
      original.call
    end
    stats = Timeout.timeout(1) { DecentJob.stats }
    _(stats[:schedulers].size).must_equal 2
    _(stats[:active_execution_thread_count]).must_equal 1
    _(stats[:execution_at]).must_equal fast.stats[:execution_at]
    _(DecentJob.idle?).must_equal false
    BlockingJob.release << true
    wait_until { DecentJob.idle? }
    _(DecentJob.stats[:total_executions_count]).must_equal 1
  end
end

describe "inactive runtime inspection" do
  after { DecentJob.reset! }

  it "reports empty aggregate statistics without opening a database" do
    DecentJob.reset!
    DecentJob.stub(:database, -> { raise "must not open a database" }) do
      _(DecentJob.idle?).must_equal true
      _(DecentJob.idle?(10)).must_equal true
      stats = DecentJob.stats
      _(stats[:schedulers]).must_equal []
      %i[empty_executions_count errored_executions_count succeeded_executions_count total_executions_count active_execution_thread_count].each do |key|
        _(stats.fetch(key)).must_equal 0
      end
      _(stats.fetch(:execution_at)).must_be_nil
      _(stats.fetch(:check_queue_at)).must_be_nil
    end
    _ { DecentJob.idle?(-1) }.must_raise ArgumentError
  end
end

describe "statistics timestamp aggregation" do
  it "compares actual instants across UTC offsets" do
    pool = DecentJob::Worker.new(database: Object.new).stats.fetch(:schedulers).first
    older = pool.merge(execution_at: "2026-11-01T01:50:00.000000-04:00", check_queue_at: "2026-11-01T01:50:00.000000-04:00")
    newer = pool.merge(execution_at: "2026-11-01T01:10:00.000000-05:00", check_queue_at: "2026-11-01T01:10:00.000000-05:00")
    result = DecentJob::Stats.aggregate([older, newer])
    _(result[:execution_at]).must_equal newer[:execution_at]
    _(result[:check_queue_at]).must_equal newer[:check_queue_at]
  end
end
