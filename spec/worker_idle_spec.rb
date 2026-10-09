# frozen_string_literal: true

require "spec_helper"
require "minitest/mock"
require "support/jobs"
require "timeout"

describe "worker idle duration" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    adapter = DecentJob::Adapter.new(database: database)
    RecordingJob.queue_adapter = BlockingJob.queue_adapter = adapter
    RecordingJob.events = Queue.new
    BlockingJob.started = Queue.new
    BlockingJob.release = Queue.new
    @time = [100.0]
    @worker = DecentJob::Worker.new(database: database, queues: "fast:1;default:1", poll_interval: 0.01, enable_listen_notify: false)
    clock = @time
    @worker.define_singleton_method(:monotonic_time) { clock.first }
  end

  after do
    5.times { BlockingJob.release << true }
    @worker.shutdown(timeout: 5)
  end

  def wait_until(&block)
    Timeout.timeout(5) { sleep 0.005 until block.call }
  end

  it "accepts optional duration and rejects invalid durations" do
    _(@worker.method(:idle?).arity).wont_equal 0
    _(@worker.idle?).must_equal true
    _(@worker.idle?(nil)).must_equal true
    _(@worker.idle?(0)).must_equal true
    [-1, Float::INFINITY, Float::NAN, "1", false, Complex(1, 1)].each do |duration|
      _ { @worker.idle?(duration) }.must_raise ArgumentError
    end
  end

  it "measures from startup without empty polls or future jobs resetting the clock" do
    _(@worker.method(:idle?).arity).wont_equal 0
    RecordingJob.set(wait: 3600).perform_later("future")
    @worker.start
    wait_until { @worker.stats[:schedulers].all? { |pool| pool[:empty_executions_count] > 0 } }
    _(@worker.idle?(10)).must_equal false
    @time[0] = 111
    @worker.wake
    wait_until { @worker.stats[:schedulers].all? { |pool| pool[:empty_executions_count] > 1 } }
    _(@worker.idle?(10)).must_equal true
    _(RecordingJob.events.size).must_equal 0
  end

  it "requires all pools to be inactive and measures elapsed time from execution start" do
    _(@worker.method(:idle?).arity).wont_equal 0
    @worker.start
    @time[0] = 110
    BlockingJob.set(queue: "fast").perform_later
    @worker.wake
    Timeout.timeout(5) { BlockingJob.started.pop }
    _(@worker.idle?).must_equal false
    @time[0] = 115
    _(@worker.idle?(1)).must_equal false
    BlockingJob.release << true
    wait_until { @worker.idle? }
    _(@worker.idle?(4)).must_equal true
    _(@worker.idle?(6)).must_equal false
  end

  it "resets its baseline on restart and ignores wall clock jumps" do
    _(@worker.method(:idle?).arity).wont_equal 0
    @worker.start
    @time[0] = 120
    Time.stub(:now, Time.utc(1900)) { _(@worker.idle?(10)).must_equal true }
    @worker.shutdown(timeout: 1)
    @worker.start
    Time.stub(:now, Time.utc(2100)) { _(@worker.idle?(10)).must_equal false }
  end
end
