# frozen_string_literal: true

require "minitest/mock"
require "spec_helper"
require "support/jobs"
require "timeout"

class ProcessWorkerFatalError < ScriptError; end

class ProcessWorkerFatalJob < ActiveJob::Base
  class_attribute :failure

  def perform
    raise failure
  end
end

describe "worker live execution capacity" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    [RecordingJob, ProcessWorkerFatalJob].each { |job| job.queue_adapter = DecentJob::Adapter.new(database: database) }
    RecordingJob.events = Queue.new
    ProcessWorkerFatalJob.failure = ProcessWorkerFatalError.new("fatal execution")
    @reporting = Thread.report_on_exception
    Thread.report_on_exception = false
    @errors = Queue.new
  end

  after do
    begin
      @worker&.shutdown(timeout: 5)
    rescue ProcessWorkerFatalError
      nil
    end
    Thread.report_on_exception = @reporting
  end

  def await
    Timeout.timeout(5) { sleep 0.005 until yield }
  end

  it "removes a dead pool thread from available capacity while other pools continue" do
    ProcessWorkerFatalJob.set(queue: "fatal").perform_later
    @worker = DecentJob::Worker.new(database: database, queues: "fatal:1;healthy:1", poll_interval: 0.01,
      enable_listen_notify: false, on_thread_error: ->(error) { @errors << error })
    @worker.start
    _(Timeout.timeout(5) { @errors.pop }).must_be_same_as ProcessWorkerFatalJob.failure
    RecordingJob.set(queue: "healthy").perform_later("survives")
    _(Timeout.timeout(5) { RecordingJob.events.pop }).must_equal "survives"
    await { @worker.stats.fetch(:schedulers).first.fetch(:active_threads).zero? }
    pools = @worker.stats.fetch(:schedulers)
    _(pools.first.fetch(:available_threads)).must_equal 0
    _(pools.first.fetch(:live_threads)).must_equal 0
    _(pools.last.fetch(:live_threads)).must_equal 1
    _(pools.first.fetch(:errored_executions_count)).must_equal 1
    _(@worker.running?).must_equal true
    error = _ { @worker.shutdown(timeout: 5) }.must_raise ProcessWorkerFatalError
    _(error).must_be_same_as ProcessWorkerFatalJob.failure
    _(@worker.stats.fetch(:schedulers).sum { |pool| pool.fetch(:available_threads) }).must_equal 0
  end

  it "keeps one process cleanup in flight across intervals and releases its guard after failure" do
    calls = Queue.new
    release = Queue.new
    offset = 0
    @worker = DecentJob::Worker.new(database: database, max_threads: 2, poll_interval: 0.01,
      enable_listen_notify: false, on_thread_error: ->(error) { @errors << error })
    @worker.define_singleton_method(:monotonic_time) { Process.clock_gettime(Process::CLOCK_MONOTONIC) + offset }
    cleanup = lambda do |**options|
      calls << options
      release.pop
      raise "cleanup unavailable"
    end
    DecentJob::ProcessRecord.stub(:cleanup, cleanup) do
      @worker.start
      first = Timeout.timeout(5) { calls.pop }
      offset = 60
      RecordingJob.perform_later("cleanup still running")
      _(Timeout.timeout(2) { RecordingJob.events.pop }).must_equal "cleanup still running"
      _(calls.size).must_equal 0
      _(first.fetch(:stopping).call).must_equal false
      release << true
      _(Timeout.timeout(5) { @errors.pop }.message).must_equal "cleanup unavailable"
      Timeout.timeout(5) { calls.pop }
      _(@worker.shutdown(timeout: 0)).must_equal false
      _(first.fetch(:stopping).call).must_equal true
    ensure
      10.times { release << true }
      @worker.shutdown(timeout: 5)
    end
  end

  it "closes its listener after the last fatal execution and can restart after draining" do
    connections = Queue.new
    original_connect = database.method(:connect)
    database.define_singleton_method(:connect) do
      original_connect.call.tap { |connection| connections << connection }
    end
    job = ProcessWorkerFatalJob.perform_later
    @worker = DecentJob::Worker.new(database: database, max_threads: 1, poll_interval: 0.01,
      on_thread_error: ->(error) { @errors << error })
    @worker.start
    previous_id = @worker.process_id
    _(Timeout.timeout(5) { @errors.pop }).must_be_same_as ProcessWorkerFatalJob.failure
    owned = 2.times.map { Timeout.timeout(5) { connections.pop } }
    await { owned.all?(&:finished?) }
    _(@worker.running?).must_equal false
    _(@worker.stats.fetch(:schedulers).first.fetch(:available_threads)).must_equal 0
    error = _ { @worker.shutdown(timeout: 5) }.must_raise ProcessWorkerFatalError
    _(error).must_be_same_as ProcessWorkerFatalJob.failure
    DecentJob::Job.find(job.provider_job_id, database: database).force_discard_job("stop retrying")
    @worker.start
    _(@worker.process_id).wont_equal previous_id
    _(@worker.stats.fetch(:total_executions_count)).must_equal 0
    RecordingJob.perform_later("restarted")
    _(Timeout.timeout(5) { RecordingJob.events.pop }).must_equal "restarted"
    _(@worker.shutdown(timeout: 5)).must_equal true
    _(@worker.stats.fetch(:schedulers).first.fetch(:live_threads)).must_equal 0
  end
end
