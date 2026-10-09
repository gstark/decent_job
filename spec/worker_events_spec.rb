# frozen_string_literal: true

require "minitest/mock"
require "spec_helper"
require "support/jobs"
require "timeout"

class EventsFatalError < ScriptError; end

class EventsFatalJob < ActiveJob::Base
  def perform
    raise EventsFatalError, "fatal attempt"
  end
end

describe "worker task and poll events" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    adapter = DecentJob::Adapter.new(database: database)
    [RecordingJob, RetryingJob, FailingJob, BlockingJob, EventsFatalJob].each { |job| job.queue_adapter = adapter }
    RecordingJob.events = Queue.new
    BlockingJob.started = Queue.new
    BlockingJob.release = Queue.new
    @subscriptions = []
    @events = Queue.new
    @errors = Queue.new
  end

  after do
    BlockingJob.release << true
    @worker&.shutdown(timeout: 5)
    @subscriptions.each { |subscription| ActiveSupport::Notifications.unsubscribe(subscription) }
    DecentJob.reset!
  end

  def worker(**options)
    @worker = DecentJob::Worker.new(database: database, max_threads: 1, poll_interval: -1,
      enable_listen_notify: false, on_thread_error: ->(error) { @errors << error }, **options)
  end

  def subscribe(name, &block)
    @subscriptions << ActiveSupport::Notifications.subscribe("#{name}.decent_job", &block)
  end

  def pop(queue = @events)
    Timeout.timeout(5) { queue.pop }
  end

  def await
    Timeout.timeout(5) { sleep 0.005 until yield }
  end

  it "emits actual success, handled retry, unhandled, and empty results with pool identity" do
    subscribe("finished_job_task") { |event| @events << event.payload.dup }
    RecordingJob.perform_later("success")
    RetryingJob.perform_later("retry")
    FailingJob.perform_later
    worker.start
    results = Array.new(5) { pop }
    _(results.count { |payload| payload[:result].nil? }).must_equal 1
    # Four executions: success, failed retry, successful retry, unhandled failure.
    attempts = results.reject { |payload| payload[:result].nil? }
    _(attempts.map { |payload| payload[:result].state }.sort).must_equal %w[discarded retried succeeded succeeded]
    _(attempts.map { |payload| payload[:error] }).must_equal [nil] * 4
    _(attempts.count { |payload| payload[:result].unhandled_error }).must_equal 1
    results.each do |payload|
      _(payload[:scheduler]).must_be_same_as @worker
      _(payload[:queues]).must_equal "*"
      _(payload[:performer_name]).must_equal "*"
      _(payload[:pool_index]).must_equal 0
      _(payload[:process_id]).must_equal Process.pid
      _(payload[:thread_name]).must_be_kind_of String
      _(payload[:time]).must_be_kind_of Time
    end
  end

  it "records completion then reports then emits before cleanup, without trusting observer mutation" do
    order = Queue.new
    worker(on_thread_error: ->(error) { order << [:report, @worker.stats[:errored_executions_count], error] })
    @worker.define_singleton_method(:cleanup_after_attempt) { order << [:cleanup] }
    subscribe("finished_job_task") do |event|
      next unless event.payload[:result]
      order << [:event, event.payload[:error], event.payload[:result]]
      event.payload[:result] = nil
      event.payload[:error] = RuntimeError.new("changed")
    end
    FailingJob.perform_later
    @worker.start
    report, event, cleanup = Array.new(3) { pop(order) }
    _(report[0..1]).must_equal [:report, 1]
    _(event[0..1]).must_equal [:event, nil]
    _(event[2].unhandled_error).must_be_same_as report[2]
    _(cleanup).must_equal [:cleanup]
  end

  it "captures retry routing before an observer mutates the result strings" do
    routed = Queue.new
    job = RetryingJob.perform_later("retried")
    subscribe("finished_job_task") do |event|
      result = event.payload[:result]
      if result&.state == "retried"
        result.job_id.replace("changed")
        result.state.replace("succeeded") unless result.state.frozen?
      end
    end
    worker
    original = DecentJob::Store.instance_method(:scheduling_hints)
    replacement = ->(ids) {
      routed << ids
      []
    }
    DecentJob::Store.define_method(:scheduling_hints, replacement)
    @worker.start
    _(pop(routed)).must_equal [job.provider_job_id]
    _(pop(RecordingJob.events)).must_equal "retried"
  ensure
    @worker&.shutdown(timeout: 5)
    DecentJob::Store.define_method(:scheduling_hints, original) if original
  end

  it "emits a task error after execution persistence fails and no extra event for cache maintenance" do
    constructor = DecentJob::Performer.method(:new)
    error = IOError.new("finish failed")
    factory = lambda do |**options|
      constructor.call(**options).tap { |performer| performer.define_singleton_method(:finish) { |*| raise error } }
    end
    subscribe("finished_job_task") { |event| @events << event.payload.dup }
    worker
    BlockingJob.perform_later
    DecentJob::Performer.stub(:new, factory) do
      @worker.start
      pop(BlockingJob.started)
      BlockingJob.release << true
      payload = pop
      _(payload[:error]).must_be_same_as error
      _(payload[:result]).must_be_nil
      @worker.shutdown(timeout: 5)
    end
    _(@errors.pop).must_be_same_as error
  end

  it "emits one empty attempt before failing cache maintenance" do
    subscribe("finished_job_task") { |event| @events << event.payload.dup }
    worker(on_thread_error: ->(error) {
      @errors << error
      @worker.shutdown(timeout: 0)
    })
    @worker.define_singleton_method(:refresh_cache) { |*| raise IOError, "cache failed" }
    @worker.start
    _(pop[:result]).must_be_nil
    _(pop(@errors).message).must_equal "cache failed"
    @worker.shutdown(timeout: 5)
    _(@events).must_be_empty
  end

  it "reports and emits excluded fatal errors before propagating through shutdown" do
    previous = Thread.report_on_exception
    Thread.report_on_exception = false
    subscribe("finished_job_task") { |event| @events << event.payload.dup }
    EventsFatalJob.perform_later
    worker.start
    payload = pop
    _(payload[:error]).must_be_kind_of EventsFatalError
    _(payload[:result]).must_be_nil
    _(pop(@errors)).must_be_same_as payload[:error]
    _(assert_raises(EventsFatalError) { @worker.shutdown(timeout: 5) }).must_be_same_as payload[:error]
  ensure
    @worker = nil
    Thread.report_on_exception = previous
  end

  it "does not claim a killed unfinished job attempt completed" do
    subscribe("finished_job_task") { |event| @events << event.payload.dup }
    BlockingJob.perform_later
    worker.start
    pop(BlockingJob.started)
    @worker.shutdown(timeout: 0, force: true)
    @worker.shutdown(timeout: 5)
    _(@events).must_be_empty
  end

  it "reports observer failures to the worker reporter and allows reentrant inspection and stop" do
    subscribe("finished_job_task") do |event|
      @events << event.payload[:scheduler].stats
      event.payload[:scheduler].shutdown(timeout: 0)
      raise "worker observer failed"
    end
    worker.start
    pop
    _(pop(@errors).message).must_equal "worker observer failed"
    _(@worker.shutdown(timeout: 5)).must_equal true
  end

  it "emits positive poll expirations for each pool, with accepted permit counts" do
    subscribe("finished_timer_task") { |event| @events << event.payload.dup }
    worker(queues: "fast:1;slow:1", poll_interval: 0.03, max_cache: 0).start
    received = []
    received << pop until received.map { |payload| payload[:queues] }.uniq.size == 2
    received.each do |payload|
      _(payload[:result]).must_equal 1
      _(payload[:error]).must_be_nil
      _(payload[:scheduler]).must_be_same_as @worker
      _(payload[:time]).must_be_kind_of Time
      _(payload[:process_id]).must_equal Process.pid
      _(payload[:thread_name]).must_be_kind_of String
    end
  end

  it "reports a failed poll wake before its event without claiming work or finishing another task" do
    order = Queue.new
    attempts = Queue.new
    error = IOError.new("poll wake failed")
    subscribe("finished_job_task") { |event| attempts << event.payload.dup }
    subscribe("finished_timer_task") do |event|
      order << [:event, event.payload.dup]
      @worker.shutdown(timeout: 0)
    end
    constructor = DecentJob::SchedulerCache.method(:new)
    factory = lambda do |**options|
      constructor.call(**options).tap do |cache|
        cache.define_singleton_method(:wake) { |*| raise error }
      end
    end
    DecentJob::SchedulerCache.stub(:new, factory) do
      worker(poll_interval: 0.02, max_cache: 0, on_thread_error: ->(error) {
        @worker.stats
        order << [:report, error]
      }).start
      _(pop(order)).must_equal [:report, error]
      event = pop(order)
      _(event.first).must_equal :event
      _(event.last[:error]).must_be_same_as error
      _(event.last[:result]).must_be_nil
      _(event.last[:time]).must_be_kind_of Time
      @worker.shutdown(timeout: 5)
      _(attempts.size).must_equal 1
      _(@worker.instance_variable_get(:@caches).map(&:busy)).must_equal [0]
    end
  end

  [0, -1].each do |interval|
    it "does not emit poll events for startup, notifications or cached deadlines with interval #{interval}" do
      subscribe("finished_timer_task") { |event| @events << event }
      job = RecordingJob.set(wait: 0.1).perform_later("cached")
      worker(poll_interval: interval).start
      @worker.wake(queue_name: "default", scheduled_at: Time.now + 0.05)
      _(pop(RecordingJob.events)).must_equal "cached"
      @worker.shutdown(timeout: 5)
      _(@events).must_be_empty
      _(job.provider_job_id).wont_be_nil
    end
  end

  it "releases a claimed permit when killed inside a timer observer" do
    subscribe("finished_timer_task") { |event|
      @events << Thread.current
      Queue.new.pop
    }
    worker(poll_interval: 0.02, max_cache: 0).start
    thread = pop
    thread.kill.join
    @worker.shutdown(timeout: 5)
    caches = @worker.instance_variable_get(:@caches)
    _(caches.map(&:busy)).must_equal [0]
    _(@worker.stats[:active_execution_thread_count]).must_equal 0
  end
end
