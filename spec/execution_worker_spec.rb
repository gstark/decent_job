# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

class WorkerFatalError < ScriptError; end

class WorkerNotImplementedJob < ActiveJob::Base
  def perform
    raise NotImplementedError, "not implemented"
  end
end

class WorkerFatalJob < ActiveJob::Base
  def perform
    raise WorkerFatalError, "fatal"
  end
end

class WorkerBlockingJob < ActiveJob::Base
  class_attribute :started, default: Queue.new
  class_attribute :release, default: Queue.new
  class_attribute :completed, default: Queue.new

  def perform
    started << true
    release.pop
    completed << true
  end
end

describe "worker execution error reporting" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    adapter = DecentJob::Adapter.new(database: database)
    [FailingJob, RetryingJob, DiscardingJob, RecordingJob, WorkerNotImplementedJob, WorkerFatalJob, WorkerBlockingJob].each { |klass| klass.queue_adapter = adapter }
    RecordingJob.events = Queue.new
    @errors = Queue.new
    WorkerBlockingJob.started = Queue.new
    WorkerBlockingJob.release = Queue.new
    WorkerBlockingJob.completed = Queue.new
  end

  after do
    @worker&.shutdown(timeout: 5)
    DecentJob.reset!
  end

  def worker(**options)
    @worker = DecentJob::Worker.new(database: database, max_threads: 1, poll_interval: 0.02, enable_listen_notify: false,
      on_thread_error: ->(error) { @errors << error }, **options)
  end

  def pop(queue)
    Timeout.timeout(5) { queue.pop }
  end

  it "reports each escaping handled error once while keeping workers available" do
    FailingJob.perform_later
    WorkerNotImplementedJob.perform_later
    RecordingJob.perform_later("next")
    worker.start
    _(pop(@errors)).must_be_kind_of RuntimeError
    _(pop(@errors)).must_be_kind_of NotImplementedError
    _(pop(RecordingJob.events)).must_equal "next"
    @worker.shutdown(timeout: 5)
    _(@errors.size).must_equal 0
  end

  it "does not report errors handled by Active Job retry or discard" do
    RetryingJob.perform_later("retried")
    DiscardingJob.perform_later
    worker.start
    _(pop(RecordingJob.events)).must_equal "retried"
    @worker.shutdown(timeout: 5)
    _(@errors.size).must_equal 0
  end

  it "isolates nonstandard exceptions from reporters" do
    FailingJob.perform_later
    RecordingJob.perform_later("reporter survived")

    worker(on_thread_error: lambda { |error|
      @errors << error
      raise NotImplementedError, "reporter unavailable"
    }).start

    _(pop(@errors)).must_be_kind_of RuntimeError
    _(pop(RecordingJob.events)).must_equal "reporter survived"
    @worker.shutdown(timeout: 5)
    _(@errors.size).must_equal 0
  end

  it "joins remaining execution threads and its listener before re-raising a fatal task error" do
    DecentJob.configuration.handled_exceptions = []
    WorkerFatalJob.set(queue: "fatal").perform_later
    holding_job = WorkerBlockingJob.set(queue: "holding").perform_later
    previous_reporting = Thread.report_on_exception
    Thread.report_on_exception = false
    worker(queues: "fatal:1;holding:1", enable_listen_notify: true).start
    error = pop(@errors)
    pop(WorkerBlockingJob.started)
    Timeout.timeout(5) { sleep 0.005 until @worker.stats.fetch(:schedulers).any? { |pool| pool.fetch(:live_threads) < pool.fetch(:max_threads) } }
    _(@worker.running?).must_equal true
    _(@worker.started?).must_equal false
    _(@worker.connected?).must_equal false

    releaser = Thread.new {
      sleep 0.1
      WorkerBlockingJob.release << true
    }

    raised = _ { @worker.shutdown(timeout: 5) }.must_raise WorkerFatalError
    _(raised).must_equal error
    _(WorkerBlockingJob.completed.size).must_equal 1
    _(DecentJob::Store.new(database).executions(holding_job.provider_job_id).last.fetch("state")).must_equal "succeeded"
  ensure
    releaser&.join

    begin
      @worker&.shutdown(timeout: 5)
    rescue WorkerFatalError
      nil
    end

    @worker = nil
    Thread.report_on_exception = previous_reporting
  end

  it "stops and drains every registered worker and clears reset state after a fatal task error" do
    DecentJob.configuration.handled_exceptions = []
    WorkerFatalJob.set(queue: "fatal").perform_later
    WorkerBlockingJob.set(queue: "holding").perform_later
    previous_reporting = Thread.report_on_exception
    Thread.report_on_exception = false
    fatal = worker(queues: "fatal")
    holding = DecentJob::Worker.new(database: database, queues: "holding", max_threads: 1, poll_interval: 0.02, enable_listen_notify: false)
    DecentJob.start_worker(fatal)
    error = pop(@errors)
    DecentJob.start_worker(holding)
    pop(WorkerBlockingJob.started)
    Timeout.timeout(5) { sleep 0.005 while fatal.running? }
    _(DecentJob.started?).must_equal false
    _(DecentJob.connected?).must_equal false

    releaser = Thread.new {
      sleep 0.1
      WorkerBlockingJob.release << true
    }

    raised = _ { DecentJob.reset! }.must_raise WorkerFatalError
    _(raised).must_equal error
    _(WorkerBlockingJob.completed.size).must_equal 1
    _(holding.running?).must_equal false
    _(DecentJob.shutdown(timeout: 0)).must_equal true
    _(DecentJob.configuration.handled_exceptions).must_equal [StandardError, NotImplementedError]
  ensure
    releaser&.join

    begin
      DecentJob.reset!
    rescue WorkerFatalError
      nil
    end

    holding&.shutdown(timeout: 5)
    @worker = nil
    Thread.report_on_exception = previous_reporting
  end

  [FailingJob, WorkerFatalJob].each do |klass|
    it "reports and propagates excluded #{klass.name} exceptions instead of retrying them as infrastructure errors" do
      DecentJob.configuration.handled_exceptions = []
      job = klass.perform_later
      RecordingJob.perform_later("must not run")
      previous_reporting = Thread.report_on_exception
      Thread.report_on_exception = false

      worker(on_thread_error: lambda { |error|
        @errors << error
        raise NotImplementedError, "reporter unavailable"
      }).start

      error = pop(@errors)
      raised = _ { @worker.shutdown(timeout: 5) }.must_raise (klass == FailingJob) ? RuntimeError : WorkerFatalError
      _(raised).must_equal error
      _(@errors.size).must_equal 0
      _(RecordingJob.events.size).must_equal 0
      store = DecentJob::Store.new(database)
      _(store.find(job.provider_job_id).fetch("state")).must_equal "running"
      _(store.executions(job.provider_job_id).size).must_equal 1
    ensure
      @worker = nil
      Thread.report_on_exception = previous_reporting
    end
  end
end
