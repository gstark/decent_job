# frozen_string_literal: true

require "spec_helper"
require "timeout"

class AdapterShutdownFailure < ScriptError; end

class AdapterShutdownBlockingJob < ActiveJob::Base
  class_attribute :started, default: Queue.new
  class_attribute :release, default: Queue.new
  class_attribute :completed, default: Queue.new

  def perform
    started << Thread.current
    release.pop
    completed << true
  end
end

class AdapterShutdownFatalJob < ActiveJob::Base
  class_attribute :failure

  def perform
    raise failure
  end
end

describe "adapter shutdown after fatal resource errors" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    DecentJob.configuration.max_threads = 1
    DecentJob.configuration.enable_listen_notify = false
    @reported = Queue.new
    DecentJob.configuration.on_thread_error = ->(error) { @reported << error }
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    [AdapterShutdownBlockingJob, AdapterShutdownFatalJob].each { |klass| klass.queue_adapter = @adapter }
    AdapterShutdownBlockingJob.started = Queue.new
    AdapterShutdownBlockingJob.release = Queue.new
    AdapterShutdownBlockingJob.completed = Queue.new
    @fatal_error = AdapterShutdownFailure.new("fatal resource")
    AdapterShutdownFatalJob.failure = @fatal_error
    @cron_started = Queue.new
    @cron_release = Queue.new
    @cron_completed = Queue.new
    @previous_reporting = Thread.report_on_exception
    Thread.report_on_exception = false
  end

  after do
    AdapterShutdownBlockingJob.release << true
    @cron_release << true
    @releaser&.join
    begin
      DecentJob.reset!
    rescue AdapterShutdownFailure
      nil
    end
    Thread.report_on_exception = @previous_reporting
  end

  def pop(queue)
    Timeout.timeout(5) { queue.pop }
  end

  it "signals and drains its async worker even when the cron stop raises" do
    AdapterShutdownBlockingJob.perform_later
    worker_thread = pop(AdapterShutdownBlockingJob.started)
    DecentJob.configuration.cron = {fatal: {class: -> {}, cron: ->(_) {
      @cron_started << Thread.current
      raise @fatal_error
    }}}
    manager = DecentJob.start_cron(database: database)
    cron_thread = pop(@cron_started)
    Timeout.timeout(5) { sleep 0.001 while cron_thread.alive? }
    @releaser = Thread.new do
      sleep 0.05
      AdapterShutdownBlockingJob.release << true
    end
    error = _ { @adapter.shutdown(timeout: 5) }.must_raise AdapterShutdownFailure
    _(error).must_be_same_as @fatal_error
    _(AdapterShutdownBlockingJob.completed.size).must_equal 1
    _(worker_thread.alive?).must_equal false
    _(manager.shutdown?).must_equal true
  end

  it "honors the deadline then drains cron callbacks even when the worker stop raises" do
    DecentJob.configuration.cron = {holding: {class: -> {
      @cron_started << Thread.current
      @cron_release.pop
      @cron_completed << true
    }, cron: ->(previous) { previous ? previous + 60 : Time.now }}}
    manager = DecentJob.start_cron(database: database)
    cron_thread = pop(@cron_started)
    AdapterShutdownFatalJob.perform_later
    _(pop(@reported)).must_be_same_as @fatal_error
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    first = _ { @adapter.shutdown(timeout: 0.01) }.must_raise AdapterShutdownFailure
    _(first).must_be_same_as @fatal_error
    _(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).must_be :<, 0.5
    _(manager.shutdown?).must_equal false
    @releaser = Thread.new do
      sleep 0.05
      @cron_release << true
    end
    error = _ { @adapter.shutdown(timeout: 5) }.must_raise AdapterShutdownFailure
    _(error).must_be_same_as @fatal_error
    _(@cron_completed.size).must_equal 1
    _(cron_thread.alive?).must_equal false
    _(manager.shutdown?).must_equal true
  end
end
