# frozen_string_literal: true

require "spec_helper"

describe "safe instrumentation" do
  include DatabaseSpec

  before do
    @subscriptions = []
    @reporter = DecentJob.configuration.on_thread_error
    @reported = []
    DecentJob.configuration.on_thread_error = ->(error) { @reported << error }
  end

  after do
    @subscriptions.each { |subscription| ActiveSupport::Notifications.unsubscribe(subscription) }
    DecentJob.configuration.on_thread_error = @reporter
  end

  def subscribe(callable = nil, &block)
    @subscriptions << ActiveSupport::Notifications.subscribe("probe.decent_job", callable, &block)
  end

  it "runs once and preserves false when evented start and finish both fail" do
    calls = []
    observer = Object.new
    observer.define_singleton_method(:start) { |*|
      calls << :start
      raise "start failed"
    }
    observer.define_singleton_method(:finish) { |*|
      calls << :finish
      raise "finish failed"
    }
    subscribe(observer)
    value = DecentJob::Instrumentation.instrument("probe.decent_job", {}, database: database) {
      calls << :application
      false
    }
    _(value).must_equal false
    _(calls).must_equal [:start, :application, :finish]
    _(@reported.map(&:message)).must_equal ["start failed", "finish failed"]
  end

  it "preserves the original application error even with a failing reporter" do
    error = ArgumentError.new("application")
    observed = nil
    subscribe { |event|
      observed = event
      raise "observer"
    }
    DecentJob.configuration.on_thread_error = ->(*) { raise "reporter" }
    actual = assert_raises(ArgumentError) do
      DecentJob::Instrumentation.instrument("probe.decent_job", {}, database: database) { raise error }
    end
    _(actual).must_be_same_as error
    _(observed.payload[:exception_object]).must_be_same_as error
    _(observed.payload[:exception]).must_equal ["ArgumentError", "application"]
  end

  it "preserves nonlocal exits and finishes once" do
    events = []
    subscribe { |event| events << event }
    result = catch(:done) do
      DecentJob::Instrumentation.instrument("probe.decent_job", {}, database: database) { throw :done, 42 }
    end
    _(result).must_equal 42
    _(events.size).must_equal 1
    _(events.first.payload).wont_include :exception
  end

  it "isolates observer SQL errors and recursive error reporting in both execution isolation modes" do
    original = ActiveSupport::IsolatedExecutionState.isolation_level
    [:thread, :fiber].each do |mode|
      ActiveSupport::IsolatedExecutionState.isolation_level = mode
      subscribe { database.with_connection { |connection| connection.exec("SELECT missing_observer_column") } }
      DecentJob.configuration.on_thread_error = lambda do |error|
        @reported << error
        DecentJob::Instrumentation.instrument("probe.decent_job", {}, database: database) { nil }
      end
      database.transaction do |connection|
        value = DecentJob::Instrumentation.instrument("probe.decent_job", {}, database: database) { 17 }
        _(value).must_equal 17
        _(connection.exec("SELECT 1").getvalue(0, 0)).must_equal "1"
      end
      @subscriptions.each { |subscription| ActiveSupport::Notifications.unsubscribe(subscription) }
      @subscriptions.clear
    end
    _(@reported.size).must_equal 2
  ensure
    ActiveSupport::IsolatedExecutionState.isolation_level = original
  end

  it "supports ordinary, monotonic and event subscribers without opening a connection" do
    events = []
    subscribe { |event| events << event }
    subscribe { |*args| events << args }
    @subscriptions << ActiveSupport::Notifications.monotonic_subscribe("probe.decent_job") { |*args| events << args }
    _(DecentJob::Instrumentation.instrument("probe.decent_job", {}, database: database) { 0 }).must_equal 0
    _(events.size).must_equal 3
    _(events[0]).must_be_kind_of ActiveSupport::Notifications::Event
    _(events[1][1]).must_be_kind_of Time
    _(events[2][1]).must_be_kind_of Float
    _(database.stats[:connections]).must_equal 0
  end
  it "finishes an interrupted start without entering the application block" do
    started = Queue.new
    finished = Queue.new
    ran = false
    observer = Object.new
    observer.define_singleton_method(:start) { |*|
      started << true
      Queue.new.pop
    }
    observer.define_singleton_method(:finish) { |name, id, payload| finished << payload.dup }
    subscribe(observer)
    thread = Thread.new do
      DecentJob::Instrumentation.instrument("probe.decent_job", {}, database: database) { ran = true }
    end
    started.pop
    thread.kill.join
    _(ran).must_equal false
    _(finished.pop[:interrupted]).must_equal true
  ensure
    thread&.kill&.join
  end

  it "finishes once with the original SQL exception when its transaction is already aborted" do
    observed = []
    subscribe { |event| observed << event }
    actual = assert_raises(PG::UndefinedColumn) do
      database.transaction do |connection|
        DecentJob::Instrumentation.instrument("probe.decent_job", {}, database: database) do
          connection.exec("SELECT missing_application_column")
        end
      end
    end
    _(observed.size).must_equal 1
    _(observed.first.payload[:exception_object]).must_be_same_as actual
  end
end
