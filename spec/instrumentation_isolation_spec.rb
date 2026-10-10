# frozen_string_literal: true

require "spec_helper"

describe "notification payload isolation" do
  before do
    @database = Object.new
    @database.define_singleton_method(:in_transaction?) { false }
    @subscriptions = []
    @reporter = DecentJob.configuration.on_thread_error
    DecentJob.configuration.on_thread_error = ->(*) {}
  end

  after do
    @subscriptions.each { |subscription| ActiveSupport::Notifications.unsubscribe(subscription) }
    DecentJob.configuration.on_thread_error = @reporter
  end

  def subscribe(observer)
    @subscriptions << ActiveSupport::Notifications.subscribe("isolation.decent_job", observer)
  end

  it "uses an explicit reporter and allows reporting to be disabled" do
    reported = []
    subscribe(->(*) { raise "observer failed" })
    reporter = ->(error) { reported << error }
    _(DecentJob::Instrumentation.instrument("isolation.decent_job", {}, database: @database, on_error: reporter) { 42 }).must_equal 42
    _(reported.map(&:message)).must_equal ["observer failed"]
    _(DecentJob::Instrumentation.instrument("isolation.decent_job", {}, database: @database, on_error: nil) { 43 }).must_equal 43
    _(reported.size).must_equal 1
  end

  [false, true].each do |raises|
    it "preserves application #{raises ? "exceptions" : "returns"} when start freezes its payload and fails" do
      observer = Object.new
      finishes = 0

      observer.define_singleton_method(:start) do |name, id, payload|
        payload.freeze
        raise "observer failed"
      end

      observer.define_singleton_method(:finish) { |*| finishes += 1 }
      subscribe(observer)
      error = ArgumentError.new("application failed")

      operation = lambda do
        DecentJob::Instrumentation.instrument("isolation.decent_job", {}, database: @database) do |payload|
          payload[:value] = 42
          raise error if raises

          42
        end
      end

      if raises
        _(assert_raises(ArgumentError, &operation)).must_be_same_as error
      else
        _(operation.call).must_equal 42
      end

      _(finishes).must_equal 1
    end
  end

  it "clears success and marks interruption when killed inside a finish observer" do
    entered = Queue.new
    observed = nil
    observer = Object.new
    observer.define_singleton_method(:start) { |*| }

    observer.define_singleton_method(:finish) do |name, id, payload|
      observed = payload
      entered << true
      Queue.new.pop
    end

    subscribe(observer)
    outcome = {}

    thread = Thread.new do
      DecentJob::Instrumentation.instrument("isolation.decent_job", outcome, database: @database) do |payload|
        payload.merge!(value: 42, error: nil, handled_error: nil, retried: false, error_event: nil)
        42
      end
    end

    entered.pop
    thread.kill.join

    [outcome, observed].each do |payload|
      _(payload[:interrupted]).must_equal true
      _(payload).wont_include :value
      _(payload).wont_include :handled_error
      _(payload).wont_include :retried
    end
  ensure
    thread&.kill&.join
  end

  it "still finishes once when killed while reporting a frozen payload before finish" do
    reporting = Queue.new
    finishes = 0
    observer = Object.new
    observer.define_singleton_method(:start) { |name, id, payload| payload.freeze }
    observer.define_singleton_method(:finish) { |*| finishes += 1 }
    subscribe(observer)

    DecentJob.configuration.on_thread_error = ->(*) {
      reporting << true
      Queue.new.pop
    }

    thread = Thread.new do
      DecentJob::Instrumentation.instrument("isolation.decent_job", {}, database: @database) { 42 }
    end

    reporting.pop
    thread.kill
    _(thread.join(1)).must_be_same_as thread
    _(finishes).must_equal 1
  ensure
    # A failed regression must not leave its deliberately blocked reporter alive.
    DecentJob.configuration.on_thread_error = ->(*) {}
    thread&.kill&.join
  end
end
