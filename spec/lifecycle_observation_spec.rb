# frozen_string_literal: true

require "spec_helper"
require "timeout"

describe "lifecycle observation boundaries" do
  before do
    @events = []
    @subscription = ActiveSupport::Notifications.subscribe("test_lifecycle.decent_job") { |event| @events << event.payload[:label] }
    @resource = Object.new
    @isolation = ActiveSupport::IsolatedExecutionState.isolation_level
  end

  after do
    ActiveSupport::Notifications.unsubscribe(@subscription)
    ActiveSupport::IsolatedExecutionState.isolation_level = @isolation
  end

  def record(label)
    DecentJob::LifecycleEvents.record(@resource, "test_lifecycle", {label: label})
  end

  [:thread, :fiber].each do |isolation|
    it "flushes nested receipts after all locks release and restores #{isolation} scopes on throw" do
      ActiveSupport::IsolatedExecutionState.isolation_level = isolation
      mutex = Mutex.new
      catch(:done) do
        DecentJob::LifecycleEvents.defer do
          mutex.synchronize do
            record(:first)
            DecentJob::LifecycleEvents.defer { record(:second) }
            _(@events).must_be_empty
            throw :done
          end
        end
      end
      _(@events).must_equal [:first, :second]
      _(DecentJob::ExecutionState[:lifecycle_events]).must_be_nil
    end

    it "does not inherit pending receipts into a shared #{isolation} context" do
      ActiveSupport::IsolatedExecutionState.isolation_level = isolation
      source = ActiveSupport::IsolatedExecutionState.context
      child = lambda do
        ActiveSupport::IsolatedExecutionState.share_with(source) do
          DecentJob::LifecycleEvents.defer { record(:child) }
        end
      end
      DecentJob::LifecycleEvents.defer do
        record(:parent)
        (isolation == :thread) ? Thread.new(&child).value : Fiber.new(&child).resume
        _(@events).must_equal [:child]
      end
      _(@events).must_equal [:child, :parent]
    end
  end

  it "restores scope and observation guards when a delivery thread is killed" do
    entered = Queue.new
    restored = Queue.new
    blocker = ActiveSupport::Notifications.subscribe("test_lifecycle.decent_job") do
      entered << true
      Queue.new.pop
    end
    thread = Thread.new do
      DecentJob::LifecycleEvents.defer { record(:killed) }
    ensure
      restored << [DecentJob::ExecutionState[:lifecycle_events], DecentJob::ExecutionState[:lifecycle_observers]]
    end
    Timeout.timeout(2) { entered.pop }
    thread.kill.join
    _(restored.pop).must_equal [nil, nil]
    ActiveSupport::Notifications.unsubscribe(blocker)
    record(:later)
    _(@events).must_equal [:killed, :later]
  ensure
    ActiveSupport::Notifications.unsubscribe(blocker) if blocker
    thread&.kill&.join
  end

  it "detaches receipt strings and arrays from later observations" do
    original = {label: +"original", nested: [+"queue"]}
    mutator = ActiveSupport::Notifications.subscribe("test_lifecycle.decent_job") do |event|
      event.payload[:label].replace("changed")
      event.payload[:nested].first.replace("changed")
    end
    DecentJob::LifecycleEvents.record(@resource, "test_lifecycle", original)
    _(original).must_equal({label: "original", nested: ["queue"]})
  ensure
    ActiveSupport::Notifications.unsubscribe(mutator) if mutator
  end
end
