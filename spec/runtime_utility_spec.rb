# frozen_string_literal: true

require "spec_helper"
require "timeout"

describe "runtime monitor utilities" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
  end

  after do
    @tracker&.stop
    @tracker&.join(2)
  end

  it "wraps heartbeat state in the host executor outside the tracker mutex" do
    executor = Class.new(ActiveSupport::Executor)
    calls = Queue.new
    completed = Queue.new
    executor.to_complete { completed << true }
    reloader = Object.new
    reloader.define_singleton_method(:wrap) { |&| raise "monitor must not wait for its registering parent to reload" }
    application = Struct.new(:executor, :reloader).new(executor, reloader)
    host = Module.new
    host.define_singleton_method(:application) { application }
    Object.const_set(:Rails, host)

    @tracker = DecentJob::ProcessTracker.new(database: database, process_heartbeat_interval: 0.02, state: -> {
      # Both calls would deadlock if host application code ran under the mutex.
      ready = @tracker.connected?
      calls << [executor.active?, ready]
      {host_executor: executor.active?}
    })

    executor.wrap do
      @tracker.register do |id|
        first = Timeout.timeout(2) { calls.pop }
        _(first).must_equal [true, false]
        Timeout.timeout(2) { completed.pop }
        record = DecentJob::ProcessRecord.find(id, database: database)
        _(record.state.fetch("host_executor")).must_equal true
      end
    end
  ensure
    Object.send(:remove_const, :Rails) if Object.const_defined?(:Rails) && Rails.equal?(host)
  end

  it "recovers from a state callback failure even when the error reporter also raises" do
    reports = Queue.new
    attempts = 0

    @tracker = DecentJob::ProcessTracker.new(database: database, process_heartbeat_interval: 0.02,
      state: -> {
        attempts += 1
        raise "state failed" if attempts == 1
        {attempt: attempts}
      }, on_thread_error: ->(error) {
        reports << error.message
        raise "reporter failed"
      })

    Timeout.timeout(2) do
      @tracker.register do |id|
        _(reports.pop).must_equal "state failed"
        _(DecentJob::ProcessRecord.find(id, database: database).state.fetch("attempt")).must_be :>=, 2
        _(@tracker.connected?).must_equal true
      end
    end

    _(@tracker.shutdown?).must_equal true
  end

  it "confines every dedicated session operation to one monitor during nested and concurrent leases" do
    calls = Queue.new
    pool = database

    pool.define_singleton_method(:connect) do
      connection = super()

      [:exec_params, :finished?, :status, :finish].each do |name|
        original = connection.method(name)

        connection.define_singleton_method(name) do |*arguments|
          calls << [object_id, Thread.current, name]
          original.call(*arguments)
        end
      end

      connection
    end

    @tracker = DecentJob::ProcessTracker.new(database: pool, advisory_lock_heartbeat: true, process_heartbeat_interval: 0.02)

    @tracker.register do |id|
      @tracker.register { |nested_id| _(nested_id).must_equal id }

      Thread.new do
        @tracker.register do |concurrent_id|
          _(concurrent_id).must_equal id
          _(@tracker.connected?).must_equal true
        end
      end.value

      _(@tracker.connected?).must_equal true
    end

    operations = []
    operations << calls.pop until calls.empty?
    _(operations.map(&:first).uniq.size).must_equal 1
    _(operations.map { |operation| operation[1] }.uniq.size).must_equal 1
    _(operations.first[1]).wont_equal Thread.current
    _(operations.map(&:last)).must_include :finish
  end
end
