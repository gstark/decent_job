# frozen_string_literal: true

require "minitest/mock"
require "spec_helper"
require "timeout"

describe "interruption during ownership release" do
  def await_sleep(thread)
    Timeout.timeout(2) { Thread.pass until thread.backtrace&.any? { |line| line.include?("synchronize") } }
  end

  it "releases the last process lease when kill arrives while acquiring its release mutex" do
    tracker = DecentJob::ProcessTracker.new(database: Object.new)
    monitor_release = Queue.new
    tracker.define_singleton_method(:monitor) { monitor_release.pop }
    tracker.instance_variable_set(:@ready, true)
    mutex = tracker.instance_variable_get(:@mutex)
    entered = Queue.new
    release = Queue.new
    thread = Thread.new {
      tracker.register(wait_for_monitor: false) {
        entered << true
        release.pop
      }
    }
    Timeout.timeout(2) { entered.pop }
    mutex.synchronize do
      release << true
      await_sleep(thread)
      thread.kill
    end
    _(thread.join(1)).must_equal thread
    _(tracker.instance_variable_get(:@registrations)).must_equal 0
  ensure
    release << true if release
    thread&.kill
    monitor_release << true if monitor_release
    thread&.join(1)
    tracker&.join(1)
  end

  it "returns an unregistered connection reservation when killed during failed-connect cleanup" do
    pool = DecentJob::Database.new(url: "unused", pool_size: 1)
    entered = Queue.new
    release = Queue.new
    pool.define_singleton_method(:connect) do
      entered << true
      release.pop
      raise "connect failed"
    end
    thread = Thread.new { pool.with_connection {} }
    thread.report_on_exception = false
    Timeout.timeout(2) { entered.pop }
    mutex = pool.instance_variable_get(:@mutex)
    mutex.synchronize do
      release << true
      await_sleep(thread)
      thread.kill
    end
    _(thread.join(1)).must_equal thread
    _(pool.stats.fetch(:connections)).must_equal 0
    _(pool.stats.fetch(:active)).must_equal 0
  ensure
    release << true if release
    thread&.kill
    thread&.join(1)
    pool&.close
  end
end

describe "bounded cancellation cleanup" do
  before do
    skip "nonblocking cancellation requires libpq 17" unless defined?(PG::CancelConnection)
    @pool = DecentJob::Database.new(url: "unused", pool_size: 1)
    @connection = Object.new
    @connection.define_singleton_method(:finished?) { !!@finished }
    @connection.define_singleton_method(:finish) { @finished = true }
    @connection.define_singleton_method(:status) { PG::CONNECTION_OK }
    @connection.define_singleton_method(:transaction_status) { PG::PQTRANS_ACTIVE }
    connection = @connection
    @pool.define_singleton_method(:connect) { connection }
    @entered = Queue.new
    @cancellation = Object.new
    @cancellation.define_singleton_method(:finish) { @finished = true }
    @cancellation.define_singleton_method(:finished?) { !!@finished }
  end

  after do
    @thread&.kill
    @thread&.join(2)
    @pool&.close
    @reader&.close
    @writer&.close
  end

  it "bounds cancellation socket waits without holding the pool mutex" do
    @reader, @writer = IO.pipe
    reader = @reader
    started = Queue.new
    @cancellation.define_singleton_method(:start) { started << true }
    @cancellation.define_singleton_method(:poll) { PG::PGRES_POLLING_READING }
    @cancellation.define_singleton_method(:socket_io) { reader }
    PG::CancelConnection.stub(:new, @cancellation) do
      @thread = Thread.new {
        @pool.with_connection {
          @entered << true
          Queue.new.pop
        }
      }
      Timeout.timeout(2) { @entered.pop }
      @thread.kill
      Timeout.timeout(2) { started.pop }
      stats = Timeout.timeout(0.2) { @pool.stats }
      _(stats.fetch(:active)).must_equal 1
      _(@thread.join(2)).must_equal @thread
      _(@pool.stats.fetch(:connections)).must_equal 0
      _(@connection.finished?).must_equal true
      _(@cancellation.finished?).must_equal true
    end
  end

  it "returns the reservation and closes both sessions if cancellation fails" do
    @cancellation.define_singleton_method(:start) { raise PG::ConnectionBad, "cancel unreachable" }
    PG::CancelConnection.stub(:new, @cancellation) do
      @thread = Thread.new {
        @pool.with_connection {
          @entered << true
          Queue.new.pop
        }
      }
      Timeout.timeout(2) { @entered.pop }
      @thread.kill
      _(@thread.join(1)).must_equal @thread
      _(@pool.stats.fetch(:connections)).must_equal 0
      _(@connection.finished?).must_equal true
      _(@cancellation.finished?).must_equal true
    end
  end
end
