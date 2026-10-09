# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"
require "decent_job/worker"

class PoolBlockingJob < BlockingJob
  class_attribute :releases

  def perform
    started << queue_name
    releases.fetch(queue_name).pop
  end
end

describe DecentJob::Worker do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)
    [RecordingJob, BlockingJob, PoolBlockingJob, FailingJob].each { |job| job.queue_adapter = @adapter }
    RecordingJob.events = Queue.new
    BlockingJob.started = Queue.new
    BlockingJob.release = Queue.new
    PoolBlockingJob.started = Queue.new
    PoolBlockingJob.releases = {"urgent" => Queue.new, "ordinary" => Queue.new}
    @workers = []
  end

  after do
    10.times { BlockingJob.release << true }
    PoolBlockingJob.releases.each_value { |release| 5.times { release << true } }
    @workers.each { |worker| worker.shutdown(timeout: 5) }
  end

  def start_worker(**options)
    worker = DecentJob::Worker.new(database: database, **options)
    @workers << worker
    worker.start
  end

  def wait_until
    Timeout.timeout(5) do
      sleep 0.01 until yield
    end
  end

  def pop(queue)
    Timeout.timeout(5) { queue.pop }
  end

  def wait_for_listener(connections, excluding: nil)
    @connection_pids ||= []
    listener = nil
    wait_until do
      @connection_pids << connections.pop(true) until connections.empty?
      encoded = PG::TextEncoder::Array.new.encode(@connection_pids)
      listener = @admin.exec_params(<<~SQL, [encoded, excluding]).first
        SELECT pid FROM pg_stat_activity WHERE pid = ANY($1::integer[])
          AND query = 'LISTEN decent_job' AND ($2::integer IS NULL OR pid <> $2)
      SQL
    end
    listener.fetch("pid").to_i
  end

  it "starts idempotently, polls for work, and leaves the database open on shutdown" do
    worker = start_worker(max_threads: 1, poll_interval: 0.02, enable_listen_notify: false)
    _(worker.start).must_equal worker
    _(worker.running?).must_equal true
    RecordingJob.perform_later("polled")
    _(pop(RecordingJob.events)).must_equal "polled"
    _(worker.shutdown(timeout: 5)).must_equal true
    _(worker.running?).must_equal false
    _(worker.shutdown(timeout: 0)).must_equal true
    _(database.with_connection { |connection| connection.exec("SELECT 1").getvalue(0, 0) }).must_equal "1"
  end

  it "honors independent queue pool concurrency and excludes other queues" do
    PoolBlockingJob.set(queue: "ordinary").perform_later
    RecordingJob.set(queue: "other").perform_later("excluded")
    worker = start_worker(queues: "urgent:1;ordinary:1", poll_interval: 0.02, enable_listen_notify: false)
    _(pop(PoolBlockingJob.started)).must_equal "ordinary"
    2.times { PoolBlockingJob.set(queue: "urgent").perform_later }
    worker.wake
    _(pop(PoolBlockingJob.started)).must_equal "urgent"
    sleep 0.08
    _(PoolBlockingJob.started.size).must_equal 0
    PoolBlockingJob.releases.fetch("urgent") << true
    _(pop(PoolBlockingJob.started)).must_equal "urgent"
    PoolBlockingJob.releases.each_value { |release| release << true }
    _(worker.shutdown(timeout: 5)).must_equal true
    _(RecordingJob.events.size).must_equal 0
  end

  it "returns on shutdown timeout without interrupting executing jobs or claiming more" do
    BlockingJob.perform_later
    worker = start_worker(max_threads: 1, poll_interval: 30, enable_listen_notify: false)
    pop(BlockingJob.started)
    RecordingJob.perform_later("not claimed")
    _(worker.shutdown(timeout: 0.01)).must_equal false
    BlockingJob.release << true
    _(worker.shutdown(timeout: 5)).must_equal true
    _(RecordingJob.events.size).must_equal 0
  end

  it "wakes sleeping workers and shuts them down without waiting for the poll interval" do
    worker = start_worker(max_threads: 1, poll_interval: 60, enable_listen_notify: false)
    sleep 0.08
    RecordingJob.perform_later("awake")
    worker.wake
    _(pop(RecordingJob.events)).must_equal "awake"
    _(worker.shutdown(timeout: 1)).must_equal true
  end

  it "executes scheduled jobs through polling when notifications are disabled" do
    RecordingJob.set(wait: 0.1).perform_later("scheduled")
    start_worker(max_threads: 1, poll_interval: 0.02, enable_listen_notify: false)
    _(pop(RecordingJob.events)).must_equal "scheduled"
  end

  it "keeps performing jobs after an unhandled job error" do
    FailingJob.perform_later
    RecordingJob.perform_later("survived")
    start_worker(max_threads: 1, poll_interval: 0.02, enable_listen_notify: false)
    _(pop(RecordingJob.events)).must_equal "survived"
  end

  it "uses a dedicated listener connection without consuming execution capacity" do
    @database.close
    @database = DecentJob::Database.new(url: @connection_options, pool_size: 1, checkout_timeout: 0.2)
    @adapter = DecentJob::Adapter.new(database: database)
    RecordingJob.queue_adapter = @adapter
    database.with_connection { |connection| connection.exec("SELECT 1") }
    errors = Queue.new
    connections = Queue.new
    database.define_singleton_method(:connect) do
      super().tap do |connection|
        connection.set_notice_processor {}
        connections << connection.backend_pid
      end
    end
    worker = start_worker(max_threads: 1, poll_interval: 60, on_thread_error: ->(error) { errors << error })
    wait_for_listener(connections)
    sleep 0.08
    RecordingJob.perform_later("notified")
    _(pop(RecordingJob.events)).must_equal "notified"
    _(worker.shutdown(timeout: 1)).must_equal true
    _(errors.size).must_equal 0
  end

  it "reconnects a terminated listener and wakes workers after reconnection" do
    connections = Queue.new
    errors = Queue.new
    database.define_singleton_method(:connect) do
      super().tap do |connection|
        connection.set_notice_processor {}
        connections << connection.backend_pid
      end
    end
    worker = start_worker(max_threads: 1, poll_interval: 60, on_thread_error: ->(error) { errors << error })
    listener_pid = wait_for_listener(connections)
    @admin.exec_params("SELECT pg_terminate_backend($1)", [listener_pid])
    _(pop(errors)).must_be_kind_of PG::Error
    new_listener_pid = wait_for_listener(connections, excluding: listener_pid)
    _(new_listener_pid).wont_equal listener_pid
    RecordingJob.perform_later("reconnected")
    _(pop(RecordingJob.events)).must_equal "reconnected"
    _(worker.shutdown(timeout: 1)).must_equal true
  end

  it "retries execution infrastructure errors even when the error callback raises" do
    RecordingJob.perform_later("recovered")
    attempts = Queue.new
    database.define_singleton_method(:with_connection) do |&block|
      attempts << true
      raise PG::ConnectionBad, "temporarily disconnected" if attempts.size == 1
      super(&block)
    end
    errors = Queue.new
    start_worker(max_threads: 1, poll_interval: 0.02, enable_listen_notify: false, on_thread_error: lambda { |error|
      errors << error
      raise "reporter unavailable"
    })
    _(pop(errors)).must_be_kind_of PG::ConnectionBad
    _(pop(RecordingJob.events)).must_equal "recovered"
  end

  it "backs off infrastructure failures and interrupts the backoff on shutdown" do
    errors = Queue.new
    database.define_singleton_method(:with_connection) do |&block|
      raise PG::ConnectionBad, "offline"
    end
    worker = start_worker(max_threads: 1, poll_interval: 60, enable_listen_notify: false, on_thread_error: ->(error) { errors << error })
    pop(errors)
    8.times do
      worker.wake
      sleep 0.01
    end
    _(errors.size).must_equal 0
    _(worker.shutdown(timeout: 0.5)).must_equal true
  end

  it "can restart after a completed shutdown" do
    worker = start_worker(max_threads: 1, poll_interval: 0.02, enable_listen_notify: false)
    _(worker.shutdown(timeout: 1)).must_equal true
    RecordingJob.perform_later("restarted")
    _(worker.start).must_equal worker
    _(pop(RecordingJob.events)).must_equal "restarted"
  end

  it "rejects invalid polling intervals" do
    [0, -1, Float::INFINITY, Float::NAN, nil, "1"].each do |interval|
      _ { DecentJob::Worker.new(database: database, poll_interval: interval) }.must_raise ArgumentError
    end
  end
  it "rejects false timeout before stopping worker threads" do
    worker = start_worker(max_threads: 1, enable_listen_notify: false)
    _ { worker.shutdown(timeout: false) }.must_raise ArgumentError
    _(worker.running?).must_equal true
  end
end
