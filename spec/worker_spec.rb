# frozen_string_literal: true

require "spec_helper"
require "minitest/mock"
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

  def pause_next_empty_claim
    checked = Queue.new
    release = Queue.new
    intercepted = false

    database.define_singleton_method(:with_connection) do |&block|
      super() do |connection|
        unless connection.singleton_class.method_defined?(:scheduler_original_exec_params)
          connection.singleton_class.alias_method(:scheduler_original_exec_params, :exec_params)

          connection.define_singleton_method(:exec_params) do |sql, *arguments|
            result = scheduler_original_exec_params(sql, *arguments)

            if sql.include?("SELECT id, hashtextextended") && !intercepted
              intercepted = true
              checked << true
              release.pop
            end

            result
          end
        end

        block.call(connection)
      end
    end

    [checked, release]
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
    errors = Queue.new
    start_worker(max_threads: 1, poll_interval: 0.02, enable_listen_notify: false, on_thread_error: ->(error) { errors << error })
    _(pop(errors).message).must_equal "unhandled error"
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

  it "discovers existing future jobs with periodic polling disabled" do
    RecordingJob.set(wait: 0.2).perform_later("cached")
    worker = start_worker(max_threads: 1, poll_interval: -1, enable_listen_notify: false)
    _(pop(RecordingJob.events)).must_equal "cached"
    _(worker.stats.fetch(:schedulers).first.fetch(:max_cache)).must_equal 10_000
  end

  it "routes counts only to matching pools and falls back when the first pool is full" do
    worker = start_worker(queues: "urgent:1;urgent:1;ordinary:1", poll_interval: -1, enable_listen_notify: false)
    wait_until { worker.stats.fetch(:schedulers).all? { |pool| pool.fetch(:empty_executions_count).positive? } }
    2.times { PoolBlockingJob.set(queue: "urgent").perform_later }
    ordinary_before = worker.stats.fetch(:schedulers).last.fetch(:empty_executions_count)
    worker.wake(queue_name: "urgent", count: 1)
    _(pop(PoolBlockingJob.started)).must_equal "urgent"
    sleep 0.08
    _(PoolBlockingJob.started.size).must_equal 0
    worker.wake(queue_name: "urgent", count: 1)
    _(pop(PoolBlockingJob.started)).must_equal "urgent"
    _(worker.stats.fetch(:schedulers).last.fetch(:empty_executions_count)).must_equal ordinary_before
  end

  it "schedules future notifications with a zero cache and no polling" do
    worker = start_worker(max_threads: 1, max_cache: 0, poll_interval: -1)
    wait_until { worker.connected? }
    RecordingJob.set(wait: 0.2).perform_later("notified future")
    _(pop(RecordingJob.events)).must_equal "notified future"
  end

  it "ignores malformed notification payloads without disconnecting or waking unrelated pools" do
    errors = Queue.new
    worker = start_worker(queues: "urgent:1;ordinary:1", poll_interval: -1, on_thread_error: ->(error) { errors << error })
    wait_until { worker.connected? && worker.stats.fetch(:schedulers).all? { |pool| pool.fetch(:empty_executions_count).positive? } }
    sleep 0.05
    before = worker.stats.fetch(:schedulers).map { |pool| pool.fetch(:empty_executions_count) }
    payloads = ["{", "[]", "null", '{"count":-1}', '{"count":1.5}', '{"scheduled_at":"tomorrow"}', '{"queue_name":false}', '{"fanout":"yes"}']

    database.with_connection do |connection|
      payloads.each { |payload| connection.exec_params("SELECT pg_notify('decent_job', $1)", [payload]) }
    end

    wait_until { errors.size == payloads.size }
    _(worker.connected?).must_equal true
    _(worker.stats.fetch(:schedulers).map { |pool| pool.fetch(:empty_executions_count) }).must_equal before
    RecordingJob.set(queue: "urgent").perform_later("valid after malformed")
    _(pop(RecordingJob.events)).must_equal "valid after malformed"
  end

  it "honors exclusion and ordered queue pools for fanout without looping through huge counts" do
    database.resize(6)
    worker = start_worker(queues: "-ordinary:2;+urgent,ordinary:2;ordinary:1", poll_interval: -1, enable_listen_notify: false)
    wait_until { worker.stats.fetch(:schedulers).all? { |pool| pool.fetch(:empty_executions_count).positive? } }
    sleep 0.05
    4.times { PoolBlockingJob.set(queue: "urgent").perform_later }
    unrelated = worker.stats.fetch(:schedulers).last.fetch(:empty_executions_count)
    worker.wake(queue_name: "urgent", count: 10**100, fanout: true)
    4.times { _(pop(PoolBlockingJob.started)).must_equal "urgent" }
    _(worker.stats.fetch(:schedulers).map { |pool| pool.fetch(:active_threads) }).must_equal [2, 2, 0]
    _(worker.stats.fetch(:schedulers).last.fetch(:empty_executions_count)).must_equal unrelated
  end

  it "accepts Time and numeric future hints and interrupts an existing later deadline" do
    worker = start_worker(max_threads: 1, max_cache: 0, poll_interval: -1, enable_listen_notify: false)
    wait_until { worker.stats.fetch(:schedulers).first.fetch(:empty_executions_count).positive? }
    worker.wake(queue_name: "default", scheduled_at: Time.now + 60)
    at = Time.now + 0.2
    RecordingJob.set(wait_until: at).perform_later("earlier")
    worker.wake(queue_name: "default", scheduled_at: at.to_f)
    sleep 0.06
    _(RecordingJob.events.size).must_equal 0
    _(pop(RecordingJob.events)).must_equal "earlier"
  end

  it "retains a wake arriving while an empty SQL claim is still finishing" do
    checked, release = pause_next_empty_claim
    worker = start_worker(max_threads: 1, max_cache: 0, poll_interval: -1, enable_listen_notify: false)
    pop(checked)
    RecordingJob.perform_later("racing wake")
    worker.wake(queue_name: "default")
    release << true
    _(pop(RecordingJob.events)).must_equal "racing wake"
  ensure
    release << true
  end

  it "discovers a job that becomes due after an empty claim before cache warming" do
    checked, release = pause_next_empty_claim
    start_worker(max_threads: 1, max_cache: 1, poll_interval: -1, enable_listen_notify: false)
    pop(checked)
    RecordingJob.set(wait: 0.03).perform_later("became due")
    sleep 0.05
    release << true
    _(pop(RecordingJob.events)).must_equal "became due"
  ensure
    release << true
  end

  it "bounds empty SQL attempts when the application clock jumps ahead of the database" do
    RecordingJob.set(wait: 0.8).perform_later("clock corrected")
    original_now = Time.method(:now)
    offset = 60

    Time.stub(:now, -> { original_now.call + offset }) do
      worker = start_worker(max_threads: 1, poll_interval: -1, enable_listen_notify: false)
      sleep 0.2
      _(RecordingJob.events.size).must_equal 0
      _(worker.stats.fetch(:schedulers).first.fetch(:empty_executions_count)).must_be :<=, 3
      offset = 0
      _(pop(RecordingJob.events)).must_equal "clock corrected"
      worker.shutdown(timeout: 2)
    end
  end

  it "rechecks a cached deadline after the wall clock moves backward and then recovers" do
    RecordingJob.set(wait: 0.25).perform_later("clock recovered")
    original_now = Time.method(:now)
    offset = 0

    Time.stub(:now, -> { original_now.call + offset }) do
      worker = start_worker(max_threads: 1, poll_interval: -1, enable_listen_notify: false)
      wait_until { worker.stats.fetch(:schedulers).first.fetch(:active_cache).positive? }
      offset = -60
      sleep 0.35
      _(RecordingJob.events.size).must_equal 0
      offset = 0
      _(pop(RecordingJob.events)).must_equal "clock recovered"
      worker.shutdown(timeout: 2)
    end
  end

  it "clears pending cached timers on shutdown and discovers them again after restart" do
    job = RecordingJob.set(wait: 60).perform_later("restarted timer")
    worker = start_worker(max_threads: 1, max_cache: 1, poll_interval: -1, enable_listen_notify: false)
    wait_until { worker.stats.fetch(:schedulers).first.fetch(:active_cache) == 1 }
    _(worker.shutdown(timeout: 1)).must_equal true
    _(worker.stats.fetch(:schedulers).first.fetch(:active_cache)).must_equal 0
    DecentJob::Job.find(job.provider_job_id, database: database).reschedule_job(Time.now + 0.15)
    worker.start
    _(pop(RecordingJob.events)).must_equal "restarted timer"
  end

  it "accepts real PostgreSQL empty maintenance notifications as broad wakes" do
    errors = Queue.new
    worker = start_worker(queues: "urgent:1;ordinary:1", max_cache: 0, poll_interval: -1, on_thread_error: ->(error) { errors << error })
    wait_until { worker.connected? && worker.stats.fetch(:schedulers).all? { |pool| pool.fetch(:empty_executions_count).positive? } }
    previous = DecentJob.configuration.enable_listen_notify
    DecentJob.configuration.enable_listen_notify = false
    RecordingJob.set(queue: "urgent").perform_later("urgent maintenance")
    RecordingJob.set(queue: "ordinary").perform_later("ordinary maintenance")
    database.with_connection { |connection| DecentJob::Notifier.notify(connection) }
    _(2.times.map { pop(RecordingJob.events) }.sort).must_equal ["ordinary maintenance", "urgent maintenance"]
    _(worker.connected?).must_equal true
    _(errors.size).must_equal 0
  ensure
    DecentJob.configuration.enable_listen_notify = previous
  end

  it "drains a PostgreSQL notification burst promptly to reach the relevant earlier deadline" do
    worker = start_worker(queues: "default", max_threads: 1, max_cache: 0, poll_interval: -1)
    wait_until { worker.connected? && worker.stats.fetch(:schedulers).first.fetch(:empty_executions_count).positive? }
    worker.wake(queue_name: "default", scheduled_at: Time.now + 60)
    previous = DecentJob.configuration.enable_listen_notify
    DecentJob.configuration.enable_listen_notify = false
    at = Time.now + 0.2
    RecordingJob.set(wait_until: at).perform_later("burst deadline")

    database.transaction do |connection|
      20.times { |index| DecentJob::Notifier.notify(connection, queue_name: "unrelated-#{index}") }
      DecentJob::Notifier.notify(connection, queue_name: "default", scheduled_at: at.iso8601(6))
    end

    _(Timeout.timeout(0.9) { RecordingJob.events.pop }).must_equal "burst deadline"
  ensure
    DecentJob.configuration.enable_listen_notify = previous
  end

  it "retains a timestamped zero-cache hint when the application clock is ahead" do
    worker = start_worker(max_threads: 1, max_cache: 0, poll_interval: -1, enable_listen_notify: false)
    wait_until { worker.stats.fetch(:schedulers).first.fetch(:empty_executions_count).positive? }
    at = Time.now + 0.3
    RecordingJob.set(wait_until: at).perform_later("zero cache skew")
    original_now = Time.method(:now)

    Time.stub(:now, -> { original_now.call + 60 }) do
      worker.wake(queue_name: "default", scheduled_at: at)
      sleep 0.1
      _(RecordingJob.events.size).must_equal 0
    end

    _(pop(RecordingJob.events)).must_equal "zero cache skew"
  end

  it "rejects invalid polling intervals" do
    [Float::INFINITY, Float::NAN, nil, "1", true, Complex(1, 2)].each do |interval|
      _ { DecentJob::Worker.new(database: database, poll_interval: interval) }.must_raise ArgumentError
    end
  end

  it "rejects false timeout before stopping worker threads" do
    worker = start_worker(max_threads: 1, enable_listen_notify: false)
    _ { worker.shutdown(timeout: false) }.must_raise ArgumentError
    _(worker.running?).must_equal true
  end
end
