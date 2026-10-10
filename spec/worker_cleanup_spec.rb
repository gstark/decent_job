# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

describe "worker cleanup" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    [RecordingJob, RetryingJob].each { |klass| klass.queue_adapter = DecentJob::Adapter.new(database: database) }
    RecordingJob.events = Queue.new
    @workers = []
    @subscribers = []
    @releases = []
  end

  after do
    @releases.each { |queue| 10.times { queue << true } }
    @workers.each { |worker| worker.shutdown(timeout: 5) }
    @subscribers.each { |subscriber| ActiveSupport::Notifications.unsubscribe(subscriber) }
    DecentJob.reset!
  end

  def start_worker(**options)
    worker = DecentJob::Worker.new(database: database, max_threads: 1, poll_interval: 0.01, enable_listen_notify: false, **options)
    @workers << worker
    worker.start
  end

  def pop(queue)
    Timeout.timeout(5) { queue.pop }
  end

  def events(&callback)
    queue = Queue.new

    @subscribers << ActiveSupport::Notifications.subscribe("cleanup_preserved_jobs.decent_job") do |event|
      queue << event.payload
      callback&.call(event)
    end

    queue
  end

  it "cleans only after the strict count threshold and resets at cleanup start" do
    notifications = events
    start_worker(cleanup_interval_jobs: 1, cleanup_interval_seconds: false, cleanup_preserved_jobs_before_seconds_ago: 0)
    RecordingJob.perform_later("first")
    pop(RecordingJob.events)
    sleep 0.05
    _(notifications.size).must_equal 0
    RecordingJob.perform_later("second")
    _(pop(notifications).fetch(:destroyed_jobs_count)).must_equal 2
    RecordingJob.perform_later("third")
    2.times { pop(RecordingJob.events) }
    sleep 0.05
    _(notifications.size).must_equal 0
    RecordingJob.perform_later("fourth")
    _(pop(notifications).fetch(:destroyed_jobs_count)).must_equal 2
  end

  it "checks elapsed time after work, never after idle polls" do
    notifications = events
    start_worker(cleanup_interval_jobs: false, cleanup_interval_seconds: 0.03)
    sleep 0.1
    _(notifications.size).must_equal 0
    RecordingJob.perform_later("elapsed")
    pop(notifications)
  end

  it "counts retry attempts and inherits configuration for directly constructed workers" do
    DecentJob.configuration.cleanup_interval_jobs = 1
    DecentJob.configuration.cleanup_interval_seconds = false
    notifications = events
    start_worker
    RetryingJob.perform_later("retry")
    pop(notifications)
    _(pop(RecordingJob.events)).must_equal "retry"
  end

  it "keeps a single cleanup in flight and owns it through shutdown" do
    release = Queue.new
    @releases << release
    notifications = events { release.pop }
    worker = start_worker(max_threads: 2, cleanup_interval_jobs: -1, cleanup_interval_seconds: false)
    RecordingJob.perform_later("first")
    pop(notifications)
    RecordingJob.perform_later("second")
    2.times { pop(RecordingJob.events) }
    sleep 0.05
    _(notifications.size).must_equal 0
    _(worker.shutdown(timeout: 0.01)).must_equal false
    release << true
    _(worker.shutdown(timeout: 5)).must_equal true
    _(notifications.size).must_equal 0
  end

  it "reports cleanup errors, releases its guard, and resumes work" do
    database.with_connection do |connection|
      connection.exec(<<~SQL)
        CREATE FUNCTION reject_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$
          BEGIN RAISE EXCEPTION 'cleanup failed'; END;
        $$;
        CREATE TRIGGER reject_cleanup BEFORE DELETE ON decent_jobs FOR EACH ROW EXECUTE FUNCTION reject_cleanup();
      SQL
    end

    errors = Queue.new
    notifications = events
    DecentJob.configuration.on_thread_error = ->(error) { errors << error }
    start_worker(cleanup_interval_jobs: -1, cleanup_interval_seconds: false, cleanup_preserved_jobs_before_seconds_ago: 0)
    RecordingJob.perform_later("first")
    _(pop(errors)).must_be_kind_of PG::RaiseException
    pop(notifications)
    database.with_connection { |connection| connection.exec("DROP TRIGGER reject_cleanup ON decent_jobs") }
    RecordingJob.perform_later("second")
    _(pop(notifications).fetch(:destroyed_jobs_count)).must_equal 2
  end

  it "keeps synchronous inline work free of cleanup threads" do
    DecentJob.configuration.cleanup_interval_jobs = -1
    notifications = events
    RecordingJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :inline)
    before_threads = Thread.list
    RecordingJob.perform_later("inline")
    _(Thread.list - before_threads).must_be_empty
    _(notifications.size).must_equal 0
  end

  it "rejects invalid cleanup settings before starting threads" do
    [-1, Float::INFINITY, Float::NAN, Complex(1, 1), false, "1"].each do |value|
      _ { DecentJob::Worker.new(database: database, cleanup_preserved_jobs_before_seconds_ago: value) }.must_raise ArgumentError
    end

    [nil, "false", 1].each do |value|
      _ { DecentJob::Worker.new(database: database, cleanup_discarded_jobs: value) }.must_raise ArgumentError
    end
  end
end
