# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

describe "process tracking" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    RecordingJob.queue_adapter = DecentJob::Adapter.new(database: database)
    RecordingJob.events = Queue.new
  end

  after do
    @tracker&.stop
    @tracker&.join(5)
  end

  def tracker(**options)
    @tracker ||= DecentJob::ProcessTracker.new(database: database, process_heartbeat_interval: 0.02, **options)
  end

  def eventually
    Timeout.timeout(5) { sleep 0.005 until yield }
  end

  it "registers nested leases under one identity and removes the record after the last lease" do
    tracker.register do |id|
      record = DecentJob::ProcessRecord.find(id, database: database)
      _(record.state.fetch("pid")).must_equal Process.pid
      _(record.id.frozen?).must_equal true
      _(tracker.process_id.frozen?).must_equal true
      _(record.basename).must_equal File.basename($PROGRAM_NAME)
      _(record.heartbeat_mode).must_equal "heartbeat"
      tracker.register { |nested| _(nested).must_equal id }
      _(DecentJob::ProcessRecord.all(database: database).map(&:id)).must_equal [id]
    end
    _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
    _(tracker.join(0)).must_equal true
  end

  it "recreates a deleted row and keeps heartbeating while the pool is full" do
    database.with_connection do |connection|
      tracker.register do |id|
        connection.exec_params("DELETE FROM decent_job_processes WHERE id = $1", [id])
        eventually { connection.exec_params("SELECT id FROM decent_job_processes WHERE id = $1", [id]).ntuples == 1 }
      end
    end
  end

  it "uses session liveness for advisory records regardless of heartbeat age" do
    tracker(advisory_lock_heartbeat: true, process_heartbeat_interval: 100).register do |id|
      database.with_connection { |c| c.exec_params("UPDATE decent_job_processes SET updated_at = clock_timestamp() - interval '1 hour' WHERE id = $1", [id]) }
      record = DecentJob::ProcessRecord.find(id, database: database)
      _(record.stale?).must_equal true
      _(record.expired?).must_equal true
      _(DecentJob::ProcessRecord.active(database: database).map(&:id)).must_equal [id]
      _(DecentJob::ProcessRecord.cleanup(database: database)).must_equal 0
    end
  end

  it "records immutable execution process UUIDs and clears current ownership" do
    job = RecordingJob.perform_later("tracked")
    DecentJob::Performer.new(database: database).perform_one
    row = DecentJob::Store.new(database).find(job.provider_job_id)
    _(row.fetch("locked_by_id")).must_be_nil
    execution = DecentJob::Execution.for_job(job.provider_job_id, database: database).first
    _(execution.process_uuid).must_match(/\A[0-9a-f-]{36}\z/)
    _(execution.process_id).must_equal Process.pid
    _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
  end

  it "does not register direct performer idle polls" do
    _(DecentJob::Performer.new(database: database).perform_one).must_be_nil
    _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
  end
end

describe "worker process state" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    [RecordingJob, FailingJob, DiscardingJob].each { |job| job.queue_adapter = DecentJob::Adapter.new(database: database) }
    RecordingJob.events = Queue.new
    @worker = DecentJob::Worker.new(database: database, max_threads: 2, poll_interval: 0.01,
      process_heartbeat_interval: 0.02, enable_listen_notify: false, on_thread_error: ->(_error) {})
  end

  after do
    @worker.shutdown(timeout: 5)
  end

  def eventually
    Timeout.timeout(5) { sleep 0.005 until yield }
  end

  it "automatically removes abandoned presence on idle worker polls" do
    database.with_connection do |connection|
      connection.exec_params("INSERT INTO decent_job_processes (id, heartbeat_mode, updated_at) VALUES ($1, 'heartbeat', clock_timestamp() - interval '1 hour')", [SecureRandom.uuid])
    end
    @worker.start
    eventually { DecentJob::ProcessRecord.inactive(database: database).empty? }
    _(DecentJob::ProcessRecord.all(database: database).map(&:id)).must_equal [@worker.process_id]
  end

  it "keeps one idle presence and resets counters on restart" do
    @worker.start
    eventually { DecentJob::ProcessRecord.all(database: database).size == 1 }
    _(@worker.idle?).must_equal true
    record = DecentJob::ProcessRecord.find(@worker.process_id, database: database)
    _(record.schedulers.first.fetch("max_threads")).must_equal 2
    _(record.state.fetch("database_connection_pool").fetch("size")).must_equal 3
    RecordingJob.perform_later("success")
    FailingJob.perform_later
    DiscardingJob.perform_later
    eventually { @worker.stats.fetch(:total_executions_count) == 3 }
    @worker.shutdown(timeout: 5)
    stats = @worker.stats
    _(stats.fetch(:total_succeeded_executions_count)).must_equal 1
    _(stats.fetch(:total_errored_executions_count)).must_equal 2
    _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
    @worker.start
    _(@worker.stats.fetch(:total_executions_count)).must_equal 0
  end
end

describe "process tracking recovery" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    RecordingJob.queue_adapter = DecentJob::Adapter.new(database: database)
    RecordingJob.events = Queue.new
    @release = Queue.new
    @threads = []
  end

  after do
    @threads.size.times { @release << true }
    @threads.each { |thread| thread.join(5) }
    @tracker&.stop
    @tracker&.join(5)
  end

  def eventually
    Timeout.timeout(5) { sleep 0.005 until yield }
  end

  def insert_process(mode: "heartbeat", age: 301)
    id = SecureRandom.uuid
    database.with_connection do |connection|
      connection.exec_params("INSERT INTO decent_job_processes (id, heartbeat_mode, updated_at) VALUES ($1, $2, clock_timestamp() - $3 * interval '1 second')", [id, mode, age])
    end
    id
  end

  it "measures heartbeat age with the database clock" do
    fresh = insert_process(age: 0)
    local_now = Time.now + 3600
    Time.stub(:now, -> { local_now }) do
      record = DecentJob::ProcessRecord.find(fresh, database: database)
      _(record.stale?).must_equal false
      _(record.expired?).must_equal false
      local_now += 31
      _(record.stale?).must_equal true
    end
  end

  it "distinguishes stale heartbeats from expired processes and unlocked advisory sessions" do
    recent = insert_process(age: 29)
    stale = insert_process(age: 31)
    expired = insert_process
    unlocked = insert_process(mode: "advisory", age: 0)
    _(DecentJob::ProcessRecord.find(recent, database: database).stale?).must_equal false
    _(DecentJob::ProcessRecord.find(stale, database: database).stale?).must_equal true
    _(DecentJob::ProcessRecord.active(database: database).map(&:id).sort).must_equal [recent, stale].sort
    _(DecentJob::ProcessRecord.inactive(database: database).map(&:id).sort).must_equal [expired, unlocked].sort
    _(DecentJob::ProcessRecord.cleanup(database: database)).must_equal 2
  end

  it "shares concurrent leases and drains after stop without prematurely removing presence" do
    @tracker = DecentJob::ProcessTracker.new(database: database, process_heartbeat_interval: 0.02)
    started = Queue.new
    @threads = 2.times.map {
      Thread.new {
        @tracker.register { |id|
          started << id
          @release.pop
        }
      }
    }
    ids = 2.times.map { Timeout.timeout(5) { started.pop } }
    _(ids.uniq).must_equal [@tracker.process_id]
    @tracker.stop
    _(@tracker.join(0)).must_equal false
    @release << true
    eventually { @threads.count(&:alive?) == 1 }
    _(DecentJob::ProcessRecord.all(database: database).map(&:id)).must_equal ids.uniq
    @release << true
    @threads.each(&:value)
    _(@tracker.join(0)).must_equal true
    _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
  end

  it "reconnects its advisory session using the same identity while jobs remain registered" do
    errors = Queue.new
    @tracker = DecentJob::ProcessTracker.new(database: database, advisory_lock_heartbeat: true,
      process_heartbeat_interval: 0.02, on_thread_error: ->(error) { errors << error })
    @tracker.register do |id|
      record = DecentJob::ProcessRecord.find(id, database: database)
      database.with_connection do |connection|
        connection.exec(<<~SQL)
          SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'advisory' AND granted
            AND objsubid = 1 AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
            AND classid = ((hashtextextended(current_schema() || '.decent_job_processes.#{id}', 0) >> 32) & 4294967295)::oid
            AND objid = (hashtextextended(current_schema() || '.decent_job_processes.#{id}', 0) & 4294967295)::oid
        SQL
      end
      eventually { !errors.empty? }
      eventually { DecentJob::ProcessRecord.find(id, database: database).updated_at > record.updated_at }
      _(DecentJob::ProcessRecord.active(database: database).map(&:id)).must_equal [id]
    end
  end

  it "skips expired ownership while any session holds its job lock, including this checkout" do
    owner = insert_process
    job = RecordingJob.perform_later("owned")
    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET locked_by_id = $2 WHERE id = $1", [job.provider_job_id, owner])
      connection.exec_params("SELECT pg_advisory_lock(hashtextextended(current_schema() || '.decent_jobs.' || $1, 0))", [job.provider_job_id])
      begin
        _(DecentJob::ProcessRecord.cleanup(database: database)).must_equal 0
        _(DecentJob::Job.find(job.provider_job_id, database: database).locked_by_id).must_equal owner
      ensure
        connection.exec_params("SELECT pg_advisory_unlock(hashtextextended(current_schema() || '.decent_jobs.' || $1, 0))", [job.provider_job_id])
      end
    end
    _(DecentJob::ProcessRecord.cleanup(database: database)).must_equal 1
    _(DecentJob::Job.find(job.provider_job_id, database: database).locked_by_id).must_be_nil
  end

  it "skips contended claim scopes and preserves waiting claims when releasing abandoned ownership" do
    owner = insert_process
    job = RecordingJob.perform_later("abandoned")
    execution_id = SecureRandom.uuid
    database.with_connection do |connection|
      connection.exec_params("INSERT INTO decent_job_executions (id, job_id, process_id, process_uuid) VALUES ($1, $2, $3, $4)", [execution_id, job.provider_job_id, Process.pid, owner])
      connection.exec_params("UPDATE decent_jobs SET locked_by_id = $2, execution_id = $3 WHERE id = $1", [job.provider_job_id, owner, execution_id])
      connection.exec_params("INSERT INTO decent_job_concurrency_claims (scope, job_id, execution_id, state) VALUES ('busy', $1, $2, 'granted'), ('waiting', $1, $2, 'waiting')", [job.provider_job_id, execution_id])
    end
    separate = database.connect
    separate.exec("SELECT pg_advisory_lock(hashtextextended(current_schema() || '.decent_job.concurrency.busy', 0))")
    _(DecentJob::ProcessRecord.cleanup(database: database)).must_equal 0
    _(DecentJob::Job.find(job.provider_job_id, database: database).locked_by_id).must_equal owner
    separate.finish
    _(DecentJob::ProcessRecord.cleanup(database: database)).must_equal 1
    database.with_connection do |connection|
      _(connection.exec("SELECT state FROM decent_job_concurrency_claims").column_values(0)).must_equal ["waiting"]
    end
    _(DecentJob::Execution.find(execution_id, database: database).process_uuid).must_equal owner
  ensure
    separate&.finish unless separate&.finished?
  end

  it "skips contended process rows and continues cleaning other expired records" do
    busy = insert_process
    free = insert_process
    acquired = Queue.new
    @threads << Thread.new do
      database.transaction do |connection|
        connection.exec_params("SELECT id FROM decent_job_processes WHERE id = $1 FOR UPDATE", [busy])
        acquired << true
        @release.pop
      end
    end
    Timeout.timeout(5) { acquired.pop }
    _(DecentJob::ProcessRecord.cleanup(database: database)).must_equal 1
    _(DecentJob::ProcessRecord.all(database: database).map(&:id)).must_equal [busy]
    _ { DecentJob::ProcessRecord.find(free, database: database) }.must_raise KeyError
  end
end

class ProcessStateBlockingJob < ActiveJob::Base
  class_attribute :release, default: Queue.new
  def perform
    release.pop
  end
end

describe "process resource boundaries" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
  end

  it "keeps duplicate queue pools' active counts separate" do
    ProcessStateBlockingJob.release = Queue.new
    ProcessStateBlockingJob.queue_adapter = DecentJob::Adapter.new(database: database)
    2.times { ProcessStateBlockingJob.perform_later }
    worker = DecentJob::Worker.new(database: database, queues: "*:1;*:1", enable_listen_notify: false)
    worker.start
    Timeout.timeout(5) { sleep 0.005 until worker.stats.fetch(:schedulers).sum { |pool| pool.fetch(:active_threads) } == 2 }
    _(worker.stats.fetch(:schedulers).map { |pool| pool.fetch(:active_threads) }).must_equal [1, 1]
    _(worker.stats.fetch(:schedulers).map { |pool| pool.fetch(:available_threads) }).must_equal [0, 0]
  ensure
    2.times { ProcessStateBlockingJob.release << true }
    worker&.shutdown(timeout: 5)
  end

  it "automatically unregisters after a timeout-zero shutdown finally drains" do
    BlockingJob.started = Queue.new
    BlockingJob.release = Queue.new
    BlockingJob.queue_adapter = DecentJob::Adapter.new(database: database)
    job = BlockingJob.perform_later
    worker = DecentJob::Worker.new(database: database, max_threads: 1, enable_listen_notify: false, process_heartbeat_interval: 0.02)
    worker.start
    Timeout.timeout(5) { BlockingJob.started.pop }
    _(worker.shutdown(timeout: 0)).must_equal false
    _(DecentJob::ProcessRecord.all(database: database).map(&:id)).must_equal [worker.process_id]
    BlockingJob.release << true
    Timeout.timeout(5) { sleep 0.005 until DecentJob::ProcessRecord.all(database: database).empty? }
    _(DecentJob::Job.find(job.provider_job_id, database: database).locked_by_id).must_be_nil
  ensure
    BlockingJob.release << true
    worker&.shutdown(timeout: 5)
  end

  it "clears an authoritative force-discarded attempt's owner when its executor returns" do
    BlockingJob.started = Queue.new
    BlockingJob.release = Queue.new
    BlockingJob.queue_adapter = DecentJob::Adapter.new(database: database)
    job = BlockingJob.perform_later
    thread = Thread.new { DecentJob::Performer.new(database: database).perform_one }
    Timeout.timeout(5) { BlockingJob.started.pop }
    record = DecentJob::Job.find(job.provider_job_id, database: database)
    owner = record.locked_by_id
    record.force_discard_job("cancel")
    BlockingJob.release << true
    _(thread.value.state).must_equal "discarded"
    _(record.reload.locked_by_id).must_be_nil
    _(record.executions.first.fetch("process_uuid")).must_equal owner
    _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
  ensure
    BlockingJob.release << true
    thread&.join(5)
  end

  it "tracks inline executions without leaving an idle process record" do
    RecordingJob.events = Queue.new
    RecordingJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :inline)
    job = RecordingJob.perform_later("inline")
    execution = DecentJob::Execution.for_job(job.provider_job_id, database: database).first
    _(execution.process_uuid).must_match(/\A[0-9a-f-]{36}\z/)
    _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
  end

  it "surfaces terminal monitor failures instead of stranding registration" do
    previous = Thread.report_on_exception
    Thread.report_on_exception = false
    tracker = DecentJob::ProcessTracker.new(database: database, state: -> { raise NotImplementedError, "state unavailable" })
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    error = _ { Timeout.timeout(2) { tracker.register { flunk "registration must fail" } } }.must_raise NotImplementedError
    _(error.message).must_equal "state unavailable"
    _(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).must_be :<, 1
  ensure
    tracker&.stop
    begin
      tracker&.join(2)
    rescue NotImplementedError
      nil
    end
    Thread.report_on_exception = previous
  end
end

describe "process tracking configuration and startup" do
  include DatabaseSpec

  it "validates heartbeat options and passes them to workers" do
    configuration = DecentJob::Configuration.new
    _(configuration.advisory_lock_heartbeat).must_equal false
    _(configuration.process_heartbeat_interval).must_equal 30
    [nil, "yes", 1, Object.new].each { |value| _ { configuration.advisory_lock_heartbeat = value }.must_raise ArgumentError }
    [nil, 0, -1, 300, Float::NAN, Float::INFINITY, "30"].each { |value| _ { configuration.process_heartbeat_interval = value }.must_raise ArgumentError }
    configuration.advisory_lock_heartbeat = "true"
    configuration.process_heartbeat_interval = 0.1
    _(configuration.worker_options.fetch(:advisory_lock_heartbeat)).must_equal true
    _(configuration.worker_options.fetch(:process_heartbeat_interval)).must_equal 0.1
  end

  it "interrupts worker startup retries when shutdown is requested" do
    errors = Queue.new
    failing_database = database
    failing_database.define_singleton_method(:connect) { raise PG::ConnectionBad, "unavailable" }
    worker = DecentJob::Worker.new(database: failing_database, enable_listen_notify: false,
      on_thread_error: ->(error) { errors << error })
    worker.start
    Timeout.timeout(5) { errors.pop }
    _(worker.shutdown(timeout: 1)).must_equal true
  ensure
    worker&.shutdown(timeout: 1)
  end

  it "rejects independently opening an inherited database after fork" do
    inherited = database
    reader, writer = IO.pipe
    pid = fork do
      reader.close
      begin
        inherited.connect.finish
        writer.write("opened")
      rescue DecentJob::ForkedDatabase
        writer.write("rejected")
      ensure
        writer.close
        exit! 0
      end
    end
    writer.close
    _(reader.read).must_equal "rejected"
    Process.wait(pid)
    database.with_connection { |connection| _(connection.exec("SELECT 1").getvalue(0, 0)).must_equal "1" }
  ensure
    reader&.close
    writer&.close unless writer&.closed?
  end
end
