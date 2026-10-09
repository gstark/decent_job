# frozen_string_literal: true

require "open3"
require "spec_helper"
require "timeout"

require File.expand_path("../support/pgbouncer", __dir__)
require File.expand_path("../support/pgbouncer_process_app", __dir__)

class PgBouncerRecordingJob < ActiveJob::Base
  class_attribute :events, default: Queue.new

  def perform(value)
    events << value
  end
end

class PgBouncerRetryJob < PgBouncerRecordingJob
  retry_on RuntimeError, wait: 0, attempts: 2

  def perform(value)
    raise "first attempt" if executions == 1
    super
  end
end

class PgBouncerCallbackJob < PgBouncerRecordingJob
  def perform(batch, options)
    events << [batch.id, options.fetch(:event)]
  end
end

class PgBouncerLimitedJob < PgBouncerRecordingJob
  include DecentJob::ActiveJobExtensions::Concurrency

  class_attribute :started, :release
  decent_job_control_concurrency_with(key: "pgbouncer", perform_limit: 1)

  def perform(value)
    started << true
    release.pop
    super
  end
end

class PgBouncerInterruptedJob < PgBouncerRecordingJob
  class_attribute :block_query, default: true

  def perform(value)
    if block_query
      self.class.queue_adapter.database.with_connection { |connection| connection.exec("SELECT pg_sleep(60)") }
    end
    super
  end
end

describe "PgBouncer transaction pooling" do
  before do
    DecentJob.reset!
    DecentJob.configure do |config|
      config.lock_strategy = :skiplocked
      config.enable_listen_notify = false
      config.advisory_lock_heartbeat = false
      config.process_heartbeat_interval = 0.05
      config.poll_interval = 0.02
    end
    @workers = []
    @threads = []
    @fixture = PgBouncerFixture.new
    @database = DecentJob::Database.new(url: @fixture.connection_options, pool_size: 5)
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    [PgBouncerRecordingJob, PgBouncerRetryJob, PgBouncerCallbackJob, PgBouncerLimitedJob, PgBouncerProcessJob, PgBouncerInterruptedJob].each { |job| job.queue_adapter = @adapter }
    PgBouncerRecordingJob.events = Queue.new
    PgBouncerLimitedJob.started = Queue.new
    PgBouncerLimitedJob.release = Queue.new
    PgBouncerInterruptedJob.block_query = true
    @store = DecentJob::Store.new(database)
    @performer = DecentJob::Performer.new(database: database)
    @log = File.join(@fixture.directory, "worker-events")
  end

  after do
    @workers.reverse_each { |worker| kill_worker(worker) }
    5.times { PgBouncerLimitedJob.release << true } if PgBouncerLimitedJob.release
    @threads.each { |thread| Timeout.timeout(5) { thread.value } }
    @adapter&.shutdown
    if @database
      # Check while backend sessions remain alive: disconnecting PgBouncer could
      # otherwise hide leaked session advisory locks.
      _(sql("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND database = (SELECT oid FROM pg_database WHERE datname = current_database())").first.fetch("count")).must_equal "0"
    end
  ensure
    @database&.close
    @fixture&.close
    DecentJob.reset!
  end

  attr_reader :database

  def sql(query, parameters = [])
    database.with_connection { |connection| connection.exec_params(query, parameters).to_a }
  end

  def await
    Timeout.timeout(12) do
      until yield
        @workers.each do |worker|
          next if worker.fetch(:process).alive? || worker[:stopping]
          raise "Worker exited: #{worker.fetch(:stderr).value}"
        end
        sleep 0.01
      end
    end
  end

  def records(type, role: nil)
    return [] unless File.exist?(@log)
    File.readlines(@log).filter_map do |line|
      row = JSON.parse(line)
      row if row.fetch("type") == type && (!role || row.fetch("role") == role)
    rescue JSON::ParserError
      nil
    end
  end

  def start_worker(role, queues: "*")
    environment = {"PGBOUNCER_PROCESS_CONNECTION" => JSON.generate(@fixture.connection_options),
                   "PGBOUNCER_PROCESS_LOG" => @log, "PGBOUNCER_PROCESS_ROLE" => role,
                   "DECENT_JOB_EXECUTION_MODE" => "external", "DECENT_JOB_ENABLE_CRON" => "false",
                   "DECENT_JOB_SUBPROCESSES" => "0", "DECENT_JOB_DAEMONIZE" => "false",
                   "DECENT_JOB_IDLE_TIMEOUT" => nil, "NOTIFY_SOCKET" => nil, "WATCHDOG_USEC" => nil}
    command = [RbConfig.ruby, "-Ilib", "exe/decent_job", "start", "--require", "integration/support/pgbouncer_process_app.rb",
      "--queues", queues, "--max-threads", "1", "--poll-interval", "0.02", "--shutdown-timeout", "3"]
    input, output, error, process = Open3.popen3(environment, *command, pgroup: true)
    input.close
    worker = {process: process, stdout: Thread.new { output.read }, stderr: Thread.new { error.read }}
    @workers << worker
    await { records("started", role: role).any? || records("empty", role: role).any? }
    worker
  end

  def stop_worker(worker)
    worker[:stopping] = true
    Process.kill("TERM", worker.fetch(:process).pid)
    status = Timeout.timeout(6) { worker.fetch(:process).value }
    @workers.delete(worker)
    _(status.exitstatus).must_equal 0, worker.fetch(:stderr).value
    worker.fetch(:stdout).join
    worker.fetch(:stderr).join
  end

  def kill_worker(worker)
    if worker.fetch(:process).alive?
      Process.kill("KILL", -worker.fetch(:process).pid)
      Timeout.timeout(5) { worker.fetch(:process).value }
    end
    @workers.delete(worker)
    worker.fetch(:stdout).join
    worker.fetch(:stderr).join
  end

  it "uses transaction mode and switches one frontend between real PostgreSQL backends" do
    configuration = @fixture.admin_query("SHOW CONFIG").to_h { |row| [row.fetch("key"), row.fetch("value")] }
    _(configuration.fetch("pool_mode")).must_equal "transaction"
    _(configuration.fetch("server_round_robin")).must_equal "1"
    _(configuration.fetch("default_pool_size")).must_equal "2"
    first = database.connect
    second = database.connect
    first.exec("BEGIN")
    first_pid = first.exec("SELECT pg_backend_pid()").getvalue(0, 0)
    second.exec("BEGIN")
    second_pid = second.exec("SELECT pg_backend_pid()").getvalue(0, 0)
    _(first_pid).wont_equal second_pid
    first.exec("COMMIT")
    second.exec("COMMIT")
    # Occupy the first backend so the original frontend must use the second.
    second.exec("BEGIN")
    _(second.exec("SELECT pg_backend_pid()").getvalue(0, 0)).must_equal first_pid
    _(first.exec("SELECT pg_backend_pid()").getvalue(0, 0)).must_equal second_pid
  ensure
    first&.finish
    second&.finish
  end

  it "commits outer work while rolling back savepoints and full transactions" do
    observer = database.connect
    rolled_back = nil
    database.transaction do
      PgBouncerRecordingJob.perform_later("outer")
      _ {
        database.transaction {
          rolled_back = PgBouncerRecordingJob.perform_later("savepoint")
          raise "rollback"
        }
      }.must_raise RuntimeError
      _(observer.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)).must_equal "0"
      _(@store.find(rolled_back.provider_job_id)).must_be_nil
    end
    _ {
      database.transaction {
        PgBouncerRecordingJob.perform_later("transaction")
        raise "rollback"
      }
    }.must_raise RuntimeError
    _(sql("SELECT payload->'arguments' AS arguments FROM decent_jobs")).must_equal [{"arguments" => '["outer"]'}]
    _(@performer.perform_one.state).must_equal "succeeded"
    _(PgBouncerRecordingJob.events.pop(true)).must_equal "outer"
  ensure
    observer&.finish
  end

  it "checks migration readiness and drains explicitly through the transaction pool" do
    DecentJob.configure { |config| config.database_url = @fixture.connection_options }
    adapter = DecentJob::Adapter.new(database: DecentJob.database, execution_mode: :external)
    [PgBouncerRecordingJob, PgBouncerRetryJob].each { |job| job.queue_adapter = adapter }
    database.transaction do |connection|
      connection.exec("SET TRANSACTION READ ONLY")
      _(DecentJob.migrated?(database: database)).must_equal true
    end
    _(DecentJob.migrated?).must_equal true
    _(DecentJob.shutdown?).must_equal true
    PgBouncerRetryJob.perform_later("retry")
    scheduled = PgBouncerRecordingJob.set(wait: 3600).perform_later("later")
    _(Timeout.timeout(10) { DecentJob.perform_inline }).must_equal 2
    _(PgBouncerRecordingJob.events.pop(true)).must_equal "retry"
    _(@store.find(scheduled.provider_job_id).fetch("state")).must_equal "ready"
    _(DecentJob.perform_inline).must_equal 0
    DecentJob.start
    _(DecentJob.shutdown?).must_equal false
    _(DecentJob.shutdown(timeout: 5)).must_equal true
    _(DecentJob.shutdown?).must_equal true
  end

  it "performs immediate and scheduled work and preserves Active Job retry history" do
    immediate = PgBouncerRecordingJob.perform_later("immediate")
    scheduled = PgBouncerRecordingJob.set(wait: 3600).perform_later("scheduled")
    retrying = PgBouncerRetryJob.perform_later("retry")
    _(@performer.perform_one(job_id: immediate.provider_job_id).state).must_equal "succeeded"
    _(@performer.perform_one(job_id: scheduled.provider_job_id)).must_be_nil
    _(@performer.perform_one(job_id: retrying.provider_job_id).state).must_equal "retried"
    _(@performer.perform_one(job_id: retrying.provider_job_id).state).must_equal "succeeded"
    DecentJob::Job.find(scheduled.provider_job_id, database: database).reschedule_job(Time.now - 1)
    _(@performer.perform_one(job_id: scheduled.provider_job_id).state).must_equal "succeeded"
    _(@store.executions(retrying.provider_job_id).map { |row| row.fetch("state") }).must_equal %w[retried succeeded]
    _(3.times.map { PgBouncerRecordingJob.events.pop(true) }).must_equal %w[immediate retry scheduled]
  end

  it "aggregates retained retry attempts through the transaction pool" do
    ordinary = PgBouncerRecordingJob.set(queue: "metrics").perform_later("ordinary")
    retrying = PgBouncerRetryJob.set(queue: "metrics").perform_later("retry")
    3.times { @performer.perform_one }
    metrics = DecentJob::ExecutionMetrics.new(database: database)
    summary = metrics.summary(queue_name: "metrics")
    _(summary.values_at(:jobs_count, :executions_count)).must_equal [2, 3]
    attempts = [ordinary, retrying].flat_map { |job| DecentJob::Execution.for_job(job.provider_job_id, database: database) }
    _(summary.fetch(:avg_execution)).must_be_close_to attempts.sum(&:runtime_latency) / attempts.size, 0.000001
    _(summary.fetch(:avg_total)).must_be_close_to attempts.sum(&:total_latency) / attempts.size, 0.000001
    grouped = metrics.grouped_by(:job_class, queue_name: "metrics")
    _(grouped.map { |row| row.values_at(:job_class, :jobs_count, :executions_count) }).must_equal [
      ["PgBouncerRecordingJob", 1, 1], ["PgBouncerRetryJob", 1, 2]
    ]
    _(metrics.summary(queue_name: "missing").fetch(:avg_execution)).must_be_nil
  end

  it "persists bulk members and dispatches each batch callback once" do
    batch = DecentJob::Batch.enqueue(database: database, on_success: PgBouncerCallbackJob, on_finish: PgBouncerCallbackJob) do
      DecentJob::Bulk.enqueue { 3.times { |index| PgBouncerRecordingJob.perform_later(index) } }
    end
    _(batch.active_jobs.size).must_equal 3
    nil while @performer.perform_one
    _(batch.reload.succeeded?).must_equal true
    _(batch.finished?).must_equal true
    _(batch.callback_active_jobs.size).must_equal 2
    values = 5.times.map { PgBouncerRecordingJob.events.pop(true) }
    _(values.grep(Integer).sort).must_equal [0, 1, 2]
    _(values.grep(Array).sort).must_equal [[batch.id, :finish], [batch.id, :success]]
    DecentJob::BatchStore.new(database).reconcile(batch.id)
    _(@performer.perform_one).must_be_nil
  end

  it "holds a perform concurrency grant across backend reuse and promotes its waiter" do
    first = PgBouncerLimitedJob.perform_later("first")
    second = PgBouncerLimitedJob.perform_later("second")
    @threads << Thread.new { @performer.perform_one(job_id: first.provider_job_id) }
    Timeout.timeout(5) { PgBouncerLimitedJob.started.pop }
    _(@performer.perform_one(job_id: second.provider_job_id).state).must_equal "retried"
    _(PgBouncerRecordingJob.events.size).must_equal 0
    PgBouncerLimitedJob.release << true
    _(@threads.last.value.state).must_equal "succeeded"
    PgBouncerLimitedJob.release << true
    _(@performer.perform_one(job_id: second.provider_job_id).state).must_equal "succeeded"
    _(2.times.map { PgBouncerRecordingJob.events.pop(true) }).must_equal %w[first second]
    _(sql("SELECT count(*) FROM decent_job_concurrency_claims").first.fetch("count")).must_equal "0"
  end

  it "supports discard, retry, destruction, and retained-history cleanup" do
    queued = PgBouncerRecordingJob.perform_later("managed")
    job = DecentJob::Job.find(queued.provider_job_id, database: database)
    job.discard_job("operator")
    _(job.discarded?).must_equal true
    _(job.retry_job.successfully_enqueued?).must_equal true
    _(@performer.perform_one.state).must_equal "succeeded"
    _(job.reload.succeeded?).must_equal true
    job.destroy_job
    _(@store.find(job.id)).must_be_nil
    PgBouncerRecordingJob.perform_later("cleanup")
    @performer.perform_one
    _(DecentJob::Cleaner.new(database: database).cleanup(older_than: 0)).must_equal 2
    _(sql("SELECT count(*) FROM decent_jobs").first.fetch("count")).must_equal "0"
    DecentJob.configuration.preserve_job_records = false
    transient = PgBouncerRecordingJob.perform_later("transient")
    _(@performer.perform_one.state).must_equal "succeeded"
    _(@store.find(transient.provider_job_id)).must_be_nil
  end

  it "deduplicates recurring occurrences and honors persisted queue pauses" do
    entry = DecentJob::CronEntry.new(key: "pooled", cron: "@daily", class: PgBouncerRecordingJob, args: ["cron"], database: database)
    at = Time.now
    first = entry.enqueue(at)
    database.transaction do
      _(entry.enqueue(at)).must_equal false
      _(sql("SELECT 42 AS value").first.fetch("value")).must_equal "42"
    end
    pauses = DecentJob::Pauses.new(database)
    pauses.pause(queue: "default")
    performer = DecentJob::Performer.new(database: database, enable_pauses: true)
    _(performer.perform_one).must_be_nil
    pauses.unpause(queue: "default")
    _(performer.perform_one.job_id).must_equal first.provider_job_id
    _(entry.jobs.size).must_equal 1
    _(PgBouncerRecordingJob.events.pop(true)).must_equal "cron"
  end

  it "polls exclusively across CLI workers while timestamp monitors share the small server pool" do
    gate = File.join(@fixture.directory, "release")
    job = PgBouncerProcessJob.set(queue: "owner").perform_later("owner", gate)
    owner = start_worker("owner", queues: "owner")
    await { records("started", role: "owner").any? }
    row = records("started", role: "owner").first
    _(row.fetch("lock_type")).must_equal "skiplocked"
    heartbeat = DecentJob::ProcessRecord.find(row.fetch("owner"), database: database)
    _(heartbeat.heartbeat_mode).must_equal "heartbeat"
    contender = start_worker("contender", queues: "owner,other")
    # Enqueue after both workers are idle/occupied: LISTEN cannot deliver it.
    peer = PgBouncerProcessJob.set(queue: "other").perform_later("peer")
    await { records("completed", role: "contender").any? { |event| event["job_id"] == peer.provider_job_id } }
    await { DecentJob::ProcessRecord.find(heartbeat.id, database: database).updated_at > heartbeat.updated_at }
    _(DecentJob::ProcessRecord.active(database: database).size).must_equal 2
    _(records("started").map { |event| event.fetch("value") }).must_equal %w[owner peer]
    _(sql("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()").first.fetch("count").to_i).must_be :<=, 2
    File.write(gate, "release")
    await { records("completed", role: "owner").any? }
    stop_worker(contender)
    stop_worker(owner)
    _(@store.executions(job.provider_job_id).map { |attempt| attempt.fetch("state") }).must_equal ["succeeded"]
    metrics = DecentJob::ExecutionMetrics.new(database: database)
    _(metrics.summary.values_at(:jobs_count, :executions_count)).must_equal [2, 2]
    _(metrics.grouped_by(:queue_name).map { |row| row.values_at(:queue_name, :executions_count) }).must_equal [["other", 1], ["owner", 1]]
    _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
    _(records("error")).must_be_empty
  end

  it "drains an active job after TERM before unregistering its heartbeat" do
    gate = File.join(@fixture.directory, "release")
    job = PgBouncerProcessJob.perform_later("drain", gate)
    worker = start_worker("drain")
    await { records("started").any? }
    worker[:stopping] = true
    Process.kill("TERM", worker.fetch(:process).pid)
    _(worker.fetch(:process).join(0.15)).must_be_nil
    _(DecentJob::ProcessRecord.active(database: database).size).must_equal 1
    File.write(gate, "release")
    _(Timeout.timeout(6) { worker.fetch(:process).value }.exitstatus).must_equal 0
    _(@store.find(job.provider_job_id).fetch("state")).must_equal "succeeded"
    _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
    _(records("error")).must_be_empty
  end

  it "keeps force-discarded running work terminal when the worker returns" do
    gate = File.join(@fixture.directory, "release")
    queued = PgBouncerProcessJob.perform_later("discard", gate)
    worker = start_worker("discard")
    await { records("started").any? }
    job = DecentJob::Job.find(queued.provider_job_id, database: database)
    _ { job.discard_job("ordinary") }.must_raise DecentJob::Job::LockedError
    job.force_discard_job("forced")
    _(job.reload.discarded?).must_equal true
    File.write(gate, "release")
    await { records("completed").any? }
    stop_worker(worker)
    _(job.reload.discarded?).must_equal true
    _(job.executions.map { |attempt| attempt.fetch("state") }).must_equal ["discarded"]
    _(job.attributes.values_at("locked_by_id", "lock_type")).must_equal [nil, nil]
  end

  it "recovers a crashed owner after heartbeat expiry with interrupted execution history" do
    gate = File.join(@fixture.directory, "release")
    job = PgBouncerProcessJob.perform_later("recover", gate)
    owner = start_worker("owner")
    await { records("started").any? }
    owner_id = records("started").first.fetch("owner")
    kill_worker(owner)
    File.write(gate, "release")
    replacement = start_worker("replacement")
    await { records("empty", role: "replacement").any? }
    _(records("started").size).must_equal 1
    _(@store.find(job.provider_job_id).fetch("locked_by_id")).must_equal owner_id
    sql("UPDATE decent_job_processes SET updated_at = clock_timestamp() - interval '6 minutes' WHERE id = $1", [owner_id])
    _(DecentJob::ProcessRecord.inactive(database: database).map(&:id)).must_include owner_id
    await {
      DecentJob::ProcessRecord.cleanup(database: database)
      records("completed", role: "replacement").any?
    }
    stop_worker(replacement)
    _(records("started").map { |row| row.fetch("role") }).must_equal %w[owner replacement]
    _(@store.executions(job.provider_job_id).map { |row| row.fetch("state") }).must_equal %w[interrupted succeeded]
    _(@store.find(job.provider_job_id).values_at("locked_by_id", "lock_type")).must_equal [nil, nil]
    _(records("error")).must_be_empty
  end

  it "cancels a blocked PostgreSQL query on forced thread shutdown and recovers its execution" do
    job = PgBouncerInterruptedJob.perform_later("recovered")
    worker = DecentJob::Worker.new(database: database, max_threads: 1, poll_interval: 0.02, enable_listen_notify: false).start
    await { sql("SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND query = 'SELECT pg_sleep(60)' AND state = 'active'").any? }
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    worker.shutdown(timeout: 0, force: true)
    _(worker.shutdown(timeout: 5)).must_equal true
    _(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).must_be :<, 5
    _(worker.interrupted_job_ids).must_equal [job.job_id]
    _(@store.find(job.provider_job_id).values_at("locked_by_id", "lock_type")).must_equal [nil, nil]
    await { sql("SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND query = 'SELECT pg_sleep(60)' AND state = 'active'").empty? }
    PgBouncerInterruptedJob.block_query = false
    _(@performer.perform_one(job_id: job.provider_job_id).state).must_equal "succeeded"
    _(@store.executions(job.provider_job_id).map { |row| row.fetch("state") }).must_equal %w[interrupted succeeded]
    _(PgBouncerRecordingJob.events.pop(true)).must_equal "recovered"
  ensure
    worker&.shutdown(timeout: 5, force: true)
  end
end
