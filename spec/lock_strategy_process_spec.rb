# frozen_string_literal: true

require "fileutils"
require "open3"
require "spec_helper"
require "support/lock_strategy_process_app"
require "timeout"
require "tmpdir"

describe "lock strategies across worker processes" do
  include DatabaseSpec

  before do
    DecentJob.reset!
    DecentJob::Schema.new(database).migrate
    @directory = Dir.mktmpdir("dj-lock-modes-", "/tmp")
    @log = File.join(@directory, "events")
    @workers = []
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    LockStrategyProcessJob.queue_adapter = @adapter
    LockStrategyLimitedProcessJob.queue_adapter = @adapter
    @store = DecentJob::Store.new(database)
  end

  after do
    @workers.reverse_each do |worker|
      if worker.fetch(:process).alive?
        Process.kill("KILL", -worker.fetch(:process).pid)
        worker.fetch(:process).join
      end

      worker.fetch(:stdout).join
      worker.fetch(:stderr).join
    end

    @adapter.shutdown
    DecentJob.reset!
    FileUtils.remove_entry(@directory)
  end

  def records(type = nil, role: nil)
    return [] unless File.exist?(@log)

    rows = File.readlines(@log).filter_map do |line|
      JSON.parse(line)
    rescue JSON::ParserError
      nil
    end

    rows.select { |row| (!type || row.fetch("type") == type) && (!role || row.fetch("role") == role) }
  end

  def await
    Timeout.timeout(15) do
      until yield
        @workers.each do |worker|
          next if worker.fetch(:process).alive?
          raise "Worker stopped: #{worker.fetch(:stderr).value}\n#{worker.fetch(:stdout).value}"
        end

        sleep 0.01
      end
    end
  end

  def start_worker(mode, role:, queues: "*")
    environment = {"LOCK_STRATEGY_PROCESS_CONNECTION" => JSON.generate(@connection_options),
                   "LOCK_STRATEGY_PROCESS_LOG" => @log, "LOCK_STRATEGY_PROCESS_ROLE" => role,
                   "DECENT_JOB_LOCK_STRATEGY" => "advisory", "DECENT_JOB_QUEUE_SELECT_LIMIT" => "1000",
                   "DECENT_JOB_DEQUEUE_QUERY_SORT" => "created_at", "DECENT_JOB_ADVISORY_LOCK_HEARTBEAT" => "false",
                   "DECENT_JOB_EXECUTION_MODE" => "external", "DECENT_JOB_ENABLE_CRON" => "false",
                   "DECENT_JOB_SUBPROCESSES" => "0", "DECENT_JOB_DAEMONIZE" => "false",
                   "DECENT_JOB_IDLE_TIMEOUT" => nil, "NOTIFY_SOCKET" => nil, "WATCHDOG_USEC" => nil}
    command = [RbConfig.ruby, "-Ilib", "exe/decent_job", "start", "--require", "spec/support/lock_strategy_process_app.rb",
      "--lock-strategy", mode, "--queues", queues, "--max-threads", "1", "--poll-interval", "0.02", "--shutdown-timeout", "3"]
    input, output, error, process = Open3.popen3(environment, *command, pgroup: true)
    input.close
    worker = {process: process, stdout: Thread.new { output.read }, stderr: Thread.new { error.read }}
    @workers << worker
    await { records(role: role).any? }
    worker
  end

  def stop_worker(worker)
    Process.kill("TERM", worker.fetch(:process).pid)
    status = Timeout.timeout(6) { worker.fetch(:process).value }
    @workers.delete(worker)
    _(status.exitstatus).must_equal 0, "#{worker.fetch(:stderr).value}\n#{worker.fetch(:stdout).value}"
    worker.fetch(:stdout).join
    worker.fetch(:stderr).join
  end

  def kill_worker(worker)
    Process.kill("KILL", -worker.fetch(:process).pid)
    Timeout.timeout(6) { worker.fetch(:process).value }
    @workers.delete(worker)
    worker.fetch(:stdout).join
    worker.fetch(:stderr).join
  end

  %w[advisory skiplocked hybrid].product(%w[advisory skiplocked hybrid]).each do |owner_mode, contender_mode|
    it "keeps a #{owner_mode} claim exclusive against a #{contender_mode} worker" do
      gate = File.join(@directory, "release")
      owner_job = LockStrategyProcessJob.set(queue: "owner").perform_later("owner", gate)
      peer = LockStrategyProcessJob.set(queue: "other").perform_later("peer")
      owner = start_worker(owner_mode, role: "owner", queues: "owner")
      await { records("started", role: "owner").any? }
      started = records("started", role: "owner").first
      _(started.fetch("lock_type")).must_equal owner_mode
      _(started.fetch("owner")).wont_be_nil
      _(started.fetch("advisory")).must_equal(owner_mode != "skiplocked")
      _(DecentJob::Job.find(owner_job.provider_job_id, database: database).running?).must_equal true
      contender = start_worker(contender_mode, role: "contender", queues: "owner,other")
      await { records("completed", role: "contender").any? { |row| row["job_id"] == peer.provider_job_id } && records("empty", role: "contender").any? }
      _(records("started").map { |row| row.fetch("value") }).must_equal %w[owner peer]
      _(@store.executions(owner_job.provider_job_id).map { |row| row.fetch("state") }).must_equal ["running"]
      File.write(gate, "release")
      await { records("completed").any? { |row| row["job_id"] == owner_job.provider_job_id } }
      stop_worker(contender)
      stop_worker(owner)

      [owner_job, peer].each do |job|
        row = @store.find(job.provider_job_id)
        _(row.values_at("state", "locked_by_id", "locked_at", "lock_type")).must_equal ["succeeded", nil, nil, nil]
        _(@store.executions(job.provider_job_id).map { |attempt| attempt.fetch("state") }).must_equal ["succeeded"]
      end

      _(records("error")).must_be_empty
    end
  end

  %w[skiplocked hybrid].each do |mode|
    it "skips a row-locked head in #{mode} mode and executes it after release" do
      first = LockStrategyProcessJob.set(priority: -10).perform_later("held row")
      second = LockStrategyProcessJob.perform_later("available")
      owner = database.connect
      owner.exec("BEGIN")
      owner.exec_params("SELECT id FROM decent_jobs WHERE id = $1 FOR UPDATE", [first.provider_job_id])
      worker = start_worker(mode, role: "worker")
      await { records("completed").any? { |row| row["job_id"] == second.provider_job_id } }
      _(records("started").map { |row| row.fetch("value") }).must_equal ["available"]
      owner.exec("COMMIT")
      await { records("completed").any? { |row| row["job_id"] == first.provider_job_id } }
      stop_worker(worker)
      _(records("started").map { |row| row.fetch("value") }).must_equal ["available", "held row"]
      _(records("error")).must_be_empty
    ensure
      owner&.finish
    end
  end

  it "recovers a killed skiplocked owner only after persisted ownership is reclaimed" do
    gate = File.join(@directory, "release")
    job = LockStrategyProcessJob.perform_later("recover", gate)
    owner = start_worker("skiplocked", role: "owner")
    await { records("started", role: "owner").any? }
    owner_id = records("started", role: "owner").first.fetch("owner")
    kill_worker(owner)
    File.write(gate, "release")
    replacement = start_worker("advisory", role: "replacement")
    await { records("empty", role: "replacement").any? }
    _(records("started").size).must_equal 1
    _(@store.find(job.provider_job_id).fetch("locked_by_id")).must_equal owner_id

    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_job_processes SET updated_at = clock_timestamp() - interval '6 minutes' WHERE id = $1", [owner_id])
    end

    await do
      DecentJob::ProcessRecord.cleanup(database: database)
      @store.find(job.provider_job_id).fetch("locked_by_id") != owner_id
    end

    await { records("completed", role: "replacement").any? }
    stop_worker(replacement)
    _(records("started").map { |row| row.fetch("role") }).must_equal %w[owner replacement]
    _(@store.executions(job.provider_job_id).map { |row| row.fetch("state") }).must_equal %w[interrupted succeeded]
    _(@store.find(job.provider_job_id).values_at("locked_by_id", "lock_type")).must_equal [nil, nil]
    _(records("error")).must_be_empty
  end

  %w[advisory skiplocked hybrid].zip(%w[skiplocked hybrid advisory]).each do |owner_mode, contender_mode|
    it "preserves a #{owner_mode} concurrency grant while a #{contender_mode} worker waits" do
      gate = File.join(@directory, "release")
      first = LockStrategyLimitedProcessJob.set(queue: "owner").perform_later("first", gate)
      second = LockStrategyLimitedProcessJob.set(queue: "other").perform_later("second")
      owner = start_worker(owner_mode, role: "owner", queues: "owner")
      await { records("started", role: "owner").any? }
      contender = start_worker(contender_mode, role: "contender", queues: "other")
      await { records("completed", role: "contender").any? { |row| row["state"] == "retried" } && records("empty", role: "contender").any? }
      _(records("started").map { |row| row.fetch("value") }).must_equal ["first"]
      File.write(gate, "release")
      await { records("completed").any? { |row| row["job_id"] == first.provider_job_id && row["state"] == "succeeded" } && records("completed").any? { |row| row["job_id"] == second.provider_job_id && row["state"] == "succeeded" } }
      stop_worker(contender)
      stop_worker(owner)
      _(records("started").map { |row| row.fetch("value") }).must_equal %w[first second]
      _(records("error")).must_be_empty
    end
  end
end
