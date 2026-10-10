# frozen_string_literal: true

require "open3"
require "spec_helper"
require "support/cli_startup"
require "support/process_tracking_cli_app"
require "timeout"
require "tmpdir"

describe "process tracking across workers" do
  include DatabaseSpec
  include CliStartupSpec

  before do
    DecentJob::Schema.new(database).migrate
    ProcessTrackingJob.queue_adapter = DecentJob::Adapter.new(database: database)
    @directory = Dir.mktmpdir("decent_job_processes")
    @started = File.join(@directory, "started")
    @release = File.join(@directory, "release")
    @result = File.join(@directory, "result")
    @errors = File.join(@directory, "errors")
  end

  after do
    if @process&.alive?
      begin
        Process.kill("KILL", @process.pid)
      rescue Errno::ESRCH
        nil
      end

      @process.join
    end

    [@input, @output, @stderr].compact.each { |io| io.close unless io.closed? }
    FileUtils.remove_entry(@directory)
    DecentJob.reset!
  end

  def await(&block)
    Timeout.timeout(8) { sleep 0.01 until block.call }
  end

  def start_worker(mode = nil)
    job = ProcessTrackingJob.perform_later(mode, @started, @release, @result) if mode
    environment = {"DECENT_JOB_PROCESS_SPEC_CONNECTION" => JSON.generate(@connection_options),
                   "DECENT_JOB_PROCESS_SPEC_ERRORS" => @errors,
                   "DECENT_JOB_ADVISORY_LOCK_HEARTBEAT" => "true",
                   "DECENT_JOB_PROCESS_HEARTBEAT_INTERVAL" => "0.03"}
    @input, @output, @stderr, @process = Open3.popen3(environment, RbConfig.ruby, "-Ilib", "exe/decent_job", "start",
      "--require", File.expand_path("support/process_tracking_cli_app.rb", __dir__), "--shutdown-timeout", "2")
    @input.close
    await_started(@output, @stderr)
    await { File.exist?(@started) } if mode
    job && DecentJob::Job.find(job.provider_job_id, database: database)
  end

  def record_for_worker
    DecentJob::ProcessRecord.all(database: database).find { |record| record.state.fetch("pid") == @process.pid }
  end

  def stop_worker
    Process.kill("TERM", @process.pid)
    assert_worker_exit
  end

  def assert_worker_exit
    _(@process.join(5)).wont_be_nil
    _(@process.value.exitstatus).must_equal 0
    _(@stderr.read).must_equal ""
  end

  it "registers an idle worker without LISTEN and removes it on clean shutdown" do
    _(DecentJob.const_defined?(:ProcessRecord)).must_equal true
    start_worker
    await { record_for_worker }
    record = record_for_worker
    _(record.id).must_match(/\A[0-9a-f-]{36}\z/)
    _(DecentJob::ProcessRecord.active(database: database).map(&:id)).must_include record.id
    _(record.state.fetch("hostname")).must_be_kind_of String
    _(record.state.fetch("memory")).must_be :>=, 0
    _(record.state.fetch("database_connection_pool").fetch("size")).must_equal 3
    stop_worker
    _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
    _(File.exist?(@errors)).must_equal false
  end

  it "renews the heartbeat while every pooled connection is occupied" do
    _(DecentJob.const_defined?(:ProcessRecord)).must_equal true
    job = start_worker("pool")
    await { record_for_worker&.state&.dig("database_connection_pool", "active") == 3 }
    record = record_for_worker
    _(job.locked_by_id).must_equal record.id
    _(job.executions.first.fetch("process_uuid")).must_equal record.id
    _(job.executions.first.fetch("process_id")).must_equal @process.pid
    await { record_for_worker.updated_at > record.updated_at }
    _(record_for_worker.state.fetch("database_connection_pool").fetch("active")).must_equal 3
    File.write(@release, "continue")
    await { job.reload.finished? }
    await { record_for_worker.state.fetch("total_succeeded_executions_count") == 1 }
    _(job.locked_by_id).must_be_nil
    _(job.executions.first.fetch("process_uuid")).must_equal record.id
    stop_worker
    _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
  end

  it "keeps the process registered and renewing until SIGTERM has drained its active job" do
    _(DecentJob.const_defined?(:ProcessRecord)).must_equal true
    job = start_worker("hold")
    record = record_for_worker
    Process.kill("TERM", @process.pid)
    await { record_for_worker.updated_at > record.updated_at }
    _(@process.alive?).must_equal true
    _(DecentJob::ProcessRecord.active(database: database).map(&:id)).must_include record.id
    _(job.reload.locked_by_id).must_equal record.id
    File.write(@release, "continue")
    assert_worker_exit
    _(job.reload.succeeded?).must_equal true
    _(job.locked_by_id).must_be_nil
    _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
  end

  it "cleans a killed worker's ownership and grants while retaining execution history" do
    _(DecentJob.const_defined?(:ProcessRecord)).must_equal true
    job = start_worker("hold")
    record = record_for_worker
    waiter = ProcessTrackingJob.perform_later("finish", @started, @release, @result)
    performer = DecentJob::Performer.new(database: database)
    _(performer.perform_one(job_id: waiter.provider_job_id).state).must_equal "retried"
    waiting = DecentJob::Job.find(waiter.provider_job_id, database: database)
    _(waiting.scheduled_at).must_be :>, Time.now
    Process.kill("KILL", @process.pid)
    _(@process.join(5)).wont_be_nil
    # The server releases the advisory lock only after it notices the closed socket.
    await { DecentJob::ProcessRecord.inactive(database: database).map(&:id).include?(record.id) }
    _(DecentJob::ProcessRecord.inactive(database: database).map(&:id)).must_include record.id
    _(DecentJob::ProcessRecord.cleanup(database: database)).must_be :>=, 1
    _(job.reload.locked_by_id).must_be_nil
    _(job.executions.first.fetch("state")).must_equal "running"
    _(job.executions.first.fetch("process_uuid")).must_equal record.id
    _(waiting.reload.scheduled_at).must_be :<=, Time.now
    File.write(@release, "continue")
    _(performer.perform_one(job_id: job.id).state).must_equal "succeeded"
    attempts = job.executions
    _(attempts.map { |attempt| attempt.fetch("state") }).must_equal %w[interrupted succeeded]
    _(attempts.first.fetch("process_uuid")).must_equal record.id
    _(attempts.last.fetch("process_uuid")).wont_equal record.id
    _(performer.perform_one(job_id: waiter.provider_job_id).state).must_equal "succeeded"
  end

  it "reconnects a lost heartbeat session without releasing a live job's grant" do
    _(DecentJob.const_defined?(:ProcessRecord)).must_equal true
    job = start_worker("hold")
    record = record_for_worker

    database.with_connection do |connection|
      backend = connection.exec_params(<<~SQL, [record.id]).first.fetch("pid")
        SELECT pg_locks.pid FROM pg_locks, decent_job_processes
        WHERE decent_job_processes.id = $1 AND locktype = 'advisory' AND granted AND objsubid = 1
          AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
          AND classid = ((#{DecentJob::ProcessRecord::KEY_SQL} >> 32) & 4294967295)::oid
          AND objid = (#{DecentJob::ProcessRecord::KEY_SQL} & 4294967295)::oid
      SQL
      _(connection.exec_params("SELECT pg_terminate_backend($1)", [backend]).getvalue(0, 0)).must_equal "t"
    end

    _(DecentJob::ProcessRecord.cleanup(database: database)).must_equal 0
    _(job.reload.locked_by_id).must_equal record.id

    database.with_connection do |connection|
      _(connection.exec_params("SELECT count(*) FROM decent_job_concurrency_claims WHERE job_id = $1 AND state = 'granted'", [job.id]).getvalue(0, 0)).must_equal "1"
    end

    await do
      record_for_worker.updated_at > record.updated_at &&
        DecentJob::ProcessRecord.active(database: database).any? { |item| item.id == record.id }
    end

    File.write(@release, "continue")
    await { job.reload.finished? }
    _(job.succeeded?).must_equal true
    _(job.executions.size).must_equal 1
    stop_worker
    _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
  end
end
