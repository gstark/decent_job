# frozen_string_literal: true

require "open3"
require "spec_helper"
require "support/cron_cli_app"
require "timeout"
require "tmpdir"

describe "cron manager processes" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @directory = Dir.mktmpdir("decent_job_cron_process")
    @marker = File.join(@directory, "performed")
    @entered = File.join(@directory, "enqueue_started")
    @environment = {
      "DECENT_JOB_CRON_SPEC_CONNECTION" => JSON.generate(@connection_options),
      "DECENT_JOB_CRON_SPEC_MARKER" => @marker,
      "DECENT_JOB_CRON_SPEC_STARTED" => @entered
    }
    @command = [RbConfig.ruby, "-Ilib", "exe/decent_job", "start", "--require", File.expand_path("support/cron_cli_app.rb", __dir__)]
    @processes = []
  end

  after do
    @processes.each do |input, output, error, process|
      if process.alive?
        begin
          Process.kill("KILL", process.pid)
        rescue Errno::ESRCH
          # The child can exit before Open3's waiter records its status.
        end
        process.join
      end
      [input, output, error].each { |io| io.close unless io.closed? }
    end
    FileUtils.remove_entry(@directory)
  end

  def start_process(*arguments)
    handles = Open3.popen3(@environment, *@command, *arguments)
    @processes << handles
    input, output, error, _process = handles
    input.close
    line = Timeout.timeout(8) { output.gets }
    _(line.to_s).must_include "DecentJob started", (error.read if line.nil?)
    handles
  end

  def wait_until
    Timeout.timeout(8) { sleep 0.01 until yield }
  end

  def stop_process(handles, status: 0)
    _input, _output, error, process = handles
    Process.kill("TERM", process.pid)
    _(process.join(5)).wont_be_nil
    _(process.value.exitstatus).must_equal status
    error.read
  end

  def jobs
    database.with_connection do |connection|
      connection.exec("SELECT * FROM decent_jobs WHERE cron_key = 'pulse' ORDER BY cron_at").map { |row| DecentJob::Store.new(database).decode(row) }
    end
  end

  def performed
    return [] unless File.exist?(@marker)

    File.open(@marker) do |file|
      file.flock(File::LOCK_SH)
      file.each_line.map { |line| JSON.parse(line) }
    end
  end

  it "runs the same schedule in two CLI processes without duplicate occurrences or executions" do
    first = start_process("--enable-cron", "--shutdown-timeout", "2")
    second = start_process("--enable-cron", "--shutdown-timeout", "2")
    wait_until { performed.size >= 3 }
    _(stop_process(first)).must_equal ""
    _(stop_process(second)).must_equal ""
    records = jobs
    _(records.size).must_be :>=, 3
    _(records.map { |row| row.fetch("cron_at") }.uniq.size).must_equal records.size
    executions = performed
    _(executions.map { |item| item.fetch("job_id") }.uniq.size).must_equal executions.size
    _(executions.map { |item| item.fetch("provider_job_id") } - records.map { |row| row.fetch("id") }).must_be_empty
  end

  it "lets the CLI disable a schedule enabled by the application" do
    @environment["DECENT_JOB_CRON_SPEC_ENABLED"] = "true"
    process = start_process("--no-enable-cron", "--cron-graceful-restart-period", "5", "--shutdown-timeout", "2")
    sleep 1.1
    _(stop_process(process)).must_equal ""
    _(jobs).must_be_empty
    _(performed).must_be_empty
  end

  it "catches up missed daily runs using the CLI restart period while ignoring future history" do
    @environment["DECENT_JOB_CRON_SPEC_EXPRESSION"] = "0 0 * * * UTC"
    current = Time.now.utc
    midnight = Time.utc(current.year, current.month, current.day)
    future = midnight + 86400
    CronProcessJob.queue_adapter = DecentJob::Adapter.new(database: database)
    DecentJob::CronEntry.new(key: :pulse, cron: "0 0 * * * UTC", class: CronProcessJob,
      args: [@marker], set: {queue: "cron", wait_until: future}, database: database).enqueue(future)
    process = start_process("--enable-cron", "--cron-graceful-restart-period", "172800", "--shutdown-timeout", "2")
    wait_until { performed.size == 2 }
    _(stop_process(process)).must_equal ""
    _(jobs.map { |row| row.fetch("cron_at") }).must_equal [midnight - 86400, midnight, future]
    _(jobs.map { |row| row.fetch("state") }).must_equal ["succeeded", "succeeded", "ready"]
  end

  it "includes a blocked cron enqueue in the CLI shutdown deadline" do
    @environment["DECENT_JOB_CRON_SPEC_BLOCK"] = "true"
    process = start_process("--enable-cron", "--shutdown-timeout", "0.05")
    wait_until { File.exist?(@entered) }
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    _(stop_process(process, status: 1)).must_include "shutdown timed out"
    _(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).must_be :<, 3
  end
end
