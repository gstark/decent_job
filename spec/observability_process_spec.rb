# frozen_string_literal: true

require "fileutils"
require "open3"
require "spec_helper"
require "support/observability_cli_app"
require "timeout"
require "tmpdir"

describe "worker observability through the CLI" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @directory = Dir.mktmpdir("dj-observability-", "/tmp")
    @started = File.join(@directory, "started.json")
    @stopping = File.join(@directory, "stopping.json")
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    ObservabilityProcessJob.queue_adapter = @adapter
    @environment = {"OBSERVABILITY_DATABASE" => JSON.generate(@connection_options),
                    "OBSERVABILITY_STARTED" => @started, "OBSERVABILITY_STOPPING" => @stopping,
                    "DECENT_JOB_EXECUTION_MODE" => "external", "DECENT_JOB_ENABLE_CRON" => "false",
                    "DECENT_JOB_SUBPROCESSES" => "0", "DECENT_JOB_DAEMONIZE" => "false",
                    "DECENT_JOB_IDLE_TIMEOUT" => nil, "DECENT_JOB_PROBE_PORT" => nil,
                    "NOTIFY_SOCKET" => nil, "WATCHDOG_USEC" => nil}
  end

  after do
    @adapter.shutdown(timeout: 5)
    FileUtils.remove_entry(@directory)
  end

  def await
    Timeout.timeout(8) { sleep 0.01 until yield }
  end

  def snapshot(path)
    JSON.parse(File.read(path)) if File.exist?(path)
  rescue JSON::ParserError
    nil
  end

  def run_worker(priority_option)
    command = [RbConfig.ruby, "-Ilib", "exe/decent_job", "start", "--require", "spec/support/observability_cli_app.rb",
      "--poll-interval", "0.05", "--shutdown-timeout", "3", "--no-listen-notify", priority_option]
    Open3.popen3(@environment, *command, pgroup: true) do |input, output, error, process|
      input.close
      stdout = Thread.new { output.read }
      stderr = Thread.new { error.read }
      await do
        raise "Worker stopped: #{stderr.value}\n#{stdout.value}" unless process.alive?
        snapshot(@started)
      end
      yield
      Process.kill("TERM", process.pid)
      status = Timeout.timeout(6) { process.value }
      _(status.exitstatus).must_equal 0, "#{stderr.value}\n#{stdout.value}"
    ensure
      if process&.alive?
        Process.kill("KILL", -process.pid)
        process.join
      end
      stdout&.join
      stderr&.join
    end
  end

  it "publishes live execution activity and exposes graceful shutdown to a lowered-priority job" do
    job = ObservabilityProcessJob.perform_later
    run_worker("--lower-thread-priority") do
      started = snapshot(@started)
      _(started.fetch("priority")).must_equal(-3)
      _(started.fetch("running")).must_equal true
      _(started.fetch("shutting_down")).must_equal false
      stats = started.fetch("stats")
      _(stats.fetch("active_execution_thread_count")).must_equal 1
      _(stats.fetch("total_executions_count")).must_equal 0
      _(stats.fetch("execution_at")).wont_be_nil
      _(stats.fetch("check_queue_at")).wont_be_nil
      await do
        DecentJob::ProcessRecord.all(database: database).any? do |record|
          record.state["active_execution_thread_count"] == 1 && record.state["execution_at"]
        end
      end
    end
    stopped = snapshot(@stopping)
    _(stopped.fetch("running")).must_equal false
    _(stopped.fetch("shutting_down")).must_equal true
    _(DecentJob::Job.find(job.provider_job_id, database: database).succeeded?).must_equal true
    _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
  end

  it "allows the CLI to turn off application-configured lowered priority" do
    ObservabilityProcessJob.perform_later
    run_worker("--no-lower-thread-priority") do
      _(snapshot(@started).fetch("priority")).must_equal 0
    end
    _(snapshot(@stopping).fetch("shutting_down")).must_equal true
  end
end
