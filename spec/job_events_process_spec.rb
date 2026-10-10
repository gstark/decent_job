# frozen_string_literal: true

require "fileutils"
require "open3"
require "spec_helper"
require "support/job_events_cli_app"
require "timeout"
require "tmpdir"

describe "job events from a standalone CLI worker" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @directory = Dir.mktmpdir("dj-job-events-", "/tmp")
    @events_path = File.join(@directory, "events")
    @snapshots_path = File.join(@directory, "snapshots")
    @errors_path = File.join(@directory, "errors")
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    JobEventsProcessJob.queue_adapter = @adapter
  end

  after do
    @adapter.shutdown
    FileUtils.remove_entry(@directory)
  end

  def events
    return [] unless File.exist?(@events_path)

    File.readlines(@events_path).filter_map do |line|
      JSON.parse(line)
    rescue JSON::ParserError
      nil
    end
  end

  def run_worker(isolation)
    environment = {"JOB_EVENTS_DATABASE" => JSON.generate(@connection_options), "JOB_EVENTS_ISOLATION" => isolation,
                   "JOB_EVENTS_LOG" => @events_path, "JOB_EVENTS_SNAPSHOTS" => @snapshots_path, "JOB_EVENTS_ERRORS" => @errors_path,
                   "DECENT_JOB_EXECUTION_MODE" => "external", "DECENT_JOB_ENABLE_CRON" => "false",
                   "DECENT_JOB_SUBPROCESSES" => "0", "DECENT_JOB_DAEMONIZE" => "false",
                   "DECENT_JOB_IDLE_TIMEOUT" => nil, "NOTIFY_SOCKET" => nil, "WATCHDOG_USEC" => nil}
    command = [RbConfig.ruby, "-Ilib", "exe/decent_job", "start", "--require", "spec/support/job_events_cli_app.rb",
      "--max-threads", "1", "--poll-interval", "0.02", "--shutdown-timeout", "3"]

    Open3.popen3(environment, *command, pgroup: true) do |input, output, error, process|
      input.close
      stdout = Thread.new { output.read }
      stderr = Thread.new { error.read }

      Timeout.timeout(12) do
        loop do
          raise "Worker stopped: #{stderr.value}\n#{stdout.value}" unless process.alive?
          break if events.count { |event| event.fetch("name") == "perform_job.decent_job" } == 3 &&
            database.with_connection { |connection| connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0) == "0" }
          sleep 0.01
        end
      end

      yield process.pid
      Process.kill("TERM", process.pid)
      status = Timeout.timeout(6) { process.value }
      _(status.exitstatus).must_equal 0, "#{stderr.value}\n#{stdout.value}"
      _(File.exist?(@errors_path) ? File.read(@errors_path) : "").must_equal ""
    ensure
      if process&.alive?
        Process.kill("KILL", -process.pid)
        process.join
      end

      stdout&.join
      stderr&.join
    end
  end

  %w[thread fiber].each do |isolation|
    it "reports retries before persistence and keeps snapshots after deletion with #{isolation} isolation" do
      jobs = %w[success retry].map { |action| JobEventsProcessJob.perform_later(action) }

      run_worker(isolation) do |pid|
        performed = events.select { |event| event.fetch("name") == "perform_job.decent_job" }
        _(performed.map { |event| event.fetch("id") }.uniq.sort).must_equal jobs.map(&:provider_job_id).sort
        _(performed.map { |event| event.fetch("pid") }.uniq).must_equal [pid]
        _(performed.all? { |event| event.fetch("thread").is_a?(String) && !event.fetch("thread").empty? }).must_equal true
        _(performed.map { |event| event.fetch("persisted") }.uniq).must_equal [{"state" => "running", "finished_at" => nil}]
        retry_events = events.select { |event| event.fetch("action") == "retry" }
        _(retry_events.map { |event| event.fetch("name") }).must_equal ["enqueue_job.decent_job", "perform_job.decent_job", "perform_job.decent_job"]
        _(retry_events.first.fetch("accepted")).must_equal true
        _(retry_events[1].values_at("retried", "error", "error_event")).must_equal [true, "RuntimeError", "retried"]
        _(retry_events.last.values_at("retried", "error", "has_value", "value")).must_equal [false, nil, true, false]
        _(performed.map { |event| event.fetch("execution_id") }.uniq.size).must_equal 3
      end

      snapshots = JSON.parse(File.read(@snapshots_path))
      _(snapshots.size).must_equal 3
      _(snapshots.all? { |snapshot| snapshot.fetch("id") == snapshot.fetch("job_id") && snapshot.fetch("job_class") == "JobEventsProcessJob" && snapshot.fetch("state") == "running" }).must_equal true
      _(snapshots.select { |snapshot| snapshot.fetch("arguments") == ["retry"] }.map { |snapshot| snapshot.fetch("number") }).must_equal [1, 2]
      _(database.with_connection { |connection| connection.exec("SELECT count(*) FROM decent_job_executions").getvalue(0, 0) }).must_equal "0"
      _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
    end
  end
end
