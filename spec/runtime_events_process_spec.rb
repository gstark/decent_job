# frozen_string_literal: true

require "fileutils"
require "open3"
require "spec_helper"
require "support/runtime_events_cli_app"
require "timeout"
require "tmpdir"

describe "worker runtime events across CLI processes" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @directory = Dir.mktmpdir("dj-runtime-events-", "/tmp")
    @events_path = File.join(@directory, "events")
    @application_name = "runtime-events-#{@schema_name}"
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    RuntimeEventsProcessJob.queue_adapter = @adapter
  end

  after do
    @adapter.shutdown
    FileUtils.remove_entry(@directory)
  end

  def events(name = nil)
    return [] unless File.exist?(@events_path)

    rows = File.readlines(@events_path).filter_map do |line|
      JSON.parse(line)
    rescue JSON::ParserError
      nil
    end

    name ? rows.select { |row| row.fetch("name") == name } : rows
  end

  def await
    Timeout.timeout(10) { sleep 0.01 until yield }
  end

  def notify(payload)
    @admin.exec_params("SELECT pg_notify('decent_job', $1)", [payload])
  end

  def run_worker(poll_interval: "0.05")
    environment = {"RUNTIME_EVENTS_DATABASE" => JSON.generate(@connection_options.merge(application_name: @application_name)),
                   "RUNTIME_EVENTS_LOG" => @events_path, "DECENT_JOB_EXECUTION_MODE" => "external",
                   "DECENT_JOB_ENABLE_CRON" => "false", "DECENT_JOB_SUBPROCESSES" => "0",
                   "DECENT_JOB_DAEMONIZE" => "false", "DECENT_JOB_IDLE_TIMEOUT" => nil,
                   "NOTIFY_SOCKET" => nil, "WATCHDOG_USEC" => nil}
    command = [RbConfig.ruby, "-Ilib", "exe/decent_job", "start", "--require", "spec/support/runtime_events_cli_app.rb",
      "--queues", "events:1", "--poll-interval", poll_interval, "--shutdown-timeout", "3"]

    Open3.popen3(environment, *command, pgroup: true) do |input, output, error, process|
      input.close
      stdout = Thread.new { output.read }
      stderr = Thread.new { error.read }

      await do
        raise "Worker stopped: #{stderr.value}\n#{stdout.value}" unless process.alive?

        !events("notifier_listen.decent_job").empty? && !events("finished_job_task.decent_job").empty?
      end

      yield process.pid
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

  it "reports completed and empty tasks, polling, raw messages, reconnect and clean unsubscription" do
    run_worker do |pid|
      jobs = %w[success fail].map { |action| RuntimeEventsProcessJob.set(queue: "events").perform_later(action) }
      await { events("finished_job_task.decent_job").count { |event| event["job_id"] } == 2 }
      completed = events("finished_job_task.decent_job").select { |event| event["job_id"] }
      _(completed.map { |event| event.fetch("job_id") }.sort).must_equal jobs.map(&:provider_job_id).sort
      _(completed.map { |event| event.fetch("state") }.sort).must_equal %w[discarded succeeded]
      _(completed.map { |event| event.fetch("error") }.uniq).must_equal [nil]
      _(completed.map { |event| event.fetch("pid") }.uniq).must_equal [pid]
      _(completed.all? { |event| event.fetch("scheduler") && event.fetch("time_class") == "Time" && !event.fetch("thread").empty? }).must_equal true
      _(completed.map { |event| event.values_at("queues", "pool_index") }.uniq).must_equal [["events", 0]]
      rows = events
      callback = rows.index { |row| row.fetch("name") == "error_callback" && row.fetch("message") == "runtime worker failure" }
      failed_task = rows.index { |row| row["unhandled_error"] == "RuntimeError" }
      _(callback).wont_be_nil
      _(callback).must_be :<, failed_task
      _(events("finished_job_task.decent_job").any? { |row| row["job_id"].nil? && row.fetch("has_result") && row.fetch("error").nil? }).must_equal true
      await { !events("finished_timer_task.decent_job").empty? }
      tick = events("finished_timer_task.decent_job").first
      _(tick.values_at("pid", "queues", "pool_index", "time_class", "error")).must_equal [pid, "events", 0, "Time", nil]

      notify("invalid runtime message")
      await { events("error_callback").any? { |row| row.fetch("error") == "JSON::ParserError" } }
      _(events("notifier_notified.decent_job").map { |row| row.fetch("raw") }).must_include "invalid runtime message"
      _(events("notifier_notify_error.decent_job")).must_be_empty
      _(events("notifier_listen.decent_job").size).must_equal 1
      listener = @admin.exec_params("SELECT pid FROM pg_stat_activity WHERE application_name = $1 AND query = 'LISTEN decent_job'", [@application_name]).first
      _(listener).wont_be_nil
      @admin.exec_params("SELECT pg_terminate_backend($1)", [listener.fetch("pid")])
      await { events("notifier_listen.decent_job").size == 2 }
      _(events("notifier_notify_error.decent_job").any? { |row| row.fetch("error").start_with?("PG::") }).must_equal true
    end

    _(events("notifier_unlisten.decent_job").size).must_equal 1
    _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
  end

  it "keeps disabled polling silent while notifications still execute work" do
    run_worker(poll_interval: "-1") do
      job = RuntimeEventsProcessJob.set(queue: "events").perform_later("notified")
      await { events("finished_job_task.decent_job").any? { |row| row["job_id"] == job.provider_job_id } }
      notify("")
      await { events("notifier_notified.decent_job").any? { |row| row["raw"].nil? || row["raw"] == "" } }
      _(events("finished_timer_task.decent_job")).must_be_empty
    end

    _(events("notifier_unlisten.decent_job").size).must_equal 1
    _(events("error_callback")).must_be_empty
  end
end
