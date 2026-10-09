# frozen_string_literal: true

require "fileutils"
require "net/http"
require "open3"
require "socket"
require "spec_helper"
require "support/scheduler_cli_app"
require "timeout"
require "tmpdir"

describe "scheduled execution across worker processes" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @directory = Dir.mktmpdir("dj-scheduler-", "/tmp")
    @started = File.join(@directory, "started")
    @finished = File.join(@directory, "finished")
    @errors = File.join(@directory, "errors")
    @application_name = "scheduler-#{@schema_name}"
    @environment = {"SCHEDULER_DATABASE" => JSON.generate(@connection_options.merge(application_name: @application_name)),
                    "SCHEDULER_STARTED" => @started, "SCHEDULER_FINISHED" => @finished, "SCHEDULER_ERRORS" => @errors,
                    "DECENT_JOB_EXECUTION_MODE" => "external", "DECENT_JOB_ENABLE_CRON" => "false",
                    "DECENT_JOB_SUBPROCESSES" => "0", "DECENT_JOB_DAEMONIZE" => "false",
                    "DECENT_JOB_IDLE_TIMEOUT" => nil, "NOTIFY_SOCKET" => nil, "WATCHDOG_USEC" => nil}
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    SchedulerProcessJob.queue_adapter = @adapter
    @port = TCPServer.open("127.0.0.1", 0) { |server| server.addr[1] }
  end

  after do
    @adapter.shutdown
    FileUtils.remove_entry(@directory)
  end

  def await(seconds: 10)
    Timeout.timeout(seconds) { sleep 0.01 until yield }
  end

  def events(path)
    return [] unless File.exist?(path)
    File.readlines(path).filter_map do |line|
      JSON.parse(line)
    rescue JSON::ParserError
      nil
    end
  end

  def scheduler_stats
    DecentJob::ProcessRecord.all(database: database).first&.state&.fetch("schedulers", []) || []
  end

  def enqueue(value, at: nil, queue: "default", notify: true, release: nil)
    SchedulerProcessJob.set(wait_until: at, queue: queue, decent_job_notify: notify).perform_later(value, release: release)
  end

  def run_worker(*options)
    command = [RbConfig.ruby, "-Ilib", "exe/decent_job", "start", "--require", "spec/support/scheduler_cli_app.rb",
      "--poll-interval", "-1", "--max-cache", "2", "--shutdown-timeout", "3", "--probe-port", @port.to_s, *options]
    Open3.popen3(@environment, *command, pgroup: true) do |input, output, error, process|
      input.close
      stdout = Thread.new { output.read }
      stderr = Thread.new { error.read }
      await do
        raise "Worker stopped: #{stderr.value}\n#{stdout.value}" unless process.alive?
        begin
          Net::HTTP.start("127.0.0.1", @port, nil, open_timeout: 0.2, read_timeout: 0.2) do |http|
            http.get("/status/connected").code == "200"
          end
        rescue Errno::ECONNREFUSED, EOFError, Net::OpenTimeout, Net::ReadTimeout
          false
        end
      end
      yield process
      Process.kill("TERM", process.pid)
      status = Timeout.timeout(6) { process.value }
      details = File.exist?(@errors) ? File.read(@errors) : ""
      _(status.exitstatus).must_equal 0, "#{stderr.value}\n#{stdout.value}\n#{details}"
    ensure
      if process&.alive?
        Process.kill("KILL", -process.pid)
        process.join
      end
      stdout&.join
      stderr&.join
    end
  end

  it "warms and refills bounded future work with polling and listening disabled" do
    at = Time.now + 2
    times = [at, at + 0.35, at + 0.7]
    times.each_with_index { |time, index| enqueue("future #{index}", at: time, notify: false) }
    run_worker("--no-listen-notify") do
      await { scheduler_stats.any? { |stats| stats.fetch("active_cache", 0).positive? } }
      stats = scheduler_stats.first
      _(stats.fetch("max_cache")).must_equal 2
      _(stats.fetch("active_cache")).must_be :<=, 2
      await { events(@finished).size == 3 }
      events(@started).each do |event|
        index = event.fetch("value").split.last.to_i
        _(event.fetch("at")).must_be :>=, times.fetch(index).to_f - 0.01
      end
    end
    _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
  end

  it "interrupts a later deadline when an earlier future notification arrives" do
    run_worker do
      enqueue("later", at: Time.now + 60)
      await { scheduler_stats.any? { |stats| stats.fetch("active_cache", 0).positive? } }
      at = Time.now + 0.5
      enqueue("earlier", at: at)
      await { events(@finished).any? { |event| event.fetch("value") == "earlier" } }
      _(events(@started).map { |event| event.fetch("value") }).must_equal ["earlier"]
      _(events(@started).first.fetch("at")).must_be :>=, at.to_f - 0.01
    end
  end

  it "runs notified future work with zero cache and disabled polling" do
    run_worker("--max-cache", "0") do
      times = [Time.now + 0.6, Time.now + 1]
      times.each_with_index { |time, index| enqueue("zero #{index}", at: time) }
      await { events(@finished).size == 2 }
      events(@started).each do |event|
        index = event.fetch("value").split.last.to_i
        _(event.fetch("at")).must_be :>=, times.fetch(index).to_f - 0.01
      end
      _(scheduler_stats.first.fetch("max_cache")).must_equal 0
    end
  end

  it "rechecks persisted state when cached jobs are rescheduled or removed" do
    run_worker do
      original = Time.now + 1
      moved = enqueue("moved", at: original)
      removed = enqueue("removed", at: original)
      await { scheduler_stats.any? { |stats| stats.fetch("active_cache", 0).positive? } }
      later = Time.now + 2
      DecentJob::Job.find(moved.provider_job_id, database: database).reschedule_job(later)
      database.with_connection do |connection|
        connection.exec_params("DELETE FROM decent_jobs WHERE id = $1", [removed.provider_job_id])
      end
      await { Time.now > original + 0.15 }
      _(events(@started)).must_be_empty
      await { events(@finished).size == 1 }
      _(events(@started).map { |event| event.fetch("value") }).must_equal ["moved"]
      _(events(@started).first.fetch("at")).must_be :>=, later.to_f - 0.01
    end
  end

  it "wakes enough matching threads for a bulk count without polling another pool" do
    run_worker("--queues", "urgent:2;ordinary:1") do
      await { scheduler_stats.size == 2 && scheduler_stats.all? { |stats| stats.fetch("empty_executions_count") >= stats.fetch("max_threads") } }
      sleep 0.1
      baseline = scheduler_stats.find { |stats| stats.fetch("queues") == "ordinary" }.fetch("empty_executions_count")
      release = File.join(@directory, "release")
      jobs = 2.times.map do |index|
        SchedulerProcessJob.new("bulk #{index}", release: release).tap { |job| job.queue_name = "urgent" }
      end
      @adapter.enqueue_all(jobs)
      await { events(@started).size == 2 }
      await { scheduler_stats.find { |stats| stats.fetch("queues") == "urgent" }.fetch("active_threads") == 2 }
      ordinary = scheduler_stats.find { |stats| stats.fetch("queues") == "ordinary" }
      _(ordinary.fetch("empty_executions_count")).must_equal baseline
      File.write(release, "finish")
      await { events(@finished).size == 2 }
    end
  end

  it "warms missed future work after its notification connection reconnects" do
    run_worker do
      listener_pid = nil
      await do
        listener_pid = @admin.exec_params("SELECT pid FROM pg_stat_activity WHERE application_name = $1 AND query = 'LISTEN decent_job'", [@application_name]).first&.fetch("pid")
      end
      @admin.exec_params("SELECT pg_terminate_backend($1)", [listener_pid])
      at = Time.now + 0.7
      enqueue("reconnected", at: at, notify: false)
      await { events(@finished).any? { |event| event.fetch("value") == "reconnected" } }
      _(events(@started).first.fetch("at")).must_be :>=, at.to_f - 0.01
      _(File.read(@errors)).must_include "PG::"
    end
  end
end
