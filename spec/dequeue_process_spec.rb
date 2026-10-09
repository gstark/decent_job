# frozen_string_literal: true

require "fileutils"
require "open3"
require "spec_helper"
require "support/dequeue_cli_app"
require "timeout"
require "tmpdir"

describe "dequeue policy across CLI processes" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @directory = Dir.mktmpdir("dj-dequeue-", "/tmp")
    @log = File.join(@directory, "events")
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    DequeueProcessJob.queue_adapter = @adapter
    @store = DecentJob::Store.new(database)
  end

  after do
    @adapter.shutdown
    FileUtils.remove_entry(@directory)
  end

  def records(type = nil)
    return [] unless File.exist?(@log)
    rows = File.readlines(@log).filter_map do |line|
      JSON.parse(line)
    rescue JSON::ParserError
      nil
    end
    type ? rows.select { |row| row.fetch("type") == type } : rows
  end

  def await
    Timeout.timeout(10) { sleep 0.01 until yield }
  end

  def run_worker(*options, sort_env: nil, app_sort: nil, app_limit: nil)
    environment = {"DEQUEUE_PROCESS_DATABASE" => JSON.generate(@connection_options), "DEQUEUE_PROCESS_LOG" => @log,
                   "DEQUEUE_APP_SORT" => app_sort, "DEQUEUE_APP_LIMIT" => app_limit,
                   "DECENT_JOB_DEQUEUE_QUERY_SORT" => sort_env, "DECENT_JOB_QUEUE_SELECT_LIMIT" => nil,
                   "DECENT_JOB_EXECUTION_MODE" => "external", "DECENT_JOB_ENABLE_CRON" => "false",
                   "DECENT_JOB_SUBPROCESSES" => "0", "DECENT_JOB_DAEMONIZE" => "false",
                   "DECENT_JOB_IDLE_TIMEOUT" => nil, "NOTIFY_SOCKET" => nil, "WATCHDOG_USEC" => nil}
    command = [RbConfig.ruby, "-Ilib", "exe/decent_job", "start", "--require", "spec/support/dequeue_cli_app.rb",
      "--max-threads", "1", "--poll-interval", "0.02", "--shutdown-timeout", "3", *options]
    Open3.popen3(environment, *command, pgroup: true) do |input, output, error, process|
      input.close
      stdout = Thread.new { output.read }
      stderr = Thread.new { error.read }
      await do
        raise "Worker stopped: #{stderr.value}\n#{stdout.value}" unless process.alive?
        records.any?
      end
      yield
      Process.kill("TERM", process.pid)
      status = Timeout.timeout(6) { process.value }
      _(status.exitstatus).must_equal 0, "#{stderr.value}\n#{stdout.value}"
      _(records("error")).must_be_empty
    ensure
      if process&.alive?
        Process.kill("KILL", -process.pid)
        process.join
      end
      stdout&.join
      stderr&.join
    end
  end

  %w[created_at scheduled_at].each do |sort|
    it "executes due jobs in #{sort} order and keeps future jobs ineligible" do
      first = DequeueProcessJob.perform_later("created first")
      second = DequeueProcessJob.perform_later("scheduled first")
      future = DequeueProcessJob.set(wait: 3600, priority: -100).perform_later("future")
      database.with_connection do |connection|
        connection.exec_params("UPDATE decent_jobs SET created_at = clock_timestamp() - interval '2 hours', scheduled_at = clock_timestamp() - interval '1 hour' WHERE id = $1", [first.provider_job_id])
        connection.exec_params("UPDATE decent_jobs SET created_at = clock_timestamp() - interval '1 hour', scheduled_at = clock_timestamp() - interval '2 hours' WHERE id = $1", [second.provider_job_id])
      end
      options = (sort == "scheduled_at") ? ["--dequeue-query-sort", sort] : []
      host_options = (sort == "scheduled_at") ? {sort_env: "created_at", app_sort: "created_at"} : {}
      run_worker(*options, **host_options) do
        await { records("performed").size == 2 }
        expected = ["created first", "scheduled first"]
        expected.reverse! if sort == "scheduled_at"
        _(records("performed").map { |row| row.fetch("value") }).must_equal expected
        _(@store.find(future.provider_job_id).fetch("state")).must_equal "ready"
      end
    end
  end

  [1, 2].each do |limit|
    it "bounds advisory candidates at #{limit} and resumes after a competing owner unlocks" do
      first = DequeueProcessJob.set(priority: -1).perform_later("locked")
      DequeueProcessJob.perform_later("second")
      DequeueProcessJob.perform_later("third")
      owner = PG.connect(@connection_options)
      key = owner.exec_params("SELECT hashtextextended(current_schema() || '.decent_jobs.' || $1::text, 0)", [first.provider_job_id]).getvalue(0, 0)
      owner.exec_params("SELECT pg_advisory_lock($1::bigint)", [key])
      run_worker("--queue-select-limit", limit.to_s, app_limit: "0") do
        await { records("empty").any? }
        expected = (limit == 1) ? [] : %w[second third]
        _(records("performed").map { |row| row.fetch("value") }).must_equal expected
        _(@store.find(first.provider_job_id).fetch("state")).must_equal "ready"
        owner.exec_params("SELECT pg_advisory_unlock($1::bigint)", [key])
        await { records("performed").size == 3 }
        expected = (limit == 1) ? %w[locked second third] : %w[second third locked]
        _(records("performed").map { |row| row.fetch("value") }).must_equal expected
      end
    ensure
      owner&.finish
    end
  end
end
