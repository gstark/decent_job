# frozen_string_literal: true

require "fileutils"
require "open3"
require "spec_helper"
require "timeout"
require "tmpdir"

describe "notification controls across processes" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @directory = Dir.mktmpdir("dj-notify-", "/tmp")
    @result_file = File.join(@directory, "results")
    @environment = {"NOTIFICATION_DATABASE" => JSON.generate(@connection_options),
                    "NOTIFICATION_RESULTS" => @result_file, "DECENT_JOB_EXECUTION_MODE" => "external",
                    "DECENT_JOB_ENABLE_CRON" => "false", "DECENT_JOB_SUBPROCESSES" => "0",
                    "DECENT_JOB_DAEMONIZE" => "false", "DECENT_JOB_IDLE_TIMEOUT" => nil,
                    "DECENT_JOB_PROBE_PORT" => nil, "NOTIFY_SOCKET" => nil}
    @listener = PG.connect(@connection_options)
    @listener.exec("LISTEN decent_job")
  end

  after do
    @listener.finish
    FileUtils.remove_entry(@directory)
  end

  def produce(jobs, enabled: true, action: "bulk")
    environment = @environment.merge("NOTIFICATION_JOBS" => JSON.generate(jobs),
      "NOTIFICATION_ENABLED" => enabled.to_s, "NOTIFICATION_ACTION" => action)
    output, error, status = Open3.capture3(environment, RbConfig.ruby, "-Ilib", "spec/support/notification_cli_app.rb")
    _(status.exitstatus).must_equal 0, error
    JSON.parse(output).fetch("ids")
  end

  def notification_message
    payload = nil
    @listener.wait_for_notify(1) { |_channel, _pid, value| payload = value }
    _(payload).wont_be_nil
    JSON.parse(payload)
  end

  def run_worker
    command = [RbConfig.ruby, "-Ilib", "exe/decent_job", "start", "--require", "spec/support/notification_cli_app.rb", "--shutdown-timeout", "3"]
    Open3.popen3(@environment, *command, pgroup: true) do |input, output, error, process|
      input.close
      stdout = Thread.new { output.read }
      stderr = Thread.new { error.read }
      yield
      Process.kill("TERM", process.pid)
      status = Timeout.timeout(8) { process.value }
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

  def await
    Timeout.timeout(8) { sleep 0.01 until yield }
  end

  def results
    File.exist?(@result_file) ? File.readlines(@result_file, chomp: true) : []
  end

  it "honors producer-wide disabling while retaining accepted jobs" do
    ids = produce([{value: "single"}], enabled: false, action: "single")
    ids += produce([{value: "bulk one"}, {value: "bulk two"}], enabled: false)
    _(ids.compact.size).must_equal 3
    _(@listener.wait_for_notify(0.05)).must_be_nil
    _(ids.map { |id| DecentJob::Store.new(database).find(id).fetch("state") }).must_equal %w[ready ready ready]
  end

  it "notifies only enabled members but allows workers to execute every accepted job" do
    ids = produce([{value: "silent", notify: false}, {value: "published", notify: true}])
    payload = notification_message
    _(payload.fetch("queue_name")).must_equal "default"
    _(payload.fetch("count")).must_equal 1
    _(payload.values).wont_include "silent"
    _(@listener.wait_for_notify(0.05)).must_be_nil
    run_worker do
      await { results.size == 2 }
      _(results.sort).must_equal %w[published silent]
      await { ids.all? { |id| DecentJob::Store.new(database).find(id).fetch("state") == "succeeded" } }
    end
  end

  it "preserves notification suppression through retry in a separate worker" do
    id = produce([{value: "retried", notify: false, retry: true}], action: "single").first
    run_worker do
      await { DecentJob::Store.new(database).find(id).fetch("state") == "succeeded" }
      _(results).must_equal ["retried"]
      _(DecentJob::Store.new(database).executions(id).size).must_equal 2
      _(@listener.wait_for_notify(0.05)).must_be_nil
    end
  end

  it "does not publish or persist a rolled-back producer transaction" do
    produce([{value: "rollback"}], action: "rollback")
    _(@listener.wait_for_notify(0.05)).must_be_nil
    database.with_connection { |connection| _(connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)).must_equal "0" }
  end
end
