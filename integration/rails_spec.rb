# frozen_string_literal: true

require "fileutils"
require "open3"
require "spec_helper"
require "timeout"
require "tmpdir"

class RailsHostJob < ActiveJob::Base; end
class RailsCliHostJob < ActiveJob::Base; end

describe "Rails 8.1 host integration" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @directory = Dir.mktmpdir("dj-rails-", "/tmp")
  end

  after { FileUtils.remove_entry(@directory) }

  def run_host(scenario, mode: "external", active_record: false, server: false, backlog: nil, environment: "test", early_load: false, expect_work: false, isolation: "thread")
    result_path = File.join(@directory, "performed") if backlog || expect_work
    DecentJob::Store.new(database).enqueue_all([RailsHostJob.new(backlog)]) if backlog
    environment = {"HOST_CONNECTION" => JSON.generate(@connection_options), "HOST_ROOT" => @directory,
                   "HOST_LOAD_EARLY" => early_load.to_s, "HOST_SCENARIO" => scenario, "HOST_MODE" => mode, "HOST_ACTIVE_RECORD" => active_record.to_s,
                   "HOST_SERVER" => server.to_s, "HOST_RESULT" => result_path, "RAILS_ENV" => environment, "HOST_ISOLATION" => isolation,
                   "DECENT_JOB_EXECUTION_MODE" => nil, "DECENT_JOB_ENABLE_CRON" => "false"}
    Open3.popen3(environment, RbConfig.ruby, "-Ilib", "integration/support/rails_host.rb", pgroup: true) do |input, output, error, process|
      input.close
      stdout = Thread.new { output.read }
      stderr = Thread.new { error.read }
      status = Timeout.timeout(20) { process.value }
      details = stderr.value
      _(status.exitstatus).must_equal 0, "Host failed: #{details}\n#{stdout.value}"
      line = stdout.value.lines.find { |value| value.start_with?("HOST_RESULT:") }
      _(line).wont_be_nil "Host did not report results: #{details}"
      JSON.parse(line.delete_prefix("HOST_RESULT:"))
    ensure
      if process&.alive?
        Process.kill("KILL", -process.pid)
        process.join
      end
      stdout&.join
      stderr&.join
    end
  end

  it "boots without Active Record, adopts the Rails logger, and keeps console async dormant" do
    result = run_host("boot", mode: "async")
    _(result.fetch("active_record_loaded")).must_equal false
    _(result.fetch("logger")).must_equal true
    _(result.fetch("async")).must_equal false
    _(result.fetch("started")).must_equal false
    _(result.fetch("events")).must_include ["early", false, false]
  end

  it "uses Rails environment defaults when no execution mode was configured" do
    {"test" => "inline", "development" => "async", "production" => "external"}.each do |environment, mode|
      result = run_host("boot", mode: nil, environment: environment)
      _(result.fetch("mode")).must_equal mode
      _(result.fetch("started")).must_equal false
    end
  end

  it "preserves an explicitly configured logger" do
    result = run_host("custom_logger")
    _(result.fetch("logger")).must_equal false
    _(result.fetch("custom_logger")).must_equal true
  end

  it "starts an early adapter after host boot and executes existing jobs in a server" do
    result = run_host("boot", mode: "async_server", server: true, backlog: "server backlog")
    _(result.fetch("events")).must_include ["early", false, false]
    _(result.fetch("performed")).must_equal "server backlog"
    _(result.fetch("stopped")).must_equal true
  end

  it "starts async_all without Active Record outside a web server" do
    result = run_host("boot", mode: "async_all", backlog: "all backlog")
    _(result.fetch("performed")).must_equal "all backlog"
    _(result.fetch("active_record_loaded")).must_equal false
  end

  it "loads configured cron jobs after Rails autoloaders are ready" do
    result = run_host("cron_boot", mode: "async_all", expect_work: true)
    _(result.fetch("performed")).must_equal "cron"
    _(result.fetch("stopped")).must_equal true
  end

  it "supports explicitly loading the Railtie after the standalone library" do
    result = run_host("boot", mode: "async_all", early_load: true, backlog: "late host")
    _(result.fetch("performed")).must_equal "late host"
    _(result.fetch("events")).must_include ["early", false, false]
  end

  it "keeps Rails async adapters dormant throughout CLI execution" do
    result_path = File.join(@directory, "cli-result")
    boot_path = File.join(@directory, "boot-result")
    DecentJob::Store.new(database).enqueue_all([RailsCliHostJob.new])
    environment = {"HOST_CONNECTION" => JSON.generate(@connection_options), "HOST_ROOT" => @directory,
                   "HOST_RESULT" => result_path, "HOST_BOOT_RESULT" => boot_path, "RAILS_ENV" => "test"}
    command = [RbConfig.ruby, "-Ilib", "exe/decent_job", "start", "--require", "integration/support/rails_cli_app.rb",
      "--max-threads", "1", "--shutdown-timeout", "3"]
    Open3.popen3(environment, *command, pgroup: true) do |input, output, error, process|
      input.close
      stdout = Thread.new { output.read }
      stderr = Thread.new { error.read }
      Timeout.timeout(12) do
        sleep 0.01 until File.exist?(result_path) || !process.alive?
      end
      _(File.exist?(result_path)).must_equal true, "CLI stopped early: #{stderr.value unless process.alive?}"
      result = JSON.parse(File.read(result_path))
      _(JSON.parse(File.read(boot_path))).must_equal false
      _(result.fetch("max_threads")).must_equal 1
      _(result.fetch("processes")).must_equal 1
      Process.kill("TERM", process.pid)
      status = Timeout.timeout(8) { process.value }
      _(status.exitstatus).must_equal 0, stderr.value
    ensure
      if process&.alive?
        Process.kill("KILL", -process.pid)
        process.join
      end
      stdout&.join
      stderr&.join
    end
  end

  it "defers enqueue to Active Record commit and drops rolled back enqueues" do
    result = run_host("transactions", active_record: true)
    _(result.fetch("commit_before_id")).must_be_nil
    _(result.fetch("commit_before_count")).must_equal 0
    _(result.fetch("commit_after")).must_equal ["commit"]
    _(result.fetch("rollback_id")).must_be_nil
    _(result.fetch("count")).must_equal 1
  end

  it "waits for the outer commit and drops jobs from rolled back savepoints" do
    result = run_host("nested", active_record: true)
    _(result.fetch("before_ids")).must_equal [nil, nil, nil]
    _(result.fetch("after_arguments")).must_equal [["outer"], ["inner"]]
    _(result.fetch("discarded_id")).must_be_nil
  end

  it "honors each bulk job's transaction deferral setting" do
    result = run_host("bulk", active_record: true)
    _(result.fetch("deferred_before")).must_be_nil
    _(result.fetch("immediate_before")).must_equal true
    _(result.fetch("deferred_after")).must_equal ["deferred"]
    _(result.fetch("rollback_id")).must_be_nil
  end

  it "retains scheduled enqueue options across the transaction boundary" do
    result = run_host("scheduled", active_record: true)
    _(result.fetch("before_id")).must_be_nil
    _(result.fetch("schedule_delta")).must_be_close_to 0, 0.001
    _(result.fetch("queue")).must_equal "scheduled"
    _(result.fetch("priority")).must_equal 9
  end

  it "runs inline work inside the host executor after commit" do
    result = run_host("inline", mode: "inline", active_record: true)
    _(result.fetch("before_perform")).must_equal false
    _(result.fetch("after_perform")).must_equal [["perform", "after commit", true]]
    _(result.fetch("events").map(&:first)).must_include "executor_complete"
  end

  it "reloads changed job classes before the next execution" do
    result = run_host("reload")
    _(result.fetch("versions")).must_equal [["version", 1, true], ["version", 2, true]]
  end

  it "isolates fiber captures and transactions and wraps heartbeat callbacks in the host executor" do
    result = run_host("runtime_isolation", isolation: "fiber")
    _(result.fetch("isolation")).must_equal "fiber"
    _(result.fetch("backend_pids").uniq.size).must_equal 2
    _(result.fetch("captured_arguments")).must_equal [["captured"]]
    _(result.fetch("committed_arguments")).must_equal ["independent"]
    _(result.fetch("callbacks")).must_equal ["second_commit", "first_rollback"]
    _(result.fetch("job_count")).must_equal 1
    _(result.fetch("bulk_context")).must_be_nil
    _(result.fetch("monitor_executor")).must_equal true
    _(result.fetch("pool_active")).must_equal 0
    _(result.fetch("process_count")).must_equal 0
  end
end
