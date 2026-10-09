# frozen_string_literal: true

require "minitest/mock"
require "open3"
require "spec_helper"
require "stringio"
require "support/cli_app"
require "timeout"
require "tmpdir"
require "decent_job/cli"

describe DecentJob::CLI do
  before do
    @stdout = StringIO.new
    @stderr = StringIO.new
    @cli = DecentJob::CLI.new(stdout: @stdout, stderr: @stderr)
  end

  after do
    DecentJob.reset!
  end

  it "prints help without connecting to the database" do
    _(@cli.run(["help"])).must_equal 0
    _(@stdout.string).must_include "Usage: decent_job"
    _(@stdout.string).must_include "--require"
    _(@stdout.string).must_include "--shutdown-timeout"
    _(@stderr.string).must_equal ""
  end

  it "accepts the help option" do
    _(@cli.run(["--help"])).must_equal 0
    _(@stdout.string).must_include "migrate"
  end

  it "accepts help after an explicit command" do
    _(@cli.run(["start", "--help"])).must_equal 0
    _(@stdout.string).must_include "Usage: decent_job"
  end

  it "prints the gem version" do
    _(@cli.run(["version"])).must_equal 0
    _(@stdout.string).must_include DecentJob::VERSION
  end

  it "accepts the version option" do
    _(@cli.run(["--version"])).must_equal 0
    _(@stdout.string).must_include DecentJob::VERSION
  end

  it "reports invalid options without a backtrace" do
    _(@cli.run(["--unknown"])).must_equal 2
    _(@stderr.string).must_include "invalid option"
    _(@stderr.string.lines.length).must_equal 1
  end

  it "rejects unexpected commands and extra positional arguments" do
    _(@cli.run(["destroy"])).must_equal 2
    _(@cli.run(["start", "extra"])).must_equal 2
  end

  it "rejects invalid configuration before opening a database" do
    _(@cli.run(["--max-threads", "0"])).must_equal 2
    _(@stderr.string).must_include "positive integer"
  end

  it "reports an application file that cannot be loaded" do
    _(@cli.run(["--require", "/no/such/decent_job_app.rb"])).must_equal 1
    _(@stderr.string).must_include "cannot load"
  end

  it "restores caller signal handlers even if startup and shutdown fail" do
    handler = proc {}
    previous = Signal.trap("TERM", handler)
    DecentJob.stub(:start, -> { raise "startup failed" }) do
      DecentJob.stub(:shutdown, ->(**) { raise "shutdown failed" }) do
        _(@cli.run(["start"])).must_equal 1
      end
    end
    _(Signal.trap("TERM", previous)).must_equal handler
  ensure
    Signal.trap("TERM", previous)
  end
end

describe "decent_job executable" do
  include DatabaseSpec

  before do
    @directory = Dir.mktmpdir("decent_job_cli")
    @marker = File.join(@directory, "performed")
    @environment = {"DECENT_JOB_CLI_SPEC_CONNECTION" => JSON.generate(@connection_options)}
    @command = [RbConfig.ruby, "-Ilib", "exe/decent_job", "--require", File.expand_path("support/cli_app.rb", __dir__)]
    @processes = []
  end

  after do
    @processes.each do |process|
      if process.alive?
        Process.kill("KILL", process.pid)
        process.join
      end
    end
    FileUtils.remove_entry(@directory)
  end

  def run_worker(*arguments)
    Open3.popen3(@environment, *@command, *arguments) do |input, output, error, process|
      @processes << process
      input.close
      yield process, output, error
    ensure
      if process.alive?
        Process.kill("KILL", process.pid)
        process.join
      end
    end
  end

  def wait_for_file(path)
    Timeout.timeout(8) { sleep 0.01 until File.exist?(path) }
  end

  def enqueue_cli_job(duration = 0)
    DecentJob::Schema.new(database).migrate
    DecentJob::Store.new(database).enqueue_all([CliRecordingJob.new(@marker, duration)])
  end

  it "installs the schema and prints success after migration" do
    output, error, status = Open3.capture3(@environment, *@command, "migrate")
    _(status.exitstatus).must_equal 0
    _(output).must_include "schema installed"
    _(error).wont_include "Error:"
    database.with_connection do |connection|
      _(connection.exec("SELECT version FROM decent_job_schema_migrations ORDER BY version").column_values(0)).must_equal ["1", "2", "3", "4", "5"]
    end
  end

  it "defaults to start, loads jobs, and applies CLI settings after application configuration" do
    enqueue_cli_job
    run_worker("--queues", "default", "--max-threads", "1", "--poll-interval", "0.02", "--shutdown-timeout", "2", "--no-listen-notify") do |process, output, error|
      wait_for_file("#{@marker}.finished")
      Process.kill("TERM", process.pid)
      _(process.join(5)).wont_be_nil
      _(process.value.exitstatus).must_equal 0
      settings = JSON.parse(File.read(@marker))
      _(settings).must_equal({"queues" => "default", "max_threads" => 1,
        "poll_interval" => 0.02, "shutdown_timeout" => 2.0, "listen_notify" => false})
      _(error.read).must_equal ""
      _(output.read).must_include "started"
    end
  end

  it "drains a running job after SIGINT" do
    enqueue_cli_job(0.3)
    run_worker("start", "--queues", "*", "--poll-interval", "0.02", "--shutdown-timeout", "2", "--listen-notify") do |process, _output, error|
      wait_for_file(@marker)
      Process.kill("INT", process.pid)
      _(process.join(5)).wont_be_nil
      _(process.value.exitstatus).must_equal 0
      _(File.exist?("#{@marker}.finished")).must_equal true
      _(error.read).must_equal ""
    end
  end

  it "exits nonzero within the shutdown deadline when a job does not finish" do
    enqueue_cli_job(60)
    run_worker("start", "--queues", "*", "--shutdown-timeout", "0.05", "--no-listen-notify") do |process, _output, error|
      wait_for_file(@marker)
      Process.kill("TERM", process.pid)
      _(process.join(3)).wont_be_nil
      _(process.value.exitstatus).must_equal 1
      _(File.exist?("#{@marker}.finished")).must_equal false
      _(error.read).must_include "shutdown timed out"
    end
  end
end
