# frozen_string_literal: true

require "net/http"
require "open3"
require "socket"
require "spec_helper"
require "support/daemon_idle_cli_app"
require "timeout"
require "tmpdir"

describe "daemon and idle CLI lifecycle" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @directory = Dir.mktmpdir("dj-daemon-", "/tmp")
    @pidfile = File.join(@directory, "worker.pid")
    @port = TCPServer.open("127.0.0.1", 0) { |server| server.addr[1] }
    @daemon_pids = []
    @environment = {
      "DECENT_JOB_IDLE_SPEC_CONNECTION" => JSON.generate(@connection_options),
      "DECENT_JOB_IDLE_SPEC_DIRECTORY" => @directory,
      "DECENT_JOB_PIDFILE" => @pidfile,
      "DECENT_JOB_DAEMONIZE" => "false",
      "DECENT_JOB_IDLE_TIMEOUT" => nil,
      "DECENT_JOB_SUBPROCESSES" => "0",
      "DECENT_JOB_PROBE_PORT" => @port.to_s,
      "NOTIFY_SOCKET" => nil,
      "WATCHDOG_USEC" => nil,
      "OBJC_DISABLE_INITIALIZE_FORK_SAFETY" => "YES"
    }
  end

  after do
    children = Dir.glob(File.join(@directory, "child-*")).map { |path| File.basename(path).delete_prefix("child-").to_i }
    (@daemon_pids + children).uniq.each do |pid|
      Process.kill("KILL", pid)
    rescue Errno::ESRCH
      nil
    end
    FileUtils.remove_entry(@directory)
  end

  def await
    Timeout.timeout(10) { sleep 0.01 until yield }
  end

  def response(path)
    Net::HTTP.start("127.0.0.1", @port, nil, open_timeout: 0.5, read_timeout: 1) { |http| http.get(path) }
  end

  def await_connected
    await do
      response("/status/connected").code == "200"
    rescue Errno::ECONNREFUSED
      false
    end
  end

  def launch(*arguments)
    command = [RbConfig.ruby, "-Ilib", "exe/decent_job", "start", "--require", File.expand_path("support/daemon_idle_cli_app.rb", __dir__), "--shutdown-timeout", "2", *arguments]
    Open3.popen3(@environment, *command, pgroup: true) do |input, output, error, process|
      input.close
      yield process, output, error
    ensure
      begin
        Process.kill("KILL", -process.pid)
      rescue Errno::ESRCH
        nil
      end
      process.join
    end
  end

  def daemon_pid
    pid = nil
    await do
      pid = Integer(File.read(@pidfile), exception: false) if File.exist?(@pidfile)
      pid && pid > 1 && pid != Process.pid
    end
    @daemon_pids << pid
    pid
  end

  def job_file(name)
    File.join(@directory, name)
  end

  def no_errors
    path = job_file("errors")
    _(File.exist?(path)).must_equal false, (File.read(path) if File.exist?(path))
  end

  it "exits an idle foreground worker without performing future jobs or writing a pidfile" do
    future = DaemonIdleProcessJob.new(job_file("future")).set(wait: 3600)
    future_id = DecentJob::Store.new(database).enqueue_all([future]).first
    @environment["DECENT_JOB_IDLE_TIMEOUT"] = "10"
    @environment["DECENT_JOB_IDLE_SPEC_CONFIG_TIMEOUT"] = "5"
    launch("--idle-timeout", "0.2") do |process, output, error|
      _(Timeout.timeout(4) { process.value.exitstatus }).must_equal 0
      _(output.read).must_include "DecentJob stopped"
      _(error.read).must_equal ""
      _(File.exist?(@pidfile)).must_equal false
      _(File.exist?(job_file("future"))).must_equal false
      _(DecentJob::Store.new(database).find(future_id).fetch("finished_at")).must_be_nil
      _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
      no_errors
    end
  end

  it "keeps a long job alive beyond idle timeout and stops after it finishes" do
    started = job_file("started")
    release = job_file("release")
    finished = job_file("finished")
    DecentJob::Store.new(database).enqueue_all([DaemonIdleProcessJob.new(started, release, finished)])
    launch("--idle-timeout", "0.2") do |process, _output, error|
      await { File.exist?(started) }
      sleep 0.4
      _(process.alive?).must_equal true
      _(File.exist?(finished)).must_equal false
      _(response("/status/started").code).must_equal "200"
      File.write(release, "finish")
      _(Timeout.timeout(3) { process.value.exitstatus }).must_equal 0
      _(File.read(finished)).must_equal "finished"
      _(error.read).must_equal ""
      no_errors
    end
  end

  it "detaches with the correct PID, preserves cwd, reconnects preloaded pools, and removes its pidfile on idle exit" do
    @environment["DECENT_JOB_IDLE_SPEC_PRELOAD"] = "true"
    started = job_file("started")
    release = job_file("release")
    DecentJob::Store.new(database).enqueue_all([DaemonIdleProcessJob.new(started, release)])
    launch("--daemonize", "--idle-timeout", "0.2") do |process, _output, error|
      _(Timeout.timeout(4) { process.value.exitstatus }).must_equal 0
      _(error.read).must_equal ""
      pid = daemon_pid
      await { File.exist?(started) }
      details = JSON.parse(File.read(started))
      _(pid).wont_equal process.pid
      _(details.fetch("pid")).must_equal pid
      _(details.fetch("cwd")).must_equal Dir.pwd
      _(details.fetch("backend")).wont_equal File.read(job_file("preloaded")).to_i
      File.write(release, "finish")
      await { !File.exist?(@pidfile) }
      _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
      no_errors
    end
  end

  it "rejects a duplicate daemon while its existing worker keeps running" do
    launch("--daemonize") do |launcher, _output, error|
      _(Timeout.timeout(4) { launcher.value.exitstatus }).must_equal 0
      _(error.read).must_equal ""
      pid = daemon_pid
      await_connected
      launch("--daemonize") do |duplicate, _other_output, duplicate_error|
        _(Timeout.timeout(4) { duplicate.value.exitstatus }).must_equal 1
        _(duplicate_error.read).must_match(/already|running|locked/i)
      end
      _(File.read(@pidfile).to_i).must_equal pid
      _(response("/status/connected").code).must_equal "200"
      Process.kill("TERM", pid)
      await { !File.exist?(@pidfile) }
      no_errors
    end
  end

  it "reclaims invalid stale PID data and removes the PID file even after a shutdown timeout" do
    File.write(@pidfile, "not a pid")
    started = job_file("started")
    DecentJob::Store.new(database).enqueue_all([DaemonIdleProcessJob.new(started, job_file("never"))])
    launch("--daemonize", "--shutdown-timeout", "0.1") do |process, _output, error|
      _(Timeout.timeout(4) { process.value.exitstatus }).must_equal 0
      _(error.read).must_equal ""
      pid = daemon_pid
      await { File.exist?(started) }
      Process.kill("TERM", pid)
      await { !File.exist?(@pidfile) }
      await { DecentJob::ProcessRecord.active(database: database).empty? }
      no_errors
    end
  end

  it "keeps the supervisor PID file through child replacement and ignores cluster idle timeout" do
    launch("--daemonize", "--subprocesses", "2", "--idle-timeout", "0.1") do |process, _output, error|
      _(Timeout.timeout(4) { process.value.exitstatus }).must_equal 0
      _(error.read).must_equal ""
      pid = daemon_pid
      await_connected
      children = Dir.glob(job_file("child-*")).map { |path| File.basename(path).delete_prefix("child-").to_i }
      _(children.size).must_equal 2
      _(children).wont_include pid
      sleep 0.25
      _(response("/status/connected").code).must_equal "200"
      Process.kill("TERM", children.first)
      await { Dir.glob(job_file("child-*")).size == 3 }
      await_connected
      _(File.read(@pidfile).to_i).must_equal pid
      Process.kill("TERM", pid)
      await { !File.exist?(@pidfile) }
      _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
      no_errors
    end
  end

  it "reclaims a killed supervisor's PID file while its stopped child still exists" do
    launch("--daemonize", "--subprocesses", "1") do |launcher, _output, error|
      _(Timeout.timeout(4) { launcher.value.exitstatus }).must_equal 0
      _(error.read).must_equal ""
      old_pid = daemon_pid
      await_connected
      child = Dir.glob(job_file("child-*")).first.then { |path| File.basename(path).delete_prefix("child-").to_i }
      Process.kill("STOP", child)
      Process.kill("KILL", old_pid)
      await do
        Process.kill(0, old_pid)
        false
      rescue Errno::ESRCH
        true
      end
      launch("--daemonize", "--subprocesses", "1") do |replacement, _replacement_output, replacement_error|
        _(Timeout.timeout(4) { replacement.value.exitstatus }).must_equal 0
        _(replacement_error.read).must_equal ""
        await { File.read(@pidfile).to_i != old_pid }
        new_pid = daemon_pid
        await_connected
        _(Process.kill(0, child)).must_equal 1
        _(new_pid).wont_equal old_pid
        Process.kill("KILL", child)
        Process.kill("TERM", new_pid)
        await { !File.exist?(@pidfile) }
        no_errors
      end
    end
  end

  it "removes the daemon PID file when worker startup fails" do
    occupied = TCPServer.new("0.0.0.0", @port)
    launch("--daemonize") do |process, _output, _error|
      _(Timeout.timeout(4) { process.value.exitstatus }).must_equal 0
      # Native daemonization returns in the launcher before detached startup.
      await { !Dir.glob(job_file("exit-*")).empty? }
      _(File.exist?(@pidfile)).must_equal false
      _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
    end
  ensure
    occupied&.close
  end
end
