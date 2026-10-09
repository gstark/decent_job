# frozen_string_literal: true

require "minitest/mock"
require "net/http"
require "open3"
require "socket"
require "spec_helper"
require "support/supervisor_cli_app"
require "timeout"
require "tmpdir"

describe "supervised CLI processes" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @directory = Dir.mktmpdir("dj-cluster-", "/tmp")
    @port = TCPServer.open("127.0.0.1", 0) { |socket| socket.addr[1] }
    @socket_path = File.join(@directory, "notify.sock")
    @notifications = Socket.new(:UNIX, :DGRAM, 0)
    @notifications.bind(Addrinfo.unix(@socket_path))
    @environment = {
      "DECENT_JOB_SUPERVISOR_SPEC_CONNECTION" => JSON.generate(@connection_options),
      "DECENT_JOB_SUPERVISOR_SPEC_DIRECTORY" => @directory,
      "DECENT_JOB_PROBE_PORT" => @port.to_s,
      "DECENT_JOB_SUBPROCESSES" => "2",
      "OBJC_DISABLE_INITIALIZE_FORK_SAFETY" => "YES",
      "NOTIFY_SOCKET" => @socket_path,
      "WATCHDOG_USEC" => nil,
      "WATCHDOG_PID" => nil
    }
  end

  after do
    @notifications.close
    FileUtils.remove_entry(@directory)
  end

  def await
    Timeout.timeout(12) { sleep 0.01 until yield }
  end

  def response(path)
    Net::HTTP.start("127.0.0.1", @port, nil, open_timeout: 0.5, read_timeout: 1) { |http| http.get(path) }
  end

  def children
    Dir.glob(File.join(@directory, "child-*")).map { |path| File.basename(path).delete_prefix("child-").to_i }
  end

  def await_connected
    await do
      response("/status/connected").code == "200"
    rescue Errno::ECONNREFUSED
      false
    end
  end

  def await_notification(message)
    Timeout.timeout(5) do
      loop do
        IO.select([@notifications])
        break if @notifications.recv(1024) == message
      end
    end
  end

  def run_cli(*arguments)
    command = [RbConfig.ruby, "-Ilib", "exe/decent_job", "start", "--require", File.expand_path("support/supervisor_cli_app.rb", __dir__), "--shutdown-timeout", "2", *arguments]
    Open3.popen3(@environment, *command, pgroup: true) do |input, output, error, process|
      input.close
      yield process, output, error
    ensure
      terminate_process_group(process.pid)
      process.join
    end
  end

  def terminate_process_group(pid)
    Process.kill("KILL", -pid)
  rescue Errno::ESRCH
    nil
  rescue Errno::EPERM
    # Darwin reports EPERM for a group containing only unreaped zombies.
    # Keep real permission failures visible whenever a live member remains.
    members, status = Open3.capture2("ps", "-eo", "pgid=,state=")
    raise unless status.success? && members.lines.none? { |line|
      group, state = line.split
      group.to_i == pid && !state.start_with?("Z")
    }
  end

  it "accepts a zombie-only process group when Darwin refuses group cleanup" do
    status = Minitest::Mock.new
    status.expect(:success?, true)
    Process.stub(:kill, ->(*) { raise Errno::EPERM }) do
      Open3.stub(:capture2, ["42 Z\n43 R\n", status]) do
        terminate_process_group(42)
      end
    end
    status.verify
  end

  it "preserves permission errors when the target process group still has a live member" do
    status = Minitest::Mock.new
    status.expect(:success?, true)
    Process.stub(:kill, ->(*) { raise Errno::EPERM }) do
      Open3.stub(:capture2, ["42 Z\n42 S\n", status]) do
        _ { terminate_process_group(42) }.must_raise Errno::EPERM
      end
    end
    status.verify
  end

  it "preserves permission errors when process-group inspection fails" do
    status = Minitest::Mock.new
    status.expect(:success?, false)
    Process.stub(:kill, ->(*) { raise Errno::EPERM }) do
      Open3.stub(:capture2, ["", status]) do
        _ { terminate_process_group(42) }.must_raise Errno::EPERM
      end
    end
    status.verify
  end

  def stop(process, error)
    Process.kill("TERM", process.pid)
    _(Timeout.timeout(6) { process.value.exitstatus }).must_equal 0
    _(error.read).must_equal ""
    _(File.exist?(File.join(@directory, "errors"))).must_equal false, (File.read(File.join(@directory, "errors")) if File.exist?(File.join(@directory, "errors")))
  end

  it "runs each pipe recipe in its own process and reconnects preloaded databases" do
    @environment["DECENT_JOB_SUPERVISOR_PRELOAD"] = "true"
    first = File.join(@directory, "first")
    second = File.join(@directory, "second")
    DecentJob::Store.new(database).enqueue_all([
      SupervisorProcessJob.new(first).set(queue: "first"),
      SupervisorProcessJob.new(second).set(queue: "second")
    ])
    run_cli("--queues", "first:2|second:3") do |process, output, error|
      await_connected
      await { File.exist?(first) && File.exist?(second) }
      records = [first, second].map { |file| JSON.parse(File.read(file)) }
      _(records.map { |record| record.fetch("pid") }.uniq.size).must_equal 2
      _(records.map { |record| record.fetch("queue") }).must_equal %w[first second]
      preloaded = JSON.parse(File.read(File.join(@directory, "preloaded"))).values
      records.each do |record|
        _(preloaded).wont_include record.fetch("global_backend")
        _(preloaded).wont_include record.fetch("explicit_backend")
      end
      states = DecentJob::ProcessRecord.all(database: database).map(&:state)
      _(states.map { |state| state.fetch("schedulers").sum { |scheduler| scheduler.fetch("max_threads") } }.sort).must_equal [2, 3]
      _(File.readlines(File.join(@directory, "forks")).map(&:to_i)).must_equal [process.pid, process.pid]
      _(children.sort).must_equal records.map { |record| record.fetch("pid") }.sort
      Process.kill("TERM", process.pid)
      status = Timeout.timeout(6) { process.value.exitstatus }
      errors = error.read
      details = File.read(File.join(@directory, "errors")) if File.exist?(File.join(@directory, "errors"))
      _(status).must_equal 0, "#{errors} #{details}"
      _(output.read).must_match(/override.*subprocess|subprocess.*ignored/i)
      _(errors).must_equal ""
      _(File.exist?(File.join(@directory, "errors"))).must_equal false, (File.read(File.join(@directory, "errors")) if File.exist?(File.join(@directory, "errors")))
    end
  end

  it "replaces a killed child and returns to full cluster readiness" do
    run_cli do |process, _output, error|
      await_connected
      original = children
      _(original.size).must_equal 2
      Process.kill("KILL", original.first)
      await { children.size == 3 }
      await_connected
      records = DecentJob::ProcessRecord.active(database: database)
      _(records.size).must_equal 2
      _(response("/status/started").code).must_equal "200"
      stop(process, error)
    end
  end

  it "keeps probes and watchdog active while children drain a job" do
    @environment["WATCHDOG_USEC"] = "100000"
    started = File.join(@directory, "started")
    release = File.join(@directory, "release")
    finished = File.join(@directory, "finished")
    DecentJob::Store.new(database).enqueue_all([SupervisorProcessJob.new(started, release, finished)])
    run_cli do |process, _output, error|
      await_connected
      await { File.exist?(started) }
      await_notification("READY=1")
      Process.kill("TERM", process.pid)
      await_notification("STOPPING=1")
      _(response("/status/started").code).must_equal "503"
      _(response("/status/connected").code).must_equal "503"
      _(response("/status").code).must_equal "200"
      2.times { await_notification("WATCHDOG=1") }
      _(process.alive?).must_equal true
      File.write(release, "finish")
      _(Timeout.timeout(6) { process.value.exitstatus }).must_equal 0
      _(File.read(finished)).must_equal "finished"
      _(error.read).must_equal ""
      _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
    end
  end

  it "terminates children whose application boot hooks have not returned" do
    @environment["DECENT_JOB_SUPERVISOR_BLOCK_BOOT"] = "true"
    run_cli do |process, _output, error|
      await { children.size == 2 }
      await do
        response("/status/connected").code == "503"
      rescue Errno::ECONNREFUSED
        false
      end
      Process.kill("TERM", process.pid)
      status = Timeout.timeout(5) { process.value.exitstatus }
      errors = error.read
      details = File.read(File.join(@directory, "errors")) if File.exist?(File.join(@directory, "errors"))
      _(status).must_equal 0, "#{errors} #{details}"
      _(errors).must_equal ""
      _(details).must_be_nil
      _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
    end
  end

  it "closes inherited probe sockets in replacement children after parent death" do
    run_cli do |process, _output, _error|
      await_connected
      Process.kill("KILL", children.first)
      await { children.size == 3 }
      await_connected
      Process.kill("KILL", process.pid)
      process.join
      rebound = nil
      Timeout.timeout(2) do
        until rebound
          begin
            rebound = TCPServer.new("0.0.0.0", @port)
          rescue Errno::EADDRINUSE
            sleep 0.01
          end
        end
      end
      _(rebound.addr[1]).must_equal @port
      await { DecentJob::ProcessRecord.active(database: database).empty? }
    ensure
      rebound&.close
    end
  end

  it "escalates a blocked job after one shared shutdown deadline" do
    started = File.join(@directory, "started")
    release = File.join(@directory, "never")
    DecentJob::Store.new(database).enqueue_all([SupervisorProcessJob.new(started, release)])
    run_cli("--shutdown-timeout", "0.2") do |process, _output, error|
      await_connected
      await { File.exist?(started) }
      began = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      Process.kill("TERM", process.pid)
      _(Timeout.timeout(3) { process.value.exitstatus }).must_equal 1
      _(Process.clock_gettime(Process::CLOCK_MONOTONIC) - began).must_be :<, 2
      _(error.read).must_match(/timed out/i)
    end
  end

  it "cleans up already forked children when the probe cannot bind" do
    occupied = TCPServer.new("0.0.0.0", @port)
    run_cli do |process, _output, error|
      _(Timeout.timeout(6) { process.value.exitstatus }).must_equal 1
      _(error.read).must_match(/address.*use|EADDRINUSE/i)
      children.each { |pid| _ { Process.kill(0, pid) }.must_raise Errno::ESRCH }
      _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
    end
  ensure
    occupied&.close
  end
end
