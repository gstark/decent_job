# frozen_string_literal: true

require "net/http"
require "open3"
require "socket"
require "spec_helper"
require "support/cli_startup"
require "support/health_services_cli_app"
require "timeout"
require "tmpdir"

describe "CLI health probes and systemd lifecycle" do
  include DatabaseSpec
  include CliStartupSpec

  before do
    DecentJob::Schema.new(database).migrate
    @directory = Dir.mktmpdir("dj-health-", "/tmp")
    @errors = File.join(@directory, "errors")
    @socket_path = File.join(@directory, "notify.sock")
    @notifications = Socket.new(:UNIX, :DGRAM, 0)
    @notifications.bind(Addrinfo.unix(@socket_path))
    @port = unused_port
    @environment = {
      "DECENT_JOB_HEALTH_SPEC_CONNECTION" => JSON.generate(@connection_options),
      "DECENT_JOB_HEALTH_ERRORS" => @errors,
      "DECENT_JOB_PROBE_PORT" => @port.to_s,
      "DECENT_JOB_ENABLE_LISTEN_NOTIFY" => "true",
      "NOTIFY_SOCKET" => @socket_path,
      "WATCHDOG_USEC" => "100000",
      "WATCHDOG_PID" => nil
    }
    @processes = []
  end

  after do
    @processes.each do |process|
      if process.alive?
        begin
          Process.kill("KILL", process.pid)
        rescue Errno::ESRCH
          nil
        end
      end
      process.join
    end
    @notifications.close
    FileUtils.remove_entry(@directory)
  end

  def unused_port
    TCPServer.open("127.0.0.1", 0) { |socket| socket.addr[1] }
  end

  def await
    Timeout.timeout(8) { sleep 0.01 until yield }
  end

  def response(path, port: @port)
    Net::HTTP.start("127.0.0.1", port, nil, open_timeout: 1, read_timeout: 1) { |http| http.get(path) }
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
    command = [RbConfig.ruby, "-Ilib", "exe/decent_job", "start", "--require", File.expand_path("support/health_services_cli_app.rb", __dir__), "--shutdown-timeout", "3", *arguments]
    Open3.popen3(@environment, *command) do |input, output, error, process|
      @processes << process
      input.close
      yield process, output, error
    ensure
      if process.alive?
        begin
          Process.kill("KILL", process.pid)
        rescue Errno::ESRCH
          nil
        end
      end
      process.join
    end
  end

  def await_boot(output, error)
    startup_output = await_started(output, error)
    await { response("/status/connected").code == "200" }
    startup_output
  end

  it "keeps probes and watchdog alive while SIGTERM drains a job" do
    started = File.join(@directory, "started")
    release = File.join(@directory, "release")
    finished = File.join(@directory, "finished")
    DecentJob::Store.new(database).enqueue_all([HealthServicesJob.new(started, release, finished)])
    run_cli do |process, output, error|
      startup_output = await_boot(output, error)
      _(startup_output.scan("[DecentJob] Pinging systemd watchdog every").size).must_equal 1
      await_notification("READY=1")
      await { File.exist?(started) }
      _(%w[/ /status /status/started /status/connected].map { |path| response(path).code }).must_equal %w[200 200 200 200]
      _(response("/missing").code).must_equal "404"

      Process.kill("TERM", process.pid)
      await_notification("STOPPING=1")
      await { response("/status/started").code == "503" }
      _(response("/status/connected").code).must_equal "503"
      _(response("/status").body).must_equal "OK"
      await_notification("WATCHDOG=1")
      _(process.alive?).must_equal true
      _(File.exist?(finished)).must_equal false
      File.write(release, "finish")
      _(Timeout.timeout(5) { process.value.exitstatus }).must_equal 0
      _(File.read(finished)).must_equal "finished"
      _(error.read).must_equal ""
      _(File.exist?(@errors)).must_equal false
      _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
      _ { TCPSocket.new("127.0.0.1", @port) }.must_raise Errno::ECONNREFUSED
      messages = []
      messages << @notifications.recv(1024) while IO.select([@notifications], nil, nil, 0)
      _(messages.count("STOPPING=1")).must_equal 0
    end
  end

  it "reports connectivity without LISTEN and ignores another PID's watchdog" do
    @environment["WATCHDOG_PID"] = "0"
    run_cli("--no-listen-notify") do |process, output, error|
      await_boot(output, error)
      await_notification("READY=1")
      _(response("/status/connected").body).must_equal "Connected"
      _(IO.select([@notifications], nil, nil, 0.2)).must_be_nil
      Process.kill("TERM", process.pid)
      await_notification("STOPPING=1")
      _(Timeout.timeout(5) { process.value.exitstatus }).must_equal 0
      _(error.read).must_equal ""
    end
  end

  it "applies CLI port precedence and serves the configured application" do
    configured_port = unused_port
    cli_port = unused_port
    @environment["DECENT_JOB_HEALTH_CONFIG_PORT"] = configured_port.to_s
    @environment["DECENT_JOB_HEALTH_CUSTOM_APP"] = "true"
    run_cli("--probe-port", cli_port.to_s) do |process, output, error|
      await_started(output, error)
      result = response("/custom?value=two", port: cli_port)
      _(result.code).must_equal "207"
      _(result.body.b).must_equal "GET /custom?value=two café".b
      _(result["content-length"].to_i).must_equal result.body.bytesize
      _ { TCPSocket.new("127.0.0.1", @port) }.must_raise Errno::ECONNREFUSED
      _ { TCPSocket.new("127.0.0.1", configured_port) }.must_raise Errno::ECONNREFUSED
      Process.kill("TERM", process.pid)
      _(Timeout.timeout(5) { process.value.exitstatus }).must_equal 0
      _(error.read).must_equal ""
    end
  end

  it "fails a probe bind cleanly without leaving worker presence behind" do
    occupied = TCPServer.new("0.0.0.0", @port)
    run_cli do |process, _output, error|
      _(Timeout.timeout(5) { process.value.exitstatus }).must_equal 1
      _(error.read).must_match(/address.*use|EADDRINUSE/i)
      _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
    end
  ensure
    occupied&.close
  end

  it "interrupts a client's incomplete request during shutdown" do
    run_cli do |process, output, error|
      await_boot(output, error)
      client = TCPSocket.new("127.0.0.1", @port)
      client.write("GET /status HTTP/1.1\r\nHost:")
      Process.kill("TERM", process.pid)
      _(Timeout.timeout(2) { process.value.exitstatus }).must_equal 0
      _(error.read).must_equal ""
    ensure
      client&.close
    end
  end

  it "keeps watchdog coverage while a custom probe callback drains" do
    @environment["DECENT_JOB_HEALTH_BLOCKING_APP"] = @directory
    run_cli do |process, output, error|
      await_boot(output, error)
      await_notification("READY=1")
      client = TCPSocket.new("127.0.0.1", @port)
      client.write("GET /hold HTTP/1.1\r\nHost: localhost\r\n\r\n")
      await { File.exist?(File.join(@directory, "probe-started")) }
      Process.kill("TERM", process.pid)
      await_notification("STOPPING=1")
      2.times { await_notification("WATCHDOG=1") }
      _(process.alive?).must_equal true
      File.write(File.join(@directory, "probe-release"), "finish")
      _(Timeout.timeout(5) { process.value.exitstatus }).must_equal 0
      _(error.read).must_equal ""
    ensure
      client&.close
    end
  end
end
