# frozen_string_literal: true

require "minitest/mock"
require "socket"
require "spec_helper"
require "timeout"
require "tmpdir"

describe "health probes" do
  before do
    @state = Struct.new(:started?, :connected?).new(false, false)
    @servers = []
  end

  after do
    @servers.each(&:stop)
  end

  def server(**options)
    @servers << DecentJob::ProbeServer.new(port: 0, state: @state, **options)
    @servers.last.start
  end

  def request(server, bytes = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
    socket = TCPSocket.new("127.0.0.1", server.port)
    socket.write(bytes)
    Timeout.timeout(3) { socket.read }
  ensure
    socket&.close
  end

  it "reports lifecycle readiness and forwards unknown middleware paths" do
    probe = server
    _(request(probe)).must_include "200 OK\r\n"
    _(request(probe, "GET /status/started HTTP/1.1\r\n\r\n")).must_include "503 Service Unavailable"
    @state[:started?] = true
    _(request(probe, "GET /status/started HTTP/1.1\r\n\r\n")).must_match(/Started\z/)
    _(request(probe, "GET /status/connected HTTP/1.1\r\n\r\n")).must_match(/Not connected\z/)
    @state[:connected?] = true
    _(request(probe, "GET /status/connected HTTP/1.1\r\n\r\n")).must_match(/Connected\z/)
    _(request(probe, "GET /missing HTTP/1.1\r\n\r\n")).must_include "404 Not Found"
    app = DecentJob::ProbeServer::HealthcheckMiddleware.new(->(_) { [201, {}, ["fallback"]] }, state: @state)
    _(app.call({"PATH_INFO" => "/missing"}).first).must_equal 201
    _(DecentJob::ProbeServer.cluster_app(@state).call({"PATH_INFO" => "/status/connected"}).first).must_equal 200
  end

  it "replaces every route with a custom app and supplies query and method with HEAD byte lengths" do
    body = Class.new(Array) {
      attr_accessor :closed
      def close = self.closed = true
    }.new(["é"])

    environments = []

    probe = server(app: ->(env) {
      environments << env
      [201, {"X-Probe" => "yes"}, body]
    })

    response = request(probe, "HEAD /status?ready=yes HTTP/1.1\r\nHost: localhost\r\nX-Test: value\r\n\r\n")
    _(response).must_include "Content-Length: 2\r\n"
    _(response.split("\r\n\r\n", 2).last).must_equal ""
    _(body.closed).must_equal true
    _(environments.first.slice("REQUEST_METHOD", "PATH_INFO", "QUERY_STRING", "HTTP_X_TEST")).must_equal({"REQUEST_METHOD" => "HEAD", "PATH_INFO" => "/status", "QUERY_STRING" => "ready=yes", "HTTP_X_TEST" => "value"})
  end

  it "rejects malformed and oversized requests and continues serving after app errors" do
    errors = []
    count = 0

    probe = server(on_thread_error: ->(error) { errors << error }, app: ->(_) {
      count += 1
      raise "app failed" if count == 1

      [200, {}, ["fine"]]
    })

    _(request(probe, "nonsense\r\n\r\n")).must_include "400 Bad Request"
    _(request(probe, "GET / HTTP/1.1\r\nX: #{"x" * 20_000}\r\n\r\n")).must_include "400 Bad Request"
    _(request(probe)).must_include "500 Internal Server Error"
    _(errors.map(&:message)).must_equal ["app failed"]
    _(request(probe)).must_match(/fine\z/)
  end

  it "bounds slow requests, stops promptly, and restarts on a fresh socket" do
    errors = []
    probe = server(request_timeout: 0.05, on_thread_error: ->(error) { errors << error })
    socket = TCPSocket.new("127.0.0.1", probe.port)
    socket.write("GET /")
    _(Timeout.timeout(1) { socket.read }).must_include "400 Bad Request"
    socket.close
    socket = TCPSocket.new("127.0.0.1", probe.port)
    socket.write("GET /")
    sleep 0.01
    Timeout.timeout(1) { probe.stop }
    _(errors).must_equal []
    _(probe.running?).must_equal false
    probe.stop
    probe.start
    _(request(probe)).must_match(/OK\z/)
  ensure
    socket&.close
  end

  it "forwards bodyless methods to custom applications" do
    probe = server(app: ->(env) { [201, {}, [env.fetch("REQUEST_METHOD")]] })
    _(request(probe, "POST / HTTP/1.1\r\nContent-Length: 0\r\n\r\n")).must_match(/POST\z/)
  end

  it "rejects response header injection and closes failed bodies" do
    body = Class.new(Array) {
      attr_accessor :closed
      def close = self.closed = true
    }.new(["unsafe"])

    probe = server(app: ->(_) { [200, {"X-Test" => "yes\r\nInjected: true"}, body] }, on_thread_error: ->(_) {})
    response = request(probe)
    _(response).must_include "500 Internal Server Error"
    _(response).wont_include "Injected:"
    _(body.closed).must_equal true
  end

  it "raises bind failures synchronously and allows closing an unstarted server" do
    probe = server
    other = DecentJob::ProbeServer.new(port: probe.port)
    other.close_socket
    _ { other.start }.must_raise Errno::EADDRINUSE
  ensure
    other&.stop
  end
end

describe "systemd notifications" do
  before do
    @saved_env = ENV.to_h.slice("NOTIFY_SOCKET", "WATCHDOG_USEC", "WATCHDOG_PID")
    %w[NOTIFY_SOCKET WATCHDOG_USEC WATCHDOG_PID].each { |key| ENV.delete(key) }
    @directory = Dir.mktmpdir("dj_notify")
    @socket = Socket.new(Socket::AF_UNIX, Socket::SOCK_DGRAM, 0)
    @path = File.join(@directory, "notify")
    @socket.bind(Socket.sockaddr_un(@path))
  end

  after do
    @service&.stop
    @socket.close
    FileUtils.remove_entry(@directory)
    %w[NOTIFY_SOCKET WATCHDOG_USEC WATCHDOG_PID].each { |key| ENV.delete(key) }
    ENV.update(@saved_env)
  end

  def notifications
    messages = []

    loop do
      message = @socket.recv_nonblock(4096, exception: false)
      break if message == :wait_readable

      messages << message
    end

    messages
  end

  it "sends newline states and helpers, returns bytes, and unsets env even on failure" do
    _(DecentJob::SdNotify.ready).must_be_nil
    ENV["NOTIFY_SOCKET"] = @path
    _(DecentJob::SdNotify.notify("READY=1\nSTATUS=working")).must_equal 22
    DecentJob::SdNotify.reloading
    DecentJob::SdNotify.errno(5)
    DecentJob::SdNotify.mainpid(123)
    DecentJob::SdNotify.status("ready", true)
    _(notifications).must_equal ["READY=1\nSTATUS=working", "RELOADING=1", "ERRNO=5", "MAINPID=123", "STATUS=ready"]
    _(ENV["NOTIFY_SOCKET"]).must_be_nil
    ENV["NOTIFY_SOCKET"] = "relative"
    _ { DecentJob::SdNotify.ready(true) }.must_raise DecentJob::SdNotify::NotifyError
    _(ENV["NOTIFY_SOCKET"]).must_be_nil
  end

  it "fails promptly when the manager datagram queue is full" do
    ENV["NOTIFY_SOCKET"] = @path
    @socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_RCVBUF, 1024)
    sent = 0

    error = _ {
      Timeout.timeout(2) do
        loop do
          DecentJob::SdNotify.notify("STATUS=" + "x" * 1000)
          sent += 1
        end
      end
    }.must_raise DecentJob::SdNotify::NotifyError

    _(sent).must_be :>, 0
    _(error.message).must_match(/EAGAIN|EWOULDBLOCK|ENOBUFS/)
  end

  it "encodes Linux abstract socket names with a leading NUL" do
    ENV["NOTIFY_SOCKET"] = "@decent-job-test"
    address = nil
    socket = Object.new

    socket.define_singleton_method(:sendmsg_nonblock) { |state, _flags, destination|
      address = Socket.unpack_sockaddr_un(destination)
      state.bytesize
    }

    socket.define_singleton_method(:close) {}
    Socket.stub(:new, socket) { _(DecentJob::SdNotify.ready).must_equal 7 }
    _(address).must_equal "\0decent-job-test"
  end

  it "validates the watchdog interval and process selection" do
    [nil, "0", "-1", "x", "1.5", "9" * 400].each do |value|
      ENV["WATCHDOG_USEC"] = value
      _(DecentJob::SdNotify.watchdog?).must_equal false
    end

    ENV["WATCHDOG_USEC"] = "100000"
    _(DecentJob::SdNotify.watchdog_interval).must_equal 0.1
    _(DecentJob::SdNotify.watchdog?).must_equal true
    ENV["WATCHDOG_PID"] = (Process.pid + 1).to_s
    _(DecentJob::SdNotify.watchdog?).must_equal false
    ENV["WATCHDOG_PID"] = Process.pid.to_s
    _(DecentJob::SdNotify.watchdog?).must_equal true
  end

  it "rejects oversized watchdog values without emitting conversion warnings" do
    verbose = $VERBOSE
    $VERBOSE = true
    ENV["WATCHDOG_USEC"] = "9" * 400
    _, error = capture_io { _(DecentJob::SdNotify.watchdog?).must_equal false }
    _(error).must_equal ""
  ensure
    $VERBOSE = verbose
  end

  it "sends ready and stopping once and retains watchdog during a failing drain" do
    ENV["NOTIFY_SOCKET"] = @path
    ENV["WATCHDOG_USEC"] = "40000"
    @service = DecentJob::SystemdService.new
    @service.start
    @service.start

    _ {
      @service.stop {
        sleep 0.07
        raise "drain failed"
      }
    }.must_raise RuntimeError

    @service.stop
    messages = notifications
    _(messages.first(2)).must_equal ["READY=1", "STOPPING=1"]
    _(messages.count("READY=1")).must_equal 1
    _(messages.count("STOPPING=1")).must_equal 1
    _(messages.count("WATCHDOG=1")).must_be :>=, 2
    _(@service.notifying?).must_equal false
    @service.start
    @service.stop
    _(notifications).must_equal ["READY=1", "STOPPING=1"]
  end

  it "instruments the half interval and watchdog errors" do
    ENV["NOTIFY_SOCKET"] = @path
    ENV["WATCHDOG_USEC"] = "40000"
    starts = []
    errors = Queue.new
    started = ActiveSupport::Notifications.subscribe("systemd_watchdog_start.decent_job") { |event| starts << event.payload }
    failed = ActiveSupport::Notifications.subscribe("systemd_watchdog_error.decent_job") { |event| errors << event.payload.fetch(:error) }
    @service = DecentJob::SystemdService.new(on_thread_error: ->(_) {})

    DecentJob::SdNotify.stub(:watchdog, -> { raise DecentJob::SdNotify::NotifyError, "manager unavailable" }) do
      @service.start
      _(Timeout.timeout(2) { errors.pop }).must_be_kind_of DecentJob::SdNotify::NotifyError
      @service.stop
    end

    _(starts).must_equal [{interval: 0.02}]
  ensure
    ActiveSupport::Notifications.unsubscribe(started) if started
    ActiveSupport::Notifications.unsubscribe(failed) if failed
  end

  it "isolates notification and reporter failures and always drains" do
    ENV["NOTIFY_SOCKET"] = "/no/such/socket"
    ENV["WATCHDOG_USEC"] = "20000"
    errors = []

    @service = DecentJob::SystemdService.new(on_thread_error: ->(error) {
      errors << error
      raise "report failed"
    })

    @service.start
    sleep 0.04
    drained = false
    @service.stop { drained = true }
    _(drained).must_equal true
    _(errors.length).must_be :>=, 3
    _(@service.notifying?).must_equal false
  end

  it "announces stopping once before a drain even without a prior start" do
    ENV["NOTIFY_SOCKET"] = @path
    @service = DecentJob::SystemdService.new
    @service.stop { _(notifications).must_equal ["STOPPING=1"] }
    @service.stop
    _(notifications).must_equal []
  end

  it "keeps one watchdog throughout overlapping drains and allows restart" do
    ENV["NOTIFY_SOCKET"] = @path
    ENV["WATCHDOG_USEC"] = "20000"
    @service = DecentJob::SystemdService.new
    @service.start
    entered = Queue.new
    release = Queue.new

    thread = Thread.new {
      @service.stop {
        entered << true
        release.pop
      }
    }

    Timeout.timeout(2) { entered.pop }
    @service.stop
    @service.start
    _(@service.notifying?).must_equal true
    sleep 0.04
    release << true
    thread.join
    _(@service.notifying?).must_equal false
    messages = notifications
    _(messages.count("READY=1")).must_equal 1
    _(messages.count("STOPPING=1")).must_equal 1
    _(messages.count("WATCHDOG=1")).must_be :>=, 1
    @service.start
    _(@service.notifying?).must_equal true
  ensure
    release << true
    thread&.join
  end

  it "retires the old watchdog when an error reporter stops and restarts its service" do
    ENV["NOTIFY_SOCKET"] = @path
    ENV["WATCHDOG_USEC"] = "10000"
    senders = Queue.new
    calls = 0

    @service = DecentJob::SystemdService.new(on_thread_error: ->(_) {
      @service.stop
      @service.start
    })

    DecentJob::SdNotify.stub(:watchdog, lambda {
      calls += 1
      raise DecentJob::SdNotify::NotifyError, "transient" if calls == 1

      senders << Thread.current
    }) do
      @service.start
      Timeout.timeout(2) { sleep 0.005 until senders.size >= 6 }
      @service.stop
    end

    threads = []
    threads << senders.pop until senders.empty?
    _(threads.uniq.size).must_equal 1
  ensure
    @service&.stop
    threads&.uniq&.each { |thread| thread.join(1) }
  end

  it "does not start a new generation during an initial stop block" do
    ENV["NOTIFY_SOCKET"] = @path
    ENV["WATCHDOG_USEC"] = "10000"
    @service = DecentJob::SystemdService.new
    entered = Queue.new
    release = Queue.new

    thread = Thread.new {
      @service.stop {
        entered << true
        release.pop
      }
    }

    Timeout.timeout(2) { entered.pop }
    @service.start
    _(@service.notifying?).must_equal false
    release << true
    thread.join
    @service.start
    _(@service.notifying?).must_equal true
  ensure
    release << true
    thread&.join
  end

  it "keeps very long finite watchdog waits interruptible" do
    previous_reporting = Thread.report_on_exception
    Thread.report_on_exception = false
    ENV["WATCHDOG_USEC"] = "9" * 300
    @service = DecentJob::SystemdService.new
    @service.start
    sleep 0.02
    _(@service.notifying?).must_equal true
    Timeout.timeout(1) { @service.stop }
  ensure
    begin
      @service&.stop
    rescue RangeError
      nil
    end

    Thread.report_on_exception = previous_reporting
  end

  it "does not create a watchdog without a matching configured process" do
    @service = DecentJob::SystemdService.new
    @service.start
    _(@service.notifying?).must_equal false
    @service.stop
    ENV["WATCHDOG_USEC"] = "10000"
    ENV["WATCHDOG_PID"] = (Process.pid + 1).to_s
    @service.start
    _(@service.notifying?).must_equal false
  end
end

describe "probe configuration and resource limits" do
  it "validates port and app and reads environment defaults" do
    original = ENV.to_h.slice("DECENT_JOB_PROBE_PORT", "DECENT_JOB_PROBE_HANDLER")
    ENV["DECENT_JOB_PROBE_PORT"] = "7001"
    ENV["DECENT_JOB_PROBE_HANDLER"] = "simple"
    configuration = DecentJob::Configuration.new
    _(configuration.probe_port).must_equal 7001
    _(configuration.probe_handler).must_equal :simple
    configuration.probe_port = nil
    _(configuration.probe_port).must_be_nil

    [0, 65_535].each { |port|
      configuration.probe_port = port
      _(configuration.probe_port).must_equal port
    }

    [-1, 65_536, "7001", false, 1.5].each { |port| _ { configuration.probe_port = port }.must_raise ArgumentError }
    _ { configuration.probe_app = Object.new }.must_raise ArgumentError
    callable = ->(_) { [200, {}, []] }
    configuration.probe_app = callable
    _(configuration.probe_app).must_equal callable
  ensure
    %w[DECENT_JOB_PROBE_PORT DECENT_JOB_PROBE_HANDLER].each { |key| ENV.delete(key) }
    ENV.update(original)
  end

  it "warns and falls back for unsupported handlers and missing WEBrick" do
    _, error = capture_io do
      probe = DecentJob::ProbeServer.new(port: 0, handler: :unknown).start
      _(probe.running?).must_equal true
      probe.stop
      probe = DecentJob::ProbeServer.new(port: 0, handler: :webrick).start
      _(probe.running?).must_equal true
    ensure
      probe&.stop
    end

    _(error).must_include "not supported"
    _(error).must_include "WEBrick is not in the load path" unless defined?(WEBrick)
  end

  it "only closes forked socket copies, leaving the parent available" do
    probe = DecentJob::ProbeServer.new(port: 0).start

    pid = fork do
      probe.close_socket
      probe.stop
      exit! 0
    end

    Process.wait(pid)
    _(probe.running?).must_equal true
    socket = TCPSocket.new("127.0.0.1", probe.port)
    socket.write("GET / HTTP/1.1\r\n\r\n")
    _(Timeout.timeout(2) { socket.read }).must_include "200 OK"
  ensure
    socket&.close
    probe&.stop
  end

  it "bounds response buffering and closes an oversized body" do
    body = Class.new(Array) {
      attr_accessor :closed
      def close = self.closed = true
    }.new(["x" * 1_048_577])

    probe = DecentJob::ProbeServer.new(port: 0, app: ->(_) { [200, {}, body] }, on_thread_error: ->(_) {}).start
    socket = TCPSocket.new("127.0.0.1", probe.port)
    socket.write("GET / HTTP/1.1\r\n\r\n")
    _(Timeout.timeout(2) { socket.read }).must_include "500 Internal Server Error"
    _(body.closed).must_equal true
  ensure
    socket&.close
    probe&.stop
  end

  it "times out a blocked response write and interrupts another write during stop" do
    listener = TCPServer.new("0.0.0.0", 0)
    listener.setsockopt(Socket::SOL_SOCKET, Socket::SO_SNDBUF, 1024)
    entered = Queue.new
    calls = 0

    app = lambda do |_|
      calls += 1
      entered << true
      [200, {}, [(calls == 2) ? "healthy" : "x" * 1_048_576]]
    end

    probe = DecentJob::ProbeServer.new(port: 0, response_timeout: 0.1, app: app)
    TCPServer.stub(:new, listener) { probe.start }
    slow = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
    slow.setsockopt(Socket::SOL_SOCKET, Socket::SO_RCVBUF, 1024)
    slow.connect(Socket.sockaddr_in(probe.port, "127.0.0.1"))
    slow.write("GET / HTTP/1.1\r\n\r\n")
    Timeout.timeout(2) { entered.pop }
    fast = TCPSocket.new("127.0.0.1", probe.port)
    fast.write("GET / HTTP/1.1\r\n\r\n")
    _(Timeout.timeout(2) { fast.read }).must_match(/healthy\z/)
    entered.pop
    slow.close
    slow = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
    slow.setsockopt(Socket::SOL_SOCKET, Socket::SO_RCVBUF, 1024)
    slow.connect(Socket.sockaddr_in(probe.port, "127.0.0.1"))
    slow.write("GET / HTTP/1.1\r\n\r\n")
    Timeout.timeout(2) { entered.pop }
    _(probe.stop(timeout: 0.5)).must_equal true
  ensure
    slow&.close
    fast&.close
    probe&.stop
    listener&.close unless listener&.closed?
  end

  it "offers a bounded stop while a custom app is blocked without killing its thread" do
    entered = Queue.new
    release = Queue.new

    probe = DecentJob::ProbeServer.new(port: 0, app: ->(_) {
      entered << true
      release.pop
      [200, {}, ["done"]]
    }).start

    socket = TCPSocket.new("127.0.0.1", probe.port)
    socket.write("GET / HTTP/1.1\r\n\r\n")
    Timeout.timeout(2) { entered.pop }
    _(probe.stop(timeout: 0.01)).must_equal false
    release << true
    _(probe.stop(timeout: 1)).must_equal true
  ensure
    release << true
    socket&.close
    probe&.stop
  end
end
