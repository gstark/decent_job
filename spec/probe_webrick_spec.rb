# frozen_string_literal: true

require "socket"
require "spec_helper"
require "timeout"

describe "optional WEBrick probe" do
  before do
    require "webrick"
  rescue LoadError
    skip "WEBrick is an optional host dependency"
  end

  after do
    @probe&.stop
  end

  it "forwards extension methods and queries to the whole custom app" do
    @probe = DecentJob::ProbeServer.new(port: 0, handler: :webrick,
      app: ->(env) { [201, {}, ["#{env.fetch("REQUEST_METHOD")}:#{env.fetch("QUERY_STRING")}"]] }).start
    socket = TCPSocket.new("127.0.0.1", @probe.port)
    socket.write("PATCH /status?value=1 HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n")
    response = Timeout.timeout(2) { socket.read }
    _(response).must_include "201 Created"
    _(response).must_match(/PATCH:value=1\z/)
  ensure
    socket&.close
  end

  it "closes the listener while a callback and a queued connection outlive bounded stop" do
    entered = Queue.new
    release = Queue.new

    @probe = DecentJob::ProbeServer.new(port: 0, handler: :webrick, app: lambda { |_|
      entered << true
      release.pop
      [200, {}, ["done"]]
    }).start

    port = @probe.port
    active = TCPSocket.new("127.0.0.1", port)
    active.write("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
    Timeout.timeout(2) { entered.pop }
    queued = TCPSocket.new("127.0.0.1", port)
    queued.write("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")

    # Wait until WEBrick is blocked on its single request token, so deferred
    # listener cleanup cannot accidentally make this regression pass.
    Timeout.timeout(2) do
      sleep 0.005 until Thread.list.any? { |thread| thread.backtrace&.join("\n")&.match?(/SizedQueue#pop.*WEBrick::GenericServer#start/m) }
    end

    _(@probe.stop(timeout: 0.01)).must_equal false
    _ { TCPSocket.new("127.0.0.1", port).close }.must_raise Errno::ECONNREFUSED
    release << true
    _(@probe.stop(timeout: 1)).must_equal true
    @probe.start
    _(@probe.running?).must_equal true
  ensure
    release << true
    active&.close
    queued&.close
  end

  it "does not join its own request thread when an app calls stop" do
    stopped = Queue.new

    @probe = DecentJob::ProbeServer.new(port: 0, handler: :webrick, app: lambda { |_|
      result = begin
        Timeout.timeout(0.2) { @probe.stop }
      rescue Timeout::Error => error
        error
      end

      stopped << result
      [200, {}, ["done"]]
    }).start

    socket = TCPSocket.new("127.0.0.1", @probe.port)
    socket.write("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
    _(Timeout.timeout(2) { stopped.pop }).must_equal false
    _(@probe.stop(timeout: 1)).must_equal true
  ensure
    socket&.close
  end

  it "closes a partial request promptly and restarts" do
    @probe = DecentJob::ProbeServer.new(port: 0, handler: :webrick).start
    socket = TCPSocket.new("127.0.0.1", @probe.port)
    socket.write("GET /")
    _(@probe.stop(timeout: 1)).must_equal true
    _(@probe.running?).must_equal false
    @probe.start
    socket.close
    socket = TCPSocket.new("127.0.0.1", @probe.port)
    socket.write("HEAD / HTTP/1.1\r\nHost: localhost\r\n\r\n")
    response = Timeout.timeout(2) { socket.read }
    _(response).must_include "200 OK"
    _(response.downcase).must_include "content-length: 2"
    _(response.split("\r\n\r\n", 2).last).must_equal ""
  ensure
    socket&.close
  end
end
