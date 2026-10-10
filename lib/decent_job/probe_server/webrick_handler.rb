# frozen_string_literal: true

require "webrick"

module DecentJob
  class ProbeServer
    # WEBrick owns at most one request thread. Socket deadlines also bound
    # writes (WEBrick's RequestTimeout alone covers only request reads).
    class WebrickHandler
      # Both explicit stop and WEBrick's final cleanup share descriptor
      # ownership. Closing now must not leave final cleanup touching stale IOs.
      class Server < WEBrick::HTTPServer
        def initialize(configuration)
          @listener_mutex = Mutex.new
          super
        end

        def close_listeners
          @listener_mutex.synchronize do
            @listeners.each { |socket| socket.close unless socket.closed? }
            @listeners.clear
          end
        end

        private

        def cleanup_listener
          close_listeners
        end
      end

      class ApplicationServlet < WEBrick::HTTPServlet::AbstractServlet
        def initialize(server, callback)
          super(server)
          @callback = callback
        end

        def service(request, response)
          @callback.call(request, response)
        end
      end

      attr_reader :port

      def initialize(app, port:, request_timeout:, response_timeout:, report:)
        @app = app
        @configured_port = @port = port
        @request_timeout = request_timeout
        @response_timeout = response_timeout
        @report = report
        @mutex = Mutex.new
        @condition = ConditionVariable.new
        @clients = []
        @request_threads = []
      end

      def start
        @mutex.synchronize do
          return self if @thread&.alive?
          @stopping = false
          @ready = false
          @pid = Process.pid
          @request_threads = []
          @server = Server.new(BindAddress: "0.0.0.0", Port: @configured_port,
            MaxClients: 1, RequestTimeout: @request_timeout, AccessLog: [],
            Logger: WEBrick::Log.new(File::NULL), DoNotReverseLookup: true,
            StartCallback: -> {
              @mutex.synchronize {
                @ready = true
                @condition.broadcast
              }
            },
            AcceptCallback: method(:accept_client))
          @port = @server.listeners.first.addr[1]
          @server.mount("/", ApplicationServlet, method(:respond))

          @thread = Thread.new do
            @server.start
          rescue => error
            @report.call(error)
          ensure
            @mutex.synchronize {
              @stopping = true
              @condition.broadcast
            }
          end

          @condition.wait(@mutex) until @ready || @stopping
        end

        self
      end

      def running?
        @mutex.synchronize { @pid == Process.pid && !@stopping && !!@thread&.alive? }
      end

      def stop(timeout: nil)
        server, thread, request_thread = @mutex.synchronize do
          @stopping = true
          @clients.each { |client| client.close unless client.closed? }
          @server&.close_listeners

          if @pid == Process.pid
            [@server, @thread, @request_threads.include?(Thread.current)]
          else
            [nil, nil, false]
          end
        end

        server&.shutdown
        return true unless thread
        return false if thread == Thread.current || request_thread
        !!thread.join(timeout)
      end

      def close_socket
        @mutex.synchronize do
          @stopping = true
          @clients.each { |client| client.close unless client.closed? }
          @server&.close_listeners
        end

        self
      end

      private

      def accept_client(client)
        @mutex.synchronize do
          if @stopping
            client.close
          else
            client.timeout = [@request_timeout, @response_timeout].min
            @clients.reject!(&:closed?)
            @clients << client
            @request_threads.reject! { |thread| !thread.alive? }
            @request_threads << Thread.current
          end
        end
      end

      def respond(request, response)
        result = begin
          env = request.meta_vars.merge("PATH_INFO" => request.path, "QUERY_STRING" => request.query_string.to_s)
          Response.build(@app, env)
        rescue => error
          @report.call(error)
          Response.error(500)
        end

        status, headers, content = result
        response.status = status
        headers.each { |key, value| response[key] = value }
        response.keep_alive = false
        response.body = content
      end
    end
  end
end
