# frozen_string_literal: true

require "webrick"

module DecentJob
  class ProbeServer
    # The optional probe handler that serves through WEBrick.
    #
    # {ProbeServer} loads this file only when the handler is :webrick and the
    # host application provides the webrick gem. DecentJob does not depend on
    # it.
    #
    # Ownership: one thread runs WEBrick's accept loop, and WEBrick owns at
    # most one request thread (MaxClients 1). The process that called
    # {#start} owns both; @pid records it. The mutex guards open clients,
    # request threads, and the stopping flag.
    #
    # Deadlines: each accepted socket gets IO#timeout set to the smaller of
    # the two timeouts. That also bounds writes, because WEBrick's
    # RequestTimeout alone covers only request reads.
    #
    # WEBrick parses requests itself, so it can accept requests that
    # {SimpleHandler} rejects. Responses still go through {Response.build}.
    class WebrickHandler
      # WEBrick server whose listener close is idempotent and thread safe.
      #
      # Both explicit stop and WEBrick's final cleanup share descriptor
      # ownership. Closing now must not leave final cleanup touching stale IOs.
      class Server < WEBrick::HTTPServer
        # @param configuration [Hash] WEBrick server options
        def initialize(configuration)
          @listener_mutex = Mutex.new
          super
        end

        # Closes and forgets every listener socket. Safe to call more than once.
        #
        # @return [Array] the emptied listener list
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

      # Forwards every request method and path to one callback.
      class ApplicationServlet < WEBrick::HTTPServlet::AbstractServlet
        # @param server [WEBrick::HTTPServer] the owning server
        # @param callback [#call] receives (request, response)
        def initialize(server, callback)
          super(server)
          @callback = callback
        end

        # @param request [WEBrick::HTTPRequest]
        # @param response [WEBrick::HTTPResponse]
        # @return [void]
        def service(request, response)
          @callback.call(request, response)
        end
      end

      # @return [Integer] the bound port after {#start}, else the configured port
      attr_reader :port

      # @param app [#call] the probe app
      # @param port [Integer] TCP port, 0 for an OS-chosen port
      # @param request_timeout [Numeric] seconds for WEBrick's RequestTimeout
      # @param response_timeout [Numeric] seconds; the socket timeout is the
      #   smaller of the two timeouts
      # @param report [#call] receives app errors and server thread errors
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

      # Binds 0.0.0.0, starts the server thread, and waits until WEBrick
      # reports that it started or the thread ended.
      #
      # @return [WebrickHandler] self. Does nothing while the thread is alive.
      # @raise [SystemCallError] when the bind fails
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

      # @return [Boolean] true when this process owns a live server thread and
      #   no stop has started
      def running?
        @mutex.synchronize { @pid == Process.pid && !@stopping && !!@thread&.alive? }
      end

      # Closes clients and listeners, shuts WEBrick down, and joins its thread.
      #
      # @param timeout [Numeric, nil] seconds to wait, or nil to wait forever
      # @return [Boolean] true when the thread exited or this process does not
      #   own it. False on timeout, or when called from the server thread or a
      #   request thread (for example, from the app), since a join there would
      #   deadlock.
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

      # Closes this process's IO copies of clients and listeners without
      # joining or shutting down any thread.
      #
      # @return [WebrickHandler] self
      # @note {Supervisor} children call this through {ProbeServer#close_socket}.
      def close_socket
        @mutex.synchronize do
          @stopping = true
          @clients.each { |client| client.close unless client.closed? }
          @server&.close_listeners
        end
        self
      end

      private

      # Runs on the request thread for each accepted socket. Records the thread
      # so {#stop} can detect a call from inside a request.
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
