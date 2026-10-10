# frozen_string_literal: true

require "stringio"

module DecentJob
  class Dashboard
    # A standalone HTTP server for the dashboard, for hosts without Rack.
    # WEBrick is an optional gem; add it to the Gemfile to use this server.
    # Production hosts should mount the Rack app in their web server instead.
    #
    # The CLI dashboard command creates it ({CLI#serve_dashboard}). WEBrick
    # runs its accept loop on a thread that {#start} creates, and handles each
    # request on its own thread, so the app must be thread safe. The server
    # always reports rack.url_scheme as "http", so the dashboard never sets
    # the Secure cookie flag here. Do not expose it to the public internet.
    #
    # See docs/dashboard.md.
    class Server
      # Raised by {#initialize} when WEBrick cannot be loaded.
      class Unavailable < StandardError; end

      # @return [Integer] the bound port after {#start}, or the requested port
      #   before it
      attr_reader :port

      # @param app [#call] a Rack-style app, usually a {Dashboard}
      # @param port [Integer] TCP port. 0 picks a free port.
      # @param host [String] bind address
      # @param logger [WEBrick::Log, nil] WEBrick logger. nil discards logs.
      # @raise [Unavailable] when the webrick gem is not available
      def initialize(app, port:, host: "127.0.0.1", logger: nil)
        begin
          require "webrick"
        rescue LoadError
          raise Unavailable, 'The dashboard server needs WEBrick. Add gem "webrick" to the Gemfile, or mount DecentJob::Dashboard in a Rack application.'
        end

        @app = app
        @host = host
        @configured_port = port
        @port = port
        @logger = logger || WEBrick::Log.new(File::NULL)
      end

      # Binds the socket and starts serving on a new thread.
      #
      # @return [Server] self
      def start
        @server = WEBrick::HTTPServer.new(BindAddress: @host, Port: @configured_port, AccessLog: [], Logger: @logger, DoNotReverseLookup: true)
        @port = @server.listeners.first.addr[1]
        @server.mount_proc("/") { |request, response| respond(request, response) }
        @thread = Thread.new { @server.start }
        self
      end

      # Blocks until the server thread exits.
      #
      # @return [void]
      def join
        @thread&.join
      end

      # Stops WEBrick and waits up to 5 seconds for its thread.
      #
      # @return [void]
      def stop
        @server&.shutdown
        @thread&.join(5)
      end

      private

      # Builds a minimal Rack env from the WEBrick request. The app joins
      # multiple cookies with "\n", so they are split into separate headers.
      def respond(request, response)
        env = request.meta_vars.merge(
          "PATH_INFO" => request.path, "QUERY_STRING" => request.query_string.to_s, "SCRIPT_NAME" => "",
          "CONTENT_TYPE" => request.content_type.to_s, "rack.input" => StringIO.new(request.body.to_s), "rack.url_scheme" => "http"
        )
        status, headers, body = @app.call(env)
        response.status = status

        headers.each do |key, value|
          if key == "set-cookie"
            value.split("\n").each { |cookie| response.cookies << cookie }
          else
            response[key] = value
          end
        end

        content = +""
        body.each { |part| content << part }
        response.body = content
      end
    end
  end
end
