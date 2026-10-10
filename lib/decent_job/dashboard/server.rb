# frozen_string_literal: true

require "stringio"

module DecentJob
  class Dashboard
    # A standalone HTTP server for the dashboard, for hosts without Rack.
    # WEBrick is an optional gem; add it to the Gemfile to use this server.
    # Production hosts should mount the Rack app in their web server instead.
    class Server
      class Unavailable < StandardError; end

      attr_reader :port

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

      def start
        @server = WEBrick::HTTPServer.new(BindAddress: @host, Port: @configured_port, AccessLog: [], Logger: @logger, DoNotReverseLookup: true)
        @port = @server.listeners.first.addr[1]
        @server.mount_proc("/") { |request, response| respond(request, response) }
        @thread = Thread.new { @server.start }
        self
      end

      def join
        @thread&.join
      end

      def stop
        @server&.shutdown
        @thread&.join(5)
      end

      private

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
