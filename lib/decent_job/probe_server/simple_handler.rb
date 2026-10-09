# frozen_string_literal: true

require "socket"

module DecentJob
  class ProbeServer
    class SimpleHandler
      class BadRequest < StandardError; end

      attr_reader :port

      def initialize(app, port:, request_timeout:, response_timeout:, report:)
        @app = app
        @configured_port = port
        @port = port
        @request_timeout = request_timeout
        @response_timeout = response_timeout
        @report = report
        @mutex = Mutex.new
      end

      def start
        @mutex.synchronize do
          return self if @thread&.alive?
          @socket = TCPServer.new("0.0.0.0", @configured_port)
          @socket.listen(16)
          @port = @socket.addr[1]
          @pid = Process.pid
          @stopping = false
          @thread = Thread.new { serve }
        end
        self
      end

      def running?
        @mutex.synchronize { @pid == Process.pid && !@stopping && !!@thread&.alive? }
      end

      def stop(timeout: nil)
        thread = @mutex.synchronize do
          @stopping = true
          close_owned_sockets
          (@pid == Process.pid) ? @thread : nil
        end
        return true unless thread
        return false if thread == Thread.current
        !!thread.join(timeout)
      end

      # In a forked child, only Ruby IO copies are closed. Never join or signal
      # the parent's serving thread, and never shutdown(2) its shared socket.
      def close_socket
        @mutex.synchronize do
          @stopping = true
          close_owned_sockets
        end
        self
      end

      private

      def close_owned_sockets
        @client&.close unless @client&.closed?
        @socket&.close unless @socket&.closed?
      end

      def serve
        loop do
          listener = @mutex.synchronize { @stopping ? nil : @socket }
          break unless listener
          client = listener.accept
          accepted = @mutex.synchronize do
            if @stopping
              client.close
              false
            else
              @client = client
              true
            end
          end
          break unless accepted
          begin
            respond(client)
          ensure
            @mutex.synchronize do
              client.close unless client.closed?
              @client = nil if @client.equal?(client)
            end
          end
        end
      rescue IOError, SystemCallError => error
        @report.call(error) unless @mutex.synchronize { @stopping }
      ensure
        @mutex.synchronize do
          @stopping = true
          close_owned_sockets
        end
      end

      def respond(client)
        env = nil
        response = begin
          env = read_request(client)
          app_response(env)
        rescue BadRequest
          Response.error(400)
        end
        status, headers, content = response
        output = "HTTP/1.1 #{status} #{Response::REASONS.fetch(status, "Response")}\r\n"
        headers.each { |key, value| output << "#{key}: #{value}\r\n" }
        output << "\r\n"
        output = output.b
        output << content.b unless env && env["REQUEST_METHOD"] == "HEAD"
        write_response(client, output)
      rescue IOError, SystemCallError
        # A disconnected or slow client must not terminate the serving loop.
        nil
      end

      def app_response(env)
        Response.build(@app, env)
      rescue => error
        @report.call(error)
        Response.error(500)
      end

      def read_request(client)
        deadline = monotonic_time + @request_timeout
        input = +"".b
        until input.include?("\r\n\r\n")
          remaining = deadline - monotonic_time
          raise BadRequest unless remaining.positive? && IO.select([client], nil, nil, remaining)
          part = client.read_nonblock(4096, exception: false)
          next if part == :wait_readable
          raise BadRequest unless part
          input << part
          raise BadRequest if input.bytesize > Response::MAX_HEADER_BYTES
        end
        lines = input.split("\r\n\r\n", 2).first.split("\r\n")
        match = /\A([A-Z]+) (\/[^\x00-\x20\x7f#]*) HTTP\/(1\.[01])\z/.match(lines.shift)
        raise BadRequest unless match
        method, target, version = match.captures
        path, query = target.split("?", 2)
        env = {"REQUEST_METHOD" => method, "PATH_INFO" => path, "QUERY_STRING" => query || "",
               "SERVER_PROTOCOL" => "HTTP/#{version}", "SERVER_NAME" => "0.0.0.0",
               "SERVER_PORT" => @port.to_s, "REMOTE_ADDR" => client.peeraddr[3], "rack.url_scheme" => "http"}
        raise BadRequest if lines.length > 100
        lines.each do |line|
          key, value = line.split(":", 2)
          raise BadRequest unless key && value && Response::TOKEN.match?(key) && !value.match?(/[\x00-\x08\x0a-\x1f\x7f]/)
          name = "HTTP_#{key.upcase.tr("-", "_")}"
          raise BadRequest if env.key?(name)
          env[name] = value.strip
        end
        raise BadRequest if env["HTTP_TRANSFER_ENCODING"] || (env["HTTP_CONTENT_LENGTH"] && env["HTTP_CONTENT_LENGTH"] != "0")
        env
      end

      def write_response(client, bytes)
        deadline = monotonic_time + @response_timeout
        offset = 0
        while offset < bytes.bytesize
          remaining = deadline - monotonic_time
          return unless remaining.positive? && IO.select(nil, [client], nil, remaining)
          written = client.write_nonblock(bytes.byteslice(offset, bytes.bytesize - offset), exception: false)
          offset += written unless written == :wait_writable
        end
      end

      def monotonic_time
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end
    end
  end
end
