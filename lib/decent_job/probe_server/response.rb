# frozen_string_literal: true

module DecentJob
  class ProbeServer
    # Deliberately bounded, buffered responses for small health endpoints. This
    # callable contract is not Rack: no request bodies, streaming, or hijacking.
    module Response
      MAX_BODY_BYTES = 1_048_576
      MAX_HEADER_BYTES = 16_384
      TOKEN = /\A[!#$%&'*+.^_`|~0-9A-Za-z-]+\z/
      REASONS = {200 => "OK", 201 => "Created", 204 => "No Content", 400 => "Bad Request",
                 404 => "Not Found", 405 => "Method Not Allowed", 500 => "Internal Server Error",
                 503 => "Service Unavailable"}.freeze

      def self.build(app, env)
        status, headers, body = app.call(env)
        raise ArgumentError, "invalid response status" unless status.is_a?(Integer) && (200..599).cover?(status)
        result = {}
        bytes = 0
        headers.each do |key, value|
          key, value = String(key), String(value)
          raise ArgumentError, "invalid response header" unless TOKEN.match?(key) && !value.match?(/[\x00-\x1f\x7f]/)
          bytes += key.bytesize + value.bytesize + 4
          raise ArgumentError, "response headers too large" if bytes > MAX_HEADER_BYTES
          next if %w[content-length connection transfer-encoding].include?(key.downcase)
          result[key] = value
        end
        content = +"".b
        body.each do |part|
          part = String(part)
          raise ArgumentError, "response body too large" if content.bytesize + part.bytesize > MAX_BODY_BYTES
          content << part.b
        end
        content.clear if [204, 304].include?(status)
        result["Content-Length"] = content.bytesize.to_s
        result["Connection"] = "close"
        [status, result, content]
      ensure
        body.close if body.respond_to?(:close)
      end

      def self.error(status)
        text = REASONS.fetch(status)
        [status, {"Content-Type" => "text/plain", "Content-Length" => text.bytesize.to_s, "Connection" => "close"}, text]
      end
    end
  end
end
