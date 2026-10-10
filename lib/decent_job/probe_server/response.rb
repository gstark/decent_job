# frozen_string_literal: true

module DecentJob
  class ProbeServer
    # Builds bounded, buffered HTTP responses for both probe handlers.
    #
    # {SimpleHandler} and {WebrickHandler} call {.build} to run the app and
    # check its result. The callable contract is Rack-like but is not Rack:
    # no request bodies, no streaming, and no hijacking. Responses always
    # close the connection.
    module Response
      # Upper limit in bytes for a buffered response body.
      MAX_BODY_BYTES = 1_048_576
      # Upper limit in bytes for response headers, and for request headers in
      # {SimpleHandler}.
      MAX_HEADER_BYTES = 16_384
      # HTTP token characters, used to check header names.
      TOKEN = /\A[!#$%&'*+.^_`|~0-9A-Za-z-]+\z/
      # Reason phrases for status lines. {SimpleHandler} uses "Response" for
      # other statuses.
      REASONS = {200 => "OK", 201 => "Created", 204 => "No Content", 400 => "Bad Request",
                 404 => "Not Found", 405 => "Method Not Allowed", 500 => "Internal Server Error",
                 503 => "Service Unavailable"}.freeze

      # Calls the app and validates and buffers its response.
      #
      # Drops app-supplied Content-Length, Connection, and Transfer-Encoding
      # headers and sets Content-Length and Connection: close. Empties the
      # body for 204 and 304.
      #
      # @param app [#call] returns [status, headers, body]; body responds to each
      # @param env [Hash{String => String}] request environment
      # @return [Array(Integer, Hash{String => String}, String)] status,
      #   headers, and a binary body
      # @raise [ArgumentError] for an invalid status or header, or when headers
      #   or body exceed their limits
      # @raise [Exception] any error the app raises. Callers map it to 500.
      # @note Closes the body when it responds to close, also on error.
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

      # @param status [Integer] a status in REASONS
      # @return [Array(Integer, Hash{String => String}, String)] a plain-text
      #   response with the reason phrase as body
      # @raise [KeyError] when status is not in REASONS
      def self.error(status)
        text = REASONS.fetch(status)
        [status, {"Content-Type" => "text/plain", "Content-Length" => text.bytesize.to_s, "Connection" => "close"}, text]
      end
    end
  end
end
