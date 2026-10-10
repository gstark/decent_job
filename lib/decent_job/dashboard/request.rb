# frozen_string_literal: true

require "uri"

module DecentJob
  class Dashboard
    # A small reader for the Rack environment. Only the pieces the dashboard
    # needs: method, path, query, form body, and cookies. Rack is not required.
    #
    # {Dashboard#call} builds one per request. Parsed values are memoized on
    # the instance. Use it on one thread only.
    class Request
      # Largest form body, in bytes, that {#form} accepts (1 MiB).
      MAX_BODY_BYTES = 1_048_576

      # env is the Rack environment. method is upper case. path is PATH_INFO,
      # or "/" when empty. script_name is the mount path without a trailing
      # "/". An empty script_name means the app is mounted at the root.
      attr_reader :env, :method, :path, :script_name

      # @param env [Hash{String => Object}] the Rack environment
      def initialize(env)
        @env = env
        @method = env.fetch("REQUEST_METHOD", "GET").upcase
        @script_name = env.fetch("SCRIPT_NAME", "").to_s.chomp("/")
        @path = env.fetch("PATH_INFO", "/").to_s
        @path = "/" if @path.empty?
      end

      # @return [Boolean] true for GET and HEAD
      def get?
        method == "GET" || method == "HEAD"
      end

      # @return [Boolean] true for POST
      def post?
        method == "POST"
      end

      # Query parameters. Repeated keys and keys ending in [] become arrays.
      #
      # @return [Hash{String => String, Array<String>}] an empty hash when the
      #   query string is malformed
      def query
        @query ||= decode(env.fetch("QUERY_STRING", "").to_s)
      end

      # The url-encoded form body of a POST, decoded like {#query}.
      #
      # @return [Hash{String => String, Array<String>}] an empty hash for
      #   other methods, other content types, or a malformed body
      # @raise [PayloadTooLarge] when the body is larger than {MAX_BODY_BYTES}
      # @note Reads rack.input and rewinds it when it can.
      def form
        @form ||= begin
          input = env["rack.input"]
          type = env.fetch("CONTENT_TYPE", "").to_s

          if post? && input && type.start_with?("application/x-www-form-urlencoded")
            body = input.read(MAX_BODY_BYTES + 1) || ""
            input.rewind if input.respond_to?(:rewind)
            raise PayloadTooLarge, "The request body is too large." if body.bytesize > MAX_BODY_BYTES
            decode(body)
          else
            {}
          end
        end
      end

      # @return [Hash{String => String}] decoded cookies. Malformed pairs are
      #   skipped.
      def cookies
        @cookies ||= env.fetch("HTTP_COOKIE", "").to_s.split(/;\s*/).each_with_object({}) do |pair, result|
          name, value = pair.split("=", 2)
          next unless name && value
          result[name.strip] = URI.decode_www_form_component(value.strip)
        rescue ArgumentError
          next
        end
      end

      private

      def decode(string)
        URI.decode_www_form(string).each_with_object({}) do |(key, value), result|
          if key.end_with?("[]")
            (result[key.delete_suffix("[]")] ||= []) << value
          elsif result.key?(key) && !result[key].is_a?(Array)
            result[key] = [result[key], value]
          elsif result.key?(key)
            result[key] << value
          else
            result[key] = value
          end
        end
      rescue ArgumentError
        {}
      end
    end
  end
end
