# frozen_string_literal: true

require "uri"

module DecentJob
  class Dashboard
    # A small reader for the Rack environment. Only the pieces the dashboard
    # needs: method, path, query, form body, and cookies. Rack is not required.
    class Request
      MAX_BODY_BYTES = 1_048_576

      attr_reader :env, :method, :path, :script_name

      def initialize(env)
        @env = env
        @method = env.fetch("REQUEST_METHOD", "GET").upcase
        @script_name = env.fetch("SCRIPT_NAME", "").to_s.chomp("/")
        @path = env.fetch("PATH_INFO", "/").to_s
        @path = "/" if @path.empty?
      end

      def get?
        method == "GET" || method == "HEAD"
      end

      def post?
        method == "POST"
      end

      # Query parameters. Repeated keys and keys ending in [] become arrays.
      def query
        @query ||= decode(env.fetch("QUERY_STRING", "").to_s)
      end

      def form
        @form ||= begin
          input = env["rack.input"]
          type = env.fetch("CONTENT_TYPE", "").to_s
          if post? && input && type.start_with?("application/x-www-form-urlencoded")
            body = input.read(MAX_BODY_BYTES + 1) || ""
            input.rewind if input.respond_to?(:rewind)
            raise ArgumentError, "request body too large" if body.bytesize > MAX_BODY_BYTES
            decode(body)
          else
            {}
          end
        end
      end

      def params
        @params ||= query.merge(form)
      end

      def cookies
        @cookies ||= env.fetch("HTTP_COOKIE", "").to_s.split(/;\s*/).each_with_object({}) do |pair, result|
          name, value = pair.split("=", 2)
          next unless name && value
          result[name.strip] = URI.decode_www_form_component(value.strip)
        rescue ArgumentError
          next
        end
      end

      def ajax?
        env["HTTP_ACCEPT"].to_s.include?("application/json")
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
