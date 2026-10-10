# frozen_string_literal: true

module DecentJob
  class ProbeServer
    # Serves the health routes and passes other paths to a fallback app.
    #
    # Routes: / and /status always return 200 (server liveness);
    # /status/started returns 200 or 503 from state.started?;
    # /status/connected returns 200 or 503 from state.connected?.
    #
    # It runs on the probe serving thread, so the state predicates must be
    # thread safe and must not do database work.
    #
    # See docs/health-services.md.
    class HealthcheckMiddleware
      # @param app [#call] fallback for other paths
      # @param state [#started?, #connected?] {DecentJob}, a {Worker}, or a
      #   {Supervisor}
      def initialize(app, state: DecentJob)
        @app = app
        @state = state
      end

      # @param env [Hash{String => String}] request environment; reads PATH_INFO
      # @return [Array(Integer, Hash, Array<String>)] status, headers, body
      def call(env)
        status, text = case env["PATH_INFO"]
        when "/", "/status"
          [200, "OK"]
        when "/status/started"
          @state.started? ? [200, "Started"] : [503, "Not started"]
        when "/status/connected"
          @state.connected? ? [200, "Connected"] : [503, "Not connected"]
        else
          return @app.call(env)
        end

        [status, {"Content-Type" => "text/plain; charset=utf-8"}, [text]]
      end
    end
  end
end
