# frozen_string_literal: true

module DecentJob
  class ProbeServer
    class HealthcheckMiddleware
      def initialize(app, state: DecentJob)
        @app = app
        @state = state
      end

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
