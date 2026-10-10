# frozen_string_literal: true

require "decent_job/probe_server/healthcheck_middleware"
require "decent_job/probe_server/response"
require "decent_job/probe_server/simple_handler"

module DecentJob
  # A small HTTP server for health probes, with no Rack dependency.
  #
  # {CLI} starts one in the single-process CLI, and {Supervisor} starts one in
  # the supervisor process. The handler does the network work:
  # {SimpleHandler} by default, or {WebrickHandler} when WEBrick is
  # available and requested. The app is a Rack-like callable, by default
  # {HealthcheckMiddleware} over a 404 fallback.
  #
  # Ownership: the handler owns one serving thread and the listening socket.
  # The socket binds to 0.0.0.0. Health routes read only in-memory state, so
  # a probe request never uses a database connection.
  #
  # Fork safety: a forked child must call {#close_socket}, not {#stop}, so it
  # never joins or shuts down the parent's serving thread or socket.
  #
  # See docs/health-services.md.
  class ProbeServer
    # Builds the default app: health routes over a 404 fallback.
    #
    # @param state [#started?, #connected?] the readiness source
    # @return [HealthcheckMiddleware]
    def self.default_app(state: DecentJob)
      HealthcheckMiddleware.new(->(_) { [404, {"Content-Type" => "text/plain"}, ["Not found"]] }, state: state)
    end

    # Builds the default app with cluster readiness.
    #
    # @param supervisor [Supervisor] the readiness source
    # @return [HealthcheckMiddleware]
    def self.cluster_app(supervisor)
      default_app(state: supervisor)
    end

    # Builds the server. It does not bind until {#start}.
    #
    # @param port [Integer] TCP port, 0 to let the OS choose one
    # @param handler [Symbol, String, nil] :simple, :webrick, or nil for
    #   :simple. Unknown values and missing WEBrick warn and fall back to
    #   :simple.
    # @param app [#call, nil] the callable, or nil for {.default_app}. A custom
    #   app replaces the health routes.
    # @param state [#started?, #connected?] the readiness source for the
    #   default app
    # @param request_timeout [Numeric] seconds allowed to read a request
    # @param response_timeout [Numeric] seconds allowed to write a response
    # @param on_thread_error [#call, nil] receives app and serving errors
    # @raise [ArgumentError] for an invalid port, timeout, or app
    def initialize(port:, handler: nil, app: nil, state: DecentJob, request_timeout: 1, response_timeout: 1,
      on_thread_error: DecentJob.configuration.on_thread_error)
      unless port.is_a?(Integer) && (0..65_535).cover?(port)
        raise ArgumentError, "probe port must be an integer between 0 and 65535"
      end

      [request_timeout, response_timeout].each do |timeout|
        raise ArgumentError, "probe timeouts must be finite and positive" unless timeout.is_a?(Numeric) && timeout.real? && timeout.finite? && timeout.positive?
      end

      raise ArgumentError, "probe app must be callable" if app && !app.respond_to?(:call)
      @on_thread_error = on_thread_error
      klass = handler_class(handler)
      @handler = klass.new(app || self.class.default_app(state: state), port: port,
        request_timeout: request_timeout, response_timeout: response_timeout, report: method(:report_error))
    end

    # Binds the socket and starts the serving thread.
    #
    # @return [ProbeServer] self. A second call while running does nothing.
    # @raise [SystemCallError] when the bind fails, for example EADDRINUSE
    def start
      @handler.start
      self
    end

    # Closes the sockets and waits for the serving thread.
    #
    # It does not interrupt an app callback that is still running. Call stop
    # again after the callback returns.
    #
    # @param timeout [Numeric, nil] seconds to wait, or nil to wait forever
    # @return [Boolean] true when the serving thread exited or this process
    #   does not own it. False on timeout or when called from the serving
    #   thread.
    # @raise [ArgumentError] when timeout is negative or not finite
    def stop(timeout: nil)
      if !timeout.nil? && (!timeout.is_a?(Numeric) || !timeout.real? || !timeout.finite? || timeout.negative?)
        raise ArgumentError, "timeout must be finite and nonnegative"
      end

      @handler.stop(timeout: timeout)
    end

    # @return [Boolean] true when this process owns a live serving thread and
    #   no stop has started
    def running?
      @handler.running?
    end

    # Closes this process's copies of the sockets without joining any thread.
    #
    # @return [ProbeServer] self
    # @note {Supervisor} calls this in every forked child.
    def close_socket
      @handler.close_socket
      self
    end

    # @return [Integer] the bound port after {#start}, else the configured port
    def port
      @handler.port
    end

    private

    def handler_class(handler)
      case handler&.to_sym
      when nil, :simple
        SimpleHandler
      when :webrick
        begin
          require "webrick"
          require "decent_job/probe_server/webrick_handler"
          WebrickHandler
        rescue LoadError
          warn 'probe_handler: :webrick is specified but WEBrick is not in the load path; using the built-in server. Add gem "webrick" to your Gemfile.'
          SimpleHandler
        end
      else
        warn "probe_handler: #{handler.inspect} is not supported; using the built-in server."
        SimpleHandler
      end
    end

    def report_error(error)
      @on_thread_error&.call(error)
    rescue Exception # standard:disable Lint/RescueException
      nil
    end
  end
end
