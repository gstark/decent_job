# frozen_string_literal: true

require "decent_job/probe_server/healthcheck_middleware"
require "decent_job/probe_server/response"
require "decent_job/probe_server/simple_handler"

module DecentJob
  class ProbeServer
    def self.default_app(state: DecentJob)
      HealthcheckMiddleware.new(->(_) { [404, {"Content-Type" => "text/plain"}, ["Not found"]] }, state: state)
    end

    def self.cluster_app(supervisor)
      default_app(state: supervisor)
    end

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

    def start
      @handler.start
      self
    end

    def stop(timeout: nil)
      if !timeout.nil? && (!timeout.is_a?(Numeric) || !timeout.real? || !timeout.finite? || timeout.negative?)
        raise ArgumentError, "timeout must be finite and nonnegative"
      end

      @handler.stop(timeout: timeout)
    end

    def running?
      @handler.running?
    end

    def close_socket
      @handler.close_socket
      self
    end

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
