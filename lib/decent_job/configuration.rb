# frozen_string_literal: true

require "logger"

module DecentJob
  class Configuration
    attr_accessor :database_url, :execution_mode, :queues, :max_threads,
      :poll_interval, :enable_listen_notify, :shutdown_timeout, :logger, :on_thread_error, :enable_pauses

    def initialize
      @database_url = ENV["DECENT_JOB_DATABASE_URL"] || ENV["DATABASE_URL"]
      @execution_mode = ENV.fetch("DECENT_JOB_EXECUTION_MODE", "external").to_sym
      @queues = ENV.fetch("DECENT_JOB_QUEUES", "*")
      @max_threads = Integer(ENV.fetch("DECENT_JOB_MAX_THREADS", "5"))
      @poll_interval = Float(ENV.fetch("DECENT_JOB_POLL_INTERVAL", "1"))
      @enable_listen_notify = ENV.fetch("DECENT_JOB_ENABLE_LISTEN_NOTIFY", "true") != "false"
      @enable_pauses = ENV.fetch("DECENT_JOB_ENABLE_PAUSES", "false") == "true"
      @shutdown_timeout = Float(ENV.fetch("DECENT_JOB_SHUTDOWN_TIMEOUT", "30"))
      @logger = Logger.new($stdout)
      @on_thread_error = ->(error) { @logger.error("#{error.class}: #{error.message}") }
    end

    def validate!
      raise ArgumentError, "execution_mode must be external, inline, or async" unless %i[external inline async].include?(@execution_mode)
      QueueSelection.pools(@queues, default_threads: @max_threads)
      raise ArgumentError, "poll_interval must be positive" unless @poll_interval.is_a?(Numeric) && @poll_interval.positive? && @poll_interval.finite?
      raise ArgumentError, "shutdown_timeout must be nonnegative" unless @shutdown_timeout.is_a?(Numeric) && @shutdown_timeout >= 0 && @shutdown_timeout.finite?
      raise ArgumentError, "on_thread_error must be callable" unless @on_thread_error.respond_to?(:call)
      self
    end

    def pool_size
      QueueSelection.pools(@queues, default_threads: @max_threads).sum { |pool| pool.fetch(:threads) } + 2
    end

    def worker_options
      {queues: @queues, max_threads: @max_threads, poll_interval: @poll_interval,
       enable_listen_notify: @enable_listen_notify, on_thread_error: @on_thread_error}
    end
  end
end
