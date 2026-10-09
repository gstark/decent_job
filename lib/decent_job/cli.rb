# frozen_string_literal: true

require "decent_job"
require "optparse"

module DecentJob
  class CLI
    class << self
      attr_accessor :within_exe, :log_to_stdout
      alias_method :within_exe?, :within_exe

      def log_to_stdout?
        log_to_stdout.nil? ? within_exe? : log_to_stdout
      end
    end

    attr_reader :shutdown_timed_out

    def initialize(stdout: $stdout, stderr: $stderr)
      @stdout = stdout
      @stderr = stderr
    end

    def run(argv)
      DecentJob.with_cli_application { run_command(argv) }
    end

    private

    def run_command(argv)
      @shutdown_timed_out = false
      options = {}
      applications = []
      parser = option_parser(options, applications)
      arguments = argv.dup
      parser.parse!(arguments)
      command = arguments.shift || "start"
      unless %w[start migrate cleanup_preserved_jobs cleanup help version].include?(command) && arguments.empty?
        raise OptionParser::InvalidArgument, "expected start, migrate, cleanup_preserved_jobs, help, or version"
      end
      command = options.delete(:command) || command
      case command
      when "help"
        @stdout.puts parser
        return 0
      when "version"
        @stdout.puts "decent_job #{DecentJob::VERSION}"
        return 0
      end

      cleanup_options = options.delete(:cleanup) || {}
      applications.each { |file| require File.expand_path(file) }
      begin
        DecentJob.apply_cli_configuration(options)
      rescue ArgumentError => error
        raise OptionParser::InvalidArgument, error.message
      end
      set_up_logging
      if %w[cleanup_preserved_jobs cleanup].include?(command)
        total = DecentJob.cleanup_preserved_jobs(**cleanup_options)
        @stdout.puts "Deleted #{total} preserved records (jobs, executions, and batches)."
        0
      elsif command == "migrate"
        DecentJob.install
        @stdout.puts "DecentJob schema installed."
        0
      else
        start
      end
    rescue OptionParser::ParseError => error
      @stderr.puts "Error: #{error.message}"
      2
    rescue LoadError, StandardError => error
      @stderr.puts "Error: #{error.message}"
      1
    end

    def set_up_logging
      return unless self.class.log_to_stdout?

      @stdout.sync = true if @stdout.respond_to?(:sync=)
      destinations = LogSubscriber.loggers
      return if destinations.any? { |logger| logger_outputs_to_stdout?(logger) }

      destinations << ActiveSupport::TaggedLogging.new(ActiveSupport::Logger.new(@stdout))
      LogSubscriber.reset_logger
    end

    def logger_outputs_to_stdout?(logger)
      if logger.is_a?(ActiveSupport::BroadcastLogger)
        logger.broadcasts.any? { |destination| logger_outputs_to_stdout?(destination) }
      else
        ActiveSupport::Logger.logger_outputs_to?(logger, @stdout)
      end
    end

    def option_parser(options, applications)
      OptionParser.new do |parser|
        parser.banner = "Usage: decent_job [start|migrate|cleanup_preserved_jobs|help|version] [options]"
        parser.separator "Database: DECENT_JOB_DATABASE_URL or DATABASE_URL"
        parser.on("-r", "--require FILE", "Load application jobs and configuration") { |file| applications << file }
        parser.on("--older-than SECONDS", "--before-seconds-ago SECONDS", Float, "Delete preserved records at least this old") { |value| options[:cleanup_preserved_jobs_before_seconds_ago] = value }
        parser.on("--in-batches-of N", Integer, "Cleanup page size") do |value|
          raise OptionParser::InvalidArgument, "in-batches-of must be positive" unless value.positive?
          (options[:cleanup] ||= {})[:in_batches_of] = value
        end
        parser.on("--[no-]include-discarded", "Include discarded preserved jobs and batches") { |value| options[:cleanup_discarded_jobs] = value }
        parser.on("--probe-port PORT", Integer, "Serve health probes on this port (0 chooses an available port)") { |value| options[:probe_port] = value }
        parser.on("--probe-handler HANDLER", "Probe server: simple or optional webrick") { |value| options[:probe_handler] = value }
        parser.on("--[no-]daemonize", "Detach and run in the background") { |value| options[:daemonize] = value }
        parser.on("--pidfile PATH", "PID file for daemon mode") { |value| options[:pidfile] = value }
        parser.on("--idle-timeout SECONDS", Float, "Exit after this many idle seconds (single process)") { |value| options[:idle_timeout] = value }
        parser.on("--dequeue-query-sort COLUMN", "Order candidates by created_at (default) or scheduled_at") { |value| options[:dequeue_query_sort] = value }
        parser.on("--lock-strategy STRATEGY", "Lock jobs using advisory (default), skiplocked, or hybrid") { |value| options[:lock_strategy] = value }
        parser.on("--queue-select-limit N", "Maximum candidates per dequeue (default: 1000)") { |value| options[:queue_select_limit] = value }
        parser.on("--queues EXPRESSION", "Queue selection and optional thread pools") { |value| options[:queues] = value }
        parser.on("--subprocesses N", Integer, "Fork and supervise N worker processes") { |value| options[:subprocesses] = value }
        parser.on("--max-threads N", Integer, "Default threads per queue pool") { |value| options[:max_threads] = value }
        parser.on("--[no-]lower-thread-priority", "Lower execution thread priority") { |value| options[:lower_thread_priority] = value }
        parser.on("--max-cache N", Integer, "Maximum cached future deadlines per queue pool") { |value| options[:max_cache] = value }
        parser.on("--poll-interval N", Float, "Polling interval in seconds (nonpositive disables polling)") { |value| options[:poll_interval] = value }
        parser.on("--shutdown-timeout N", Float, "Time allowed for running jobs to finish") { |value| options[:shutdown_timeout] = value }
        parser.on("--[no-]enable-cron", "Enable recurring jobs") { |value| options[:enable_cron] = value }
        parser.on("--cron-graceful-restart-period N", Float, "Replay missed recurring jobs from this many seconds ago") { |value| options[:cron_graceful_restart_period] = value }
        parser.on("--[no-]listen-notify", "Enable PostgreSQL notification wakeups") { |value| options[:enable_listen_notify] = value }
        parser.on("-h", "--help", "Print help") { options[:command] = "help" }
        parser.on("-v", "--version", "Print version") { options[:command] = "version" }
      end
    end

    def start
      if DecentJob.configuration.daemonize
        daemon = Daemon.new(configuration: DecentJob.configuration)
        daemon.daemonize
      end
      start_workers
    ensure
      original_failure = $!
      begin
        daemon&.cleanup
      rescue Exception # standard:disable Lint/RescueException
        raise unless original_failure
      end
    end

    def start_workers
      configuration = DecentJob.configuration
      if configuration.subprocesses.positive?
        if Process.respond_to?(:fork)
          configuration.logger.warn("idle_timeout is ignored when running subprocesses") unless configuration.idle_timeout.nil?
          return start_cluster
        end
        @stderr.puts "Warning: fork is unavailable; running queue groups in one process."
        DecentJob.apply_cli_configuration(queues: configuration.single_process_config.queues, subprocesses: 0)
      end
      reader, writer = IO.pipe
      previous_traps = {}
      begin
        %w[INT TERM].each do |signal|
          previous_traps[signal] = Signal.trap(signal) { writer.write_nonblock(".", exception: false) }
        end
        configuration = DecentJob.configuration
        if configuration.probe_port
          probe = ProbeServer.new(port: configuration.probe_port, handler: configuration.probe_handler, app: configuration.probe_app)
          probe.start
        end
        worker_started = true
        worker = DecentJob.start
        service = SystemdService.new
        service.start
        @stdout.puts "DecentJob started. Send SIGINT or SIGTERM to stop."
        @stdout.flush
        loop do
          break if IO.select([reader], nil, nil, 0.1)
          break if worker.shutdown? || (!configuration.idle_timeout.nil? && worker.idle?(configuration.idle_timeout))
        end
      ensure
        original_failure = $!
        cleanup_failure = nil
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + DecentJob.configuration.shutdown_timeout
        actions = [
          -> { service&.stopping },
          -> { @shutdown_timed_out = !DecentJob.shutdown(timeout: remaining_time(deadline)) if worker_started },
          -> { @shutdown_timed_out = true if probe && !probe.stop(timeout: remaining_time(deadline)) },
          -> { service&.stop },
          *previous_traps.map { |signal, handler| -> { Signal.trap(signal, handler) } },
          -> { reader.close },
          -> { writer.close }
        ]
        actions.each do |action|
          action.call
        rescue Exception => error # standard:disable Lint/RescueException
          cleanup_failure ||= error
        end
        raise cleanup_failure if cleanup_failure && !original_failure
      end
      if @shutdown_timed_out
        @stderr.puts "Error: shutdown timed out; jobs, cron callbacks, or probe callbacks are still running."
        1
      else
        @stdout.puts "DecentJob stopped."
        0
      end
    end

    def start_cluster
      supervisor = Supervisor.new
      service = nil
      begin
        supervisor.start(
          on_start: -> {
            service = SystemdService.new
            service.start
            @stdout.puts "DecentJob started. Send SIGINT or SIGTERM to stop."
            @stdout.flush
          },
          on_shutdown: -> { service&.stopping }
        )
      ensure
        @shutdown_timed_out = supervisor.shutdown_timed_out
        service&.stop
      end
      if @shutdown_timed_out
        @stderr.puts "Error: shutdown timed out; subprocesses or probe callbacks did not drain."
        1
      else
        @stdout.puts "DecentJob stopped."
        0
      end
    end

    def remaining_time(deadline)
      [deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC), 0].max
    end
  end
end
