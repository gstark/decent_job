# frozen_string_literal: true

require "decent_job"
require "optparse"

module DecentJob
  class CLI
    attr_reader :shutdown_timed_out

    def initialize(stdout: $stdout, stderr: $stderr)
      @stdout = stdout
      @stderr = stderr
    end

    def run(argv)
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
        DecentJob.configure do |configuration|
          options.each { |name, value| configuration.public_send("#{name}=", value) }
        end
      rescue ArgumentError => error
        raise OptionParser::InvalidArgument, error.message
      end
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

    private

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
        parser.on("--queues EXPRESSION", "Queue selection and optional thread pools") { |value| options[:queues] = value }
        parser.on("--max-threads N", Integer, "Default threads per queue pool") { |value| options[:max_threads] = value }
        parser.on("--poll-interval N", Float, "Polling interval in seconds") { |value| options[:poll_interval] = value }
        parser.on("--shutdown-timeout N", Float, "Time allowed for running jobs to finish") { |value| options[:shutdown_timeout] = value }
        parser.on("--[no-]enable-cron", "Enable recurring jobs") { |value| options[:enable_cron] = value }
        parser.on("--cron-graceful-restart-period N", Float, "Replay missed recurring jobs from this many seconds ago") { |value| options[:cron_graceful_restart_period] = value }
        parser.on("--[no-]listen-notify", "Enable PostgreSQL notification wakeups") { |value| options[:enable_listen_notify] = value }
        parser.on("-h", "--help", "Print help") { options[:command] = "help" }
        parser.on("-v", "--version", "Print version") { options[:command] = "version" }
      end
    end

    def start
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
        DecentJob.start
        service = SystemdService.new
        service.start
        @stdout.puts "DecentJob started. Send SIGINT or SIGTERM to stop."
        @stdout.flush
        reader.read(1)
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

    def remaining_time(deadline)
      [deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC), 0].max
    end
  end
end
