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
      unless %w[start migrate help version].include?(command) && arguments.empty?
        raise OptionParser::InvalidArgument, "expected start, migrate, help, or version"
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

      applications.each { |file| require File.expand_path(file) }
      DecentJob.configure do |configuration|
        options.each { |name, value| configuration.public_send("#{name}=", value) }
      end
      if command == "migrate"
        DecentJob.install
        @stdout.puts "DecentJob schema installed."
        0
      else
        start
      end
    rescue OptionParser::ParseError, ArgumentError => error
      @stderr.puts "Error: #{error.message}"
      2
    rescue LoadError, StandardError => error
      @stderr.puts "Error: #{error.message}"
      1
    end

    private

    def option_parser(options, applications)
      OptionParser.new do |parser|
        parser.banner = "Usage: decent_job [start|migrate|help|version] [options]"
        parser.separator "Database: DECENT_JOB_DATABASE_URL or DATABASE_URL"
        parser.on("-r", "--require FILE", "Load application jobs and configuration") { |file| applications << file }
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
      %w[INT TERM].each do |signal|
        previous_traps[signal] = Signal.trap(signal) { writer.write_nonblock(".", exception: false) }
      end
      started = true
      DecentJob.start
      @stdout.puts "DecentJob started. Send SIGINT or SIGTERM to stop."
      @stdout.flush
      reader.read(1)
      shutdown_attempted = true
      @shutdown_timed_out = !DecentJob.shutdown(timeout: DecentJob.configuration.shutdown_timeout)
      if @shutdown_timed_out
        @stderr.puts "Error: shutdown timed out; jobs or cron callbacks are still running."
        1
      else
        @stdout.puts "DecentJob stopped."
        0
      end
    ensure
      begin
        DecentJob.shutdown(timeout: DecentJob.configuration.shutdown_timeout) if started && !shutdown_attempted
      ensure
        previous_traps&.each { |signal, handler| Signal.trap(signal, handler) }
        reader&.close
        writer&.close
      end
    end
  end
end
