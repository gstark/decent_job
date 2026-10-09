# frozen_string_literal: true

module DecentJob
  class Subprocess
    CHECK_INTERVAL = 1

    def initialize(configuration:, index: 0, supervisor_pid: Process.ppid, readiness_writer: nil)
      @configuration, @index, @supervisor_pid = configuration, index, supervisor_pid
      @readiness_writer = readiness_writer
      @stopped = false
    end

    def run
      perform
      @drained
    end

    private

    def perform
      $PROGRAM_NAME = "decent_job subprocess #{@index}"
      DecentJob.run_lifecycle_hooks(:before_subprocess_boot)
      @reader, @writer = IO.pipe
      @previous_traps = {}
      %w[INT TERM QUIT].each do |signal|
        @previous_traps[signal] = Signal.trap(signal) do
          @stopped = true
          @immediate = true if signal == "QUIT"
          begin
            @writer.write_nonblock(".", exception: false)
          rescue IOError, Errno::EPIPE
            nil
          end
        end
      end
      DecentJob.start unless @stopped
      ActiveSupport::Notifications.instrument("subprocess_start.decent_job", pid: Process.pid, index: @index)
      until @stopped || Process.ppid != @supervisor_pid || !DecentJob.started?
        report_readiness
        IO.select([@reader], nil, nil, CHECK_INTERVAL)
        @reader.read_nonblock(4096, exception: false)
      end
      true
    ensure
      begin
        ActiveSupport::Notifications.instrument("subprocess_shutdown_start.decent_job", pid: Process.pid, index: @index)
      rescue Exception => error # standard:disable Lint/RescueException
        DecentJob.report_thread_error(error)
      end
      begin
        @drained = DecentJob.shutdown(timeout: @immediate ? 0 : @configuration.shutdown_timeout)
        begin
          ActiveSupport::Notifications.instrument("subprocess_shutdown.decent_job", pid: Process.pid, index: @index)
        rescue Exception => error # standard:disable Lint/RescueException
          DecentJob.report_thread_error(error)
        end
      ensure
        @previous_traps&.each { |signal, handler| Signal.trap(signal, handler) }
        @readiness_writer&.close unless @readiness_writer&.closed?
        @reader&.close unless @reader&.closed?
        @writer&.close unless @writer&.closed?
      end
    end

    private

    def report_readiness
      return unless @readiness_writer && DecentJob.started? && DecentJob.connected?
      @readiness_writer.write_nonblock("1", exception: false)
    rescue IOError, Errno::EPIPE
      @stopped = true
    end
  end
end
