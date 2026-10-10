# frozen_string_literal: true

module DecentJob
  # Runs the worker runtime inside one supervised child process.
  #
  # {Supervisor} forks the child and calls {#run} there, after
  # {DecentJob.after_supervisor_fork} has reset the inherited runtime. The
  # child runs before_subprocess_boot hooks, starts {DecentJob.start}, and
  # reports readiness to the supervisor until a stop.
  #
  # Ownership: one object per child process, used only on the child's main
  # thread. It owns a self-pipe for its trap handlers and the write end of the
  # readiness pipe.
  #
  # Signals: boot hooks run with DEFAULT traps for INT, TERM, and QUIT, so a
  # signal during a boot hook ends the child at once. After the hooks, INT and
  # TERM request a graceful drain and QUIT requests a drain with a zero
  # timeout. The handlers only set flags and write to the self-pipe, because a
  # trap context cannot take a Mutex.
  #
  # The loop also stops when the parent pid changes (the supervisor died) or
  # when {DecentJob.started?} turns false (for example, all execution threads
  # died). The supervisor then forks a replacement, unless it is gone.
  #
  # See docs/supervisor.md.
  class Subprocess
    # Seconds between parent checks and readiness reports.
    CHECK_INTERVAL = 1

    # @param configuration [Configuration] the child recipe. Only
    #   {Configuration#shutdown_timeout} is read here.
    # @param index [Integer] the supervisor slot index, used in the process title
    # @param supervisor_pid [Integer] the parent pid. A different
    #   Process.ppid means the child is orphaned and must drain.
    # @param readiness_writer [IO, nil] write end of the readiness pipe, or nil
    #   to skip readiness reports
    def initialize(configuration:, index: 0, supervisor_pid: Process.ppid, readiness_writer: nil)
      @configuration, @index, @supervisor_pid = configuration, index, supervisor_pid
      @readiness_writer = readiness_writer
      @stopped = false
    end

    # Boots the runtime, waits for a stop, and drains.
    #
    # @return [Boolean] true when {DecentJob.shutdown} drained everything in
    #   time, false when the drain timed out. The supervisor maps this to
    #   exit status 0 or {Supervisor::DRAIN_TIMEOUT_STATUS}.
    # @note Sets $PROGRAM_NAME, installs INT, TERM, and QUIT traps, and
    #   restores the previous traps before return. Closes the readiness pipe
    #   on return, which the supervisor sees as EOF.
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
      Notifications.instrument("subprocess_start.decent_job", pid: Process.pid, index: @index)
      until @stopped || Process.ppid != @supervisor_pid || !DecentJob.started?
        report_readiness
        IO.select([@reader], nil, nil, CHECK_INTERVAL)
        @reader.read_nonblock(4096, exception: false)
      end
      true
    ensure
      begin
        Notifications.instrument("subprocess_shutdown_start.decent_job", pid: Process.pid, index: @index)
      rescue Exception => error # standard:disable Lint/RescueException
        DecentJob.report_thread_error(error)
      end
      begin
        @drained = DecentJob.shutdown(timeout: @immediate ? 0 : @configuration.shutdown_timeout)
        begin
          Notifications.instrument("subprocess_shutdown.decent_job", pid: Process.pid, index: @index)
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

    # Writes one byte only while workers are started and connected. A broken
    # pipe means the supervisor is gone, so the child stops.
    def report_readiness
      return unless @readiness_writer && DecentJob.started? && DecentJob.connected?
      @readiness_writer.write_nonblock("1", exception: false)
    rescue IOError, Errno::EPIPE
      @stopped = true
    end
  end
end
