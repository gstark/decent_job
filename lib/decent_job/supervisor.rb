# frozen_string_literal: true

module DecentJob
  # All forks and registry writes happen on the calling main thread. Probe
  # readers take a snapshot; delayed replacements never block signal handling.
  class Supervisor
    CONNECTED_TIMEOUT = 15
    MIN_HEALTHY_UPTIME = 10
    MAX_BACKOFF_DELAY = 30
    SIGNALS = %w[INT TERM QUIT CHLD].freeze

    class SubprocessHandle
      attr_reader :pid, :recipe, :index, :restart_count, :readiness_reader, :started_at

      def initialize(pid:, recipe:, index:, restart_count: 0, readiness_reader: nil)
        @pid, @recipe, @index = pid, recipe, index
        @restart_count, @readiness_reader = restart_count, readiness_reader
        @started_at = Supervisor.monotonic
      end

      def mark_ready(at)
        @last_ready_at = at
      end

      def connected?(now, timeout = CONNECTED_TIMEOUT)
        @last_ready_at && now - @last_ready_at <= timeout
      end

      def close_reader
        @readiness_reader&.close unless @readiness_reader&.closed?
        @readiness_reader = nil
      end
    end

    attr_reader :shutdown_timed_out

    def self.monotonic
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    def initialize(configuration = DecentJob.configuration)
      @configuration = configuration
      @configuration.validate!
      @recipes = @configuration.subprocess_configs
      raise ArgumentError, "supervisor requires at least one subprocess" if @recipes.empty?
      @mutex = Mutex.new
      @subprocesses = {}
      @pending = []
      @stopped = false
      @shutdown_timed_out = false
    end

    def subprocesses
      @mutex.synchronize { @subprocesses.dup }
    end

    def running?
      @supervisor_pid == Process.pid && !@stopped
    end

    def started?
      running? && subprocesses.size == @recipes.size
    end

    def connected?
      handles = subprocesses.values
      running? && handles.size == @recipes.size && handles.all? { |handle| handle.connected?(self.class.monotonic) }
    end

    def start(on_start: nil, on_shutdown: nil)
      raise ArgumentError, "supervisor must run on the main thread" unless Thread.current == Thread.main
      @supervisor_pid = Process.pid
      @reader, @writer = IO.pipe
      @previous_traps = {}
      original_title = $PROGRAM_NAME
      $PROGRAM_NAME = "decent_job supervisor [#{@recipes.size} subprocesses]"
      install_signal_handlers
      instrument("cluster_start", subprocesses: @recipes.size)
      @recipes.each_with_index do |recipe, index|
        break if @stopped
        spawn_subprocess(recipe, index: index)
      end
      start_probe unless @stopped
      on_start&.call unless @stopped
      until @stopped
        reap_children
        spawn_pending unless @stopped
        wait_for_wakeup unless @stopped
      end
    ensure
      failure = $!
      @stopped = true
      deadline = self.class.monotonic + ((failure || @immediate) ? 0 : @configuration.shutdown_timeout)
      begin
        begin
          on_shutdown&.call
        rescue Exception => error # standard:disable Lint/RescueException
          report_error(error)
        end
        terminate_children(deadline)
      ensure
        begin
          @shutdown_timed_out = true if @probe_server && !@probe_server.stop(timeout: [deadline - self.class.monotonic, 0].max)
        rescue Exception => error # standard:disable Lint/RescueException
          cleanup_failure = error
        ensure
          @previous_traps&.each { |signal, handler| Signal.trap(signal, handler) }
          @reader&.close unless @reader&.closed?
          @writer&.close unless @writer&.closed?
          $PROGRAM_NAME = original_title if original_title
        end
      end
      instrument_cleanup("cluster_shutdown", pid: @supervisor_pid)
      raise cleanup_failure if cleanup_failure && !failure
    end

    private

    def instrument(event, **payload)
      ActiveSupport::Notifications.instrument("#{event}.decent_job", payload)
    end

    def report_error(error)
      @configuration.on_thread_error&.call(error)
    rescue Exception # standard:disable Lint/RescueException
      nil
    end

    def instrument_cleanup(event, **payload)
      instrument(event, **payload)
    rescue Exception => error # standard:disable Lint/RescueException
      report_error(error)
    end

    def install_signal_handlers
      SIGNALS.each do |signal|
        @previous_traps[signal] = Signal.trap(signal) do
          @stopped = true unless signal == "CHLD"
          @immediate = true if signal == "QUIT"
          begin
            @writer.write_nonblock(".", exception: false)
          rescue IOError, Errno::EPIPE
            nil
          end
        end
      end
    end

    def spawn_subprocess(recipe, index:, restart_count: 0)
      reader, writer = IO.pipe
      pid = nil
      DecentJob.with_prepared_fork do
        pid = fork do
          status = 1
          begin
            # Reset traps before any application hook, and close descriptors owned
            # by the supervisor before starting child resources or application code.
            SIGNALS.each { |signal| Signal.trap(signal, "DEFAULT") }
            # A signal between fork and trap reset ran the inherited handler.
            # Preserve that stop request instead of booting a fresh child loop.
            exit! 0 if @stopped
            reader.close
            @reader.close
            @writer.close
            @subprocesses.each_value(&:close_reader)
            @probe_server&.close_socket
            DecentJob.after_supervisor_fork(recipe)
            status = Subprocess.new(configuration: recipe, index: index,
              supervisor_pid: @supervisor_pid, readiness_writer: writer).run ? 0 : 1
          rescue SignalException => error
            # During boot the child deliberately retains default traps. Ruby
            # delivers their termination as SignalException; this is an expected
            # stop request, not a failed boot or a timed-out drain.
            if %w[INT TERM QUIT].any? { |signal| Signal.list[signal] == error.signo }
              status = 0
            else
              report_error(error)
            end
          rescue Exception => error # standard:disable Lint/RescueException
            report_error(error)
          ensure
            writer.close unless writer.closed?
            exit! status
          end
        end
      end
      writer.close
      handle = SubprocessHandle.new(pid: pid, recipe: recipe, index: index,
        restart_count: restart_count, readiness_reader: reader)
      @mutex.synchronize { @subprocesses[pid] = handle }
      instrument("cluster_spawn", pid: pid, index: index, restart_count: restart_count)
    ensure
      writer&.close unless writer&.closed?
      reader&.close if !pid && !reader&.closed?
    end

    def reap_children
      subprocesses.each do |pid, handle|
        result = begin
          Process.wait2(pid, Process::WNOHANG)
        rescue Errno::ECHILD
          [pid, nil]
        end
        next unless result
        @mutex.synchronize { @subprocesses.delete(pid) }
        handle.close_reader
        status = result.last
        @shutdown_timed_out = true if @stopped && !@immediate && status&.exited? && !status.success?
        instrument_cleanup("cluster_reap", pid: pid, index: handle.index, status: status&.exitstatus, signal: status&.termsig)
        next if @stopped
        count = (self.class.monotonic - handle.started_at < MIN_HEALTHY_UPTIME) ? handle.restart_count + 1 : 0
        delay = (count < 2) ? 0 : [2**[count - 2, 5].min, MAX_BACKOFF_DELAY].min
        @pending << {recipe: handle.recipe, index: handle.index, restart_count: count, at: self.class.monotonic + delay}
        instrument("cluster_backoff", index: handle.index, restart_count: count, delay: delay)
      end
    end

    def spawn_pending
      due, @pending = @pending.partition { |entry| entry[:at] <= self.class.monotonic }
      due.each do |entry|
        break if @stopped
        spawn_subprocess(entry[:recipe], index: entry[:index], restart_count: entry[:restart_count])
      end
    end

    def wait_for_wakeup(timeout = nil)
      handles = subprocesses.values
      readers = handles.filter_map(&:readiness_reader)
      delay = timeout || [1, *@pending.map { |entry| [entry[:at] - self.class.monotonic, 0].max }].min
      ready = IO.select([@reader, *readers], nil, nil, delay)&.first || []
      ready.each do |reader|
        data = reader.read_nonblock(4096, exception: false)
        next if reader == @reader
        handle = handles.find { |candidate| candidate.readiness_reader == reader }
        if data.nil?
          handle&.close_reader
        elsif data.is_a?(String)
          handle&.mark_ready(self.class.monotonic)
        end
      end
    end

    def signal_children(signal)
      subprocesses.each_key do |pid|
        Process.kill(signal, pid)
      rescue Errno::ESRCH
        nil
      end
    end

    def terminate_children(deadline)
      signal_children(@immediate ? "QUIT" : "TERM")
      until subprocesses.empty?
        reap_children
        break if subprocesses.empty?
        remaining = deadline - self.class.monotonic
        break unless remaining.positive? && !@immediate
        wait_for_wakeup([remaining, 0.05].min)
      end
      return if subprocesses.empty?
      @shutdown_timed_out = true unless @immediate
      signal_children("KILL")
      subprocesses.each do |pid, handle|
        _, status = Process.wait2(pid)
        instrument_cleanup("cluster_reap", pid: pid, index: handle.index, status: status.exitstatus, signal: status.termsig)
      rescue Errno::ECHILD
        nil
      ensure
        handle.close_reader
        @mutex.synchronize { @subprocesses.delete(pid) }
      end
    end

    def start_probe
      return unless @configuration.probe_port
      @probe_server = ProbeServer.new(port: @configuration.probe_port, handler: @configuration.probe_handler,
        app: @configuration.probe_app || ProbeServer.cluster_app(self))
      @probe_server.start
    end
  end
end
