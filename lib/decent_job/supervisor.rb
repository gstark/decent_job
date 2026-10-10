# frozen_string_literal: true

module DecentJob
  # Forks one worker process per subprocess recipe, replaces children that
  # exit, and stops them all on shutdown.
  #
  # {CLI} creates the supervisor in cluster mode and calls {#start} on the main
  # thread. Each recipe is a child {Configuration} from
  # {Configuration#subprocess_configs}, and each child runs a {Subprocess}.
  # The supervisor keeps a stable slot (the recipe index) for each recipe, so
  # a replacement child takes the slot of the child it replaces.
  #
  # Ownership: all forks, reaps, and registry writes happen on the main thread
  # that called {#start}. The optional {ProbeServer} thread only reads the
  # registry through {#subprocesses}, {#started?}, and {#connected?}, which
  # take a snapshot under the mutex. Delayed replacements wait in a pending
  # list, never in a sleep, so they never block signal handling.
  #
  # Signals: the supervisor traps INT, TERM, QUIT, and CHLD. INT and TERM
  # start a graceful stop, QUIT starts an immediate stop, and CHLD only wakes
  # the loop to reap. The trap handlers only set flags and write one byte to a
  # self-pipe, because a trap context cannot take a Mutex or do blocking IO.
  #
  # Readiness: each child writes to its own readiness pipe about once a second
  # while its workers are started and connected. A child counts as connected
  # for {CONNECTED_TIMEOUT} seconds after its last report.
  #
  # See docs/supervisor.md and docs/shutdown.md.
  class Supervisor
    # Seconds a readiness report stays fresh for {#connected?}.
    CONNECTED_TIMEOUT = 15
    # Seconds a child must live before its exit resets the restart backoff.
    MIN_HEALTHY_UPTIME = 10
    # Upper limit in seconds for one restart delay.
    MAX_BACKOFF_DELAY = 30
    # Exit status of a child whose drain timed out. Other failures exit with 1,
    # so the supervisor reports a timeout only for this status.
    DRAIN_TIMEOUT_STATUS = 3
    # Signals the supervisor traps. Children reset these to DEFAULT after fork.
    SIGNALS = %w[INT TERM QUIT CHLD].freeze

    # The supervisor's record of one live child process.
    #
    # Only the supervisor main thread creates and mutates handles. The probe
    # thread can read a handle from a {Supervisor#subprocesses} snapshot.
    class SubprocessHandle
      # @return [Integer] the child OS pid
      # @return [Configuration] the recipe the child runs
      # @return [Integer] the stable slot index of the recipe
      # @return [Integer] count of consecutive short-lived exits in this slot
      # @return [IO, nil] read end of the readiness pipe, nil after close
      # @return [Float] monotonic time in seconds when the handle was created
      attr_reader :pid, :recipe, :index, :restart_count, :readiness_reader, :started_at

      # @param pid [Integer] the forked child pid
      # @param recipe [Configuration] the child configuration
      # @param index [Integer] the stable slot index
      # @param restart_count [Integer] consecutive short-lived exits before this fork
      # @param readiness_reader [IO, nil] read end of the child's readiness pipe
      def initialize(pid:, recipe:, index:, restart_count: 0, readiness_reader: nil)
        @pid, @recipe, @index = pid, recipe, index
        @restart_count, @readiness_reader = restart_count, readiness_reader
        @started_at = Supervisor.monotonic
      end

      # Records a readiness report from the child.
      #
      # @param at [Float] monotonic time in seconds
      # @return [Float] the recorded time
      def mark_ready(at)
        @last_ready_at = at
      end

      # Reports whether the child sent a readiness report recently.
      #
      # @param now [Float] monotonic time in seconds
      # @param timeout [Numeric] report age limit in seconds
      # @return [Boolean, nil] nil when the child never reported
      def connected?(now, timeout = CONNECTED_TIMEOUT)
        @last_ready_at && now - @last_ready_at <= timeout
      end

      # Closes the readiness pipe. Safe to call more than once.
      #
      # @return [nil]
      # @note Forked children call this on every inherited handle so they do
      #   not keep sibling pipes open.
      def close_reader
        @readiness_reader&.close unless @readiness_reader&.closed?
        @readiness_reader = nil
      end
    end

    # @return [Boolean] true when a graceful stop did not finish in time.
    #   This includes children that needed SIGKILL, children that exited
    #   with {DRAIN_TIMEOUT_STATUS} during a graceful stop, and a probe
    #   server that did not stop by the deadline. A child that fails for
    #   another reason does not count. An
    #   immediate (QUIT) stop does not set it for child exits.
    attr_reader :shutdown_timed_out

    # @return [Float] monotonic clock time in seconds
    # @api private
    def self.monotonic
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    # @param configuration [Configuration] the parent configuration. It must
    #   produce at least one recipe from {Configuration#subprocess_configs}.
    # @raise [ArgumentError] when the configuration is invalid or has no recipes
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

    # Returns a snapshot of the live children.
    #
    # @return [Hash{Integer => SubprocessHandle}] handles keyed by child pid
    # @note Thread safe. The probe thread calls this.
    def subprocesses
      @mutex.synchronize { @subprocesses.dup }
    end

    # Reports whether this process is the supervisor and no stop has started.
    #
    # @return [Boolean] false in forked children, before {#start}, and after a
    #   stop signal
    def running?
      @supervisor_pid == Process.pid && !@stopped
    end

    # Reports whether every recipe slot has a live child.
    #
    # Backs the /status/started probe route.
    #
    # @return [Boolean] false while a slot waits for a delayed replacement
    def started?
      running? && subprocesses.size == @recipes.size
    end

    # Reports whether every slot has a child with a fresh readiness report.
    #
    # Backs the /status/connected probe route. It reads only in-memory state.
    #
    # @return [Boolean]
    def connected?
      handles = subprocesses.values
      running? && handles.size == @recipes.size && handles.all? { |handle| handle.connected?(self.class.monotonic) }
    end

    # Runs the cluster until a stop signal arrives, then stops all children.
    #
    # The call forks every child, starts the probe server when
    # {Configuration#probe_port} is set, calls on_start, and then loops to
    # reap, replace, and read readiness reports. It returns only after shutdown.
    #
    # Shutdown order: on_shutdown, then TERM (or QUIT for an immediate stop) to
    # every child, then SIGKILL to children still alive at the deadline, then
    # probe server stop, then trap and process title restore. All steps share
    # one deadline of {Configuration#shutdown_timeout} seconds. The deadline
    # is zero after QUIT or when {#start} raises.
    #
    # @param on_start [#call, nil] called once on the main thread after the
    #   initial forks and probe start, unless a stop already arrived
    # @param on_shutdown [#call, nil] called first during shutdown, before
    #   children get a signal. Errors go to {Configuration#on_thread_error}.
    # @return [void]
    # @raise [ArgumentError] when called off the main thread
    # @raise [Exception] a cleanup failure, when no earlier error is propagating
    # @note Installs process-wide traps for INT, TERM, QUIT, and CHLD, and
    #   restores the previous handlers before return. Sets $PROGRAM_NAME.
    # @note Must run on the main thread because Ruby runs trap handlers there.
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
      Notifications.instrument("#{event}.decent_job", payload)
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

    # Trap handlers run in trap context: no Mutex, no logging, no blocking IO.
    # They set flags and wake {#wait_for_wakeup} through the self-pipe.
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

    # Forks one child inside {DecentJob.with_prepared_fork}, which drains
    # library resources and runs before_supervisor_fork hooks first. The child
    # never returns: it always leaves through exit! so it skips at_exit
    # handlers inherited from the supervisor. Exit status 0 means the child
    # drained or got a stop signal during boot; {DRAIN_TIMEOUT_STATUS} means
    # a drain timeout; 1 means any other failure.
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
              supervisor_pid: @supervisor_pid, readiness_writer: writer).run ? 0 : DRAIN_TIMEOUT_STATUS
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

    # Reaps exited children without blocking and queues a replacement for each
    # slot unless a stop has started. A child that lived less than
    # MIN_HEALTHY_UPTIME increments the slot's restart count. The delay is 0
    # for the first two counts, then 1, 2, 4, 8, 16, and 30 seconds.
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
        @shutdown_timed_out = true if @stopped && !@immediate && status&.exitstatus == DRAIN_TIMEOUT_STATUS
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

    # Blocks on the self-pipe and all readiness pipes. Without a timeout it
    # wakes after at most one second, or sooner when a pending replacement is
    # due. EOF on a readiness pipe means the child closed it, so the reader is
    # closed; any data marks the child ready.
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

    # Sends TERM (QUIT for an immediate stop), reaps until the deadline, then
    # sends SIGKILL to survivors and reaps them with a blocking wait. An
    # immediate stop does one reap pass and then kills at once.
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

    # The probe starts after the initial forks, so only replacement children
    # inherit its socket. Each child closes that copy after fork.
    def start_probe
      return unless @configuration.probe_port
      @probe_server = ProbeServer.new(port: @configuration.probe_port, handler: @configuration.probe_handler,
        app: @configuration.probe_app || ProbeServer.cluster_app(self))
      @probe_server.start
    end
  end
end
