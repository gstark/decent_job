# frozen_string_literal: true

require "json"
require "open3"
require "securerandom"
require "socket"

module DecentJob
  # Keeps one decent_job_processes row alive while work is registered under
  # its process UUID.
  #
  # {Worker} creates one tracker per run, and {Claim} creates one for
  # standalone performers. Callers wrap work in {#register}. The first
  # registration starts a monitor thread. The monitor opens a dedicated
  # PostgreSQL connection with {Database#connect}, outside the pool, and
  # upserts the row every process_heartbeat_interval seconds. In advisory mode
  # it also holds a session advisory lock on the process UUID, so
  # {ProcessRecord.active} sees the process as live while that session lives.
  #
  # Ownership: the monitor thread owns the dedicated connection. No other
  # thread may touch it. The registration mutex guards only counters and
  # flags. No SQL and no application state callback runs under the mutex.
  #
  # Lifecycle: registrations are leases. While at least one lease is open,
  # the monitor keeps the row fresh and reconnects after errors. When the last
  # lease closes, the monitor deletes the row (unless decent_jobs rows still
  # name it in locked_by_id) and closes its connection. {#stop} blocks new
  # leases but does not end open ones.
  #
  # Constraints: a tracker belongs to the process that created it. After fork,
  # create a new tracker; an inherited one rejects registration and never
  # deletes the parent's row.
  #
  # See docs/process-tracking.md. Schema: db/migrations/009_add_processes.sql.
  class ProcessTracker
    # @return [Database] the database the monitor connects to
    # @return [String] the process UUID, fixed for the tracker's life
    attr_reader :database, :process_id

    # @param database [Database] the database for the dedicated connection
    # @param advisory_lock_heartbeat [Boolean] true for heartbeat_mode
    #   'advisory', false for 'heartbeat'
    # @param process_heartbeat_interval [Numeric] seconds between heartbeat
    #   writes. Must be positive and less than 300.
    # @param state [#call] returns a Hash merged into the state JSON on each
    #   heartbeat. It runs on the monitor thread.
    # @param on_thread_error [#call, nil] receives monitor errors
    # @raise [ArgumentError] when the heartbeat options are invalid
    def initialize(database:, advisory_lock_heartbeat: DecentJob.configuration.advisory_lock_heartbeat,
      process_heartbeat_interval: DecentJob.configuration.process_heartbeat_interval,
      state: -> { {} }, on_thread_error: DecentJob.configuration.on_thread_error)
      Configuration.validate_process_options!(advisory_lock_heartbeat, process_heartbeat_interval)
      @database = database
      @mode = advisory_lock_heartbeat ? "advisory" : "heartbeat"
      @interval = process_heartbeat_interval
      @state = state
      @on_thread_error = on_thread_error
      @process_id = SecureRandom.uuid.freeze
      @pid = Process.pid
      @mutex = Mutex.new
      @condition = ConditionVariable.new
      @registrations = 0
      @ready = false
      @stopped = false
    end

    # Opens a lease, waits for the first heartbeat, and yields the process UUID.
    #
    # If the previous monitor is still finishing after its last lease closed,
    # this waits for it to exit first. Otherwise the old monitor could delete
    # the row after the new lease starts.
    #
    # @param stopping [#call] returns true to give up while waiting for the
    #   first heartbeat
    # @param wait_for_monitor [Boolean] when true and this is the last lease,
    #   join the monitor on exit so the row is deleted before return. {Worker}
    #   passes false and joins under its own shutdown deadline.
    # @yieldparam process_id [String] the process UUID
    # @return [Object, nil] the block result. Nil without a yield when the
    #   tracker is stopped, a fork is being prepared, or stopping returned true.
    # @raise [ForkedDatabase] when called in a process other than the creator
    # @raise [Exception] the error that ended the monitor thread
    # @note Thread safe. The lease count changes with interrupts deferred, so
    #   Thread#raise or Thread#kill cannot leak a lease.
    def register(stopping: -> { false }, wait_for_monitor: true)
      raise ForkedDatabase, "create a new process tracker after forking" if @pid != Process.pid
      registered = false
      loop do
        previous = DecentJob.synchronize_resource_start(skip_during_fork: true) do
          @mutex.synchronize do
            return nil if @stopped
            if @monitor&.alive? && @registrations.zero?
              @monitor
            else
              Thread.handle_interrupt(Object => :never) do
                @registrations += 1
                registered = true
                # The new thread inherits the :never mask, so restore
                # :immediate to let forced shutdown interrupt the monitor.
                @monitor = Thread.new { Thread.handle_interrupt(Object => :immediate) { monitor } } unless @monitor&.alive?
              end
              nil
            end
          end
        end
        break if registered
        return nil unless previous
        previous.join
      end
      loop do
        return nil if stopping.call
        ready, stopped, failure = @mutex.synchronize do
          @condition.wait(@mutex, 0.05) unless @ready || @stopped
          [@ready, @stopped, @failure]
        end
        raise failure if failure
        return nil if stopped
        break if ready
      end
      yield process_id
    ensure
      if registered
        last = Thread.handle_interrupt(Object => :never) do
          @mutex.synchronize do
            @registrations -= 1
            @condition.broadcast
            @registrations.zero? ? @monitor : nil
          end
        end
        # Workers drain the monitor under their shared shutdown deadline.
        # Standalone leases retain their synchronous cleanup guarantee.
        last&.join if wait_for_monitor && Thread.current.status != "aborting"
      end
    end

    # @return [Boolean] true when no monitor thread is alive
    def shutdown?
      # ObjectSpace scans can find an instance whose initialize raised before
      # @mutex was set.
      return true unless @mutex
      @mutex.synchronize { !@monitor&.alive? }
    end

    # Reports whether the monitor wrote its last heartbeat successfully.
    #
    # Reads only cached monitor state. It never touches the monitor's PG
    # session, so probe threads can call it.
    #
    # @return [Boolean] false in a forked child, after {#stop}, or after a
    #   heartbeat error until the next success
    def connected?
      @mutex.synchronize { @pid == Process.pid && @ready && !@stopped && !!@monitor&.alive? }
    end

    # Blocks new leases and wakes waiters. Open leases and the monitor keep
    # running until the last lease closes.
    #
    # @return [ProcessTracker, nil] self, or nil in a forked child
    def stop
      return if @pid != Process.pid
      @mutex.synchronize do
        @stopped = true
        @condition.broadcast
      end
      self
    end

    # Waits for the monitor thread to exit.
    #
    # @param timeout [Numeric, nil] seconds to wait, or nil to wait forever
    # @return [Boolean] true when no monitor runs or it exited. False on
    #   timeout or when called from the monitor thread. Always true in a
    #   forked child.
    def join(timeout = nil)
      return true if @pid != Process.pid
      monitor = @mutex.synchronize { @monitor }
      !monitor || (monitor != Thread.current && !!monitor.join(timeout))
    end

    private

    # Monitor thread body. Runs while leases are open. A StandardError drops
    # the connection, clears ready, and retries after at most 0.1 seconds.
    # ForkedDatabase and non-StandardError exceptions end the thread; the
    # error goes to @failure so waiting registrations raise it. The advisory
    # lock key must match ProcessRecord::KEY_SQL.
    def monitor
      connection = nil
      loop do
        break unless registered?
        begin
          unless connection
            connection = @database.connect
            if @mode == "advisory"
              connection.exec_params("SELECT pg_advisory_lock(hashtextextended(current_schema() || '.decent_job_processes.' || $1, 0))", [process_id])
            end
          end
          # Registration may wait inside an application executor. A reloader
          # could wait for that parent; heartbeat work only needs its executor.
          DecentJob.wrap_execution(reload: false) { refresh(connection) }
          @mutex.synchronize do
            @ready = true
            @condition.broadcast
            @condition.wait(@mutex, @interval) if @registrations.positive?
          end
        rescue ForkedDatabase
          raise
        rescue => error
          @mutex.synchronize { @ready = false }
          connection&.finish unless connection&.finished?
          connection = nil
          report_error(error)
          @mutex.synchronize { @condition.wait(@mutex, [@interval, 0.1].min) if @registrations.positive? }
        end
      end
    rescue Exception => error # standard:disable Lint/RescueException
      @mutex.synchronize do
        @failure = error
        @stopped = true
        @condition.broadcast
      end
      raise
    ensure
      begin
        if connection && !connection.finished? && connection.status == PG::CONNECTION_OK
          connection.exec_params("DELETE FROM decent_job_processes WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM decent_jobs WHERE locked_by_id = $1)", [process_id])
        end
      rescue => error
        report_error(error)
      ensure
        connection&.finish unless connection&.finished?
        @mutex.synchronize do
          @ready = false
          @condition.broadcast
        end
      end
    end

    def registered?
      @mutex.synchronize { @registrations.positive? }
    end

    # Upserts the row. The upsert also recreates a row that cleanup deleted
    # while a lease was still open.
    def refresh(connection)
      configuration = DecentJob.configuration
      policy = configuration.preserve_job_records
      state = {hostname: Socket.gethostname, pid: Process.pid, memory: memory_usage, proctitle: $PROGRAM_NAME,
               preserve_job_records: policy.respond_to?(:call) ? "callable" : policy,
               retry_on_unhandled_error: configuration.retry_on_unhandled_error,
               cron_enabled: configuration.enable_cron, database_connection_pool: @database.stats}.merge(@state.call)
      connection.exec_params(<<~SQL, [process_id, JSON.generate(state), @mode])
        INSERT INTO decent_job_processes (id, state, heartbeat_mode) VALUES ($1, $2, $3)
        ON CONFLICT (id) DO UPDATE SET state = EXCLUDED.state, heartbeat_mode = EXCLUDED.heartbeat_mode,
          updated_at = clock_timestamp()
      SQL
    end

    # Resident set size in kilobytes, or 0 when unknown. Linux reads /proc;
    # other platforms run ps.
    def memory_usage
      if File.exist?("/proc/#{Process.pid}/status")
        File.foreach("/proc/#{Process.pid}/status") { |line| return line.split[1].to_i if line.start_with?("VmRSS:") }
        0
      else
        output, status = Open3.capture2("ps", "-o", "rss=", "-p", Process.pid.to_s)
        status.success? ? output.to_i : 0
      end
    rescue
      0
    end

    def report_error(error)
      @on_thread_error&.call(error)
    rescue Exception # standard:disable Lint/RescueException
      nil
    end
  end
end
