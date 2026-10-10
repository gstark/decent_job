# frozen_string_literal: true

require "json"
require "open3"
require "securerandom"
require "socket"

module DecentJob
  # Registration leases own one monitor and its dedicated PostgreSQL session.
  # No SQL or application state callbacks execute under the registration mutex.
  class ProcessTracker
    attr_reader :database, :process_id

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

    def shutdown?
      return true unless @mutex
      @mutex.synchronize { !@monitor&.alive? }
    end

    # Read only cached monitor state: never touch another thread's PG session.
    def connected?
      @mutex.synchronize { @pid == Process.pid && @ready && !@stopped && !!@monitor&.alive? }
    end

    def stop
      return if @pid != Process.pid

      @mutex.synchronize do
        @stopped = true
        @condition.broadcast
      end

      self
    end

    def join(timeout = nil)
      return true if @pid != Process.pid
      monitor = @mutex.synchronize { @monitor }
      !monitor || (monitor != Thread.current && !!monitor.join(timeout))
    end

    private

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
