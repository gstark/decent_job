# frozen_string_literal: true

require "decent_job/cleaner"
require "decent_job/cleanup_tracker"
require "decent_job/performer"
require "decent_job/queue_selection"

module DecentJob
  # Each execution thread owns its database checkout for the entire job. The
  # notification listener uses its own connection and only hints workers to poll.
  class Worker
    def initialize(database:, queues: "*", max_threads: 5, poll_interval: 1, enable_listen_notify: true, on_thread_error: DecentJob.configuration.on_thread_error,
      advisory_lock_heartbeat: DecentJob.configuration.advisory_lock_heartbeat,
      process_heartbeat_interval: DecentJob.configuration.process_heartbeat_interval,
      cleanup_interval_jobs: DecentJob.configuration.cleanup_interval_jobs,
      cleanup_interval_seconds: DecentJob.configuration.cleanup_interval_seconds,
      cleanup_preserved_jobs_before_seconds_ago: DecentJob.configuration.cleanup_preserved_jobs_before_seconds_ago,
      cleanup_discarded_jobs: DecentJob.configuration.cleanup_discarded_jobs?)
      unless poll_interval.is_a?(Numeric) && poll_interval.finite? && poll_interval.positive?
        raise ArgumentError, "poll_interval must be a finite positive number"
      end

      Configuration.validate_process_options!(advisory_lock_heartbeat, process_heartbeat_interval)
      @process_options = {advisory_lock_heartbeat: advisory_lock_heartbeat, process_heartbeat_interval: process_heartbeat_interval}
      @cleanup_tracker = CleanupTracker.new(cleanup_interval_jobs: cleanup_interval_jobs, cleanup_interval_seconds: cleanup_interval_seconds)
      @cleanup_options = {older_than: cleanup_preserved_jobs_before_seconds_ago, include_discarded: cleanup_discarded_jobs}
      Cleaner.validate_options!(**@cleanup_options)
      @cleanup_running = false
      @database = database
      @pools = QueueSelection.pools(queues, default_threads: max_threads)
      @poll_interval = poll_interval
      @enable_listen_notify = enable_listen_notify
      @on_thread_error = on_thread_error
      @mutex = Mutex.new
      @condition = ConditionVariable.new
      @generation = 0
      @stopping = true
      @threads = []
      @execution_threads = []
      reset_stats
    end

    def start
      @mutex.synchronize do
        return self if @threads.any?(&:alive?)

        @stopping = false
        @listener_connected = false
        reset_stats(running: true)
        @failure = nil
        @process_cleanup_at = 0
        @process_cleanup_running = false
        @process_tracker = ProcessTracker.new(database: @database, **@process_options, state: method(:stats), on_thread_error: @on_thread_error)
        @execution_threads = @pools.each_with_index.flat_map do |pool, index|
          Array.new(pool.fetch(:threads)) do
            Thread.new { run(pool, index) }
          end
        end
        @threads = @execution_threads.dup
        if @enable_listen_notify
          @listener_reader, @listener_writer = IO.pipe
          @threads << Thread.new { listen }
        end
      end
      self
    end

    def running?
      @mutex.synchronize { !@stopping && @execution_threads.any?(&:alive?) }
    end

    def started?
      @mutex.synchronize { started_locked? }
    end

    def connected?
      @mutex.synchronize do
        started_locked? && @process_tracker.connected? && (!@enable_listen_notify || @listener_connected)
      end
    end

    def wake
      @mutex.synchronize do
        @generation += 1
        @condition.broadcast
      end
      self
    end

    # A timeout leaves running jobs alone; callers may wait again later.
    def shutdown(timeout: nil)
      if !timeout.nil? && (!timeout.is_a?(Numeric) || !timeout.finite? || timeout.negative?)
        raise ArgumentError, "timeout must be a finite nonnegative number"
      end
      deadline = monotonic_time + timeout if timeout
      threads = @mutex.synchronize do
        @stopping = true
        @condition.broadcast
        @listener_writer&.write_nonblock(".", exception: false)
        @threads.dup
      end
      @process_tracker&.stop
      # A fatal task can make join raise; finish draining every owned thread.
      failure = @mutex.synchronize { @failure }
      threads.each do |thread|
        next if thread == Thread.current

        remaining = deadline && [deadline - monotonic_time, 0].max
        begin
          thread.join(remaining)
        rescue Exception => error # standard:disable Lint/RescueException
          failure ||= error
        end
      end
      remaining = deadline && [deadline - monotonic_time, 0].max
      begin
        monitor_stopped = @process_tracker.nil? || @process_tracker.join(remaining)
      rescue Exception => error # standard:disable Lint/RescueException
        failure ||= error
      end
      failure = @mutex.synchronize { @failure } || failure
      raise failure if failure
      threads.none?(&:alive?) && monitor_stopped
    end

    def process_id
      @process_tracker&.process_id
    end

    def stats
      @mutex.synchronize do
        schedulers = @scheduler_stats.map(&:dup)
        {schedulers: schedulers,
         total_executions_count: schedulers.sum { |pool| pool.fetch(:total_executions_count) },
         total_succeeded_executions_count: schedulers.sum { |pool| pool.fetch(:succeeded_executions_count) },
         total_errored_executions_count: schedulers.sum { |pool| pool.fetch(:errored_executions_count) }}
      end
    end

    def idle?
      stats.fetch(:schedulers).all? { |pool| pool.fetch(:active_threads).zero? }
    end

    private

    def started_locked?
      !@stopping && @scheduler_stats.all? { |pool| pool.fetch(:live_threads) == pool.fetch(:max_threads) } &&
        @execution_threads.all?(&:alive?)
    end

    def reset_stats(running: false)
      @scheduler_stats = @pools.map do |pool|
        selection = pool.fetch(:selection)
        prefix = if selection.exclude?
          "-"
        elsif selection.ordered?
          "+"
        else
          ""
        end
        queues = selection.queues.empty? ? "*" : prefix + selection.queues.join(",")
        live_threads = running ? pool.fetch(:threads) : 0
        {queues: queues, live_threads: live_threads, max_threads: pool.fetch(:threads), active_threads: 0,
         available_threads: live_threads, total_executions_count: 0, succeeded_executions_count: 0,
         errored_executions_count: 0, empty_executions_count: 0, started_at: Time.now.iso8601(6),
         last_execution_at: nil, last_success_at: nil, last_error_at: nil, last_empty_at: nil}
      end
    end

    def record_attempt(index, error:)
      @mutex.synchronize do
        counters = @scheduler_stats.fetch(index)
        counters[:total_executions_count] += 1
        counters[error ? :errored_executions_count : :succeeded_executions_count] += 1
        now = Time.now.iso8601(6)
        counters[:last_execution_at] = now
        counters[error ? :last_error_at : :last_success_at] = now
      end
    end

    def run(pool, index)
      @process_tracker.register(stopping: method(:stopping?)) { run_registered(pool, index) }
    rescue Exception => error # standard:disable Lint/RescueException
      @mutex.synchronize { @failure ||= error }
      raise
    ensure
      last = @mutex.synchronize do
        counters = @scheduler_stats.fetch(index)
        counters[:live_threads] -= 1
        counters[:available_threads] -= 1
        if @scheduler_stats.all? { |pool_stats| pool_stats.fetch(:live_threads).zero? }
          @stopping = true
          @condition.broadcast
          @listener_writer&.write_nonblock(".", exception: false)
          true
        end
      end
      @process_tracker.stop if last
    end

    def run_registered(pool, index)
      selection = pool.fetch(:selection)
      execution_error = nil
      executing = false
      performer = Performer.new(database: @database, process_tracker: @process_tracker,
        on_execution_start: lambda {
          executing = true
          @mutex.synchronize do
            counters = @scheduler_stats.fetch(index)
            counters[:active_threads] += 1
            counters[:available_threads] -= 1
          end
        }, on_unhandled_exception: ->(error) { execution_error = error })
      loop do
        generation = @mutex.synchronize { @stopping ? nil : @generation }
        break unless generation

        begin
          execution_error = nil
          cleanup_processes
          result = performer.perform_one(queues: selection, stopping: method(:stopping?))
          if result
            record_attempt(index, error: result.error)
            report_error(result.unhandled_error) if result.unhandled_error
            cleanup_after_attempt
          else
            @mutex.synchronize do
              counters = @scheduler_stats.fetch(index)
              counters[:empty_executions_count] += 1
              counters[:last_empty_at] = Time.now.iso8601(6)
            end
            wait(@poll_interval, generation)
          end
        rescue Exception => error # standard:disable Lint/RescueException
          record_attempt(index, error: error) if error.equal?(execution_error)
          report_error(error)
          raise if error.equal?(execution_error) || !error.is_a?(StandardError)
          wait(retry_interval)
        ensure
          if executing
            @mutex.synchronize do
              counters = @scheduler_stats.fetch(index)
              counters[:active_threads] -= 1
              counters[:available_threads] += 1
            end
            executing = false
          end
        end
      end
    end

    def cleanup_processes
      due = @mutex.synchronize do
        now = monotonic_time
        next false if @stopping || @process_cleanup_running || now < @process_cleanup_at
        @process_cleanup_running = true
        @process_cleanup_at = now + ProcessRecord::STALE_INTERVAL
        true
      end
      return unless due

      begin
        ProcessRecord.cleanup(database: @database, stopping: method(:stopping?))
      ensure
        @mutex.synchronize { @process_cleanup_running = false }
      end
    end

    def cleanup_after_attempt
      acquired = @mutex.synchronize do
        @cleanup_tracker.increment
        next false if @stopping || @cleanup_running || !@cleanup_tracker.cleanup?

        @cleanup_running = true
        @cleanup_tracker.reset
        true
      end
      return unless acquired

      begin
        Cleaner.new(database: @database).cleanup(**@cleanup_options, stopping: method(:stopping?))
      rescue => error
        report_error(error)
      ensure
        @mutex.synchronize { @cleanup_running = false }
      end
    end

    def listen
      until stopping?
        connection = nil
        begin
          connection = @database.connect
          connection.exec("LISTEN decent_job")
          @mutex.synchronize { @listener_connected = true }
          wake
          until stopping?
            readable = IO.select([connection.socket_io, @listener_reader], nil, nil, 1)
            next unless readable
            break if readable.first.include?(@listener_reader)

            connection.wait_for_notify(0) { wake }
          end
        rescue => error
          @mutex.synchronize { @listener_connected = false }
          report_error(error) unless stopping?
        ensure
          @mutex.synchronize { @listener_connected = false }
          connection&.finish unless connection&.finished?
        end
        wait(retry_interval) unless stopping?
      end
    ensure
      @mutex.synchronize do
        @listener_reader&.close
        @listener_writer&.close
        @listener_reader = @listener_writer = nil
      end
    end

    def stopping?
      @mutex.synchronize { @stopping }
    end

    def wait(interval, generation = nil)
      deadline = monotonic_time + interval
      @mutex.synchronize do
        loop do
          break if @stopping || (generation && generation != @generation)

          remaining = deadline - monotonic_time
          break unless remaining.positive?

          @condition.wait(@mutex, remaining)
        end
      end
    end

    def retry_interval
      @poll_interval.clamp(0.1, 1)
    end

    def report_error(error)
      @on_thread_error&.call(error)
    rescue Exception # standard:disable Lint/RescueException
      # Error reporting is external code and must not terminate a worker.
      nil
    end

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
