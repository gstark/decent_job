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
      cleanup_interval_jobs: DecentJob.configuration.cleanup_interval_jobs,
      cleanup_interval_seconds: DecentJob.configuration.cleanup_interval_seconds,
      cleanup_preserved_jobs_before_seconds_ago: DecentJob.configuration.cleanup_preserved_jobs_before_seconds_ago,
      cleanup_discarded_jobs: DecentJob.configuration.cleanup_discarded_jobs?)
      unless poll_interval.is_a?(Numeric) && poll_interval.finite? && poll_interval.positive?
        raise ArgumentError, "poll_interval must be a finite positive number"
      end

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
    end

    def start
      @mutex.synchronize do
        return self if @threads.any?(&:alive?)

        @stopping = false
        @threads = @pools.flat_map do |pool|
          Array.new(pool.fetch(:threads)) do
            Thread.new { run(pool.fetch(:selection)) }
          end
        end
        if @enable_listen_notify
          @listener_reader, @listener_writer = IO.pipe
          @threads << Thread.new { listen }
        end
      end
      self
    end

    def running?
      @mutex.synchronize { !@stopping && @threads.any?(&:alive?) }
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
      threads.each do |thread|
        next if thread == Thread.current

        remaining = deadline && [deadline - monotonic_time, 0].max
        thread.join(remaining)
      end
      threads.none?(&:alive?)
    end

    private

    def run(selection)
      performer = Performer.new(database: @database)
      loop do
        generation = @mutex.synchronize { @stopping ? nil : @generation }
        break unless generation

        begin
          result = performer.perform_one(queues: selection, stopping: method(:stopping?))
          if result
            cleanup_after_attempt
          else
            wait(@poll_interval, generation)
          end
        rescue => error
          report_error(error)
          wait(retry_interval)
        end
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
          wake
          until stopping?
            readable = IO.select([connection.socket_io, @listener_reader], nil, nil, 1)
            next unless readable
            break if readable.first.include?(@listener_reader)

            connection.wait_for_notify(0) { wake }
          end
        rescue => error
          report_error(error) unless stopping?
        ensure
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
    rescue
      # Error reporting is external code and must not terminate a worker.
      nil
    end

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
