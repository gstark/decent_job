# frozen_string_literal: true

require "decent_job/lifecycle_events"
require "decent_job/resource_shutdown"

module DecentJob
  # Each entry owns a timer and two delivery threads, plus a catchup thread when
  # enabled. A single pending slot bounds backlog memory and isolates slow entries.
  # Catchup is serial and independent of live delivery.
  class CronManager
    attr_reader :cron_entries

    def initialize(entries = [], start_on_initialize: false, graceful_restart_period: nil, on_thread_error: nil)
      if !graceful_restart_period.nil? && (!graceful_restart_period.is_a?(Numeric) || !graceful_restart_period.finite? || !graceful_restart_period.positive?)
        raise ArgumentError, "graceful_restart_period must be a finite positive number or nil"
      end

      @cron_entries = entries.dup.freeze
      @graceful_restart_period = graceful_restart_period
      @on_thread_error = on_thread_error
      @mutex = Mutex.new
      @condition = ConditionVariable.new
      @threads = []
      @interrupted_threads = {}
      @stopping = true
      @lifecycle_revision = 0
      start if start_on_initialize
    end

    def start(expected_revision: nil, automatic: false)
      DecentJob.synchronize_resource_start(automatic: automatic) do
        @mutex.synchronize do
          return false if automatic && @stopping && @lifecycle_revision.positive?
          return false if expected_revision && expected_revision != @lifecycle_revision
          return self if !@stopping || @threads.any?(&:alive?)

          began = monotonic_time
          @stopping = false
          @lifecycle_revision += 1
          @run_id = (@run_id || 0) + 1
          @interrupted_threads = {}
          started_at = Time.now
          zone = Time.zone if Time.respond_to?(:zone)
          @threads = []
          allocated = false

          begin
            Thread.handle_interrupt(Object => :never) do
              cron_entries.each do |entry|
                state = {pending: [], ready: false}
                allocate_thread { with_zone(zone) { run(entry, state, started_at) } }
                2.times { allocate_thread { with_zone(zone) { dispatch(entry, state) } } }
                allocate_thread { with_zone(zone) { replay(entry, state, started_at) } } if @graceful_restart_period
              end
            end

            allocated = true
          ensure
            unless allocated
              @stopping = true
              @threads.each(&:kill)
            end
          end

          LifecycleEvents.record(self, "cron_manager_start",
            {cron_manager: self, cron_entries: cron_entries.dup, run_id: @run_id,
             operation_duration: monotonic_time - began}, on_error: @on_thread_error)
        end
      end

      self
    end

    def running?
      @mutex.synchronize { !@stopping }
    end

    def shutdown?
      return true unless @threads
      @mutex.synchronize { @stopping && @threads.none?(&:alive?) }
    end

    def shutdown(timeout: nil, force: false)
      deadline = ResourceShutdown.deadline(timeout)
      begin_shutdown.wait(deadline: deadline, force: force, timeout: timeout)
    end

    def begin_shutdown
      @mutex.synchronize do
        @lifecycle_revision += 1
        @stopping = true
        @condition.broadcast
        ResourceShutdown.new(self, @lifecycle_revision, [@threads.dup, @interrupted_threads])
      end
    end

    private def drain_shutdown(snapshot, deadline:, force:)
      threads, interrupted_threads = snapshot
      # Fatal callbacks must not prevent sibling deliveries from draining.
      failure = nil

      threads.each do |thread|
        next if thread == Thread.current

        begin
          thread.join(ResourceShutdown.remaining(deadline))
        rescue Exception => error # standard:disable Lint/RescueException
          failure ||= error
        end
      end

      if force && deadline && ResourceShutdown.remaining(deadline).zero?
        targets = @mutex.synchronize do
          threads.select do |thread|
            next false if thread == Thread.current || !thread.alive? || interrupted_threads.key?(thread)
            interrupted_threads[thread] = true
          end
        end

        targets.each(&:kill)
      end

      raise failure if failure
      threads.none?(&:alive?)
    end

    def restart(timeout: nil, force: false)
      deadline = ResourceShutdown.deadline(timeout)
      handle = begin_shutdown
      handle.wait(deadline: deadline, force: force, timeout: timeout) ? handle.restart : false
    end

    private

    # Threads start with the main thread's Active Support time zone when one is set.
    def with_zone(zone, &block)
      zone ? Time.use_zone(zone, &block) : yield
    end

    def allocate_thread(&block)
      @threads << Thread.new do
        Thread.handle_interrupt(Object => :immediate) do
          @mutex.synchronize {} # Do not invoke entries before allocation succeeds.
          block.call
        end
      end
    end

    def run(entry, state, started_at)
      historical = safely { in_context { CronEntry.last_jobs_by_key([entry], before: started_at)[entry.key.to_s] } } if @graceful_restart_period
      occurrence = next_occurrence(entry)

      @mutex.synchronize do
        state[:historical] = historical
        state[:ready] = true
        @condition.broadcast
      end

      while occurrence && wait_until(occurrence)
        following = next_occurrence(entry, previously_at: occurrence)
        break unless submit(state, occurrence)
        occurrence = following
      end
    rescue => error
      report_error(error)
    end

    def submit(state, occurrence)
      @mutex.synchronize do
        @condition.wait(@mutex) while !@stopping && !state[:pending].empty?
        return false if @stopping
        state[:pending] << occurrence
        @condition.broadcast
      end

      true
    end

    def dispatch(entry, state)
      loop do
        occurrence = @mutex.synchronize do
          @condition.wait(@mutex) while !@stopping && state[:pending].empty?
          break if @stopping
          state[:pending].shift.tap { @condition.broadcast }
        end

        break unless occurrence
        deliver(entry, occurrence) if wait_until(occurrence)
      end
    end

    def replay(entry, state, started_at)
      ready = @mutex.synchronize do
        @condition.wait(@mutex) until @stopping || state[:ready]
        !@stopping
      end

      catch_up(entry, started_at, state[:historical]) if ready
    end

    def next_occurrence(entry, **options)
      safely do
        occurrence = in_context { entry.next_at(**options) }

        if occurrence && (!occurrence.is_a?(Time) || (options[:previously_at] && occurrence <= options[:previously_at]))
          raise ArgumentError, "cron entry #{entry.key} must return an advancing Time or nil"
        end

        occurrence
      end
    end

    def catch_up(entry, started_at, historical)
      safely do
        last_at = historical && historical["cron_at"]
        beginning = [started_at - @graceful_restart_period, last_at].compact.max

        in_context do
          entry.each_within(beginning...started_at, previously_at: last_at) do |occurrence|
            break unless running?
            deliver(entry, occurrence, historical: true)
          end
        end
      end
    end

    def deliver(entry, occurrence, historical: false)
      safely do
        in_context do
          loop do
            break unless running? && entry.enabled? && running?

            if !historical && occurrence > Time.now
              break unless wait_until(occurrence)
              next
            end

            entry.enqueue(occurrence)
            break
          end
        end
      end
    end

    # ConditionVariable uses elapsed-time waits; wall time is consulted anew on
    # every bounded wakeup, including after a backward or forward clock change.
    def wait_until(occurrence)
      @mutex.synchronize do
        until @stopping
          delay = occurrence - Time.now
          return true unless delay.positive?
          deadline = monotonic_time + [delay, 0.25].min
          remaining = deadline - monotonic_time
          @condition.wait(@mutex, remaining) if remaining.positive?
        end
      end

      false
    end

    def in_context(&block)
      DecentJob.wrap_execution(&block)
    end

    def safely
      yield
    rescue => error
      report_error(error)
      nil
    end

    def report_error(error)
      @on_thread_error&.call(error)
    rescue
      nil
    end

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
