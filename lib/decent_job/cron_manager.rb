# frozen_string_literal: true

require "decent_job/lifecycle_events"
require "decent_job/resource_shutdown"

module DecentJob
  # Runs the schedules of a fixed list of {CronEntry} objects on threads it
  # owns, and enqueues each occurrence.
  #
  # {DecentJob.start_cron} creates one manager per database and keeps it in
  # its registry. Applications can also create and own a manager directly.
  # The owner must shut it down before it closes the database.
  # {ResourceShutdown} drives shutdown and restart through {#begin_shutdown}
  # and the private +drain_shutdown+.
  #
  # Threads per entry, all created by {#start}:
  #
  # - One timer thread (+run+). It computes each occurrence, waits for it,
  #   and puts it in the pending slot of the entry.
  # - Two delivery threads (+dispatch+). They take occurrences from the slot
  #   and call {CronEntry#enqueue}. Two enqueues of one entry can overlap.
  # - One catch-up thread (+replay+), only with +graceful_restart_period+.
  #   It replays missed occurrences one at a time, then exits.
  #
  # Each thread can hold one database connection, so the pool needs three
  # connections per entry, or four with catch-up. The pending slot holds at
  # most one occurrence. When both delivery threads are busy, the timer
  # blocks. It has already computed the following occurrence, and it skips
  # expression occurrences that pass while it blocks. A callable that returns
  # a +Time+ advances from each previous occurrence, so it does not skip.
  # No thread or array grows with the number of missed occurrences. A slow
  # entry blocks only its own threads.
  #
  # One mutex and one condition variable guard +@stopping+, the pending
  # slots, and the thread list. Shutdown sets +@stopping+ and broadcasts, so
  # every wait wakes up. +@lifecycle_revision+ increases on each start and
  # each shutdown. A restart handle starts the manager only when the revision
  # did not change after its shutdown.
  #
  # Restart catch-up: at start, the timer thread reads the latest stored job
  # with +cron_at+ at or before the start time. The catch-up thread replays
  # expression occurrences strictly between the later of (start time minus
  # the period) and that +cron_at+, and the start time. It goes oldest first
  # and stops at shutdown. Its order relative to live delivery is not fixed.
  # Other processes can replay the same occurrences. The unique index on
  # (+cron_key+, +cron_at+) keeps one job per occurrence, so a duplicate
  # enqueue returns false and is not an error.
  #
  # Errors from an entry go to +on_thread_error+. They never stop other
  # entries. An entry whose schedule fails or does not advance stops its
  # timer until the next start.
  #
  # See docs/recurring-jobs.md and docs/lifecycle.md.
  class CronManager
    # @return [Array<CronEntry>] the frozen entry list
    attr_reader :cron_entries

    # @param entries [Array<CronEntry>] the entries to schedule; the list is
    #   copied and frozen
    # @param start_on_initialize [Boolean] true to call {#start} at once
    # @param graceful_restart_period [Numeric, nil] the catch-up window in
    #   seconds, or nil to turn off restart catch-up
    # @param on_thread_error [#call, nil] receives each error from an entry;
    #   errors that it raises are ignored
    # @raise [ArgumentError] when +graceful_restart_period+ is not a finite
    #   positive number or nil
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

    # Allocates the threads of every entry and starts the schedules.
    #
    # The start time is captured once, and catch-up uses it as its upper
    # bound. The threads inherit the +Time.zone+ of the calling thread when
    # Active Support is loaded. They wait on the mutex until all threads are
    # allocated, so no entry code runs while this method holds the lock. If
    # allocation fails, all new threads are killed.
    #
    # @param expected_revision [Integer, nil] start only when the lifecycle
    #   revision still has this value; {ResourceShutdown#restart} uses it
    # @param automatic [Boolean] true for an automatic start; it never reopens
    #   a manager that was started and then stopped
    # @return [CronManager, false, nil] self when started or already
    #   running. false when refused, including when stopped threads from the
    #   last run are still alive. nil when the runtime skipped an automatic
    #   start during a transition.
    # @raise [ForkInProgress] when the process is preparing to fork
    # @raise [LifecycleInProgress] when an explicit start runs during a
    #   configure or reset transition
    # @note Runs under the {DecentJob} registry lock. Records a
    #   +cron_manager_start+ lifecycle event, which observers receive after
    #   the lifecycle locks are released.
    def start(expected_revision: nil, automatic: false)
      DecentJob.synchronize_resource_start(automatic: automatic) do
        @mutex.synchronize do
          return false if automatic && @stopping && @lifecycle_revision.positive?
          return false if expected_revision && expected_revision != @lifecycle_revision
          return self unless @stopping
          # A stopped run that still drains cannot start again yet.
          return false if @threads.any?(&:alive?)

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

    # @return [Boolean] true from {#start} until shutdown begins; threads can
    #   still be alive when this is false
    def running?
      @mutex.synchronize { !@stopping }
    end

    # @return [Boolean] true when shutdown began and no thread is alive
    def shutdown?
      return true unless @threads
      @mutex.synchronize { @stopping && @threads.none?(&:alive?) }
    end

    # Stops the schedules and waits for the threads.
    #
    # @param timeout [Numeric, nil] seconds to wait, or nil to wait without limit
    # @param force [Boolean] true to kill threads that are still alive at the
    #   deadline; Ruby cleanup can still run after that
    # @return [Boolean] true when all threads stopped; false when some are
    #   still alive, and a later shutdown can wait for them
    # @raise [Exception] the first exception that a thread died with, after
    #   all threads were joined
    def shutdown(timeout: nil, force: false)
      deadline = ResourceShutdown.deadline(timeout)
      begin_shutdown.wait(deadline: deadline, force: force, timeout: timeout)
    end

    # Marks the manager as stopping, wakes all waits, and returns a handle
    # that waits for the current threads.
    #
    # @return [ResourceShutdown] a handle bound to the new lifecycle revision
    # @api private
    def begin_shutdown
      @mutex.synchronize do
        @lifecycle_revision += 1
        @stopping = true
        @condition.broadcast
        ResourceShutdown.new(self, @lifecycle_revision, [@threads.dup, @interrupted_threads])
      end
    end

    # Joins the threads of a shutdown snapshot until the deadline.
    # {ResourceShutdown#wait} calls it with +send+. A thread that calls
    # shutdown from its own callback skips itself. With +force+, each live
    # thread is killed at most once per run.
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

    # Shuts down, then starts again when all old threads stopped in time.
    #
    # @param timeout [Numeric, nil] seconds to wait for the old threads, or nil
    # @param force [Boolean] true to kill old threads at the deadline
    # @return [CronManager, false] self when restarted; false when old threads
    #   are still alive or another lifecycle change happened in between
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

    # New threads accept interrupts at once, so a forced shutdown can kill
    # them. The empty synchronize blocks until {#start} releases the mutex.
    def allocate_thread(&block)
      @threads << Thread.new do
        Thread.handle_interrupt(Object => :immediate) do
          @mutex.synchronize {} # Do not invoke entries before allocation succeeds.
          block.call
        end
      end
    end

    # Timer thread. It reads the catch-up snapshot first and sets
    # +state[:ready]+, which releases the catch-up thread. It computes the
    # following occurrence before it submits the current one. A nil
    # occurrence or a failed schedule ends the loop for this entry.
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

    # Puts one occurrence in the single pending slot. It blocks while the slot
    # is full, which gives back pressure to the timer.
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

    # Rejects a result that is not a Time, or that does not advance past
    # +previously_at+. The error is reported, and the nil result stops the timer.
    def next_occurrence(entry, **options)
      safely do
        occurrence = in_context { entry.next_at(**options) }

        if occurrence && (!occurrence.is_a?(Time) || (options[:previously_at] && occurrence <= options[:previously_at]))
          raise ArgumentError, "cron entry #{entry.key} must return an advancing Time or nil"
        end

        occurrence
      end
    end

    # Replays occurrences strictly between the window start and +started_at+.
    # A callable +cron+ that returns a Time yields nothing here, because its
    # timer chain replays it. Deduplication makes overlap with other
    # processes, or with live delivery, safe.
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

    # Checks +running?+ and the persistent override before each enqueue. A
    # live occurrence that is in the future (after a backward clock change)
    # waits for its wall time, then checks again. Catch-up does not wait.
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

    # Waits until the wall clock reaches +occurrence+. Returns false when
    # shutdown begins. ConditionVariable uses elapsed-time waits, so each wait
    # is at most 0.25 seconds and wall time is read again after each wakeup.
    # A backward or forward clock change so takes effect within 0.25 seconds.
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

    # Wraps entry code in the Rails reloader or executor when one exists.
    def in_context(&block)
      DecentJob.wrap_execution(&block)
    end

    # Reports a StandardError and returns nil, so one failure does not end
    # the thread. Other exceptions end the thread.
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
