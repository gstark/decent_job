# frozen_string_literal: true

require "decent_job/cleaner"
require "decent_job/cleanup_tracker"
require "decent_job/instrumentation"
require "decent_job/lifecycle_events"
require "decent_job/performer"
require "decent_job/queue_selection"
require "decent_job/resource_shutdown"
require "decent_job/scheduler_cache"
require "decent_job/stats"
require "decent_job/thread_status"

module DecentJob
  # Runs jobs from one or more queue pools on a fixed set of owned threads.
  #
  # The global runtime ({DecentJob.start}), an async {Adapter}, the CLI, or
  # application code creates a worker and calls {#start}. Each run allocates:
  #
  # - one execution thread per configured pool thread. Each thread loops in
  #   {#run_registered}, waits for a permit from its pool's {SchedulerCache},
  #   and calls {Performer#perform_one}.
  # - one listener thread when LISTEN/NOTIFY is enabled. It owns a dedicated
  #   PostgreSQL session and only hints pools to claim. It never claims a job.
  # - one {ProcessTracker} with its own monitor thread and dedicated session.
  #   Every execution thread holds a registration on it for the whole run.
  #
  # In advisory and hybrid lock modes, an execution thread keeps its database
  # checkout for the entire job, because the session advisory lock belongs to
  # that connection. SKIP LOCKED mode returns the checkout before the job body.
  #
  # Locking: +@mutex+ protects all mutable run state: +@stopping+,
  # +@lifecycle_revision+, +@run_id+, the thread lists, +@caches+,
  # +@scheduler_stats+, +@executing_job_ids+, the interruption records,
  # +@idle_since+, +@listener_connected+, +@failure+, the cleanup flags, and
  # +@lower_thread_priority+. +@condition+ uses +@mutex+. Setting +@stopping+,
  # {#wake}, and cache refreshes broadcast on it. Waiters also recheck at least
  # every 0.5 seconds. Do not run SQL or user callbacks while you hold
  # +@mutex+. Lifecycle events recorded under it are delivered later by
  # {LifecycleEvents.defer}.
  #
  # Lifecycle: +@stopping+ is true before the first start and after a run
  # stops. {#start} clears it, and {#begin_shutdown} or the exit of the last
  # execution thread sets it. +@lifecycle_revision+ increases on every start
  # and every {#begin_shutdown}. A {ResourceShutdown} handle records the
  # revision that it stopped. {ResourceShutdown#restart} starts again only if
  # no other start or stop happened since. +@run_id+ increases once per started
  # run and labels lifecycle events.
  #
  # Invariants:
  # - A new run does not start while any thread or the tracker of the previous
  #   run is still alive.
  # - {#start} replaces +@caches+ and +@scheduler_stats+ (see {#reset_stats}).
  #   Only threads of the current run touch them.
  #
  # See docs/scheduling.md, docs/shutdown.md, docs/worker-observability.md,
  # docs/daemon-and-idle.md, and docs/runtime-resources.md.
  class Worker
    # @return [Database] the pool that execution threads and cleanup check out from
    attr_reader :database

    # Builds a stopped worker. It allocates no threads and opens no connections.
    #
    # @param database [Database] pool for claims, cleanup, and cache refreshes
    # @param queues [String] queue pool expression, for example
    #   "urgent:2;-urgent". See {QueueSelection.pools}.
    # @param max_threads [Integer] thread count for a pool that does not set one
    # @param poll_interval [Numeric] seconds between periodic claim attempts.
    #   Zero or a negative value disables polling.
    # @param max_cache [Integer] maximum cached future deadlines per pool. Zero
    #   disables startup cache warming. See {SchedulerCache}.
    # @param enable_listen_notify [Boolean] start a listener thread for NOTIFY hints
    # @param on_thread_error [#call, nil] receives errors from owned threads.
    #   Its own errors are ignored.
    # @param lower_thread_priority [Boolean] use Ruby priority -3 on execution threads
    # @param advisory_lock_heartbeat [Boolean] passed to {ProcessTracker}
    # @param process_heartbeat_interval [Numeric] seconds between process
    #   heartbeats, passed to {ProcessTracker}
    # @param cleanup_interval_jobs [Integer, false] attempts between record
    #   cleanups, -1 for every attempt, or false to disable. See {CleanupTracker}.
    # @param cleanup_interval_seconds [Numeric, false] seconds between record
    #   cleanups, -1 for every attempt, or false to disable
    # @param cleanup_preserved_jobs_before_seconds_ago [Numeric] age in seconds
    #   of preserved records that cleanup deletes
    # @param cleanup_discarded_jobs [Boolean] also delete discarded records
    # @raise [ArgumentError] if an option is invalid
    def initialize(database:, queues: "*", max_threads: 5, poll_interval: 10, max_cache: 10_000, enable_listen_notify: true, on_thread_error: DecentJob.configuration.on_thread_error,
      lower_thread_priority: DecentJob.configuration.lower_thread_priority,
      advisory_lock_heartbeat: DecentJob.configuration.advisory_lock_heartbeat,
      process_heartbeat_interval: DecentJob.configuration.process_heartbeat_interval,
      cleanup_interval_jobs: DecentJob.configuration.cleanup_interval_jobs,
      cleanup_interval_seconds: DecentJob.configuration.cleanup_interval_seconds,
      cleanup_preserved_jobs_before_seconds_ago: DecentJob.configuration.cleanup_preserved_jobs_before_seconds_ago,
      cleanup_discarded_jobs: DecentJob.configuration.cleanup_discarded_jobs?)
      unless poll_interval.is_a?(Numeric) && poll_interval.real? && poll_interval.finite?
        raise ArgumentError, "poll_interval must be a finite real number"
      end

      unless max_cache.is_a?(Integer) && max_cache >= 0
        raise ArgumentError, "max_cache must be a nonnegative integer"
      end

      @max_cache = max_cache
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
      self.lower_thread_priority = lower_thread_priority
      @condition = ConditionVariable.new
      @stopping = true
      @lifecycle_revision = 0
      @threads = []
      @execution_threads = []
      @executing_job_ids = {}
      @interrupted_job_ids = []
      @interrupted_threads = {}
      reset_stats
    end

    # Allocates the threads and process tracker of a new run.
    #
    # @param expected_revision [Integer, nil] start only if
    #   +@lifecycle_revision+ still has this value. {ResourceShutdown#restart}
    #   passes the revision of its stop. Nil skips the check.
    # @param automatic [Boolean] true for implicit starts, such as the first
    #   async enqueue. An automatic start does nothing after a run has stopped,
    #   so an explicit shutdown stays in effect. This also covers a run that
    #   stopped because every execution thread died from an excluded error.
    #   Restarting it would clear +@failure+ before shutdown can raise it.
    # @return [Worker, false] self when a run started or a run is already
    #   active. false when the start was refused: the revision changed, an
    #   automatic start came after a stop, a stopped run still drains, or the
    #   runtime skipped an automatic start during a lifecycle transition.
    # @raise [ForkInProgress] if the runtime prepares to fork
    # @raise [LifecycleInProgress] if a non-automatic start races a runtime transition
    # @note Holds the runtime mutex and +@mutex+ while it allocates threads.
    #   New threads wait on +@mutex+ until allocation completes. Lifecycle
    #   observers run after the runtime mutex is released.
    def start(expected_revision: nil, automatic: false)
      start_run(expected_revision: expected_revision, automatic: automatic).first
    end

    # Returns [result, run_id]. run_id is nil when no new run started, which
    # lets {ResourceShutdown#restart} tell a refused restart from a new run.
    # If thread allocation is interrupted, the ensure block kills the partial
    # run and leaves the worker stopped.
    private def start_run(expected_revision: nil, automatic: false)
      result = DecentJob.synchronize_resource_start(automatic: automatic) do
        @mutex.synchronize do
          return [false, nil] if automatic && @stopping && @lifecycle_revision.positive?
          return [false, nil] if expected_revision && expected_revision != @lifecycle_revision

          if @threads.any?(&:alive?) || (@process_tracker && !@process_tracker.shutdown?)
            # A running worker is already started. A stopped run that still
            # drains cannot start again until its threads exit.
            return [@stopping ? false : self, nil]
          end

          began = monotonic_time
          @stopping = false
          @lifecycle_revision += 1
          @run_id = (@run_id || 0) + 1
          @executing_job_ids = {}
          @interrupted_job_ids = []
          @interrupted_threads = {}
          @idle_since = monotonic_time
          @listener_connected = false
          reset_stats(running: true)
          @failure = nil
          @process_cleanup_at = 0
          @process_cleanup_running = false
          @process_tracker = ProcessTracker.new(database: @database, **@process_options, state: method(:stats), on_thread_error: @on_thread_error)
          @execution_pool_indexes = {}
          @execution_threads = []
          @threads = []
          allocated = false

          begin
            Thread.handle_interrupt(Object => :never) do
              @pools.each_with_index do |pool, index|
                pool.fetch(:threads).times do
                  thread = Thread.new do
                    Thread.handle_interrupt(Object => :immediate) do
                      @mutex.synchronize {} # Allocation must complete before execution starts.
                      run(pool, index)
                    end
                  end

                  @threads << thread
                  @execution_threads << thread
                  @execution_pool_indexes[thread] = index
                end
              end

              if @enable_listen_notify
                @listener_reader, @listener_writer = IO.pipe

                @threads << Thread.new do
                  Thread.handle_interrupt(Object => :immediate) do
                    @mutex.synchronize {}
                    listen
                  end
                end
              end
            end

            allocated = true
          ensure
            unless allocated
              @stopping = true
              @threads.each(&:kill)
              @listener_reader&.close
              @listener_writer&.close
              @listener_reader = @listener_writer = nil
              @process_tracker.stop
              reset_stats
            end
          end

          lifecycle_pools.each do |payload|
            observe_lifecycle("scheduler_create_pool", payload.merge(operation_duration: monotonic_time - began))
          end

          [self, @run_id]
        end
      end

      result || [false, nil]
    end

    # Reports whether every thread and the tracker of the last run have exited.
    #
    # @return [Boolean] true for a worker that never started
    # @note The +@threads+ guard handles a worker whose initializer raised.
    #   Fork checks find such objects through ObjectSpace.
    def shutdown?
      return true unless @threads
      @mutex.synchronize { @threads.none?(&:alive?) && (!@process_tracker || @process_tracker.shutdown?) }
    end

    # Reports whether the worker accepts work. Jobs read this through
    # {ThreadStatus#current_thread_running?}.
    #
    # @return [Boolean] false after shutdown begins, even while jobs still run
    def running?
      @mutex.synchronize { !@stopping && @execution_threads.any?(&:alive?) }
    end

    # Reports whether the run has its full set of live execution threads.
    #
    # @return [Boolean] false when stopping or when any execution thread died
    def started?
      @mutex.synchronize { started_locked? }
    end

    # Reports readiness for health probes.
    #
    # @return [Boolean] true when {#started?}, the process tracker is connected,
    #   and the listener holds LISTEN (when LISTEN/NOTIFY is enabled)
    def connected?
      @mutex.synchronize do
        started_locked? && @process_tracker.connected? && (!@enable_listen_notify || @listener_connected)
      end
    end

    # Gives pools permits to claim, or caches a future deadline.
    #
    # A hint never names a job. It only lets threads run a claim query, and the
    # query does all due, pause, and ownership checks.
    #
    # @param state [Hash, nil] a scheduling hint with String or Symbol keys:
    #   +queue_name+ (only pools that select this queue), +count+ (permits to
    #   hand out across pools in order, default 1), +fanout+ (wake every thread
    #   of each matching pool), and +scheduled_at+ (Time, epoch seconds,
    #   ISO 8601 String, or one JSON-encoded ISO 8601 String). Nil or an empty
    #   hash wakes every thread of every pool. A future +scheduled_at+ only
    #   caches the deadline.
    # @return [Worker] self, also for an invalid hint
    # @note An invalid hint goes to +on_thread_error+ and schedules nothing.
    #   Does nothing while stopping. Thread safe.
    def wake(state = nil)
      state = normalize_hint(state)

      @mutex.synchronize do
        return self if @stopping
        remaining = state.fetch(:count, 1)
        broad = state.empty?
        future = state[:scheduled_at] && state[:scheduled_at] > Time.now

        @pools.each_with_index do |pool, index|
          next if state[:queue_name] && !pool.fetch(:selection).include?(state[:queue_name])

          cache = @caches.fetch(index)
          cache.notify_at(state.fetch(:scheduled_at)) if state[:scheduled_at]
          next if future

          if broad || state[:fanout]
            cache.request(pool.fetch(:threads))
          else
            remaining -= cache.request(remaining)
            break unless remaining.positive?
          end
        end

        @condition.broadcast
      end

      self
    rescue ArgumentError => error
      report_error(error)
      self
    end

    # Stops claiming and waits for the current run to drain.
    #
    # Without force, a timeout leaves jobs running. Callers can wait again later.
    #
    # @param timeout [Numeric, nil] grace period in seconds. Nil waits without
    #   limit. Zero signals and returns at once.
    # @param force [Boolean] kill execution threads that are still alive when
    #   the timeout expires. Has no effect when timeout is nil.
    # @return [Boolean] true when all threads and the tracker of the run exited
    # @raise [ArgumentError] if timeout is negative, infinite, or not numeric
    # @raise [Exception] the first fatal error of an owned thread, after the drain
    def shutdown(timeout: nil, force: false)
      deadline = ResourceShutdown.deadline(timeout)
      begin_shutdown.wait(deadline: deadline, force: force, timeout: timeout)
    end

    # Shuts down the current run, then starts a new run.
    #
    # @param timeout [Numeric, nil] see {#shutdown}
    # @param force [Boolean] see {#shutdown}
    # @return [Worker, false] self after a new run started. False when the drain
    #   did not finish, or when another start or stop happened during the drain.
    # @raise [Exception] the first fatal error of an owned thread
    def restart(timeout: nil, force: false)
      deadline = ResourceShutdown.deadline(timeout)
      handle = begin_shutdown
      ResourceShutdown.prepare_restart([handle], deadline: deadline, force: force, timeout: timeout) ? handle.restart : false
    end

    # Signals the current run to stop and captures it in a handle.
    #
    # Internal lifecycle protocol: signal only, with no joins or user callbacks.
    # The runtime and {Adapter} call it under their own locks for every
    # resource first, then wait on all handles with one shared deadline.
    #
    # @api private
    # @return [ResourceShutdown] a handle for this run only. Its snapshot keeps
    #   the thread list, tracker, and interruption records of this run, so a
    #   later run cannot change what the handle waits for.
    # @note Increments +@lifecycle_revision+, clears cached deadlines and
    #   permits, wakes waiting threads, writes to the listener pipe, and asks
    #   the tracker to stop.
    def begin_shutdown
      @mutex.synchronize do
        active = @threads.any?(&:alive?) || (@process_tracker && !@process_tracker.shutdown?)
        @lifecycle_revision += 1
        @stopping = true
        @caches.each(&:clear)
        @condition.broadcast
        @listener_writer&.write_nonblock(".", exception: false)
        @process_tracker&.stop
        ResourceShutdown.new(self, @lifecycle_revision,
          [@threads.dup, @process_tracker,
            {threads: @execution_threads.dup, job_ids: @executing_job_ids,
             interrupted_threads: @interrupted_threads, interrupted_job_ids: @interrupted_job_ids,
             pools: lifecycle_pools, pool_indexes: @execution_pool_indexes || {}}, @failure],
          lifecycle_pools: lifecycle_pools, observe_shutdown: active)
      end
    end

    # The methods below form the protocol that {ResourceShutdown} calls with send.

    private def shutdown_snapshot_stopped?(snapshot)
      threads, tracker = snapshot
      threads.none?(&:alive?) && (!tracker || tracker.shutdown?)
    end

    private def observe_lifecycle(name, payload)
      LifecycleEvents.record(self, name, payload, database: @database, on_error: @on_thread_error)
    end

    private def lifecycle_pools
      @scheduler_stats.each_with_index.map do |pool, index|
        queues = pool.fetch(:queues).dup
        {scheduler: self, queues: queues, performer_name: queues.dup,
         pool_index: index, max_threads: pool.fetch(:max_threads), run_id: @run_id}
      end
    end

    # Joins the captured threads and tracker until the deadline. When the
    # calling thread is one of the captured threads (a job that shuts down its
    # own worker), it skips itself and the tracker join, because its own
    # registration keeps the tracker alive. The result is then false. A fatal
    # error of the current run wins over join errors, unless a newer run has
    # replaced the tracker.
    private def drain_shutdown(snapshot, deadline:, force:)
      threads, tracker, interruption, failure = snapshot
      tracker&.stop

      # A fatal task can make join raise; finish draining every owned thread.
      threads.each do |thread|
        next if thread == Thread.current

        remaining = ResourceShutdown.remaining(deadline)

        begin
          thread.join(remaining)
        rescue Exception => error # standard:disable Lint/RescueException
          failure ||= error
        end
      end

      remaining = ResourceShutdown.remaining(deadline)

      begin
        monitor_stopped = tracker.nil? || (!threads.include?(Thread.current) && tracker.join(remaining))
      rescue Exception => error # standard:disable Lint/RescueException
        failure ||= error
      end

      interrupt_executions(**interruption) if force && deadline && ResourceShutdown.remaining(deadline).zero?
      failure = @mutex.synchronize { @failure if @process_tracker.equal?(tracker) } || failure
      raise failure if failure
      threads.none?(&:alive?) && monitor_stopped
    end

    # Lists the Active Job IDs that forced shutdown targeted in the current run.
    #
    # Returns a detached report. Callers cannot mutate execution bookkeeping.
    #
    # @return [Array<String>] Active Job +job_id+ values, not provider job IDs.
    #   {#start} resets the list.
    def interrupted_job_ids
      @mutex.synchronize { @interrupted_job_ids.map(&:dup) }
    end

    # @return [String, nil] the UUID of the decent_job_processes record of the
    #   current or last run, or nil before the first start
    def process_id
      @mutex.synchronize { @process_tracker&.process_id }
    end

    # Builds a snapshot of per-pool counters and cache sizes.
    #
    # @return [Hash{Symbol => Object}] see {Stats.aggregate}. The snapshot is a
    #   deep copy. {#start} resets the counters.
    # @note The process tracker heartbeat calls this to persist worker state.
    def stats
      @mutex.synchronize do
        schedulers = @scheduler_stats.each_with_index.map { |pool, index| pool.merge(@caches.fetch(index).stats) }
        Stats.aggregate(schedulers)
      end
    end

    # Sets whether execution threads use Ruby priority -3.
    #
    # @param value [Boolean]
    # @raise [ArgumentError] if value is not a Boolean
    # @note Each execution thread applies the value before its next attempt. A
    #   running job keeps its current priority.
    def lower_thread_priority=(value)
      Configuration.validate_lower_thread_priority!(value)
      @mutex.synchronize { @lower_thread_priority = value }
    end

    # @return [Boolean] whether execution threads lower their priority
    def lower_thread_priority
      @mutex.synchronize { @lower_thread_priority }
    end

    # Reports whether no pool has an active execution.
    #
    # @param duration [Numeric, nil] also require this many seconds of
    #   monotonic time since the run started or since the last execution start
    #   or finish. Empty claim attempts do not reset this clock. Nil skips the
    #   time check.
    # @return [Boolean] true for a worker that never started
    # @raise [ArgumentError] if duration is negative or not finite
    def idle?(duration = nil)
      Configuration.validate_idle_timeout!(duration)

      @mutex.synchronize do
        @scheduler_stats.all? { |pool| pool.fetch(:active_threads).zero? } &&
          (duration.nil? || @idle_since.nil? || monotonic_time - @idle_since >= duration)
      end
    end

    private

    # Kills each captured execution thread once. Records the targets and job
    # IDs under +@mutex+, kills every target, and only then emits events.
    # Errors go to +on_thread_error+ so a failed observer cannot stop shutdown.
    def interrupt_executions(threads:, job_ids:, interrupted_threads:, interrupted_job_ids:, pools:, pool_indexes:)
      targets, reports = @mutex.synchronize do
        targets = threads.select do |thread|
          thread != Thread.current && thread.alive? && !interrupted_threads.key?(thread)
        end

        targets.each { |thread| interrupted_threads[thread] = true }
        ids = targets.filter_map { |thread| job_ids[thread] }.uniq
        interrupted_job_ids.concat(ids.map(&:dup)).uniq!

        reports = targets.group_by { |thread| pool_indexes.fetch(thread) }.map do |index, pool_threads|
          pools.fetch(index).merge(active_job_ids: pool_threads.filter_map { |thread| job_ids[thread]&.dup }.uniq)
        end

        [targets, reports]
      end

      return if targets.empty?

      # Request every interruption before invoking observers, which may reenter
      # shutdown or raise. Keep the report independent of observer mutation.
      targets.each(&:kill)
      reports.each { |payload| observe_lifecycle("scheduler_shutdown_kill", payload) }
    rescue Exception => error # standard:disable Lint/RescueException
      report_error(error)
    end

    # Validates a hint from {#wake} and returns it with Symbol keys and a Time
    # +scheduled_at+. Raises ArgumentError for any invalid field.
    def normalize_hint(state)
      return {} if state.nil?
      raise ArgumentError, "notification must be an object" unless state.is_a?(Hash)
      state = state.transform_keys { |key| key.is_a?(String) ? key.to_sym : key }

      unless (state.keys - %i[queue_name count fanout scheduled_at]).empty?
        raise ArgumentError, "unknown notification fields"
      end

      if state.key?(:queue_name) && !(state[:queue_name].is_a?(String) && !state[:queue_name].empty?)
        raise ArgumentError, "notification queue_name must be a nonempty string"
      end

      if state.key?(:count) && !(state[:count].is_a?(Integer) && state[:count].positive?)
        raise ArgumentError, "notification count must be a positive integer"
      end

      if state.key?(:fanout) && ![true, false].include?(state[:fanout])
        raise ArgumentError, "notification fanout must be true or false"
      end

      if state.key?(:scheduled_at)
        value = state[:scheduled_at]

        state[:scheduled_at] = case value
        when Time then value
        when String
          value = JSON.parse(value, max_nesting: 1) if value.lstrip.start_with?('"')
          Time.iso8601(value)
        when Numeric
          raise ArgumentError, "notification scheduled_at must be finite and real" unless value.real? && value.finite?
          Time.at(value)
        else
          raise ArgumentError, "notification scheduled_at must be a time"
        end
      end

      state
    rescue JSON::ParserError, RangeError, TypeError => error
      raise ArgumentError, error.message
    end

    # Caller must hold +@mutex+.
    def started_locked?
      !@stopping && @scheduler_stats.all? { |pool| pool.fetch(:live_threads) == pool.fetch(:max_threads) } &&
        @execution_threads.all?(&:alive?)
    end

    # Replaces every pool cache and counter hash. Caller must hold +@mutex+, or
    # run from the initializer. A new cache starts with one permit per thread,
    # so each thread claims once at startup.
    def reset_stats(running: false)
      @caches = @pools.map { |pool| SchedulerCache.new(max_cache: @max_cache, threads: pool.fetch(:threads)) }

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
         execution_at: nil, check_queue_at: nil, last_execution_at: nil, last_success_at: nil, last_error_at: nil, last_empty_at: nil}
      end
    end

    def record_attempt(index, error:)
      @mutex.synchronize do
        counters = @scheduler_stats.fetch(index)
        counters[:total_executions_count] += 1
        counters[error ? :errored_executions_count : :succeeded_executions_count] += 1
        now = Time.now.iso8601(6)
        @idle_since = monotonic_time
        counters[:execution_at] = now
        counters[:last_execution_at] = now
        counters[error ? :last_error_at : :last_success_at] = now
      end
    end

    # Body of one execution thread. The thread holds a tracker registration for
    # its whole life, so the process record stays alive while it runs. A fatal
    # error is stored in +@failure+ for {#drain_shutdown} to raise. The last
    # execution thread to exit stops the whole run, also when no one called
    # {#begin_shutdown}. This does not change +@lifecycle_revision+.
    def run(pool, index)
      original_priority = Thread.current.priority

      @process_tracker.register(stopping: method(:stopping?), wait_for_monitor: false) do
        ThreadStatus.with_worker(self) { run_registered(pool, index, original_priority) }
      end
    rescue Exception => error # standard:disable Lint/RescueException
      @mutex.synchronize { @failure ||= error }
      raise
    ensure
      last = Thread.handle_interrupt(Object => :never) do
        @mutex.synchronize do
          counters = @scheduler_stats.fetch(index)
          counters[:live_threads] -= 1
          counters[:available_threads] -= 1

          if @scheduler_stats.all? { |pool_stats| pool_stats.fetch(:live_threads).zero? }
            @stopping = true
            @caches.each(&:clear)
            @condition.broadcast
            @listener_writer&.write_nonblock(".", exception: false)
            true
          end
        end
      end

      @process_tracker.stop if last
    end

    # Claim loop of one execution thread. Each pass takes one cache permit,
    # runs one claim, and updates counters. On exit from a pass, the thread
    # returns its busy slot and wakes one more thread when it ran a job, hit
    # an error, or the cache revision changed during the pass.
    #
    # Error rules: a StandardError from infrastructure is reported, and the
    # thread retries after {#retry_interval}. An excluded execution error
    # (outside handled_exceptions) or a non-StandardError ends the thread.
    # Infrastructure errors do not count as attempts.
    #
    # Counter and cache updates run with interrupts deferred, so a kill cannot
    # leave them half updated. {#await_work} accepts interrupts only while it
    # blocks.
    def run_registered(pool, index, original_priority)
      selection = pool.fetch(:selection)
      execution_error = nil
      executing = false
      lowered_priority = false

      performer = Performer.new(database: @database, process_tracker: @process_tracker,
        on_execution_start: lambda { |active_job_id|
          Thread.handle_interrupt(Object => :never) do
            @mutex.synchronize do
              executing = true
              @executing_job_ids[Thread.current] = active_job_id
              counters = @scheduler_stats.fetch(index)
              @idle_since = monotonic_time
              counters[:execution_at] = Time.now.iso8601(6)
              counters[:active_threads] += 1
              counters[:available_threads] -= 1
            end
          end
        }, on_unhandled_exception: ->(error) { execution_error = error })

      loop do
        revision = poll = task_error = nil

        begin
          reported_error = nil
          result = retry_job_id = nil
          retry_attempt = false
          Thread.handle_interrupt(Object => :on_blocking) { revision, poll = await_work(index) }

          if poll
            reported_error = poll[:error]
            report_error(reported_error) if reported_error
            instrument_pool("finished_timer_task", index, **poll)
            raise reported_error if reported_error
          end

          break unless revision

          execution_error = nil
          execution_finished = false
          lowered_priority = apply_thread_priority(original_priority, lowered_priority)
          cleanup_processes
          @mutex.synchronize { @scheduler_stats.fetch(index)[:check_queue_at] = Time.now.iso8601(6) }
          task_error = nil

          begin
            result = performer.perform_one(queues: selection, stopping: method(:stopping?))
          rescue Exception => error # standard:disable Lint/RescueException
            task_error = error
          end

          break if Thread.current.status == "aborting"

          completed_at = Time.now

          if result
            record_attempt(index, error: result.error)
            execution_finished = true
          elsif task_error
            if task_error.equal?(execution_error)
              record_attempt(index, error: task_error)
              execution_finished = true
            elsif executing
              @mutex.synchronize do
                @idle_since = monotonic_time
                @scheduler_stats.fetch(index)[:execution_at] = Time.now.iso8601(6)
              end

              execution_finished = true
            end
          else
            @mutex.synchronize do
              counters = @scheduler_stats.fetch(index)
              counters[:empty_executions_count] += 1
              counters[:last_empty_at] = Time.now.iso8601(6)
            end
          end

          retry_job_id = result.job_id.dup if result&.state == "retried"
          reported_error = task_error || result&.unhandled_error
          report_error(reported_error) if reported_error
          instrument_pool("finished_job_task", index, result: result, error: task_error, time: completed_at)
          raise task_error if task_error
          reported_error = nil

          if result
            if retry_job_id
              Store.new(@database).scheduling_hints([retry_job_id]).each { |state| wake(state) }
            end

            cleanup_after_attempt
          else
            refresh_cache(pool, index)
          end
        rescue Exception => error # standard:disable Lint/RescueException
          report_error(error) unless error.equal?(reported_error)
          raise if error.equal?(execution_error) || !error.is_a?(StandardError)
          wait(retry_interval)
          retry_attempt = true
        ensure
          Thread.handle_interrupt(Object => :never) do
            if revision
              @mutex.synchronize do
                cache = @caches.fetch(index)
                cache.busy -= 1
                cache.wake(1) if result || retry_attempt || cache.revision != revision
              end
            end

            if executing
              @mutex.synchronize do
                counters = @scheduler_stats.fetch(index)

                unless execution_finished
                  @idle_since = monotonic_time
                  counters[:execution_at] = Time.now.iso8601(6)
                end

                counters[:active_threads] -= 1
                counters[:available_threads] += 1
              end

              @mutex.synchronize { @executing_job_ids.delete(Thread.current) }
              executing = false
            end
          end
        end
      end
    end

    def instrument_pool(name, index, **payload)
      queues = @mutex.synchronize { @scheduler_stats.fetch(index).fetch(:queues).dup }
      instrument(name, payload.merge(scheduler: self, queues: queues, performer_name: queues.dup, pool_index: index))
    end

    def instrument(name, payload = {}, &block)
      payload = payload.merge(process_id: Process.pid, thread_name: (Thread.current.name || Thread.current.object_id).to_s)

      Instrumentation.instrument("#{name}.decent_job", payload, database: @database, on_error: @on_thread_error) do
        block&.call
      end
    end

    # Returns whether the priority is now lowered. Restores the original
    # priority only if this thread lowered it before.
    def apply_thread_priority(original_priority, previously_lowered)
      enabled = lower_thread_priority
      Thread.current.priority = enabled ? -3 : original_priority if enabled || previously_lowered
      enabled
    rescue NotImplementedError
      # Some Ruby engines do not support adjusting native thread priority.
      false
    end

    # Blocks on +@condition+ until the pool has a permit, then takes it and
    # marks the thread busy. Returns false when stopping, [revision, poll] for
    # a permit, or [nil, poll] when the poll step raised. A due cached deadline
    # gives a permit to every free pool thread. A due poll interval adds one. The wait is
    # at most 0.5 seconds so wall-clock deadlines are rechecked. Poll timing
    # uses monotonic time.
    def await_work(index)
      poll_at = monotonic_time + @poll_interval if @poll_interval.positive?

      @mutex.synchronize do
        cache = @caches.fetch(index)

        loop do
          return false if @stopping
          now = Time.now

          if cache.due?(now) && monotonic_time >= cache.due_retry_at
            cache.expire(now)
            cache.request(@pools.fetch(index).fetch(:threads))
          end

          if poll_at && monotonic_time >= poll_at
            begin
              poll = {result: cache.wake(1), error: nil, time: Time.now}
            rescue Exception => error # standard:disable Lint/RescueException
              return [nil, {result: nil, error: error, time: Time.now}]
            end
          end

          if cache.permits.positive?
            cache.permits -= 1
            cache.busy += 1
            return [cache.revision, poll]
          end

          waits = [0.5]
          waits << poll_at - monotonic_time if poll_at
          waits << [cache.next_at - now, cache.due_retry_at - monotonic_time].max if cache.next_at
          @condition.wait(@mutex, [waits.min, 0.001].max)
        end
      end
    end

    # Reloads future deadlines for one pool after an empty claim. The SQL runs
    # outside +@mutex+. +refreshing+ keeps one refresh per pool at a time, and
    # the captured revision lets {SchedulerCache#replace} keep deadlines that
    # arrived during the query.
    def refresh_cache(pool, index)
      revision = @mutex.synchronize do
        cache = @caches.fetch(index)
        next unless !@stopping && cache.refresh?
        cache.refreshing = true
        cache.revision
      end

      return unless revision

      begin
        cache = @caches.fetch(index)
        times = Claim.new(@database, enable_pauses: DecentJob.configuration.enable_pauses).next_scheduled_at(queues: pool.fetch(:selection), limit: cache.limit, now_limit: pool.fetch(:threads))

        @mutex.synchronize do
          unless @stopping
            cache.replace(times, revision: revision)
            # A forward wall-clock jump cannot make SQL execute future work.
            # Back off a rejected deadline instead of repeatedly querying it.
            cache.due_retry_at = monotonic_time + 0.5 if cache.due?(Time.now)
          end

          @condition.broadcast
        end
      ensure
        @mutex.synchronize { @caches.fetch(index).refreshing = false }
      end
    end

    # Deletes stale process records at most once per
    # ProcessRecord::STALE_INTERVAL per worker. One execution thread runs it
    # before a claim. The others skip it.
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

    # Counts an attempt and runs record cleanup on this execution thread when
    # {CleanupTracker} says it is due. Only one thread cleans at a time.
    # Cleanup errors are reported and do not stop the thread.
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

    # Body of the listener thread. It owns one dedicated connection outside the
    # pool. After each successful LISTEN it wakes all pools to catch up on
    # missed work, then reconnects after errors. It waits on the socket and on
    # +@listener_reader+, so {#begin_shutdown} can interrupt the select by
    # writing to the pipe. The ensure block closes the pipe.
    def listen
      until stopping?
        connection = nil
        listening = false
        task_error = nil

        begin
          Thread.handle_interrupt(Object => :on_blocking) { connection = @database.connect }

          instrument("notifier_listen") do
            Thread.handle_interrupt(Object => :on_blocking) do
              connection.exec("LISTEN decent_job")
              listening = true
            end
          end

          @mutex.synchronize { @listener_connected = true }
          wake
          keepalive_at = monotonic_time + 10

          until stopping?
            if monotonic_time >= keepalive_at
              connection.exec("SELECT 1")
              keepalive_at = monotonic_time + 10
            end

            # pg can buffer several notifications after draining the socket.
            # Drain those before selecting again, with bounded batches so a
            # busy publisher cannot starve shutdown checks or keepalives.
            pending = false

            100.times do
              break if stopping?
              pending = connection.wait_for_notify(0) { |_channel, _pid, payload| receive_notification(payload) }
              break unless pending
            end

            break if stopping?
            next if pending

            readable = IO.select([connection.socket_io, @listener_reader], nil, nil, 1)
            break if readable&.first&.include?(@listener_reader)
          end
        rescue Exception => error # standard:disable Lint/RescueException
          task_error = error unless stopping? && error.is_a?(StandardError)
        ensure
          Thread.handle_interrupt(Object => :never) do
            @mutex.synchronize { @listener_connected = false }

            begin
              # A disconnected or unfinished LISTEN session cannot UNLISTEN.
              if listening && !connection.finished? && connection.status == PG::CONNECTION_OK
                instrument("notifier_unlisten") { connection.exec("UNLISTEN decent_job") }
              end
            rescue Exception => error # standard:disable Lint/RescueException
              task_error ||= error
            ensure
              begin
                connection.finish if connection && !connection.finished?
              rescue Exception => error # standard:disable Lint/RescueException
                task_error ||= error
              end
            end
          end
        end

        if task_error
          instrument("notifier_notify_error", error: task_error)
          report_error(task_error)
          raise task_error unless task_error.is_a?(StandardError)
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

    # Parses one NOTIFY payload and passes it to {#wake}. Errors are reported
    # and the listener keeps its connection.
    def receive_notification(payload)
      instrument("notifier_notified", payload: payload&.dup)
      # pg represents a PostgreSQL NOTIFY with no payload as nil.
      payload = "" if payload.nil?
      raise ArgumentError, "notification payload is too large" if payload.bytesize > Notifier::MAX_PAYLOAD_BYTES
      state = payload.empty? ? nil : JSON.parse(payload, max_nesting: 4)
      raise ArgumentError, "notification must be an object" if !payload.empty? && !state.is_a?(Hash)
      application = Rails.application if defined?(Rails) && Rails.respond_to?(:application)

      if application&.respond_to?(:executor)
        application.executor.wrap { wake(state) }
      else
        wake(state)
      end
    rescue => error
      report_error(error)
    end

    def stopping?
      @mutex.synchronize { @stopping }
    end

    # Sleeps up to interval seconds. Returns early when shutdown begins.
    def wait(interval)
      deadline = monotonic_time + interval

      @mutex.synchronize do
        loop do
          break if @stopping

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
