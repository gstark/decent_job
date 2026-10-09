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
  # Each execution thread owns its database checkout for the entire job. The
  # notification listener uses its own connection and only hints workers to poll.
  class Worker
    attr_reader :database

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

    def start(expected_revision: nil, automatic: false)
      start_run(expected_revision: expected_revision, automatic: automatic).first
    end

    private def start_run(expected_revision: nil, automatic: false)
      result = DecentJob.synchronize_resource_start(automatic: automatic) do
        @mutex.synchronize do
          return [false, nil] if automatic && @stopping && @lifecycle_revision.positive?
          return [false, nil] if expected_revision && expected_revision != @lifecycle_revision
          return [self, nil] if @threads.any?(&:alive?) || (@process_tracker && !@process_tracker.shutdown?)

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
      result || [self, nil]
    end

    def shutdown?
      return true unless @threads
      @mutex.synchronize { @threads.none?(&:alive?) && (!@process_tracker || @process_tracker.shutdown?) }
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

    # Without force, a timeout leaves jobs running; callers may wait again later.
    def shutdown(timeout: nil, force: false)
      deadline = ResourceShutdown.deadline(timeout)
      begin_shutdown.wait(deadline: deadline, force: force, timeout: timeout)
    end

    def restart(timeout: nil, force: false)
      deadline = ResourceShutdown.deadline(timeout)
      handle = begin_shutdown
      ResourceShutdown.prepare_restart([handle], deadline: deadline, force: force, timeout: timeout) ? handle.restart : false
    end

    # Internal lifecycle protocol: signal only, with no joins or user callbacks.
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

    # Return a detached report; callers cannot mutate execution bookkeeping.
    def interrupted_job_ids
      @mutex.synchronize { @interrupted_job_ids.map(&:dup) }
    end

    def process_id
      @mutex.synchronize { @process_tracker&.process_id }
    end

    def stats
      @mutex.synchronize do
        schedulers = @scheduler_stats.each_with_index.map { |pool, index| pool.merge(@caches.fetch(index).stats) }
        Stats.aggregate(schedulers)
      end
    end

    def lower_thread_priority=(value)
      Configuration.validate_lower_thread_priority!(value)
      @mutex.synchronize { @lower_thread_priority = value }
    end

    def lower_thread_priority
      @mutex.synchronize { @lower_thread_priority }
    end

    def idle?(duration = nil)
      Configuration.validate_idle_timeout!(duration)
      @mutex.synchronize do
        @scheduler_stats.all? { |pool| pool.fetch(:active_threads).zero? } &&
          (duration.nil? || @idle_since.nil? || monotonic_time - @idle_since >= duration)
      end
    end

    private

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
        when String then Time.iso8601(value)
        when Numeric
          raise ArgumentError, "notification scheduled_at must be finite and real" unless value.real? && value.finite?
          Time.at(value)
        else
          raise ArgumentError, "notification scheduled_at must be a time"
        end
      end
      state
    rescue RangeError, TypeError => error
      raise ArgumentError, error.message
    end

    def started_locked?
      !@stopping && @scheduler_stats.all? { |pool| pool.fetch(:live_threads) == pool.fetch(:max_threads) } &&
        @execution_threads.all?(&:alive?)
    end

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

    def apply_thread_priority(original_priority, previously_lowered)
      enabled = lower_thread_priority
      Thread.current.priority = enabled ? -3 : original_priority if enabled || previously_lowered
      enabled
    rescue NotImplementedError
      # Some Ruby engines do not support adjusting native thread priority.
      false
    end

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
