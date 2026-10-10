# frozen_string_literal: true

module DecentJob
  # Persists jobs to PostgreSQL and, in async mode, owns the worker that runs
  # them.
  #
  # This is the queue adapter for both native jobs and Active Job. It does not
  # need Active Job: {DecentJob.default_adapter} creates one for native jobs, and
  # {ActiveJob::QueueAdapters::DecentJobAdapter} subclasses it for Active Job.
  # Enqueue goes through {Store} and {ConcurrencyControls}. After commit, the
  # execution mode picks what happens next: inline mode runs the jobs through
  # {InlineBuffer}, async mode starts and wakes the adapter's {Worker}, and
  # external mode only persists (the insert sends NOTIFY for remote workers).
  #
  # Ownership: an async adapter owns at most one {Worker}. `@worker_mutex`
  # guards `@worker`, `@stopping`, `@retired`, and `@lifecycle_revision`. The
  # revision changes on each shutdown, restart, retire, and worker reset, so a
  # deferred restart can detect that a newer lifecycle operation superseded it.
  #
  # Invariants:
  # - A retired adapter never starts again. {DecentJob.reset!} retires every
  #   registered adapter; construct a new adapter after reset.
  # - An explicit {#shutdown} keeps the adapter stopped until {#restart}.
  #   Enqueue wakeups and framework readiness do not reopen it.
  #
  # Each adapter registers itself with {DecentJob.register_adapter} when it is
  # built.
  #
  # See docs/rails-integration.md and docs/lifecycle.md.
  class Adapter
    # @param database [Database, nil] the pool for this adapter, or nil to use
    #   {DecentJob.database} when first needed
    # @param execution_mode [Symbol, nil] one of {Configuration::EXECUTION_MODES},
    #   or nil to follow {Configuration#execution_mode} at each call
    # @raise [ArgumentError] when execution_mode is not a known mode
    # @note Registers the adapter with the global runtime. Registration can
    #   start the async worker when the host is ready, and retires the adapter
    #   at once when a reset is in progress.
    def initialize(database: nil, execution_mode: nil)
      Configuration.validate_execution_mode!(execution_mode) unless execution_mode.nil?
      @database = database
      @execution_mode = execution_mode
      @worker_mutex = Mutex.new
      @stopping = false
      @lifecycle_revision = 0
      DecentJob.register_adapter(self)
    end

    # Drops state inherited from the parent process after a supervisor fork.
    #
    # An async mode becomes :external, because the supervised child runs the
    # workers that the CLI configures. The parent's worker and mutex are
    # discarded. A retired adapter stays stopped.
    #
    # @return [void]
    # @api private
    # @note Called from {DecentJob.after_supervisor_fork} for every live
    #   adapter. It takes no lock, because the child has only one thread.
    def reset_after_fork
      @execution_mode = :external if Configuration::ASYNC_MODES.include?(execution_mode)
      @worker_mutex = Mutex.new
      @worker = nil
      @stopping = !!@retired
      @lifecycle_revision = 0
    end

    # Stops the adapter for good.
    #
    # Reset retires adapters permanently; a new adapter chooses a fresh pool.
    # This method does not drain the worker. Call {#shutdown} first.
    #
    # @return [void]
    # @api private
    def retire
      @worker_mutex.synchronize do
        @retired = true
        @stopping = true
        @lifecycle_revision += 1
      end
    end

    # Forgets the worker so the next start builds one from the new
    # configuration.
    #
    # CLI reconfiguration preserves adapter ownership and its explicit stop flag.
    # The caller must drain the old worker first.
    #
    # @return [void]
    # @api private
    def reset_worker
      @worker_mutex.synchronize do
        @worker = nil
        @lifecycle_revision += 1
      end
    end

    # @return [Database] the explicit pool, or the global {DecentJob.database}
    # @note The global pool is cached on first use. A later change to the
    #   global pool does not move this adapter.
    def database
      @database ||= DecentJob.database
    end

    # Enqueues one job now. This is the Active Job adapter entry point.
    #
    # When a bulk capture is active, the job joins the buffer and nothing is
    # written yet. When the job is the one that runs now (same job_id and
    # database), its existing row is rescheduled instead of inserting a copy.
    #
    # @param job [Object] an Active Job or {Base} instance
    # @return [String, nil] the provider job ID (the decent_jobs.id UUID), or
    #   nil when a bulk capture buffered the job
    # @raise [EnqueueError] for native jobs when the insert fails
    # @raise [ActiveJob::EnqueueError] for Active Job classes, with the
    #   original error as the cause
    # @note Writes decent_jobs and sends NOTIFY in the caller's transaction
    #   when one is open. Inline execution and async wakeups wait for commit.
    def enqueue(job)
      raising_integration_errors(job) { enqueue_job(job) }
    end

    # Enqueues one job to run at a later time.
    #
    # @param job [Object] an Active Job or {Base} instance
    # @param timestamp [Numeric, nil] the run time in seconds since the Unix
    #   epoch, or nil to keep the job's own scheduled_at
    # @return [String, nil] the provider job ID, or nil when a bulk capture
    #   buffered the job
    # @raise (see #enqueue)
    def enqueue_at(job, timestamp)
      job.scheduled_at = Time.at(timestamp).utc unless timestamp.nil?
      raising_integration_errors(job) { enqueue_job(job, scheduled_at: timestamp) }
    end

    # Enqueues several jobs. Called by Active Job's perform_all_later, by
    # {Base.perform_all_later}, and when a {Bulk::Buffer} flushes.
    #
    # Plain jobs go in one multi-row INSERT without enqueue callbacks.
    # Concurrency-controlled jobs (see {ConcurrencyControls.controlled?}) go
    # through individual enqueue, so each one gets its admission check.
    # Duplicate job objects are enqueued once.
    #
    # @param jobs [Array<Object>] job instances for this adapter
    # @param callbacks [Boolean] false when the enqueue callbacks of the
    #   controlled jobs already ran, for example in {Bulk::Buffer#prepare}
    # @return [Integer] the number of jobs accepted. A bulk capture accepts all
    #   jobs and returns their count.
    # @raise (see #enqueue)
    # @note Sets provider_job_id, successfully_enqueued, and enqueue_error on
    #   each job. A plain job that hits a duplicate key gets an enqueue_error
    #   instead of raising.
    def enqueue_all(jobs, callbacks: true)
      raising_integration_errors(jobs.first) do
        jobs = jobs.uniq
        if Bulk.capture(jobs, queue_adapter: self)
          jobs.each { |job| job.successfully_enqueued = true }
          return jobs.size
        end

        controlled, plain = jobs.partition { |job| ConcurrencyControls.controlled?(job) }
        count = persist(plain, skip_duplicates: true)
        controlled.each do |job|
          if callbacks
            job.enqueue
          else
            enqueue_without_callbacks(job)
          end
          count += 1 if job.successfully_enqueued?
        end
        count
      end
    end

    # Active Job's adapter preference for transaction deferral.
    #
    # Active Job 8.1 reads the job-class setting instead and ignores this
    # value. See docs/rails-integration.md.
    #
    # @return [true]
    def enqueue_after_transaction_commit?
      true
    end

    # Tells a running job whether it should stop.
    #
    # Active Job 8.1 calls this with no argument. Jobs can also pass themselves.
    # The checks run in this order: adapter or global shutdown, operator
    # force-discard of the current execution, then persistent pauses.
    #
    # @param job_instance [Object, nil] the job to check, or nil to use the
    #   execution that runs on this thread
    # @return [Symbol, false] :stopping, :force_discarded, :queue_name_paused,
    #   :job_class_paused, :label_paused, or false to continue
    # @note Reads decent_job_executions to detect force-discard, and reads the
    #   decent_job_pauses table when {Configuration#enable_pauses} is on. Manual retry
    #   callbacks have no execution, so they see only shutdown and, when
    #   job_instance is given, pauses.
    def stopping?(job_instance = nil)
      context = Current.execution
      context = nil unless context && !context[:manual_retry] && context[:database].equal?(database)
      return :stopping if @worker_mutex.synchronize { @stopping } || DecentJob.stopping? || context&.fetch(:stopping, nil)&.call

      matching = context && (!job_instance || job_instance.job_id == context[:job_id])
      if matching && context[:execution_id]
        finished = database.with_connection do |connection|
          connection.exec_params("SELECT 1 FROM decent_job_executions WHERE id = $1 AND job_id = $2 AND finished_at IS NOT NULL", [context[:execution_id], context[:id]]).ntuples.positive?
        end
        return :force_discarded if finished
      end
      return false unless DecentJob.configuration.enable_pauses
      return Pauses.new(database).paused?(job_instance: job_instance) if job_instance
      return false unless matching && context[:id]

      row = store.find(context[:id])
      row ? Pauses.new(database).reason(queue: row.fetch("queue_name"), job_class: row.fetch("job_class"), labels: row.fetch("labels")) : false
    end

    # Stops the adapter's worker and its database's cron manager, and waits
    # for them to drain.
    #
    # @param timeout [Numeric, nil] seconds to wait, or nil to wait forever
    # @param force [Boolean] true to interrupt executions still running at the
    #   deadline
    # @return [Boolean] true when every resource drained
    # @raise [ArgumentError] when timeout is negative or not finite
    # @note The adapter stays stopped until {#restart}, even when it returns
    #   false. The cron manager stop also affects other adapters that share
    #   the same pool.
    def shutdown(timeout: nil, force: false)
      deadline = ResourceShutdown.deadline(timeout)
      _revision, handles = begin_shutdown
      ResourceShutdown.wait_all(handles, deadline: deadline, force: force, timeout: timeout)
    end

    # Stops the adapter, waits for the old run to drain, then starts it again.
    #
    # @param timeout [Numeric, nil] seconds to wait for the drain, or nil to
    #   wait forever
    # @param force [Boolean] true to interrupt executions still running at the
    #   deadline
    # @return [Adapter, false] self on success. False when the adapter is
    #   retired, the drain did not finish, the runtime is stopping, or a newer
    #   lifecycle operation superseded this restart.
    # @raise [ArgumentError] when timeout is negative or not finite
    def restart(timeout: nil, force: false)
      deadline = ResourceShutdown.deadline(timeout)
      return false if @worker_mutex.synchronize { @retired }
      revision, handles = begin_shutdown
      return false unless ResourceShutdown.prepare_restart(handles, deadline: deadline, force: force, timeout: timeout)

      LifecycleEvents.defer do
        @worker_mutex.synchronize do
          if @retired || revision != @lifecycle_revision
            handles.each { |handle| handle.restart_failed(:refused) }
            return false
          end
          DecentJob.synchronize_resource_start do
            if DecentJob.stopping?
              handles.each { |handle| handle.restart_failed(:refused) }
              return false
            end
            return false unless handles.map(&:restart).all?
            @lifecycle_revision += 1
            @stopping = false
            DecentJob.locally_resume_worker(@worker, database: @database)
          end
        end
      end
      start_async
      self
    end

    # Marks the adapter stopped and starts the shutdown of its worker and of
    # the cron manager for its explicit database.
    #
    # @return [Array(Integer, Array<ResourceShutdown>)] the new lifecycle
    #   revision and the handles to wait on
    # @api private
    # @note Takes `@worker_mutex`, then {DecentJob.runtime_mutex}. Keep this
    #   lock order.
    def begin_shutdown
      @worker_mutex.synchronize do
        @lifecycle_revision += 1
        @stopping = true
        DecentJob.runtime_mutex.synchronize do
          DecentJob.locally_stop_worker(@worker)
          handles = @database ? DecentJob.begin_cron_shutdown(database: @database) : []
          handles.concat(ResourceShutdown.capture_all([@worker])) if @worker
          [@lifecycle_revision, handles]
        end
      end
    end

    # Schedules the follow-up work for jobs that were just persisted.
    #
    # Inline mode runs the jobs after the outer transaction commits. It clears
    # the execution, bulk, cron, and batch contexts first, so the inline jobs
    # do not inherit the enqueuer's context. Async mode starts the worker after
    # commit and wakes it with the queue and deadline of each job. External
    # mode does nothing here.
    #
    # @param ids [Array<String>] decent_jobs.id values
    # @return [void]
    # @api private
    # @note Called by {Job#retry_job} and by enqueue in this class.
    def after_enqueue(ids)
      if execute_inline?
        database.after_commit do
          Current.with(nil) do
            Bulk.with_buffer(nil) do
              CronEntry.with_context(nil) { Batch.with_context(nil) { InlineBuffer.perform(database, ids) } }
            end
          end
        end
      elsif execute_async?
        database.after_commit do
          worker = start_async
          store.scheduling_hints(ids).each { |state| worker.wake(state) } if worker
        end
      end
    end

    # Puts an existing job row back in the ready state instead of inserting a
    # new row. Used for a running job that enqueues itself again (retry_on)
    # and for operator retries.
    #
    # Management retries supply their callback-prepared payload so unavailable
    # arguments do not need to deserialize again under the persistence locks.
    #
    # @param job [Object] the job instance
    # @param context [Hash] the execution context. :id is the decent_jobs.id,
    #   and :manual_retry marks an operator retry.
    # @param payload [Hash, nil] a prepared payload, or nil to serialize job
    # @param attributes [Hash, nil] the current row attributes for the
    #   instrumentation event, or nil to build them from job
    # @param scheduled_at [Numeric, nil] the run time in seconds since the
    #   epoch, for the instrumentation event only
    # @return [String] the provider job ID
    # @raise (see #enqueue)
    # @api private
    def reschedule(job, context, payload: nil, attributes: nil, scheduled_at: nil)
      raising_integration_errors(job) { reschedule_job(job, context, payload: payload, attributes: attributes, scheduled_at: scheduled_at) }
    end

    # @return [Symbol] the explicit mode, or the configured mode when none was
    #   given
    def execution_mode
      @execution_mode.nil? ? DecentJob.configuration.execution_mode : @execution_mode
    end

    # @return [Boolean] true when this process runs jobs in background
    #   threads. :async and :async_server count only inside a web server.
    def execute_async?
      execution_mode == :async_all || (Configuration::SERVER_ASYNC_MODES.include?(execution_mode) && DecentJob.configuration.in_webserver?)
    end

    # @return [Boolean] true when jobs run synchronously after commit
    def execute_inline?
      execution_mode == :inline
    end

    # @return [Boolean] true when this process only persists jobs. This
    #   includes an async mode outside a web server.
    def execute_external?
      !execute_async? && !execute_inline?
    end

    # @return [Boolean] true when the adapter's worker exists and has started
    def async_started?
      @worker_mutex.synchronize { !!@worker&.started? }
    end

    # Starts the async worker, and the cron manager when cron is enabled.
    #
    # It does nothing when the mode is not async for this process, the host is
    # not ready, no database is configured, or the adapter is stopped.
    # Explicit {#shutdown} and {#retire} set the stopped flag.
    #
    # @return [Worker, nil] the running worker, or nil when nothing started
    # @note Safe to call often. Enqueue calls it after each commit. The
    #   worker is created once and reused until {#reset_worker}.
    def start_async
      LifecycleEvents.defer do
        return unless DecentJob.async_ready? && execute_async?
        return unless @database || DecentJob.configuration.database_url

        return if @worker_mutex.synchronize { @stopping }
        connection_pool = @database || DecentJob.database(automatic: true)
        return unless connection_pool
        worker = @worker_mutex.synchronize do
          return if @stopping
          DecentJob.synchronize_resource_start(automatic: true) do
            @database ||= connection_pool
            @worker ||= Worker.new(database: connection_pool, **DecentJob.configuration.worker_options)
            DecentJob.start_worker(@worker)
          end
        end
        DecentJob.start_cron(database: database, automatic: true) if worker && DecentJob.configuration.enable_cron
        worker
      end
    end

    private

    # Copies the stored attributes for the instrumentation event, then updates
    # the row in place. The UPDATE checks execution ownership.
    def reschedule_job(job, context, payload:, attributes:, scheduled_at:)
      attempted = store.enqueue_attributes(job, id: context.fetch(:id), payload: payload)
      if attributes
        attempted = attributes.merge(attempted.slice("payload", "queue_name", "priority", "labels", "concurrency_keys", "scheduled_at"))
          .merge("state" => "ready", "finished_at" => nil, "performed_at" => nil, "error_event" => "retried")
      end
      instrument_enqueue(job, scheduled_at: scheduled_at, attributes: attempted) do |event, prepared|
        store.reschedule(job, context, payload: prepared.fetch("payload")) { |row| event[:job] = Job.from_attributes(row, database: database) }
        event[:accepted] = true
      end
      job.provider_job_id
    end

    # The admission check and the insert share one transaction that holds the
    # concurrency scope locks. A nil provider ID after the insert means the
    # cron occurrence constraint skipped the row.
    def enqueue_job(job, scheduled_at: nil)
      return if Bulk.capture(job, queue_adapter: self, callbacks_run: true)

      context = Current.execution
      if context && context[:database].equal?(database) && context[:job_id] == job.job_id
        reschedule(job, context, scheduled_at: scheduled_at)
        return job.provider_job_id
      end
      ConcurrencyControls.new(database).enqueue(job) do
        instrument_enqueue(job, scheduled_at: scheduled_at) do |payload, attributes|
          persist([job], attributes: [attributes], dispatch: false) { |row| payload[:job] = Job.from_attributes(row, database: database) }
          payload[:accepted] = !job.provider_job_id.nil?
        end
        after_enqueue([job.provider_job_id]) if job.provider_job_id
      end
      raise EnqueueError, "cron occurrence already enqueued" unless job.provider_job_id
      job.provider_job_id
    end

    def instrument_enqueue(job, scheduled_at:, attributes: nil)
      attributes ||= store.enqueue_attributes(job)
      payload = {job_instance: job, scheduled_at: scheduled_at, create_with_advisory_lock: false,
                 job: Job.from_attributes(attributes, database: database), accepted: false}
      Instrumentation.instrument("enqueue_job.decent_job", payload, database: database) { yield payload, attributes }
    end

    # Bulk flush already ran this job's callbacks. Report failures on the job,
    # as Active Job's enqueue does, instead of raising. A database error inside
    # a cron capture still raises.
    def enqueue_without_callbacks(job)
      job.successfully_enqueued = false
      job.enqueue_error = nil
      enqueue_job(job)
    rescue ConcurrencyControls::AdmissionRejected => error
      job.enqueue_error = Integrations.for_job(job).enqueue_error(EnqueueError.new(error.message))
    rescue EnqueueError => error
      raise if CronEntry.context_for(database) && error.cause.is_a?(PG::Error)

      job.enqueue_error = Integrations.for_job(job).enqueue_error(error)
    end

    def persist(jobs, attributes: nil, dispatch: true, skip_duplicates: false, &block)
      return 0 if jobs.empty?

      # Cron capture also flushes through bulk enqueue, but only its occurrence
      # constraint may reject silently. Other uniqueness failures must escape.
      skip_duplicates = false if CronEntry.context_for(database)
      ids = begin
        store.enqueue_all(jobs, attributes: attributes, skip_duplicates: skip_duplicates, &block)
      rescue PG::Error => error
        raise EnqueueError, error.message
      end
      jobs.zip(ids).each do |job, id|
        job.provider_job_id = id
        job.successfully_enqueued = !id.nil?
        job.enqueue_error = id ? nil : Integrations.for_job(job).enqueue_error(EnqueueError.new("job or cron occurrence already enqueued"))
      end
      ids = ids.compact
      after_enqueue(ids) if dispatch
      ids.size
    end

    # Enqueue callers see the integration's error class, keeping the cause.
    def raising_integration_errors(job)
      yield
    rescue EnqueueError => error
      integration = Integrations.for_class(job.class)
      translated = integration ? integration.enqueue_error(error) : error
      raise error if translated.equal?(error)

      raise translated, cause: error.cause
    end

    def store
      @store ||= Store.new(database)
    end
  end
end
