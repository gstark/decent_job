# frozen_string_literal: true

module DecentJob
  class Adapter
    def initialize(database: nil, execution_mode: nil)
      Configuration.validate_execution_mode!(execution_mode) unless execution_mode.nil?
      @database = database
      @execution_mode = execution_mode
      @worker_mutex = Mutex.new
      @stopping = false
      @lifecycle_revision = 0
      DecentJob.register_adapter(self)
    end

    def reset_after_fork
      @execution_mode = :external if Configuration::ASYNC_MODES.include?(execution_mode)
      @worker_mutex = Mutex.new
      @worker = nil
      @stopping = !!@retired
      @lifecycle_revision = 0
    end

    # Reset retires adapters permanently; a new adapter chooses a fresh pool.
    def retire
      @worker_mutex.synchronize do
        @retired = true
        @stopping = true
        @lifecycle_revision += 1
      end
    end

    # CLI reconfiguration preserves adapter ownership and its explicit stop flag.
    def reset_worker
      @worker_mutex.synchronize do
        @worker = nil
        @lifecycle_revision += 1
      end
    end

    def database
      @database ||= DecentJob.database
    end

    def enqueue(job)
      enqueue_job(job)
    end

    def enqueue_at(job, timestamp)
      job.scheduled_at = Time.at(timestamp).utc unless timestamp.nil?
      enqueue_job(job, scheduled_at: timestamp)
    end

    def enqueue_all(jobs, callbacks: true)
      jobs = jobs.uniq
      if Bulk.capture(jobs, queue_adapter: self)
        jobs.each { |job| job.successfully_enqueued = true }
        return jobs.size
      end

      controlled, plain = jobs.partition { |job| job.respond_to?(:decent_job_resolved_concurrency_rules) }
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

    def enqueue_after_transaction_commit?
      true
    end

    def stopping?(active_job = nil)
      context = Current.execution
      context = nil unless context && !context[:manual_retry] && context[:database].equal?(database)
      return :stopping if @worker_mutex.synchronize { @stopping } || DecentJob.stopping? || context&.fetch(:stopping, nil)&.call

      matching = context && (!active_job || active_job.job_id == context[:job_id])
      if matching && context[:execution_id]
        finished = database.with_connection do |connection|
          connection.exec_params("SELECT 1 FROM decent_job_executions WHERE id = $1 AND job_id = $2 AND finished_at IS NOT NULL", [context[:execution_id], context[:id]]).ntuples.positive?
        end
        return :force_discarded if finished
      end
      return false unless DecentJob.configuration.enable_pauses
      return Pauses.new(database).paused?(active_job: active_job) if active_job
      return false unless matching && context[:id]

      row = store.find(context[:id])
      row ? Pauses.new(database).reason(queue: row.fetch("queue_name"), job_class: row.fetch("job_class"), labels: row.fetch("labels")) : false
    end

    def shutdown(timeout: nil, force: false)
      deadline = ResourceShutdown.deadline(timeout)
      _revision, handles = begin_shutdown
      ResourceShutdown.wait_all(handles, deadline: deadline, force: force, timeout: timeout)
    end

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

    # Management retries supply their callback-prepared payload so unavailable
    # arguments do not need to deserialize again under the persistence locks.
    def reschedule(job, context, payload: nil, attributes: nil, scheduled_at: nil)
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

    def execution_mode
      @execution_mode.nil? ? DecentJob.configuration.execution_mode : @execution_mode
    end

    def execute_async?
      execution_mode == :async_all || (Configuration::SERVER_ASYNC_MODES.include?(execution_mode) && DecentJob.configuration.in_webserver?)
    end

    def execute_inline?
      execution_mode == :inline
    end

    def execute_external?
      !execute_async? && !execute_inline?
    end

    def async_started?
      @worker_mutex.synchronize { !!@worker&.started? }
    end

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
      raise ActiveJob::EnqueueError, "cron occurrence already enqueued" unless job.provider_job_id
      job.provider_job_id
    end

    def instrument_enqueue(job, scheduled_at:, attributes: nil)
      attributes ||= store.enqueue_attributes(job)
      payload = {active_job: job, scheduled_at: scheduled_at, create_with_advisory_lock: false,
                 job: Job.from_attributes(attributes, database: database), accepted: false}
      Instrumentation.instrument("enqueue_job.decent_job", payload, database: database) { yield payload, attributes }
    end

    def enqueue_without_callbacks(job)
      job.successfully_enqueued = false
      job.enqueue_error = nil
      enqueue(job)
    rescue ConcurrencyControls::AdmissionRejected => error
      job.enqueue_error = ActiveJob::EnqueueError.new(error.message)
    rescue ActiveJob::EnqueueError => error
      raise if CronEntry.context_for(database) && error.cause.is_a?(PG::Error)

      job.enqueue_error = error
    end

    def persist(jobs, attributes: nil, dispatch: true, skip_duplicates: false, &block)
      return 0 if jobs.empty?

      # Cron capture also flushes through bulk enqueue, but only its occurrence
      # constraint may reject silently. Other uniqueness failures must escape.
      skip_duplicates = false if CronEntry.context_for(database)
      ids = begin
        store.enqueue_all(jobs, attributes: attributes, skip_duplicates: skip_duplicates, &block)
      rescue PG::Error => error
        raise ActiveJob::EnqueueError, error.message
      end
      jobs.zip(ids).each do |job, id|
        job.provider_job_id = id
        job.successfully_enqueued = !id.nil?
        job.enqueue_error = id ? nil : ActiveJob::EnqueueError.new("job or cron occurrence already enqueued")
      end
      ids = ids.compact
      after_enqueue(ids) if dispatch
      ids.size
    end

    def store
      @store ||= Store.new(database)
    end
  end
end
