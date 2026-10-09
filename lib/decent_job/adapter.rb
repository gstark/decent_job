# frozen_string_literal: true

module DecentJob
  class Adapter
    def initialize(database: nil, execution_mode: nil)
      Configuration.validate_execution_mode!(execution_mode) unless execution_mode.nil?
      @database = database
      @execution_mode = execution_mode
      @worker_mutex = Mutex.new
      @stopping = false
      DecentJob.register_adapter(self)
    end

    def reset_after_fork
      @execution_mode = :external if Configuration::ASYNC_MODES.include?(execution_mode)
      @worker_mutex = Mutex.new
      @worker = nil
      @stopping = false
    end

    def database
      @database ||= DecentJob.database
    end

    def enqueue(job)
      return if Bulk.capture(job, queue_adapter: self, callbacks_run: true)

      context = Current.execution
      if context && context[:database].equal?(database) && context[:job_id] == job.job_id
        store.reschedule(job, context)
        return job.provider_job_id
      end
      ConcurrencyControls.new(database).enqueue(job) { persist([job]) }
      raise ActiveJob::EnqueueError, "cron occurrence already enqueued" unless job.provider_job_id
      job.provider_job_id
    end

    def enqueue_at(job, timestamp)
      job.scheduled_at = Time.at(timestamp).utc
      enqueue(job)
    end

    def enqueue_all(jobs, callbacks: true)
      if Bulk.capture(jobs, queue_adapter: self)
        jobs.each { |job| job.successfully_enqueued = true }
        return jobs.size
      end

      controlled, plain = jobs.partition { |job| job.respond_to?(:decent_job_resolved_concurrency_rules) }
      count = persist(plain)
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

    def shutdown(timeout: nil)
      if !timeout.nil? && (!timeout.is_a?(Numeric) || !timeout.finite? || timeout.negative?)
        raise ArgumentError, "timeout must be a finite nonnegative number"
      end
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout if timeout
      worker = @worker_mutex.synchronize do
        @stopping = true
        @worker
      end
      # Signal both resources, then drain both within the same deadline, even
      # when an owned thread's fatal exception makes shutdown raise.
      failure = nil
      attempt_shutdown = lambda do |&operation|
        operation.call
      rescue Exception => error # standard:disable Lint/RescueException
        failure ||= error
        false
      end
      attempt_shutdown.call { DecentJob.stop_cron(database: @database, timeout: 0) } if @database
      attempt_shutdown.call { worker.shutdown(timeout: 0) } if worker
      cron_stopped = !@database || attempt_shutdown.call { DecentJob.stop_cron(database: @database, timeout: remaining_time(deadline)) }
      worker_stopped = !worker || attempt_shutdown.call { worker.shutdown(timeout: remaining_time(deadline)) }
      raise failure if failure
      cron_stopped && worker_stopped
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
      return unless DecentJob.async_ready? && execute_async?
      return unless @database || DecentJob.configuration.database_url

      worker = @worker_mutex.synchronize do
        return if @stopping
        connection_pool = database
        DecentJob.synchronize_resource_start do
          @worker ||= Worker.new(database: connection_pool, **DecentJob.configuration.worker_options)
          DecentJob.start_worker(@worker)
        end
      end
      DecentJob.start_cron(database: database, automatic: true) if worker && DecentJob.configuration.enable_cron
      worker
    end

    private

    def remaining_time(deadline)
      deadline && [deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC), 0].max
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

    def persist(jobs)
      return 0 if jobs.empty?

      ids = begin
        store.enqueue_all(jobs)
      rescue PG::Error => error
        raise ActiveJob::EnqueueError, error.message
      end
      jobs.zip(ids).each do |job, id|
        job.provider_job_id = id
        job.successfully_enqueued = !id.nil?
        job.enqueue_error = ActiveJob::EnqueueError.new("cron occurrence already enqueued") unless id
      end
      ids = ids.compact
      after_enqueue(ids)
      ids.size
    end

    def store
      @store ||= Store.new(database)
    end
  end
end
