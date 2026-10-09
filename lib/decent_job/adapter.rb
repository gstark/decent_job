# frozen_string_literal: true

module DecentJob
  class Adapter
    attr_reader :execution_mode

    def initialize(database: nil, execution_mode: DecentJob.configuration.execution_mode)
      raise ArgumentError, "unknown execution mode" unless %i[external inline async].include?(execution_mode)
      @database = database
      @execution_mode = execution_mode
      @worker_mutex = Mutex.new
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

    def shutdown(timeout: nil)
      if !timeout.nil? && (!timeout.is_a?(Numeric) || !timeout.finite? || timeout.negative?)
        raise ArgumentError, "timeout must be a finite nonnegative number"
      end
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout if timeout
      worker = @worker_mutex.synchronize do
        @stopping = true
        @worker
      end
      DecentJob.stop_cron(database: @database, timeout: 0) if @database
      worker&.shutdown(timeout: 0)
      cron_stopped = !@database || DecentJob.stop_cron(database: @database, timeout: remaining_time(deadline))
      worker_stopped = !worker || worker.shutdown(timeout: remaining_time(deadline))
      cron_stopped && worker_stopped
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
      if @execution_mode == :inline
        database.after_commit { CronEntry.with_context(nil) { InlineBuffer.perform(database, ids) } }
      elsif @execution_mode == :async
        database.after_commit { start_async&.wake }
      end
      ids.size
    end

    def store
      @store ||= Store.new(database)
    end

    def start_async
      worker = @worker_mutex.synchronize do
        return if @stopping
        @worker ||= Worker.new(database: database, **DecentJob.configuration.worker_options)
        DecentJob.start_worker(@worker)
      end
      DecentJob.start_cron(database: database, automatic: true) if worker && DecentJob.configuration.enable_cron
      worker
    end
  end
end
