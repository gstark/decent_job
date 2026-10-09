# frozen_string_literal: true

module DecentJob
  class Adapter
    def initialize(database: nil, execution_mode: DecentJob.configuration.execution_mode)
      raise ArgumentError, "unknown execution mode" unless %i[external inline async].include?(execution_mode)
      @database = database
      @execution_mode = execution_mode
      @worker_mutex = Mutex.new
    end

    def database
      @database ||= DecentJob.database
    end

    def enqueue(job)
      context = Current.execution
      if context && context[:database].equal?(database) && context[:job_id] == job.job_id
        store.reschedule(job, context)
        return job.provider_job_id
      end
      enqueue_all([job])
      job.provider_job_id
    end

    def enqueue_at(job, timestamp)
      job.scheduled_at = Time.at(timestamp).utc
      enqueue(job)
    end

    def enqueue_all(jobs)
      ids = begin
        store.enqueue_all(jobs)
      rescue PG::Error => error
        raise ActiveJob::EnqueueError, error.message
      end
      jobs.zip(ids).each do |job, id|
        job.provider_job_id = id
        job.successfully_enqueued = true
      end
      if @execution_mode == :inline
        database.after_commit { InlineBuffer.perform(database, ids) }
      elsif @execution_mode == :async
        database.after_commit { start_async&.wake }
      end
      ids.size
    end

    def enqueue_after_transaction_commit?
      true
    end

    def shutdown(timeout: nil)
      worker = @worker_mutex.synchronize { @worker }
      !worker || worker.shutdown(timeout: timeout)
    end

    private

    def store
      @store ||= Store.new(database)
    end

    def start_async
      @worker_mutex.synchronize do
        @worker ||= Worker.new(database: database, **DecentJob.configuration.worker_options)
        DecentJob.start_worker(@worker)
      end
    end
  end
end
