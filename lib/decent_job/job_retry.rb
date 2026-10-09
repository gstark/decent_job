# frozen_string_literal: true

module DecentJob
  # Run enqueue callbacks before locking job rows, then persist their captured
  # payload and children together under the complete set of concurrency locks.
  class JobRetry
    def initialize(record)
      @record = record
      @database = record.database
    end

    def prepare
      context = {database: @database, id: @record.id, job_id: @record.active_job_id,
                 execution_id: @record.execution_id, manual_retry: true,
                 batch_id: @record.batch_id, batch_callback_id: @record.batch_callback_id}
      rollback_jobs = {}
      @database.after_rollback do
        rollback_jobs.each do |entry, previous_id|
          entry.provider_job_id = previous_id
          entry.successfully_enqueued = false
        end
      end
      payload = nil
      job = @record.job_instance(ignore_deserialization_errors: true)
      raise Job::DeserializationError, "Cannot deserialize #{@record.job_class}" unless job
      integration = Integrations.for_job(job)
      validate_adapter(job)
      job.executions = (job.executions || 0) + 1
      # retry_job restores the scheduled time, queue and priority after it
      # enqueues, so record the payload when the adapter accepts the job.
      buffer = Bulk::Buffer.new(on_capture: ->(captured) { payload = serialize(captured, integration) if captured.equal?(job) })
      Current.with(context) do
        CronEntry.with_context(nil) do
          Batch.within_thread(database: @database) do
            buffer.capture { integration.with_context(job) { job.retry_job(wait: 0, error: stored_error) } }
            buffer.prepare
          end
        end
      end
      buffer.jobs.each { |child| validate_adapter(child) }
      retried_job = integration.deserialize(payload) if payload && buffer.jobs.include?(job)
      {context: context, buffer: buffer, job: retried_job, payload: payload}
    ensure
      if rollback_jobs
        # Capture original identifiers before any flush, including preparation
        # failures. Existing unrelated jobs in the buffer retain their state.
        [job, retried_job].compact.each { |entry| rollback_jobs[entry] = @record.id }
        buffer&.jobs&.each { |entry| rollback_jobs[entry] = nil unless entry.provider_job_id }
      end
    end

    private

    def serialize(job, integration)
      job.serialize
    rescue integration.deserialization_error
      # Extensions can resolve labels or concurrency keys from arguments during
      # serialization. An operator retry can retain that stored metadata when
      # the arguments are no longer loadable, as Active Job itself does.
      @record.payload.merge(integration.serialize_core(job))
    end

    def validate_adapter(job)
      unless job.queue_adapter.is_a?(Adapter) && job.queue_adapter.database.equal?(@database)
        raise Job::AdapterMismatchError, "Jobs must use a DecentJob adapter with the job's database"
      end
    end

    def stored_error
      details = @record.error || {}
      klass = Support.constantize(details.fetch("class"))
      raise TypeError, "Stored error is not an exception" unless klass <= Exception
      klass.new(details["message"]).tap { |error| error.set_backtrace(details["backtrace"]) }
    rescue
      StandardError.new([details["class"], details["message"]].compact.join(": "))
    end
  end
end
