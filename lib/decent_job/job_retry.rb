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
      buffer = Bulk::Buffer.new
      rollback_jobs = {}
      @database.after_rollback do
        rollback_jobs.each do |entry, previous_id|
          entry.provider_job_id = previous_id
          entry.successfully_enqueued = false
        end
      end
      payload = nil
      job = @record.active_job(ignore_deserialization_errors: true)
      raise Job::DeserializationError, "Cannot deserialize #{@record.job_class}" unless job
      validate_adapter(job)
      job.executions = (job.executions || 0) + 1
      current_thread = Thread.current
      subscriber = lambda do |_name, _start, _finish, _id, event|
        if Thread.current == current_thread && event[:job].equal?(job) && job.successfully_enqueued?
          payload = serialize(job)
        end
      end
      Current.with(context) do
        CronEntry.with_context(nil) do
          Batch.within_thread(database: @database) do
            ActiveSupport::Notifications.subscribed(subscriber, /\Aenqueue(?:_at)?\.active_job\z/) do
              buffer.capture { I18n.with_locale(job.locale) { job.retry_job(wait: 0, error: stored_error) } }
            end
            buffer.prepare
          end
        end
      end
      buffer.active_jobs.each { |child| validate_adapter(child) }
      retried_job = ActiveJob::Base.deserialize(payload) if payload && buffer.active_jobs.include?(job)
      {context: context, buffer: buffer, job: retried_job, payload: payload}
    ensure
      if rollback_jobs
        # Capture original identifiers before any flush, including preparation
        # failures. Existing unrelated jobs in the buffer retain their state.
        [job, retried_job].compact.each { |entry| rollback_jobs[entry] = @record.id }
        buffer.active_jobs.each { |entry| rollback_jobs[entry] = nil unless entry.provider_job_id }
      end
    end

    private

    def serialize(job)
      job.serialize
    rescue ActiveJob::DeserializationError
      # Extensions can resolve labels or concurrency keys from arguments during
      # serialization. An operator retry can retain that stored metadata when
      # the arguments are no longer loadable, as Active Job itself does.
      @record.payload.merge(ActiveJob::Core.instance_method(:serialize).bind_call(job))
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
