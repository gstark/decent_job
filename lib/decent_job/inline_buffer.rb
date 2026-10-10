# frozen_string_literal: true

module DecentJob
  # Runs inline-mode jobs on the enqueuing thread, one at a time.
  #
  # {Adapter} (inline execution mode) and {Batch} call {.perform} after the
  # enqueue transaction commits. Nested enqueues append work to the current
  # drain instead of recursing. The buffer lives in {ExecutionState} under
  # +:inline_buffer+, so it is scoped to the current thread or fiber.
  #
  # See docs/execution-recovery.md.
  module InlineBuffer
    # Runs the given jobs, or queues them on the drain that already runs in
    # this context.
    #
    # @param database [Database] pool that holds the jobs
    # @param ids [Array<String>] decent_jobs.id values, run in order
    # @return [Array, nil] the shared buffer when a drain already runs in this
    #   context, else nil after the drain finishes
    # @raise [Exception] the first unhandled job error. Jobs still in the
    #   buffer do not run and stay in the database.
    # @note A job that ends in the "retried" state goes to the back of the
    #   buffer and runs again. A job that is not due cannot be claimed, so
    #   the drain skips it.
    def self.perform(database, ids)
      entries = ids.map { |id| [database, id] }
      existing = ExecutionState[:inline_buffer]
      return existing.concat(entries) if existing

      ExecutionState.with(:inline_buffer, entries) do
        while (entry = entries.shift)
          pool, id = entry
          result = Performer.new(database: pool).perform_one(job_id: id, raise_errors: true)
          entries << entry if result&.state == "retried"
        end
      end
    end
  end
end
