# frozen_string_literal: true

module DecentJob
  # Nested enqueues append work to the current drain instead of recursing.
  module InlineBuffer
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
