# frozen_string_literal: true

require "decent_job/current"

module DecentJob
  module JobExtensions
    # Batch support for job classes. {Base} includes it. Active Job classes
    # get it through ActiveJobExtensions::Batches.
    #
    # The before_enqueue check rejects a job enqueued inside a batch block
    # (see {Batch.current_context}) unless it uses a DecentJob {Adapter} for
    # the batch database. A batch stores its members in that database, so a
    # job in another database cannot join it.
    #
    # See the Batches section of README.md.
    module Batches
      def self.included(base)
        base.before_enqueue do |job|
          context = Batch.current_context
          if context
            adapter = job.queue_adapter
            unless adapter.is_a?(Adapter) && adapter.database.equal?(context.fetch(:database))
              raise ArgumentError, "Batch jobs must use a DecentJob adapter with the batch database"
            end
          end
        end
      end

      # Returns the batch of the job that runs in this thread.
      #
      # @return [Batch, nil] the batch, or nil when the job is not in a
      #   batch or this job is not the one that the current execution runs
      # @note Queries the decent_jobs row on each call.
      def batch
        context = Current.execution
        return unless context && context[:job_id] == job_id

        database = context.fetch(:database)
        row = Store.new(database).find(context.fetch(:id))
        Batch.find(row.fetch("batch_id"), database: database) if row && row["batch_id"]
      end
      alias_method :batch?, :batch
    end

    # Cron entry validation for job classes. {Base} includes it.
    #
    # Inside a cron entry run (see {CronEntry.current_context}), the
    # before_enqueue check calls {CronEntry.validate_jobs}, so a cron entry
    # cannot enqueue jobs that do not fit its database.
    #
    # See docs/recurring-jobs.md.
    module Cron
      def self.included(base)
        base.before_enqueue { |job| CronEntry.validate_jobs([job]) if CronEntry.current_context }
      end
    end
  end
end
