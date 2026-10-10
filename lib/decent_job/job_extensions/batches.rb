# frozen_string_literal: true

require "decent_job/current"

module DecentJob
  module JobExtensions
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

      def batch
        context = Current.execution
        return unless context && context[:job_id] == job_id

        database = context.fetch(:database)
        row = Store.new(database).find(context.fetch(:id))
        Batch.find(row.fetch("batch_id"), database: database) if row && row["batch_id"]
      end
      alias_method :batch?, :batch
    end

    module Cron
      def self.included(base)
        base.before_enqueue { |job| CronEntry.validate_jobs([job]) if CronEntry.current_context }
      end
    end
  end
end
