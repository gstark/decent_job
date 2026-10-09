# frozen_string_literal: true

module DecentJob
  module ActiveJobExtensions
    module Cron
      extend ActiveSupport::Concern

      included do
        before_enqueue { |job| CronEntry.validate_jobs([job]) if CronEntry.current_context }
      end
    end

    module CronBulk
      def perform_all_later(*jobs)
        CronEntry.validate_jobs(jobs.flatten) if CronEntry.current_context
        super
      end
    end
  end
end
