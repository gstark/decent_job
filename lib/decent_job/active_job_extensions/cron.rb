# frozen_string_literal: true

require "decent_job/job_extensions/batches"

module DecentJob
  module ActiveJobExtensions
    Cron = JobExtensions::Cron

    module CronBulk
      def perform_all_later(*jobs)
        CronEntry.validate_jobs(jobs.flatten) if CronEntry.current_context
        super
      end
    end
  end
end
