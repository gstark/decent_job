# frozen_string_literal: true

require "decent_job/job_extensions/batches"

module DecentJob
  module ActiveJobExtensions
    # Adds the cron enqueue check to Active Job classes. Same module as
    # {JobExtensions::Cron}, which lives in decent_job/job_extensions/batches.
    #
    # Loaded only with Active Job. decent_job/active_job includes it in
    # ActiveJob::Base.
    Cron = JobExtensions::Cron

    # Validates bulk enqueues made while a cron entry enqueues its job.
    #
    # ActiveJob.perform_all_later skips before_enqueue callbacks, so {Cron}
    # alone does not see those jobs. decent_job/active_job prepends this
    # module to ActiveJob's singleton class.
    #
    # See docs/recurring-jobs.md.
    module CronBulk
      # @param jobs [Array<ActiveJob::Base>] the jobs, possibly nested arrays
      # @return [Integer, nil] the result of ActiveJob.perform_all_later
      # @raise [ArgumentError] when a cron entry is enqueuing and a job does not
      #   use a DecentJob adapter with the cron entry's database
      def perform_all_later(*jobs)
        CronEntry.validate_jobs(jobs.flatten) if CronEntry.current_context
        super
      end
    end
  end
end
