# frozen_string_literal: true

require "decent_job/job_extensions/batches"

module DecentJob
  module ActiveJobExtensions
    # Adds the batch enqueue check and {JobExtensions::Batches#batch} to Active
    # Job classes. Same module as {JobExtensions::Batches}.
    #
    # Loaded only with Active Job. decent_job/active_job includes it in
    # ActiveJob::Base, so every Active Job class gets it.
    #
    # See README.md "Batches".
    Batches = JobExtensions::Batches
  end
end
