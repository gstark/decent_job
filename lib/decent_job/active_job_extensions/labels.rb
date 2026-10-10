# frozen_string_literal: true

require "decent_job/job_extensions/labels"

module DecentJob
  module ActiveJobExtensions
    # Opt-in module that adds labels to Active Job classes. Same module as
    # {JobExtensions::Labels}.
    #
    # Loaded only with Active Job. Job classes include it explicitly.
    # {Concurrency} includes it on its own.
    Labels = JobExtensions::Labels
  end
end
