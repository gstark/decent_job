# frozen_string_literal: true

require "decent_job/job_extensions/interrupt_errors"

module DecentJob
  module ActiveJobExtensions
    # Opt-in module that lets Active Job retry and discard handlers see
    # interrupted attempts. Same module as {JobExtensions::InterruptErrors}.
    #
    # Loaded only with Active Job. Job classes include it explicitly.
    #
    # See docs/execution-recovery.md.
    InterruptErrors = JobExtensions::InterruptErrors
  end
end
