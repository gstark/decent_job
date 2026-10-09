# frozen_string_literal: true

require "decent_job/job_extensions/interrupt_errors"

module DecentJob
  module ActiveJobExtensions
    InterruptErrors = JobExtensions::InterruptErrors
  end
end
