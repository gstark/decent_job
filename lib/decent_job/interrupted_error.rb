# frozen_string_literal: true

require "decent_job/interrupt_error"

module DecentJob
  # Describes a stored abandoned attempt; handlers should rescue InterruptError.
  #
  # {Claim} writes this class name into the error column of the old attempt
  # and the job when a new claim replaces an abandoned execution. The code
  # never raises it; it exists so the stored class name resolves.
  class InterruptedError < InterruptError; end
end
