# frozen_string_literal: true

require "decent_job/interrupt_error"

module DecentJob
  # Describes a stored abandoned attempt; handlers should rescue InterruptError.
  class InterruptedError < InterruptError; end
end
