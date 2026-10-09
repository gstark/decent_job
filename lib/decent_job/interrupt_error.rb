# frozen_string_literal: true

module DecentJob
  # Raised by the opt-in Active Job hook when replacing an abandoned execution.
  class InterruptError < StandardError; end
end
