# frozen_string_literal: true

module DecentJob
  # Raised by the opt-in {JobExtensions::InterruptErrors} hook, for native and
  # Active Job classes, when a job starts again after an abandoned execution.
  # Rescue this class, not {InterruptedError}, to handle both. {Performer}
  # records an unhandled InterruptError with error_event "interrupted".
  #
  # See docs/execution-recovery.md.
  class InterruptError < StandardError; end
end
