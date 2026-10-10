# frozen_string_literal: true

module DecentJob
  # Decides when a {Worker} runs an automatic cleanup sweep.
  #
  # It counts completed attempts and remembers the last sweep time. One
  # tracker belongs to each {Worker} and is shared by all its queue pools.
  # The worker increments it after each completed attempt, then asks
  # {#cleanup?}. It has no thread or timer, so an idle worker never sweeps.
  #
  # Worker owners serialize access to this counter alongside their cleanup guard.
  # The tracker itself is not thread safe.
  #
  # {Configuration} also builds one to validate interval settings.
  #
  # See docs/record-preservation.md "Automatic cleanup".
  class CleanupTracker
    # cleanup_interval_jobs is attempts between sweeps (Integer), and
    # cleanup_interval_seconds is seconds between sweeps (Numeric). For both,
    # -1 means every attempt and false turns that threshold off.
    attr_reader :cleanup_interval_jobs, :cleanup_interval_seconds
    # job_count is attempts completed since the last {#reset}. last_at is the
    # Time of the last {#reset}.
    attr_accessor :job_count, :last_at

    # @param cleanup_interval_jobs [Integer, false] positive count, -1, or false
    # @param cleanup_interval_seconds [Numeric, false] positive seconds, -1, or
    #   false
    # @raise [ArgumentError] when an interval is not positive, -1, or false.
    #   Zero and nil are invalid here. {Configuration} converts them first.
    def initialize(cleanup_interval_jobs: false, cleanup_interval_seconds: false)
      self.cleanup_interval_jobs = cleanup_interval_jobs
      self.cleanup_interval_seconds = cleanup_interval_seconds
      reset
    end

    # @param value [Integer, false] positive count, -1, or false
    # @raise [ArgumentError] when value is not an Integer, -1, or false
    def cleanup_interval_jobs=(value)
      validate_interval!(value, integer: true)
      @cleanup_interval_jobs = value
    end

    # @param value [Numeric, false] positive seconds, -1, or false
    # @raise [ArgumentError] when value is not positive, -1, or false
    def cleanup_interval_seconds=(value)
      validate_interval!(value)
      @cleanup_interval_seconds = value
    end

    # Counts one completed attempt.
    #
    # @return [Integer] the new count
    def increment
      @job_count += 1
    end

    # Clears the count and restarts the timer. The worker calls it when it
    # starts a sweep, not when the sweep ends.
    #
    # @return [Time] the new {#last_at}
    def reset
      @job_count = 0
      @last_at = Time.now
    end

    # The comparisons are strict: an interval of 1000 triggers after attempt
    # 1001. An interval of -1 always triggers.
    #
    # @return [Boolean] true when either threshold is exceeded
    def cleanup?
      (@cleanup_interval_jobs && @job_count > @cleanup_interval_jobs) ||
        (@cleanup_interval_seconds && @last_at < Time.now - @cleanup_interval_seconds) || false
    end

    private

    def validate_interval!(value, integer: false)
      return if value == false

      valid = value.is_a?(Numeric) && value.real? && value.finite? && (value == -1 || value.positive?)
      valid &&= value.is_a?(Integer) if integer
      raise ArgumentError, "cleanup interval must be positive, -1 (always), or false (disabled)" unless valid
    end
  end
end
