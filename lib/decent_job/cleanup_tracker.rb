# frozen_string_literal: true

module DecentJob
  # Worker owners serialize access to this counter alongside their cleanup guard.
  class CleanupTracker
    attr_reader :cleanup_interval_jobs, :cleanup_interval_seconds
    attr_accessor :job_count, :last_at

    def initialize(cleanup_interval_jobs: false, cleanup_interval_seconds: false)
      self.cleanup_interval_jobs = cleanup_interval_jobs
      self.cleanup_interval_seconds = cleanup_interval_seconds
      reset
    end

    def cleanup_interval_jobs=(value)
      validate_interval!(value, integer: true)
      @cleanup_interval_jobs = value
    end

    def cleanup_interval_seconds=(value)
      validate_interval!(value)
      @cleanup_interval_seconds = value
    end

    def increment
      @job_count += 1
    end

    def reset
      @job_count = 0
      @last_at = Time.now
    end

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
