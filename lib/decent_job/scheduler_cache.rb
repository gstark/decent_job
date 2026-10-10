# frozen_string_literal: true

module DecentJob
  # Tracks claim permits and future deadlines for one {Worker} queue pool.
  #
  # {Worker} creates one cache per pool on each start and drops it on the next
  # start. Accessed under the worker mutex. The cache has no lock of its own.
  #
  # Deadlines only request another SQL claim; they never identify a job to
  # execute or replace the database's due check. Equal deadlines share one
  # entry.
  #
  # Counters:
  # - +permits+: claim attempts granted but not yet taken by a thread.
  # - +busy+: threads that took a permit and have not finished their attempt.
  # - capacity: threads with neither, so +permits+ plus +busy+ never exceeds
  #   the pool thread count through {#wake}.
  # - +revision+: increases on every {#request} and {#notify_at}. The worker
  #   compares it to tell whether new work arrived during an attempt or a
  #   refresh.
  #
  # See docs/scheduling.md.
  class SchedulerCache
    # +max_cache+ is the configured deadline limit. +revision+ is the change
    # counter described above.
    attr_reader :max_cache, :revision
    # +refreshing+ is true while one thread reloads deadlines from SQL.
    # +due_retry_at+ is a monotonic time in seconds. Before it, a due deadline
    # does not trigger another claim. See {Worker#refresh_cache}.
    attr_accessor :permits, :busy, :refreshing, :due_retry_at

    # @param max_cache [Integer] maximum cached deadlines. Zero keeps at most
    #   one deadline, and only after a notification.
    # @param threads [Integer] thread count of the pool. Every thread starts
    #   with a permit, so each one claims once at startup.
    def initialize(max_cache:, threads:)
      @max_cache = max_cache
      @threads = threads
      @permits = threads
      @busy = 0
      @revision = 0
      @deadlines = {}
      @refreshing = false
      @notified = false
      @due_retry_at = 0
    end

    # Drops all deadlines and unused permits. The worker calls this when it stops.
    #
    # @return [void]
    def clear
      @deadlines.clear
      @notified = false
      @permits = 0
    end

    # @return [Integer] threads that are neither busy nor already permitted
    def capacity
      [@threads - busy - permits, 0].max
    end

    # Grants up to count permits, bounded by {#capacity}. Does not change
    # {#revision}.
    #
    # @param count [Integer]
    # @return [Integer] the number of permits granted
    def wake(count)
      accepted = [count, capacity].min
      @permits += accepted
      accepted
    end

    # Records new work and grants up to count permits.
    #
    # @param count [Integer]
    # @return [Integer] the number of permits granted
    def request(count)
      @revision += 1
      wake(count)
    end

    # Caches a deadline from a notification. Keeps only the earliest {#limit}
    # deadlines.
    #
    # @param time [Time]
    # @return [void]
    def notify_at(time)
      @revision += 1
      @notified = true
      @deadlines[time] = @revision
      trim
    end

    # @return [Boolean] true when no refresh runs and the cache needs one. With
    #   max_cache zero, only a pending notification needs a refresh.
    def refresh?
      !refreshing && (max_cache.positive? || @notified)
    end

    # @return [Integer] the deadline limit for SQL discovery and trimming, at least 1
    def limit
      [max_cache, 1].max
    end

    # Replaces cached deadlines with a SQL result.
    #
    # @param times [Array<Time>] deadlines from {Claim#next_scheduled_at}
    # @param revision [Integer] {#revision} when the query started
    # @return [void]
    def replace(times, revision:)
      # A notification arriving during the query must survive its older snapshot.
      @deadlines.select! { |_time, version| version > revision }
      times.each { |time| @deadlines[time] ||= revision }
      trim
      @notified = false if @deadlines.empty? && @revision == revision
    end

    # @param now [Time] wall-clock time
    # @return [Boolean] whether any deadline is at or before now
    def due?(now)
      @deadlines.any? { |time, _| time <= now }
    end

    # Removes deadlines at or before now.
    #
    # @param now [Time] wall-clock time
    # @return [void]
    def expire(now)
      @deadlines.reject! { |time, _| time <= now }
    end

    # @return [Time, nil] the earliest deadline, or nil when the cache is empty
    def next_at
      @deadlines.keys.min
    end

    # @return [Hash{Symbol => Integer}] max_cache, active_cache (entries in
    #   use), and available_cache (free entries, zero when max_cache is zero)
    def stats
      {max_cache: max_cache, active_cache: @deadlines.size,
       available_cache: [max_cache - @deadlines.size, 0].max}
    end

    private

    def trim
      @deadlines = @deadlines.sort_by { |time, _| time }.first(limit).to_h
    end
  end
end
