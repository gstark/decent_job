# frozen_string_literal: true

module DecentJob
  # Accessed under the worker mutex. Deadlines only request another SQL claim;
  # they never identify a job to execute or replace the database's due check.
  class SchedulerCache
    attr_reader :max_cache, :revision
    attr_accessor :permits, :busy, :refreshing, :due_retry_at

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

    def clear
      @deadlines.clear
      @notified = false
      @permits = 0
    end

    def capacity
      [@threads - busy - permits, 0].max
    end

    def wake(count)
      accepted = [count, capacity].min
      @permits += accepted
      accepted
    end

    def request(count)
      @revision += 1
      wake(count)
    end

    def notify_at(time)
      @revision += 1
      @notified = true
      @deadlines[time] = @revision
      trim
    end

    def refresh?
      !refreshing && (max_cache.positive? || @notified)
    end

    def limit
      [max_cache, 1].max
    end

    def replace(times, revision:)
      # A notification arriving during the query must survive its older snapshot.
      @deadlines.select! { |_time, version| version > revision }
      times.each { |time| @deadlines[time] ||= revision }
      trim
      @notified = false if @deadlines.empty? && @revision == revision
    end

    def due?(now)
      @deadlines.any? { |time, _| time <= now }
    end

    def expire(now)
      @deadlines.reject! { |time, _| time <= now }
    end

    def next_at
      @deadlines.keys.min
    end

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
