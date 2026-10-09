# frozen_string_literal: true

require "spec_helper"
require "decent_job/scheduler_cache"

describe DecentJob::SchedulerCache do
  it "bounds duplicate and overflowing deadlines and preserves hints arriving during warming" do
    cache = DecentJob::SchedulerCache.new(max_cache: 2, threads: 3)
    now = Time.now
    revision = cache.revision
    cache.notify_at(now + 1)
    5.times { cache.notify_at(now + 1) }
    cache.replace([now + 2, now + 3], revision: revision)
    _(cache.next_at).must_equal now + 1
    _(cache.stats).must_equal({max_cache: 2, active_cache: 2, available_cache: 0})
    cache.expire(now + 1)
    _(cache.next_at).must_equal now + 2
  end

  it "disables zero-cache discovery until notified and keeps only one rearm deadline" do
    cache = DecentJob::SchedulerCache.new(max_cache: 0, threads: 1)
    _(cache.refresh?).must_equal false
    now = Time.now
    cache.notify_at(now + 2)
    cache.notify_at(now + 1)
    _(cache.stats).must_equal({max_cache: 0, active_cache: 1, available_cache: 0})
    _(cache.refresh?).must_equal true
    cache.expire(now + 1)
    cache.replace([now + 2], revision: cache.revision)
    _(cache.next_at).must_equal now + 2
    cache.replace([], revision: cache.revision)
    _(cache.refresh?).must_equal false
  end

  it "rechecks wall time for forward and backward clock changes" do
    cache = DecentJob::SchedulerCache.new(max_cache: 1, threads: 1)
    now = Time.now
    cache.notify_at(now + 5)
    _(cache.due?(now - 100)).must_equal false
    _(cache.due?(now + 6)).must_equal true
    _(cache.due?(now + 3)).must_equal false
  end

  it "clears cached deadlines and wake permits on shutdown" do
    cache = DecentJob::SchedulerCache.new(max_cache: 2, threads: 2)
    cache.notify_at(Time.now + 10)
    cache.clear
    _(cache.stats.fetch(:active_cache)).must_equal 0
    _(cache.permits).must_equal 0
  end
end
