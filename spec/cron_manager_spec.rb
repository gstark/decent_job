# frozen_string_literal: true

require "spec_helper"
require "timeout"

class CronManagerJob < ActiveJob::Base
  def perform(*)
  end
end

describe "cron manager" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    CronManagerJob.queue_adapter = DecentJob::Adapter.new(database: database)
    @managers = []
    @errors = Queue.new
  end

  after { @managers.each(&:shutdown) }

  def manager(entries, **options)
    @managers << DecentJob::CronManager.new(entries, on_thread_error: ->(error) { @errors << error }, **options)
    @managers.last
  end

  def entry(key = :recurring, **attributes)
    DecentJob::CronEntry.new({key: key, class: CronManagerJob, cron: "* * * * * * UTC"}.merge(attributes), database: database)
  end

  def await(&block)
    Timeout.timeout(5) { sleep 0.005 until block.call }
  end

  it "starts concurrently only once, enqueues once, and restarts after shutdown" do
    occurrence = Time.now + 0.05
    recurring = entry(cron: ->(previous) { previous ? previous + 60 : occurrence })
    scheduler = manager([recurring])
    _(scheduler.shutdown?).must_equal true
    threads = Array.new(8) { Thread.new { scheduler.start } }
    threads.each(&:join)
    await { recurring.jobs.size == 1 }
    _(scheduler.running?).must_equal true
    _(recurring.last_job_at).must_be_close_to occurrence, 0.00001
    _(scheduler.shutdown(timeout: 1)).must_equal true
    _(scheduler.shutdown?).must_equal true
    _(scheduler.restart(timeout: 1)).must_be_same_as scheduler
    _(scheduler.running?).must_equal true
  end

  it "deduplicates simultaneous managers and replays callable times from the last persisted job" do
    start = Time.now - 0.4
    recurring = entry(cron: ->(previous) { previous ? previous + 0.1 : start })
    recurring.enqueue(start)
    2.times { manager([recurring], start_on_initialize: true) }
    await { recurring.jobs.size >= 5 }
    @managers.each(&:shutdown)
    times = recurring.jobs.map { |row| row.fetch("cron_at") }
    _(times.uniq).must_equal times
    _(times.sort.first(5).map { |time| (time - start).round(1) }).must_equal [0, 0.1, 0.2, 0.3, 0.4]
  end

  it "advances disabled schedules and observes persistent enable changes at delivery" do
    observed = Queue.new
    start = Time.now + 0.02
    recurring = entry(cron: ->(previous) {
      observed << previous
      previous ? previous + 0.05 : start
    })
    recurring.disable
    scheduler = manager([recurring], start_on_initialize: true)
    await { observed.size >= 3 }
    _(recurring.jobs).must_equal []
    recurring.enable
    await { !recurring.jobs.empty? }
    scheduler.shutdown
    _(recurring.jobs.last.fetch("cron_at")).must_be :>, start
  end

  it "computes the next occurrence before a slow enqueue and does not block other entries" do
    entered = Queue.new
    release = Queue.new
    calculations = Queue.new
    start = Time.now - 0.01
    schedule = ->(previous) {
      calculations << previous
      previous ? previous + 60 : start
    }
    slow = entry(:slow, cron: schedule, class: -> {
      entered << true
      release.pop
    })
    fast = entry(:fast, cron: ->(previous) { previous ? previous + 60 : start })
    scheduler = manager([slow, fast], start_on_initialize: true)
    Timeout.timeout(5) { entered.pop }
    _(calculations.size).must_equal 2
    await { fast.jobs.size == 1 }
    _(scheduler.shutdown(timeout: 0.01)).must_equal false
    _(scheduler.running?).must_equal false
    _(scheduler.shutdown?).must_equal false
    _(scheduler.start).must_equal false
    release << true
    _(scheduler.shutdown(timeout: 1)).must_equal true
  ensure
    release << true if release
  end

  it "isolates enqueue and error-handler exceptions while retaining the next occurrence" do
    attempts = Queue.new
    start = Time.now - 0.01
    recurring = entry(cron: ->(previous) { previous ? previous + 0.02 : start }, class: -> {
      attempts << true
      raise "callback failed"
    })
    scheduler = manager([recurring], on_thread_error: ->(_) { raise "report failed" }, start_on_initialize: true)
    await { attempts.size >= 3 }
    _(scheduler.running?).must_equal true
  end

  it "isolates invalid and nonadvancing schedules instead of spinning" do
    start = Time.now - 0.01
    invalid = entry(:invalid, cron: ->(_) {})
    stuck = entry(:stuck, cron: ->(_) { start })
    fast = entry(:fast, cron: ->(previous) { previous ? previous + 60 : start })
    manager([invalid, stuck, fast], start_on_initialize: true)
    await { fast.jobs.size == 1 && stuck.jobs.size == 1 && @errors.size == 1 }
    _(stuck.jobs.size).must_equal 1
    _(invalid.jobs).must_equal []
    _(@errors.pop).must_be_kind_of ArgumentError
  end

  it "catches up oldest first with a startup cutoff that excludes future records" do
    recurring = entry
    recurring.enqueue(Time.now + 3600)
    scheduler = manager([recurring], graceful_restart_period: 3.5, start_on_initialize: true)
    await { recurring.jobs.size >= 4 }
    scheduler.shutdown
    times = recurring.jobs.map { |row| row.fetch("cron_at") }.sort
    _(times.first(3).each_cons(2).map { |a, b| b - a }).must_equal [1, 1]
    _(times.first).must_be :>, Time.now - 5
  end

  it "stops catchup before the next callback without eagerly enqueuing the backlog" do
    entered = Queue.new
    release = Queue.new
    recurring = entry(class: -> {
      entered << true
      release.pop
    })
    scheduler = manager([recurring], graceful_restart_period: 100_000, start_on_initialize: true)
    Timeout.timeout(5) { entered.pop }
    _(scheduler.shutdown(timeout: 0)).must_equal false
    release << true
    _(scheduler.shutdown(timeout: 1)).must_equal true
    _(entered.size).must_equal 0
  ensure
    release << true if release
  end

  it "carries Time.zone into scheduling and delivery callbacks" do
    zones = Queue.new
    Time.use_zone("America/New_York") do
      recurring = entry(cron: ->(previous) {
        zones << Time.zone.name
        previous ? previous + 60 : Time.now
      }, class: -> { zones << Time.zone.name })
      manager([recurring], start_on_initialize: true)
    end
    await { zones.size >= 3 }
    _(3.times.map { zones.pop }).must_equal ["America/New_York"] * 3
  end
  it "delivers the next live occurrence while the preceding callback is still running" do
    entered = Queue.new
    release = Queue.new
    recurring = entry(cron: ->(previous) { previous ? previous + 0.03 : Time.now }, class: -> {
      entered << true
      release.pop
    })
    scheduler = manager([recurring], start_on_initialize: true)
    await { entered.size >= 2 }
    _(scheduler.shutdown(timeout: 0)).must_equal false
  ensure
    scheduler&.shutdown(timeout: 0)
    4.times { release << true } if release
  end

  it "continues live delivery while catchup is blocked" do
    entered = Queue.new
    release = Queue.new
    recurring = entry(class: -> {
      entered << DecentJob::CronEntry.current_context.fetch(:at)
      release.pop
    })
    scheduler = manager([recurring], graceful_restart_period: 30, start_on_initialize: true)
    await { entered.size >= 2 }
    times = 2.times.map { entered.pop }
    _(times.max - times.min).must_be :>, 20
    _(scheduler.shutdown(timeout: 0)).must_equal false
  ensure
    scheduler&.shutdown(timeout: 0)
    5.times { release << true } if release
  end

  it "rechecks wall time across backward and forward clock jumps" do
    clock_mutex = Mutex.new
    origin = Time.now
    now = origin
    scheduled = Queue.new
    recurring = entry(cron: ->(previous) {
      scheduled << true
      previous ? previous + 60 : origin + 0.1
    })
    Time.stub(:now, -> { clock_mutex.synchronize { now } }) do
      scheduler = manager([recurring], start_on_initialize: true)
      Timeout.timeout(5) { scheduled.pop }
      clock_mutex.synchronize { now = origin - 60 }
      sleep 0.3
      _(recurring.jobs).must_equal []
      clock_mutex.synchronize { now = origin + 1 }
      await { recurring.jobs.size == 1 }
      _(scheduler.shutdown(timeout: 1)).must_equal true
    end
  end

  it "retains a current occurrence when calculating the next occurrence raises" do
    start = Time.now
    recurring = entry(cron: ->(previous) {
      raise "schedule failed" if previous
      start
    })
    manager([recurring], start_on_initialize: true)
    await { recurring.jobs.size == 1 && @errors.size == 1 }
    _(@errors.pop.message).must_equal "schedule failed"
  end

  it "keeps live scheduling when evaluating the historical schedule raises" do
    calls = 0
    recurring = entry(cron: ->(previous) {
      calls += 1
      raise "historical schedule failed" if calls == 2
      "* * * * * * UTC"
    })
    manager([recurring], graceful_restart_period: 30, start_on_initialize: true)
    await { recurring.jobs.size >= 1 && @errors.size == 1 }
    _(@errors.pop.message).must_equal "historical schedule failed"
  end

  it "bounds concurrent callbacks and joins all owned threads after a timed-out stop" do
    entered = Queue.new
    release = Queue.new
    start = Time.now - 1
    recurring = entry(cron: ->(previous) { previous ? previous + 0.001 : start }, class: -> {
      entered << Thread.current
      release.pop
    })
    scheduler = manager([recurring], start_on_initialize: true)
    await { entered.size == 2 }
    sleep 0.05
    _(entered.size).must_equal 2
    _(scheduler.restart(timeout: 0)).must_equal false
    threads = 2.times.map { entered.pop }
    2.times { release << true }
    _(scheduler.shutdown(timeout: 1)).must_equal true
    _(threads.none?(&:alive?)).must_equal true
  ensure
    scheduler&.shutdown(timeout: 0)
    4.times { release << true } if release
  end

  it "passes the historical cutoff snapshot to callable expression catchup" do
    seen = Queue.new
    recurring = entry(cron: ->(previous) {
      seen << previous
      "@daily UTC"
    })
    recurring.enqueue(Time.now + 86400)
    scheduler = manager([recurring], graceful_restart_period: 172800, start_on_initialize: true)
    await { seen.size >= 2 }
    values = 2.times.map { seen.pop }
    _(values).must_include nil
    _(scheduler.shutdown(timeout: 1)).must_equal true
  end

  it "skips a forward clock gap for expression schedules without replaying unrequested history" do
    clock_mutex = Mutex.new
    origin = Time.at(Time.now.to_i)
    now = origin
    scheduled = Queue.new
    recurring = entry(cron: ->(_) { "* * * * * * UTC" })
    calculate_next = recurring.method(:next_at)
    recurring.define_singleton_method(:next_at) do |**options|
      calculate_next.call(**options).tap { scheduled << true }
    end
    Time.stub(:now, -> { clock_mutex.synchronize { now } }) do
      scheduler = manager([recurring], start_on_initialize: true)
      Timeout.timeout(5) { scheduled.pop }
      clock_mutex.synchronize { now = origin + 60 }
      await { recurring.jobs.size >= 1 }
      sleep 0.1
      _(recurring.jobs.size).must_equal 1
      _(recurring.last_job_at).must_equal origin + 1
      _(scheduler.shutdown(timeout: 1)).must_equal true
    end
  end
  it "rechecks wall time after a blocking enabled predicate before live delivery" do
    clock_mutex = Mutex.new
    now = Time.at(100)
    entered = Queue.new
    release = Queue.new
    first_check = true
    recurring = entry(cron: ->(previous) { previous ? previous + 60 : Time.at(100) }, enabled_by_default: -> {
      if first_check
        first_check = false
        entered << true
        release.pop
      end
      true
    })
    Time.stub(:now, -> { clock_mutex.synchronize { now } }) do
      scheduler = manager([recurring], start_on_initialize: true)
      Timeout.timeout(5) { entered.pop }
      clock_mutex.synchronize { now = Time.at(50) }
      release << true
      sleep 0.3
      _(recurring.jobs).must_equal []
      clock_mutex.synchronize { now = Time.at(100) }
      await { recurring.jobs.size == 1 }
      _(scheduler.shutdown(timeout: 1)).must_equal true
    ensure
      scheduler&.shutdown(timeout: 0)
      release << true
    end
  end

  it "rejects nonnumeric timeouts before stopping the manager" do
    scheduler = manager([], start_on_initialize: true)
    [false, true, "1", -1, Float::INFINITY, Float::NAN].each do |timeout|
      _ { scheduler.shutdown(timeout: timeout) }.must_raise ArgumentError
      _(scheduler.running?).must_equal true
    end
  end

  it "retains expression scheduling when the clock moves backward inside the schedule callback" do
    clock_mutex = Mutex.new
    now = Time.at(100)
    calls = Queue.new
    moved = false
    recurring = entry(cron: ->(previous) {
      if previous && !moved
        moved = true
        clock_mutex.synchronize { now = Time.at(50) }
      end
      "* * * * * * UTC"
    })
    schedule = recurring.method(:next_at)
    recurring.define_singleton_method(:next_at) do |**options|
      schedule.call(**options).tap { calls << true }
    end
    Time.stub(:now, -> { clock_mutex.synchronize { now } }) do
      scheduler = manager([recurring], start_on_initialize: true)
      Timeout.timeout(5) { calls.pop }
      clock_mutex.synchronize { now = Time.at(101) }
      Timeout.timeout(5) { calls.pop }
      clock_mutex.synchronize { now = Time.at(105) }
      await { recurring.jobs.size >= 2 }
      _(scheduler.shutdown(timeout: 1)).must_equal true
      _(@errors.size).must_equal 0
      _(recurring.jobs.map { |row| row.fetch("cron_at").to_i }.sort).must_equal [101, 102]
    ensure
      scheduler&.shutdown(timeout: 0)
    end
  end

  it "keeps historical catchup independent of a backward wall clock change" do
    clock_mutex = Mutex.new
    now = Time.at(100)
    recurring = entry(enabled_by_default: -> {
      clock_mutex.synchronize { now = Time.at(50) }
      true
    })
    Time.stub(:now, -> { clock_mutex.synchronize { now } }) do
      scheduler = manager([recurring], graceful_restart_period: 2, start_on_initialize: true)
      await { recurring.jobs.size == 1 }
      _(recurring.last_job_at).must_equal Time.at(99)
      _(scheduler.shutdown(timeout: 1)).must_equal true
    ensure
      scheduler&.shutdown(timeout: 0)
    end
  end

  it "wraps schedule, enabled, and enqueue callbacks in the host executor" do
    original_rails = Object.const_get(:Rails) if Object.const_defined?(:Rails)
    Object.send(:remove_const, :Rails) if original_rails
    events = Queue.new
    restored = Queue.new
    executor = Object.new
    executor.define_singleton_method(:wrap) do |&block|
      previous = Thread.current.thread_variable_get(:cron_spec_executor)
      Thread.current.thread_variable_set(:cron_spec_executor, true)
      block.call
    ensure
      Thread.current.thread_variable_set(:cron_spec_executor, previous)
      restored << Thread.current.thread_variable_get(:cron_spec_executor)
    end
    host = Module.new
    application = Struct.new(:executor, :reloader).new(Class.new(ActiveSupport::Executor), executor)
    host.define_singleton_method(:application) { application }
    Object.const_set(:Rails, host)
    recurring = entry(cron: ->(previous) {
      events << [:schedule, Thread.current.thread_variable_get(:cron_spec_executor)]
      previous ? previous + 60 : Time.now
    }, enabled_by_default: -> {
      events << [:enabled, Thread.current.thread_variable_get(:cron_spec_executor)]
      true
    }, class: -> { events << [:enqueue, Thread.current.thread_variable_get(:cron_spec_executor)] })
    scheduler = manager([recurring], start_on_initialize: true)
    await { events.size >= 4 }
    _(scheduler.shutdown(timeout: 1)).must_equal true
    observed = events.size.times.map { events.pop }
    _(observed.map(&:first).uniq.sort).must_equal [:enabled, :enqueue, :schedule]
    _(observed.map(&:last).uniq).must_equal [true]
    _(restored.size.times.map { restored.pop }.uniq).must_equal [nil]
  ensure
    scheduler&.shutdown
    Object.send(:remove_const, :Rails) if Object.const_defined?(:Rails)
    Object.const_set(:Rails, original_rails) if original_rails
  end
end
