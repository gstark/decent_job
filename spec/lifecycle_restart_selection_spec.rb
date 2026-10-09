# frozen_string_literal: true

require "spec_helper"
require "timeout"

describe "global restart observation selection" do
  before do
    @workers = []
    @releases = []
    @subscriptions = []
    @events = []
    @database = Object.new
    @database.define_singleton_method(:in_transaction?) { false }
    @subscriptions << ActiveSupport::Notifications.subscribe("scheduler_restart_pools.decent_job") { |event| @events << event.payload.dup }
    @locally_stopped, release = registered_worker
    release << true
    @locally_stopped.shutdown(timeout: 2)
    DecentJob.runtime_mutex.synchronize { DecentJob.locally_stop_worker(@locally_stopped) }
    @target, @target_release = registered_worker
  end

  after do
    @subscriptions.each { |subscription| ActiveSupport::Notifications.unsubscribe(subscription) }
    @releases.each { |release| release << true }
    @workers.each { |worker| worker.shutdown(timeout: 2) }
    DecentJob.reset!
  end

  def registered_worker
    entered = Queue.new
    release = Queue.new
    worker = DecentJob::Worker.new(database: @database, max_threads: 1, enable_listen_notify: false)
    worker.define_singleton_method(:run) do |*|
      entered << true
      release.pop
    end
    @workers << worker
    @releases << release
    DecentJob.start_worker(worker)
    Timeout.timeout(2) { entered.pop }
    [worker, release]
  end

  def assert_target_outcome(outcome)
    _(@events.size).must_equal 1
    _(@events.first[:scheduler]).must_be_same_as @target
    _(@events.first.values_at(:outcome, :restarted, :new_run_id)).must_equal [outcome, false, nil]
    _(@locally_stopped.shutdown?).must_equal true
  end

  it "does not report an incomplete restart for an owner excluded by its local stop" do
    _(DecentJob.restart(timeout: 0)).must_equal false
    assert_target_outcome(:incomplete)
  end

  it "does not report a failed restart for an owner excluded by its local stop" do
    failure = IOError.new("drain failed")
    @target_release << true
    @target.define_singleton_method(:drain_shutdown) do |*arguments, **options|
      value = super(*arguments, **options)
      unless @reported_test_failure
        @reported_test_failure = true
        raise failure
      end
      value
    end
    _(assert_raises(IOError) { DecentJob.restart(timeout: 2) }).must_be_same_as failure
    assert_target_outcome(:failed)
    _(@events.first[:error]).must_be_same_as failure
  end

  it "does not report a refused restart for an owner excluded before a newer global stop" do
    @subscriptions << ActiveSupport::Notifications.subscribe("scheduler_shutdown_start.decent_job") { @target_release << true }
    @subscriptions << ActiveSupport::Notifications.subscribe("scheduler_shutdown.decent_job") { DecentJob.shutdown(timeout: 0) }
    _(Timeout.timeout(2) { DecentJob.restart(timeout: 2) }).must_equal false
    assert_target_outcome(:refused)
  end
end
