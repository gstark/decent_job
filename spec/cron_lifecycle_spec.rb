# frozen_string_literal: true

require "spec_helper"
require "timeout"
require "decent_job/cli"
require "stringio"

class CronLifecycleJob < ActiveJob::Base
  def perform(*)
  end
end

describe "cron lifecycle" do
  include DatabaseSpec

  before do
    DecentJob.configure do |config|
      config.database_url = @connection_options
      config.enable_listen_notify = false
      config.poll_interval = 0.02
      config.max_threads = 1
      config.cron = {recurring: {class: CronLifecycleJob, cron: ->(previous) { previous ? previous + 60 : Time.now }}}
    end
    DecentJob.install
    CronLifecycleJob.queue_adapter = DecentJob::Adapter.new
  end

  after { DecentJob.reset! }

  def await(&block)
    Timeout.timeout(5) { sleep 0.005 until block.call }
  end

  it "runs cron independently of workers and clears managers during reset" do
    scheduler = DecentJob.start_cron
    _(DecentJob.cron_running?).must_equal true
    await { DecentJob::CronEntry.find(:recurring).jobs.size == 1 }
    _(DecentJob.stop_cron(timeout: 1)).must_equal true
    _(scheduler.shutdown?).must_equal true
    _(DecentJob.cron_running?).must_equal false
    DecentJob.reset!
    _(DecentJob.cron_managers).must_equal []
  end

  it "starts configured cron alongside workers only when enabled" do
    DecentJob.start
    _(DecentJob.cron_running?).must_equal false
    DecentJob.configuration.enable_cron = true
    DecentJob.start
    _(DecentJob.cron_running?).must_equal true
    _(DecentJob.shutdown(timeout: 1)).must_equal true
    _(DecentJob.cron_running?).must_equal false
  end

  it "automatically binds async cron to its explicit adapter database without opening the global database" do
    DecentJob.reset!
    DecentJob.configure do |config|
      config.database_url = nil
      config.enable_cron = true
      config.enable_listen_notify = false
      config.max_threads = 1
      config.cron = {recurring: {class: CronLifecycleJob, cron: ->(previous) { previous ? previous + 60 : Time.now }}}
    end
    DecentJob::Schema.new(database).migrate
    CronLifecycleJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :async)
    CronLifecycleJob.perform_later
    await { DecentJob.cron_running? }
    _(DecentJob.cron_managers.first.cron_entries.first.database).must_be_same_as database
    await { DecentJob::CronStore.new(database).jobs(:recurring).size == 1 }
    _(DecentJob.shutdown(timeout: 1)).must_equal true
  end

  it "shares one shutdown budget and waits for in-flight cron callbacks on a subsequent shutdown" do
    entered = Queue.new
    release = Queue.new
    DecentJob.configuration.cron = {slow: {cron: ->(previous) { previous ? previous + 60 : Time.now }, class: -> {
      entered << true
      release.pop
    }}}
    DecentJob.start_cron
    Timeout.timeout(5) { entered.pop }
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    _(DecentJob.shutdown(timeout: 0.03)).must_equal false
    _(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).must_be :<, 0.2
    release << true
    _(DecentJob.shutdown(timeout: 1)).must_equal true
  ensure
    release << true if release
  end

  it "does not evaluate scheduling callables while holding the global mutex" do
    DecentJob.configuration.cron = {nested: {cron: ->(previous) {
      DecentJob.database
      previous ? previous + 60 : Time.now
    }, class: CronLifecycleJob}}
    DecentJob.start_cron
    await { DecentJob::CronEntry.find(:nested).jobs.size == 1 }
  end
  it "stops adapter cron and prevents a draining callback from restarting async execution" do
    entered = Queue.new
    release = Queue.new
    DecentJob.configuration.enable_cron = true
    DecentJob.configuration.cron = {slow: {cron: ->(previous) { previous ? previous + 60 : Time.now }, class: -> {
      entered << true
      release.pop
      CronLifecycleJob.perform_later
    }}}
    adapter = DecentJob::Adapter.new(database: DecentJob.database, execution_mode: :async)
    CronLifecycleJob.queue_adapter = adapter
    CronLifecycleJob.perform_later
    Timeout.timeout(5) { entered.pop }
    _(adapter.shutdown(timeout: 0.01)).must_equal false
    _(DecentJob.cron_running?).must_equal false
    release << true
    _(adapter.shutdown(timeout: 1)).must_equal true
    _(DecentJob.cron_running?).must_equal false
    job = DecentJob::CronEntry.find(:slow).last_job
    _(job.fetch("finished_at")).must_be_nil
  ensure
    release << true if release
  end

  it "rejects invalid shutdown timeouts before changing lifecycle state" do
    DecentJob.start_cron
    _ { DecentJob.shutdown(timeout: -1) }.must_raise ArgumentError
    _(DecentJob.cron_running?).must_equal true
    _ { DecentJob.stop_cron(timeout: Float::INFINITY) }.must_raise ArgumentError
    _(DecentJob.cron_running?).must_equal true
  end

  it "stops only the cron manager bound to the adapter database" do
    DecentJob.configuration.cron = {}
    first = DecentJob.start_cron
    second = DecentJob.start_cron(database: database)
    adapter = DecentJob::Adapter.new(database: DecentJob.database)
    _(adapter.shutdown(timeout: 1)).must_equal true
    _(first.shutdown?).must_equal true
    _(second.running?).must_equal true
  end

  it "rejects automatic static job definitions whose adapter uses another pool" do
    CronLifecycleJob.queue_adapter = DecentJob::Adapter.new(database: database)
    _ { DecentJob.start_cron }.must_raise ArgumentError
    _(DecentJob.cron_managers).must_equal []
  end

  it "waits for callbacks and closes its database before reset returns" do
    entered = Queue.new
    release = Queue.new
    DecentJob.configuration.cron = {slow: {cron: ->(previous) { previous ? previous + 60 : Time.now }, class: -> {
      entered << true
      release.pop
    }}}
    pool = DecentJob.database
    scheduler = DecentJob.start_cron
    Timeout.timeout(5) { entered.pop }
    reset = Thread.new { DecentJob.reset! }
    await { !scheduler.running? }
    _(reset.alive?).must_equal true
    release << true
    _(reset.join(2)).must_equal reset
    _(scheduler.shutdown?).must_equal true
    _ { pool.with_connection {} }.must_raise DecentJob::ClosedDatabase
    _(DecentJob.cron_managers).must_equal []
  ensure
    release << true if release
    reset&.join(2)
  end
  it "rejects false timeout across global and adapter shutdown without stopping resources" do
    scheduler = DecentJob.start_cron
    adapter = CronLifecycleJob.queue_adapter
    _ { DecentJob.shutdown(timeout: false) }.must_raise ArgumentError
    _(scheduler.running?).must_equal true
    _ { DecentJob.stop_cron(timeout: false) }.must_raise ArgumentError
    _(scheduler.running?).must_equal true
    _ { adapter.shutdown(timeout: false) }.must_raise ArgumentError
    _(scheduler.running?).must_equal true
  end
end

describe "cron configuration" do
  it "defaults cron off and validates the restart period and enable flag" do
    configuration = DecentJob::Configuration.new
    _(configuration.enable_cron).must_equal false
    _(configuration.cron_graceful_restart_period).must_be_nil
    [false, 0, -1, Float::INFINITY, Float::NAN, "4"].each do |value|
      configuration.cron_graceful_restart_period = value
      _ { configuration.validate! }.must_raise ArgumentError
    end
    configuration.cron_graceful_restart_period = 3
    _(configuration.cron_manager_options[:graceful_restart_period]).must_equal 3
    configuration.enable_cron = "true"
    _ { configuration.validate! }.must_raise ArgumentError
  end

  it "loads cron environment options and allows explicit configuration to override them" do
    previous = ENV.to_h.slice("DECENT_JOB_ENABLE_CRON", "DECENT_JOB_CRON_GRACEFUL_RESTART_PERIOD")
    ENV["DECENT_JOB_ENABLE_CRON"] = "true"
    ENV["DECENT_JOB_CRON_GRACEFUL_RESTART_PERIOD"] = "12.5"
    configuration = DecentJob::Configuration.new
    _(configuration.enable_cron).must_equal true
    _(configuration.cron_graceful_restart_period).must_equal 12.5
    configuration.enable_cron = false
    configuration.cron_graceful_restart_period = nil
    _(configuration.validate!.enable_cron).must_equal false
  ensure
    %w[DECENT_JOB_ENABLE_CRON DECENT_JOB_CRON_GRACEFUL_RESTART_PERIOD].each { |name| ENV[name] = previous[name] }
  end

  it "accounts for cron database concurrency in the global pool" do
    configuration = DecentJob::Configuration.new
    base = configuration.pool_size
    configuration.enable_cron = true
    configuration.cron = {first: {cron: "@daily"}, second: {cron: "@hourly"}}
    _(configuration.pool_size).must_equal base + 6
    configuration.cron_graceful_restart_period = 30
    _(configuration.pool_size).must_equal base + 8
  end

  it "accepts CLI cron overrides after loading application configuration" do
    output = StringIO.new
    error = StringIO.new
    cli = DecentJob::CLI.new(stdout: output, stderr: error)
    DecentJob.stub(:install, true) do
      _(cli.run(["migrate", "--enable-cron", "--cron-graceful-restart-period", "2.5"])).must_equal 0
      _(DecentJob.configuration.enable_cron).must_equal true
      _(DecentJob.configuration.cron_graceful_restart_period).must_equal 2.5
      _(cli.run(["migrate", "--no-enable-cron"])).must_equal 0
      _(DecentJob.configuration.enable_cron).must_equal false
    end
  ensure
    DecentJob.reset!
  end
end
