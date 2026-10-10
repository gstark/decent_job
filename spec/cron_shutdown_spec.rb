# frozen_string_literal: true

require "spec_helper"
require "timeout"

class CronShutdownFailure < ScriptError; end

describe "cron shutdown after fatal callback errors" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @fatal_error = CronShutdownFailure.new("fatal schedule")
    @fatal_started = Queue.new
    @callback_started = Queue.new
    @callback_release = Queue.new
    @callback_completed = Queue.new
    @previous_reporting = Thread.report_on_exception
    Thread.report_on_exception = false
  end

  after do
    @callback_release << true
    @callback_thread&.join
    @releaser&.join

    begin
      @manager&.shutdown
    rescue CronShutdownFailure
      nil
    end

    begin
      DecentJob.reset!
    rescue CronShutdownFailure
      nil
    end

    Thread.report_on_exception = @previous_reporting
  end

  def entries(connection_pool)
    {
      fatal: {class: -> {}, cron: ->(_) {
        @fatal_started << Thread.current
        raise @fatal_error
      }},
      holding: {class: -> {
        @callback_started << Thread.current
        @callback_release.pop
        value = connection_pool.with_connection { |connection| connection.exec("SELECT 1").getvalue(0, 0) }
        @callback_completed << value
      }, cron: ->(previous) { previous ? previous + 60 : Time.now }}
    }
  end

  def await_callbacks
    fatal_thread = Timeout.timeout(5) { @fatal_started.pop }
    @callback_thread = Timeout.timeout(5) { @callback_started.pop }
    Timeout.timeout(5) { sleep 0.001 while fatal_thread.alive? }
  end

  it "honors a bounded stop then drains all callback threads before re-raising the same failure" do
    configured = entries(database).map { |key, options| DecentJob::CronEntry.new(options.merge(key: key), database: database) }
    @manager = DecentJob::CronManager.new(configured).start
    await_callbacks
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    first = _ { @manager.shutdown(timeout: 0.01) }.must_raise CronShutdownFailure
    _(first).must_be_same_as @fatal_error
    _(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).must_be :<, 0.5
    _(@manager.shutdown?).must_equal false

    @releaser = Thread.new do
      sleep 0.05
      @callback_release << true
    end

    second = _ { @manager.shutdown }.must_raise CronShutdownFailure
    _(second).must_be_same_as @fatal_error
    _(@callback_completed.size).must_equal 1
    _(@callback_thread.alive?).must_equal false
    _(@manager.shutdown?).must_equal true
  end

  it "keeps the global database open until all callbacks drain and then clears reset state" do
    DecentJob.configure { |config| config.database_url = @connection_options }
    connection_pool = DecentJob.database
    DecentJob.configuration.cron = entries(connection_pool)
    @manager = DecentJob.start_cron
    await_callbacks

    @releaser = Thread.new do
      sleep 0.05
      @callback_release << true
    end

    error = _ { DecentJob.reset! }.must_raise CronShutdownFailure
    _(error).must_be_same_as @fatal_error
    _(@callback_completed.size).must_equal 1
    _(@callback_completed.pop).must_equal "1"
    _(@callback_thread.alive?).must_equal false
    _(@manager.shutdown?).must_equal true
    _(DecentJob.cron_managers).must_be_empty
    _(DecentJob.configuration.database_url).must_be_nil
  end
end
