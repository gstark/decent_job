# frozen_string_literal: true

require "spec_helper"
require "timeout"

class ForcedShutdownJob < ActiveJob::Base
  class_attribute :started, :cleanup, :release, :finished, :shutdown_worker

  def perform(mode = "ruby")
    started << [Thread.current, DecentJob::Current.execution]
    if mode == "self"
      finished << shutdown_worker.shutdown(force: true)
      nil
    elsif mode == "complete"
      nil
    elsif mode == "pg"
      DecentJob::Current.execution.fetch(:database).with_connection { |connection| connection.exec("SELECT pg_sleep(60)") }
    else
      Queue.new.pop
    end
  ensure
    cleanup << Thread.current
    release.pop unless ["self", "complete"].include?(mode)
    finished << true
    raise "cleanup failed" if mode == "ensure_error"
  end
end

class ForcedShutdownRescuedJob < ForcedShutdownJob
  rescue_from StandardError, with: ->(_error) {}
end

describe "forced worker shutdown" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    DecentJob.configuration.enable_listen_notify = false
    DecentJob.configuration.on_thread_error = ->(_error) {}
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    ForcedShutdownJob.queue_adapter = @adapter
    ForcedShutdownRescuedJob.queue_adapter = @adapter
    ForcedShutdownJob.started = Queue.new
    ForcedShutdownJob.cleanup = Queue.new
    ForcedShutdownJob.release = Queue.new
    ForcedShutdownJob.finished = Queue.new
    @worker = DecentJob::Worker.new(database: database, max_threads: 1, enable_listen_notify: false)
  end

  after do
    ForcedShutdownJob.release << true
    @thread&.kill if @thread&.alive?
    @worker.shutdown(timeout: 5)
    DecentJob.reset!
  end

  def pop(queue)
    Timeout.timeout(5) { queue.pop }
  end

  ["ruby", "pg"].each do |mode|
    it "interrupts blocked #{mode} work without releasing its claim during user ensure" do
      job = ForcedShutdownJob.perform_later(mode)
      @worker.start
      @thread, context = pop(ForcedShutdownJob.started)
      _(context.fetch(:job_id)).must_equal job.job_id
      started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      _(@worker.shutdown(timeout: 0.02, force: true)).must_equal false
      _(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at).must_be :<, 0.5
      _(pop(ForcedShutdownJob.cleanup)).must_equal @thread
      _(@worker.interrupted_job_ids).must_equal [job.job_id]
      report = @worker.interrupted_job_ids
      report.first.replace("changed")
      report.clear
      _(@worker.interrupted_job_ids).must_equal [job.job_id]
      _(@worker.shutdown?).must_equal false
      process_id = @worker.process_id
      _(@worker.start).must_equal false
      _(@worker.process_id).must_equal process_id
      _(DecentJob::Claim.new(database).with_next(job_id: job.provider_job_id) { flunk "claim must stay locked" }).must_be_nil
      ForcedShutdownJob.release << true
      _(@worker.shutdown(timeout: 5)).must_equal true
      _(database.stats.fetch(:active)).must_equal 0
      _(database.with_connection { |connection| connection.exec("SELECT 1").getvalue(0, 0) }).must_equal "1"
      row = DecentJob::Store.new(database).find(job.provider_job_id)
      _(row.fetch("state")).must_equal "running"
      _(row["finished_at"]).must_be_nil
      @worker.start
      _(@worker.interrupted_job_ids).must_be_empty
      @worker.shutdown(timeout: 0, force: true)
    end
  end

  [ForcedShutdownJob, ForcedShutdownRescuedJob].each do |job_class|
    it "leaves #{job_class.name} unfinished when user cleanup raises during interruption" do
      job = job_class.perform_later("ensure_error")
      @worker.start
      @thread, context = pop(ForcedShutdownJob.started)
      _(@worker.shutdown(timeout: 0, force: true)).must_equal false
      pop(ForcedShutdownJob.cleanup)
      ForcedShutdownJob.release << true
      _(@worker.shutdown(timeout: 5)).must_equal true
      stats = @worker.stats
      _(stats.fetch(:total_executions_count)).must_equal 0
      _(stats.fetch(:succeeded_executions_count)).must_equal 0
      _(stats.fetch(:errored_executions_count)).must_equal 0
      _(stats.fetch(:empty_executions_count)).must_equal 0
      stored = DecentJob::Store.new(database).find(job.provider_job_id)
      _(stored.fetch("state")).must_equal "running"
      _(stored["finished_at"]).must_be_nil
      execution_id = context.fetch(:execution_id)
      database.with_connection do |connection|
        execution = connection.exec_params("SELECT state, finished_at FROM decent_job_executions WHERE id = $1", [execution_id]).first
        _(execution.fetch("state")).must_equal "running"
        _(execution["finished_at"]).must_be_nil
      end
      recovered = DecentJob::Claim.new(database).with_next(job_id: job.provider_job_id) { |row| row }
      _(recovered.fetch("execution_interrupted")).wont_be_nil
      database.with_connection do |connection|
        _(connection.exec_params("SELECT state FROM decent_job_executions WHERE id = $1", [execution_id]).getvalue(0, 0)).must_equal "interrupted"
      end
    end
  end

  [false, true].each do |rescued|
    it "rolls back interrupted retention finalization when cleanup errors are #{rescued ? "rescued" : "raised"}" do
      policy_entered = Queue.new
      policy = lambda do |_job, _error, _event|
        begin
          policy_entered << true
          Queue.new.pop
        ensure
          raise "retention cleanup failed"
        end
      rescue
        raise unless rescued
        true
      end
      DecentJob.configuration.preserve_job_records = policy
      job = ForcedShutdownJob.perform_later("complete")
      @worker.start
      @thread, context = pop(ForcedShutdownJob.started)
      pop(policy_entered)
      _(@worker.shutdown(timeout: 0, force: true)).must_equal false
      _(@worker.shutdown(timeout: 5)).must_equal true
      stored = DecentJob::Store.new(database).find(job.provider_job_id)
      _(stored.fetch("state")).must_equal "running"
      _(stored["finished_at"]).must_be_nil
      _(@worker.stats.fetch(:total_executions_count)).must_equal 0
      recovered = DecentJob::Claim.new(database).with_next(job_id: job.provider_job_id) { |row| row }
      _(recovered.fetch("execution_interrupted")).wont_be_nil
      database.with_connection do |connection|
        _(connection.exec_params("SELECT state FROM decent_job_executions WHERE id = $1", [context.fetch(:execution_id)]).getvalue(0, 0)).must_equal "interrupted"
      end
    end
  end

  it "reports once when an observer reenters shutdown and raises" do
    job = ForcedShutdownJob.perform_later
    @worker.start
    @thread, = pop(ForcedShutdownJob.started)
    events = []
    subscriber = ActiveSupport::Notifications.subscribe("scheduler_shutdown_kill.decent_job") do |*args|
      events << args.last
      @worker.shutdown(timeout: 0, force: true)
      raise "observer failed"
    end
    _(@worker.shutdown(timeout: 0, force: true)).must_equal false
    pop(ForcedShutdownJob.cleanup)
    _(@worker.shutdown(timeout: 0, force: true)).must_equal false
    _(events.map { |event| event.fetch(:active_job_ids) }).must_equal [[job.job_id]]
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
  end

  it "releases activity counters when interrupted during normal attempt cleanup" do
    cleanup_entered = Queue.new
    cleanup_release = Queue.new
    @worker.define_singleton_method(:cleanup_after_attempt) do
      cleanup_entered << true
      cleanup_release.pop
    end
    ForcedShutdownJob.perform_later("complete")
    @worker.start
    @thread, = pop(ForcedShutdownJob.started)
    pop(cleanup_entered)
    mutex = @worker.instance_variable_get(:@mutex)
    mutex.synchronize do
      cleanup_release << true
      Timeout.timeout(2) do
        Thread.pass until @thread.backtrace&.any? { |line| line.include?("synchronize") }
      end
      @thread.kill
    end
    _(@worker.shutdown(timeout: 5)).must_equal true
    _(@worker.stats.fetch(:schedulers).first.fetch(:active_threads)).must_equal 0
  ensure
    cleanup_release << true
  end

  it "returns from shutdown invoked by its own executing job without killing the caller" do
    ForcedShutdownJob.shutdown_worker = @worker
    ForcedShutdownJob.perform_later("self")
    @worker.start
    @thread, = pop(ForcedShutdownJob.started)
    _(pop(ForcedShutdownJob.finished)).must_equal false
    _(pop(ForcedShutdownJob.finished)).must_equal true
    _(@worker.shutdown(timeout: 5)).must_equal true
    _(@worker.interrupted_job_ids).must_be_empty
  end

  it "keeps the original tracker until its blocked monitor callback drains" do
    entered = Queue.new
    release = Queue.new
    @worker.define_singleton_method(:stats) do
      entered << true
      release.pop
      super()
    end
    @worker.start
    pop(entered)
    process_id = @worker.process_id
    _(@worker.shutdown(timeout: 0, force: true)).must_equal false
    _(@worker.shutdown?).must_equal false
    @worker.start
    _(@worker.process_id).must_equal process_id
    release << true
    _(@worker.shutdown(timeout: 5)).must_equal true
  ensure
    release << true
  end

  it "returns from shutdown invoked by its tracker state callback" do
    returned = Queue.new
    @worker.define_singleton_method(:stats) do
      returned << shutdown(force: true)
      super()
    end
    @worker.start
    _(pop(returned)).must_equal false
    _(@worker.shutdown(timeout: 5)).must_equal true
  end

  it "deduplicates concurrent interruption requests while ensure still runs" do
    job = ForcedShutdownJob.perform_later
    @worker.start
    @thread, = pop(ForcedShutdownJob.started)
    events = Queue.new
    subscriber = ActiveSupport::Notifications.subscribe("scheduler_shutdown_kill.decent_job") { |*args| events << args.last }
    callers = 4.times.map { Thread.new { @worker.shutdown(timeout: 0, force: true) } }
    _(callers.map(&:value)).must_equal [false] * 4
    pop(ForcedShutdownJob.cleanup)
    _(events.size).must_equal 1
    _(@worker.interrupted_job_ids).must_equal [job.job_id]
    _(ForcedShutdownJob.finished.size).must_equal 0
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
    callers&.each(&:join)
  end

  it "does not force when the timeout is nil" do
    ForcedShutdownJob.perform_later
    @worker.start
    @thread, = pop(ForcedShutdownJob.started)
    shutdown = Thread.new { @worker.shutdown(timeout: nil, force: true) }
    sleep 0.02
    _(ForcedShutdownJob.cleanup.size).must_equal 0
    _(@worker.interrupted_job_ids).must_be_empty
    @thread.kill
    pop(ForcedShutdownJob.cleanup)
    ForcedShutdownJob.release << true
    _(shutdown.value).must_equal true
  ensure
    shutdown&.kill
  end
end

describe "forced shutdown across registered resources" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    DecentJob.configuration.enable_listen_notify = false
    DecentJob.configuration.max_threads = 1
    ForcedShutdownJob.started = Queue.new
    ForcedShutdownJob.cleanup = Queue.new
    ForcedShutdownJob.release = Queue.new
    ForcedShutdownJob.finished = Queue.new
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    ForcedShutdownJob.queue_adapter = @adapter
    @cron_started = Queue.new
    @cron_cleanup = Queue.new
    @cron_release = Queue.new
    DecentJob.configuration.cron = {blocked: {class: -> {
      begin
        @cron_started << Thread.current
        Queue.new.pop
      ensure
        @cron_cleanup << true
        @cron_release.pop
      end
    }, cron: ->(previous) { previous ? previous + 60 : Time.now }}}
  end

  after do
    ForcedShutdownJob.release << true
    @cron_release << true
    @job_thread&.kill
    @cron_thread&.kill
    @adapter.shutdown(timeout: 5)
    DecentJob.reset!
  end

  def pop(queue)
    Timeout.timeout(5) { queue.pop }
  end

  [:adapter, :global].each do |scope|
    it "shares the #{scope} grace period before interrupting worker and cron callbacks" do
      ForcedShutdownJob.perform_later
      @job_thread, = pop(ForcedShutdownJob.started)
      @manager = DecentJob.start_cron(database: database)
      @cron_thread = pop(@cron_started)
      started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      target = (scope == :adapter) ? @adapter : DecentJob
      _(target.shutdown(timeout: 0.08, force: true)).must_equal false
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
      _(elapsed).must_be :>=, 0.07
      _(elapsed).must_be :<, 0.5
      pop(@cron_cleanup)
      pop(ForcedShutdownJob.cleanup)
      _(@manager.shutdown?).must_equal false
    end
  end

  it "lets cron callbacks finish inside the grace period without interruption" do
    finished = Queue.new
    DecentJob.configuration.cron = {quick: {class: -> {
      @cron_started << true
      sleep 0.03
      finished << true
    }, cron: ->(previous) { previous ? previous + 60 : Time.now }}}
    @manager = DecentJob.start_cron(database: database)
    pop(@cron_started)
    _(@manager.shutdown(timeout: 1, force: true)).must_equal true
    _(finished.size).must_equal 1
  end
end
