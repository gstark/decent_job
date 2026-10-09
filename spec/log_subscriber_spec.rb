# frozen_string_literal: true

require "spec_helper"
require "stringio"

class LoggingJob < ActiveJob::Base
end

describe "DecentJob logging" do
  before do
    @subscriber = DecentJob::LogSubscriber
    @saved_destinations = @subscriber.loggers.dup
    @output = StringIO.new
    @logger = ActiveSupport::TaggedLogging.new(ActiveSupport::Logger.new(@output))
    @subscriber.loggers.replace([@logger])
    @subscriber.reset_logger
  end

  after do
    @subscriber.loggers.replace(@saved_destinations)
    @subscriber.reset_logger
    DecentJob.reset!
  end

  it "attaches once and supports detach and reattach" do
    _(@subscriber.subscribers.count { |subscriber| subscriber.is_a?(@subscriber) }).must_equal 1
    DecentJob::Notifications.instrument("notifier_listen.decent_job")
    _(@output.string.scan("Notifier subscribed with LISTEN").size).must_equal 1
    @subscriber.detach_from(:decent_job)
    DecentJob::Notifications.instrument("notifier_listen.decent_job")
    _(@output.string.scan("Notifier subscribed with LISTEN").size).must_equal 1
  ensure
    @subscriber.attach_to(:decent_job)
  end

  {
    scheduler_create_pool: [{max_threads: 3, performer_name: "mail", process_id: 42}, :info, "started scheduler with queues=mail max_threads=3."],
    scheduler_shutdown_start: [{process_id: 42}, :info, "shutting down scheduler..."],
    scheduler_shutdown: [{process_id: 42}, :info, "scheduler is shut down."],
    scheduler_shutdown_kill: [{process_id: 42, active_job_ids: ["one", "two"]}, :warn, "Active Jobs were interrupted: one two"],
    scheduler_restart_pools: [{process_id: 42}, :info, "scheduler has restarted."],
    cron_manager_start: [{cron_entries: [1]}, :info, "started cron with 1 job."],
    cluster_start: [{subprocesses: 2}, :info, "started supervisor with subprocesses=2."],
    cluster_spawn: [{pid: 42}, :info, "forked subprocess (PID: 42)."],
    cluster_reap: [{pid: 42, status: 1}, :info, "reaped subprocess (PID: 42, exit status: 1)."],
    cluster_backoff: [{restart_count: 4, delay: 2}, :warn, "by 2 seconds (consecutive unhealthy exits: 4)."],
    cluster_shutdown: [{}, :info, "supervisor is shut down."],
    subprocess_start: [{pid: 42}, :info, "started subprocess (PID: 42)."],
    subprocess_shutdown_start: [{pid: 42}, :info, "shutting down subprocess (PID: 42)..."],
    subprocess_shutdown: [{pid: 42}, :info, "subprocess is shut down (PID: 42)."],
    notifier_listen: [{}, :info, "Notifier subscribed with LISTEN"],
    notifier_notified: [{payload: "raw-notice"}, :debug, "Notifier received payload: raw-notice"],
    notifier_unlisten: [{}, :info, "Notifier unsubscribed with UNLISTEN"],
    cleanup_preserved_jobs: [{timestamp: "cutoff", destroyed_records_count: 2}, :info, "destroyed 2 preserved job execution records finished before cutoff."],
    systemd_watchdog_start: [{interval: 1.23}, :info, "Pinging systemd watchdog every 1.2 seconds"]
  }.each do |name, (payload, severity, message)|
    it "logs #{name} at #{severity} level" do
      @logger.level = Logger.const_get(severity.upcase)
      DecentJob::Notifications.instrument("#{name}.decent_job", payload)
      _(@output.string).must_include "[DecentJob]"
      _(@output.string).must_include message
      @output.truncate(0)
      @output.rewind
      @logger.level += 1
      DecentJob::Notifications.instrument("#{name}.decent_job", payload)
      _(@output.string).must_equal ""
    end
  end

  %i[finished_timer_task finished_job_task notifier_notify_error systemd_watchdog_error].each do |name|
    it "logs #{name} with exception class, message and backtrace" do
      error = ArgumentError.new("broken")
      error.set_backtrace(["job.rb:123"])
      @logger.level = Logger::ERROR
      DecentJob::Notifications.instrument("#{name}.decent_job", error: error)
      _(@output.string).must_include "ArgumentError: broken"
      _(@output.string).must_include "job.rb:123"
    end
  end

  it "describes unfinished shutdown and unsuccessful restart truthfully" do
    DecentJob::Notifications.instrument("scheduler_shutdown.decent_job", stopped: false)
    _(@output.string).must_include "scheduler is still draining."
    _(@output.string).wont_include "scheduler is shut down."
    DecentJob::Notifications.instrument("scheduler_restart_pools.decent_job", restarted: false, outcome: :refused)
    _(@output.string).must_include "scheduler restart refused."
    _(@output.string).wont_include "scheduler has restarted."
  end

  it "ignores successful timer and job tasks" do
    %i[finished_timer_task finished_job_task].each { |name| DecentJob::Notifications.instrument("#{name}.decent_job", error: nil) }
    _(@output.string).must_equal ""
  end

  it "logs job identifiers without serializing arguments and restores caller tags" do
    job = Struct.new(:id).new("job-123")
    @logger.tagged("ActiveJob", "caller") do
      DecentJob::Notifications.instrument("perform_job.decent_job", job: job, process_id: 42, thread_name: "worker")
      _(@logger.formatter.current_tags).must_equal ["ActiveJob", "caller"]
    end
    _(@output.string).must_include "[ActiveJob] [caller] [ActiveJob] [DecentJob] [42] [worker] Executed DecentJob job-123"
    _(@logger.formatter.current_tags).must_equal []
  end

  it "pluralizes cron and concurrency messages" do
    job = LoggingJob.new("secret-argument")
    [1, 2].each do |count|
      DecentJob::Notifications.instrument("cron_manager_start.decent_job", cron_entries: Array.new(count))
      %i[enqueue_concurrency_limit_exceeded enqueue_concurrency_throttle_exceeded].each do |name|
        DecentJob::Notifications.instrument("#{name}.decent_job", job: job, key: "account", limit: count)
      end
      _(@output.string).must_include "#{count} #{(count == 1) ? "job" : "jobs"}"
    end
    _(@output.string).must_include "Aborted enqueue of LoggingJob (Job ID: #{job.job_id})"
    _(@output.string).must_include "concurrency key 'account'"
    _(@output.string).wont_include "secret-argument"
  end

  it "logs kills without active jobs and subprocess signals" do
    DecentJob::Notifications.instrument("scheduler_shutdown_kill.decent_job")
    DecentJob::Notifications.instrument("cluster_reap.decent_job", pid: 42, signal: Signal.list.fetch("TERM"))
    _(@output.string).must_include "scheduler has been killed."
    _(@output.string).wont_include "interrupted"
    _(@output.string).must_include "signal: TERM"
  end

  it "broadcasts to independent levels and evaluates suppressed messages lazily" do
    second_output = StringIO.new
    second_logger = Logger.new(second_output, level: Logger::WARN)
    @subscriber.loggers << second_logger
    @subscriber.reset_logger
    DecentJob::Notifications.instrument("notifier_listen.decent_job")
    _(@output.string).must_include "LISTEN"
    _(second_output.string).must_equal ""
    DecentJob::Notifications.instrument("scheduler_shutdown_kill.decent_job")
    _(second_output.string).must_include "killed"
    @logger.level = Logger::WARN
    job = Object.new
    def job.id = raise("must remain lazy")
    DecentJob::Notifications.instrument("perform_job.decent_job", job: job)
    _(@output.string).wont_include "must remain lazy"
  end

  it "writes once per tagged destination inside an ordinary broadcast logger" do
    second_output = StringIO.new
    second = ActiveSupport::TaggedLogging.new(ActiveSupport::Logger.new(second_output))
    broadcast = ActiveSupport::BroadcastLogger.new(@logger, second)
    @subscriber.loggers.replace([broadcast])
    @subscriber.reset_logger
    @logger.tagged("caller") do
      DecentJob::Notifications.instrument("scheduler_shutdown_start.decent_job", process_id: 42)
      _(@logger.formatter.current_tags).must_equal ["caller"]
      _(second.formatter.current_tags).must_equal []
    end
    _(@output.string.lines).must_equal ["[caller] [DecentJob] [42] DecentJob shutting down scheduler...\n"]
    _(second_output.string.lines).must_equal ["[DecentJob] [42] DecentJob shutting down scheduler...\n"]
    _(broadcast.broadcasts).must_equal [@logger, second]
  end

  it "tags nested mixed destinations while preserving each level and restoring tags after failure" do
    second_output = StringIO.new
    second = ActiveSupport::TaggedLogging.new(ActiveSupport::Logger.new(second_output, level: Logger::WARN))
    plain_output = StringIO.new
    plain = Logger.new(plain_output, level: Logger::ERROR)
    inner = ActiveSupport::BroadcastLogger.new(second, plain)
    broadcast = ActiveSupport::BroadcastLogger.new(@logger, inner)
    @subscriber.loggers.replace([broadcast])
    @subscriber.reset_logger
    DecentJob::Notifications.instrument("scheduler_shutdown_start.decent_job", process_id: 42)
    _(@output.string.lines.size).must_equal 1
    _(second_output.string).must_equal ""
    _(plain_output.string).must_equal ""
    DecentJob::Notifications.instrument("scheduler_shutdown_kill.decent_job", process_id: 42)
    _(@output.string.lines.size).must_equal 2
    _(second_output.string.lines).must_equal ["[DecentJob] [42] DecentJob scheduler has been killed.\n"]
    _(plain_output.string).must_equal ""
    def second.warn(*) = raise("broken nested sink")
    def second.error(*) = raise("broken nested error sink")
    @logger.tagged("outer") do
      second.tagged("nested") do
        DecentJob::Notifications.instrument("scheduler_shutdown_kill.decent_job", process_id: 42)
        _(@logger.formatter.current_tags).must_equal ["outer"]
        _(second.formatter.current_tags).must_equal ["nested"]
      end
    end
    _(@logger.formatter.current_tags).must_equal []
    _(second.formatter.current_tags).must_equal []
    _(broadcast.broadcasts).must_equal [@logger, inner]
    _(inner.broadcasts).must_equal [second, plain]
    _(second.level).must_equal Logger::WARN
    _(plain.level).must_equal Logger::ERROR
  end

  it "tags shared leaf destinations once while preserving explicit duplicate broadcast edges" do
    inner = ActiveSupport::BroadcastLogger.new(@logger)
    broadcast = ActiveSupport::BroadcastLogger.new(@logger, inner)
    @subscriber.loggers.replace([broadcast])
    @subscriber.reset_logger
    DecentJob::Notifications.instrument("scheduler_shutdown_start.decent_job", process_id: 42)
    _(@output.string.lines).must_equal Array.new(2, "[DecentJob] [42] DecentJob shutting down scheduler...\n")
    _(@logger.formatter.current_tags).must_equal []
  end

  it "supports a tagged logger with a nil formatter" do
    @logger.formatter = nil
    DecentJob::Notifications.instrument("notifier_listen.decent_job")
    _(@output.string).must_include "Notifier subscribed with LISTEN"
  end

  it "restores tags and contains failing log destinations" do
    def @logger.info(*) = raise("broken sink")
    @logger.tagged("caller") do
      DecentJob::Notifications.instrument("notifier_listen.decent_job")
      _(@logger.formatter.current_tags).must_equal ["caller"]
    end
  end

  it "exposes a cached broadcast logger rebuilt on reset" do
    logger = @subscriber.logger
    _(logger).must_be_kind_of DecentJob::Logging::BroadcastLogger
    _(@subscriber.logger).must_be_same_as logger
    @subscriber.reset_logger
    _(@subscriber.logger).wont_be_same_as logger
  end

  it "keeps explicitly replaced or appended destinations through reconfiguration and reset" do
    DecentJob.logger = Logger.new(StringIO.new)
    DecentJob.configure { |config| config.logger = Logger.new(StringIO.new) }
    DecentJob.reset!
    _(@subscriber.loggers).must_equal [@logger]
    DecentJob::Notifications.instrument("notifier_listen.decent_job")
    _(@output.string).must_include "LISTEN"
  end
end
