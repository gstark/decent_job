# frozen_string_literal: true

require "minitest/mock"
require "spec_helper"
require "stringio"
require "timeout"

module CapturedRuntimeLogging
  def before_setup
    super
    @destinations = DecentJob::LogSubscriber.loggers.dup
    @output = StringIO.new
    @logger = ActiveSupport::TaggedLogging.new(ActiveSupport::Logger.new(@output))
    DecentJob::LogSubscriber.loggers.replace([@logger])
    DecentJob::LogSubscriber.reset_logger
  end

  def after_teardown
    DecentJob::LogSubscriber.loggers.replace(@destinations)
    DecentJob::LogSubscriber.reset_logger
    super
  end
end

class RuntimeLoggingJob < ActiveJob::Base
  class_attribute :started

  def perform(block = false)
    return unless block
    started << true
    Queue.new.pop
  end
end

describe "database runtime logging" do
  include DatabaseSpec
  include CapturedRuntimeLogging

  before do
    DecentJob::Schema.new(database).migrate
    RuntimeLoggingJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
  end

  after do
    @worker&.shutdown(timeout: 5, force: true)
    DecentJob.reset!
  end

  it "logs real cleanup totals after deletion" do
    job = RuntimeLoggingJob.perform_later
    DecentJob::Performer.new(database: database).perform_one(job_id: job.provider_job_id)
    _(DecentJob.cleanup_preserved_jobs(database: database, older_than: 0)).must_equal 2
    _(@output.string).must_match(/\[DecentJob\] DecentJob destroyed 2 preserved job execution records finished before/)
    _(DecentJob::Store.new(database).find(job.provider_job_id)).must_be_nil
  end

  it "logs the real forced shutdown job identifier and releases its claim" do
    RuntimeLoggingJob.started = Queue.new
    job = RuntimeLoggingJob.perform_later(true)
    @worker = DecentJob::Worker.new(database: database, max_threads: 1, enable_listen_notify: false)
    @worker.start
    Timeout.timeout(5) { RuntimeLoggingJob.started.pop }
    @worker.shutdown(timeout: 0, force: true)
    _(@worker.shutdown(timeout: 5)).must_equal true
    _(@output.string).must_include "Active Jobs were interrupted: #{job.job_id}"
    claimed = DecentJob::Claim.new(database).with_next(job_id: job.provider_job_id) { |row| row }
    _(claimed).wont_be_nil
  end

  it "completes cleanup even when its logger and error reporting both fail" do
    def @logger.info(*) = raise("unavailable log")
    def @logger.error(*) = raise("unavailable error log")
    job = RuntimeLoggingJob.perform_later
    DecentJob::Performer.new(database: database).perform_one(job_id: job.provider_job_id)
    _(DecentJob.cleanup_preserved_jobs(database: database, older_than: 0)).must_equal 2
    _(DecentJob::Store.new(database).find(job.provider_job_id)).must_be_nil
    _(@logger.formatter.current_tags).must_equal []
  end
end

describe "process runtime logging" do
  include CapturedRuntimeLogging

  after { DecentJob.clear_lifecycle_hooks }

  it "logs supervisor start, spawn, reap and shutdown during real child cleanup" do
    config = DecentJob::Configuration.new
    config.subprocesses = 1
    config.shutdown_timeout = 0.1
    config.on_thread_error = ->(_) {}
    DecentJob.before_subprocess_boot { sleep 10 }
    supervisor = DecentJob::Supervisor.new(config)
    supervisor.start(on_start: -> { Process.kill("TERM", Process.pid) })
    _(@output.string).must_include "started supervisor with subprocesses=1"
    _(@output.string).must_include "supervisor forked subprocess"
    _(@output.string).must_include "supervisor reaped subprocess"
    _(@output.string).must_include "supervisor is shut down"
    _(supervisor.subprocesses).must_be_empty
  end

  it "logs watchdog start and callback exceptions while preserving service shutdown" do
    errors = Queue.new
    service = DecentJob::SystemdService.new(on_thread_error: ->(error) { errors << error })

    DecentJob::SdNotify.stub(:ready, nil) do
      DecentJob::SdNotify.stub(:stopping, nil) do
        DecentJob::SdNotify.stub(:watchdog_interval, 0.02) do
          DecentJob::SdNotify.stub(:watchdog, -> { raise IOError, "watchdog unavailable" }) do
            service.start
            Timeout.timeout(5) { errors.pop }
            service.stop
          end
        end
      end
    end

    _(@output.string).must_include "Pinging systemd watchdog every"
    _(@output.string).must_include "Error pinging systemd: IOError: watchdog unavailable"
  ensure
    service&.stop
  end
end
