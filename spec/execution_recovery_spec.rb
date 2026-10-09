# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

class RecoveryFatalError < ScriptError; end

class RecoveryErrorJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::Labels

  class_attribute :events, default: []
  before_enqueue { events << :enqueue }
  after_perform { events << :after_perform }

  def perform(error_class = "RuntimeError")
    events << [job_id, provider_job_id, executions]
    raise error_class.constantize, "escaping error"
  end
end

class RecoveryReenqueueJob < ActiveJob::Base
  def perform
    retry_job(wait: 300, queue: "later", priority: 17)
    raise "after reenqueue"
  end
end

class RecoveryBlockingErrorJob < RecoveryErrorJob
  include DecentJob::ActiveJobExtensions::Concurrency

  decent_job_control_concurrency_with(key: "backend-retry", perform_limit: 1)
  class_attribute :started, default: Queue.new
  class_attribute :release, default: Queue.new

  def perform
    started << true
    release.pop
    super
  end
end

class RecoveryBatchCallbackJob < ActiveJob::Base
  def perform(_batch, _options)
    raise "premature batch callback"
  end
end

describe "execution failure configuration" do
  it "defaults to standard and not implemented errors with backend retry disabled" do
    config = DecentJob::Configuration.new
    _(config.respond_to?(:handled_exception_classes)).must_equal true
    _(config.handled_exception_classes).must_equal [StandardError, NotImplementedError]
    _(config.retry_on_unhandled_error).must_equal false
  end

  it "resolves configured class names and permits exceptions outside StandardError" do
    config = DecentJob::Configuration.new
    _(config.respond_to?(:handled_exceptions=)).must_equal true
    config.handled_exceptions = ["RuntimeError", RecoveryFatalError]
    _(config.validate!.handled_exception_classes).must_equal [RuntimeError, RecoveryFatalError]
    config.handled_exceptions = []
    _(config.validate!.handled_exception_classes).must_equal []
  end

  it "defers exception class-name resolution until execution after application boot" do
    config = DecentJob::Configuration.new
    config.handled_exceptions = ["RecoveryLateDefinedError"]
    _(config.validate!).must_equal config
    Object.const_set(:RecoveryLateDefinedError, Class.new(ScriptError))
    _(config.handled_exception_classes).must_equal [RecoveryLateDefinedError]
  ensure
    Object.send(:remove_const, :RecoveryLateDefinedError) if defined?(RecoveryLateDefinedError)
  end

  it "rejects invalid exception lists and nonboolean retry policies" do
    config = DecentJob::Configuration.new
    _(config.respond_to?(:handled_exceptions=)).must_equal true
    [nil, "StandardError", [Object], [Object.new], [""]].each do |invalid|
      _ {
        config.handled_exceptions = invalid
        config.validate!
      }.must_raise ArgumentError
    end
    [["String"], ["MissingRecoveryError"]].each do |invalid|
      config.handled_exceptions = invalid
      _ { config.handled_exception_classes }.must_raise ArgumentError
    end
    config.handled_exceptions = [StandardError]
    [nil, "true", 1].each do |invalid|
      _ {
        config.retry_on_unhandled_error = invalid
        config.validate!
      }.must_raise ArgumentError
    end
  end
end

describe "execution recovery" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)
    [RecoveryErrorJob, RecoveryReenqueueJob, RecoveryBlockingErrorJob, RecoveryBatchCallbackJob, RecordingJob, RetryingJob, DiscardingJob].each { |job| job.queue_adapter = @adapter }
    RecoveryErrorJob.events = []
    RecoveryBlockingErrorJob.started = Queue.new
    RecoveryBlockingErrorJob.release = Queue.new
    @threads = []
    RecordingJob.events = Queue.new
    @store = DecentJob::Store.new(database)
    @performer = DecentJob::Performer.new(database: database)
  end

  after do
    10.times { RecoveryBlockingErrorJob.release << true }
    @threads.each(&:value)
    DecentJob.reset!
  end

  it "persists NotImplementedError by default and exposes escaping failure metadata" do
    job = RecoveryErrorJob.perform_later("NotImplementedError")
    result = @performer.perform_one
    _(result.state).must_equal "discarded"
    _(result.unhandled_error).must_be_kind_of NotImplementedError
    _(result.error_event).must_equal "unhandled"
    _(@store.find(job.provider_job_id).fetch("error").fetch("class")).must_equal "NotImplementedError"
  end

  it "catches a configured exception class name outside StandardError" do
    DecentJob.configuration.handled_exceptions = ["RecoveryFatalError"]
    RecoveryErrorJob.perform_later("RecoveryFatalError")
    result = @performer.perform_one
    _(result.unhandled_error).must_be_kind_of RecoveryFatalError
    _(result.state).must_equal "discarded"
  end

  it "propagates excluded exceptions leaving one abandoned attempt and releases its session lock" do
    DecentJob.configuration.handled_exceptions = []
    job = RecoveryErrorJob.perform_later
    _ { @performer.perform_one }.must_raise RuntimeError
    row = @store.find(job.provider_job_id)
    _(row.fetch("state")).must_equal "running"
    _(@store.executions(job.provider_job_id).first.fetch("finished_at")).must_be_nil
    other = DecentJob::Database.new(url: @connection_options, pool_size: 1)
    DecentJob.configuration.handled_exceptions = [StandardError]
    _(DecentJob::Performer.new(database: other).perform_one.state).must_equal "discarded"
    _(@store.executions(job.provider_job_id).map { |attempt| attempt.fetch("state") }).must_equal ["interrupted", "discarded"]
  ensure
    other&.close
  end

  it "retries unhandled failures without enqueue callbacks or changing serialized Active Job state" do
    DecentJob.configuration.retry_on_unhandled_error = true
    job = RecoveryErrorJob.new
    job.decent_job_labels = ["recovery"]
    job.queue_name = "critical"
    job.priority = -7
    job.enqueue
    original = @store.find(job.provider_job_id)
    result = @performer.perform_one
    _(result.state).must_equal "retried"
    _(result.unhandled_error).must_be_kind_of RuntimeError
    row = @store.find(job.provider_job_id)
    %w[id active_job_id payload queue_name priority labels scheduled_at cron_key cron_at batch_id batch_callback_id].each do |key|
      _(row[key]).must_equal original[key] unless original[key].nil?
      _(row[key]).must_be_nil if original[key].nil?
    end
    _(row.fetch("state")).must_equal "ready"
    _(row.fetch("finished_at")).must_be_nil
    _(row.fetch("scheduled_at")).must_be :<=, Time.now
    _(row.fetch("error_event")).must_equal "unhandled"
    _(row.fetch("error").fetch("message")).must_equal "escaping error"
    @performer.perform_one
    _(RecoveryErrorJob.events).must_equal [:enqueue, [job.job_id, job.provider_job_id, 1], [job.job_id, job.provider_job_id, 1]]
    _(@store.executions(job.provider_job_id).map { |attempt| attempt.fetch("state") }).must_equal ["retried", "retried"]
  end

  it "retains backend retries with preservation disabled and leaves batches unfinished" do
    DecentJob.configuration.retry_on_unhandled_error = true
    DecentJob.configuration.preserve_job_records = false
    job = RecoveryErrorJob.new
    batch = DecentJob::Batch.enqueue([job], database: database, on_finish: RecoveryBatchCallbackJob, on_discard: RecoveryBatchCallbackJob)
    @performer.perform_one
    _(@store.find(job.provider_job_id).fetch("state")).must_equal "ready"
    _(batch.reload.finished?).must_equal false
    _(batch.discarded?).must_equal false
    database.with_connection do |connection|
      _(connection.exec("SELECT count(*) FROM decent_job_batch_events").getvalue(0, 0)).must_equal "0"
    end
  end

  it "retains nonempty cron and batch associations without evaluating a terminal preservation predicate" do
    DecentJob.configuration.retry_on_unhandled_error = true
    DecentJob.configuration.preserve_job_records = ->(*) { flunk "unfinished job reached preservation predicate" }
    job = RecoveryErrorJob.new
    batch = DecentJob::CronEntry.with_context(database: database, key: "recovery-cron", at: Time.now) do
      DecentJob::Batch.enqueue([job], database: database)
    end
    original = @store.find(job.provider_job_id)
    _(original.fetch("cron_key")).must_equal "recovery-cron"
    _(original.fetch("batch_id")).must_equal batch.id
    @performer.perform_one
    row = @store.find(job.provider_job_id)
    %w[cron_key cron_at batch_id payload].each { |key| _(row.fetch(key)).must_equal original.fetch(key) }
    _(batch.reload.finished?).must_equal false
  end

  it "keeps a failed batch callback associated and leaves its batch unfinished while retrying" do
    DecentJob.configuration.retry_on_unhandled_error = true
    DecentJob.configuration.preserve_job_records = false
    job = RecordingJob.new("finished member")
    batch = DecentJob::Batch.enqueue([job], database: database, on_finish: RecoveryBatchCallbackJob)
    @performer.perform_one(job_id: job.provider_job_id)
    result = @performer.perform_one
    _(result.state).must_equal "retried"
    row = @store.find(result.job_id)
    _(row.fetch("batch_callback_id")).must_equal batch.id
    _(row.fetch("finished_at")).must_be_nil
    _(batch.reload.finished?).must_equal false
    _(batch.discarded?).must_equal false
  end

  it "releases concurrency grants and promotes waiting jobs before retaining a backend retry" do
    DecentJob.configuration.retry_on_unhandled_error = true
    DecentJob.configuration.preserve_job_records = false
    first = RecoveryBlockingErrorJob.perform_later
    second = RecoveryBlockingErrorJob.perform_later
    @threads << Thread.new { @performer.perform_one(job_id: first.provider_job_id) }
    Timeout.timeout(5) { RecoveryBlockingErrorJob.started.pop }
    _(@performer.perform_one(job_id: second.provider_job_id).state).must_equal "retried"
    _(@store.find(second.provider_job_id).fetch("scheduled_at")).must_be :>, Time.now
    RecoveryBlockingErrorJob.release << true
    _(@threads.last.value.state).must_equal "retried"
    _(@store.find(first.provider_job_id).fetch("state")).must_equal "ready"
    _(@store.find(second.provider_job_id).fetch("scheduled_at")).must_be :<=, Time.now
    database.with_connection do |connection|
      _(connection.exec("SELECT count(*) FROM decent_job_concurrency_claims WHERE state = 'granted'").getvalue(0, 0)).must_equal "0"
    end
    RecoveryBlockingErrorJob.release << true
    _(@performer.perform_one(job_id: second.provider_job_id).state).must_equal "retried"
    database.with_connection do |connection|
      _(connection.exec("SELECT count(*) FROM decent_job_concurrency_claims").getvalue(0, 0)).must_equal "0"
    end
  end

  it "never resurrects an operator force-discard when the owner later raises" do
    DecentJob.configuration.retry_on_unhandled_error = true
    DecentJob.configuration.preserve_job_records = false
    job = RecoveryBlockingErrorJob.perform_later
    @threads << Thread.new { @performer.perform_one(job_id: job.provider_job_id) }
    Timeout.timeout(5) { RecoveryBlockingErrorJob.started.pop }
    DecentJob::Job.find(job.provider_job_id, database: database).force_discard_job("operator stopped")
    RecoveryBlockingErrorJob.release << true
    result = @threads.last.value
    _(result.state).must_equal "discarded"
    _(result.error_event).must_equal "discarded"
    _(result.error).must_be_kind_of DecentJob::Job::DiscardJobError
    _(result.unhandled_error).must_be_kind_of RuntimeError
    row = @store.find(job.provider_job_id)
    _(row.fetch("state")).must_equal "discarded"
    _(row.fetch("finished_at")).wont_be_nil
    _(row.fetch("error").fetch("message")).must_equal "operator stopped"
    _(@store.executions(job.provider_job_id).first.fetch("error").fetch("message")).must_equal "operator stopped"
    _(@performer.perform_one).must_be_nil
  end

  it "rolls back retry state, attempt completion, grant release, and batch requests atomically" do
    DecentJob.configuration.retry_on_unhandled_error = true
    DecentJob.configuration.preserve_job_records = false
    job = RecoveryBlockingErrorJob.new
    batch = DecentJob::Batch.enqueue([job], database: database)
    database.with_connection do |connection|
      connection.exec(<<~SQL)
        CREATE FUNCTION reject_retry_finish() RETURNS trigger LANGUAGE plpgsql AS $$
          BEGIN RAISE EXCEPTION 'finish failed'; END;
        $$;
        CREATE TRIGGER reject_retry_finish BEFORE UPDATE ON decent_job_executions
          FOR EACH ROW WHEN (NEW.finished_at IS NOT NULL) EXECUTE FUNCTION reject_retry_finish();
      SQL
    end
    RecoveryBlockingErrorJob.release << true
    _ { @performer.perform_one }.must_raise PG::RaiseException
    row = @store.find(job.provider_job_id)
    _(row.fetch("state")).must_equal "running"
    _(row.fetch("finished_at")).must_be_nil
    _(@store.executions(job.provider_job_id).first.fetch("finished_at")).must_be_nil
    _(batch.reload.finished?).must_equal false
    database.with_connection do |connection|
      _(connection.exec("SELECT count(*) FROM decent_job_concurrency_claims WHERE state = 'granted'").getvalue(0, 0)).must_equal "1"
      _(connection.exec("SELECT count(*) FROM decent_job_batch_updates").getvalue(0, 0)).must_equal "0"
    end
  end

  it "preserves an explicit reenqueue schedule and payload when the job subsequently raises" do
    DecentJob.configuration.retry_on_unhandled_error = true
    job = RecoveryReenqueueJob.perform_later
    result = @performer.perform_one
    _(result.state).must_equal "retried"
    _(result.unhandled_error).must_be_kind_of RuntimeError
    row = @store.find(job.provider_job_id)
    _(row.fetch("queue_name")).must_equal "later"
    _(row.fetch("priority")).must_equal 17
    _(row.fetch("scheduled_at")).must_be :>, Time.now + 250
    _(row.fetch("payload").fetch("executions")).must_equal 1
    _(@performer.perform_one).must_be_nil
  end

  it "raises inline errors after persisting exactly one retry" do
    DecentJob.configuration.retry_on_unhandled_error = true
    RecoveryErrorJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :inline)
    job = RecoveryErrorJob.new
    _ { job.enqueue }.must_raise RuntimeError
    _(@store.find(job.provider_job_id).fetch("state")).must_equal "ready"
    _(@store.executions(job.provider_job_id).size).must_equal 1
    _(DecentJob::Current.execution).must_be_nil
    _(Thread.current[:decent_job_inline_buffer]).must_be_nil
  end

  it "does not label Active Job handled retries or discards as unhandled" do
    RetryingJob.perform_later("ok")
    result = @performer.perform_one
    _(result.unhandled_error).must_be_nil
    _(result.error_event).must_equal "retried"
    @performer.perform_one
    DiscardingJob.perform_later
    result = @performer.perform_one
    _(result.unhandled_error).must_be_nil
    _(result.error_event).must_equal "discarded"
  end
end
