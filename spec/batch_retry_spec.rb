# frozen_string_literal: true

require "spec_helper"
require "timeout"

class BatchManualRetryJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::Concurrency

  class_attribute :enqueue_events, default: []
  class_attribute :abort_retry, default: false
  class_attribute :retry_action

  before_enqueue do
    enqueue_events << [:before, executions]
    if executions.positive?
      throw :abort if abort_retry
      self.decent_job_labels = ["manual-retry"]
      self.arguments = ["retried", {attempt: executions}]
      self.queue_name = "manual"
      self.priority = 7
      retry_action&.call(self)
    end
  end
  after_enqueue { enqueue_events << [:after, executions] }

  def perform(value, *)
    raise "manual failure" if value == "fail"
  end
end

class BatchManualSuccessfulJob < ActiveJob::Base
  def perform
  end
end

class BatchRetryChildJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::Concurrency

  class_attribute :parent_id
  class_attribute :enqueue_events, default: []
  before_enqueue do
    enqueue_events << job_id
    connection = queue_adapter.database.connect
    connection.exec_params("SELECT id FROM decent_jobs WHERE id = $1 FOR UPDATE NOWAIT", [parent_id])
  ensure
    connection&.finish
  end

  def perform
  end
end

describe "manual batch retries" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)
    BatchManualRetryJob.queue_adapter = @adapter
    BatchManualSuccessfulJob.queue_adapter = @adapter
    BatchRetryChildJob.queue_adapter = @adapter
    BatchManualRetryJob.enqueue_events = []
    BatchManualRetryJob.abort_retry = false
    BatchManualRetryJob.retry_action = nil
    BatchManualRetryJob.decent_job_concurrency_config = {}
    BatchManualRetryJob.decent_job_concurrency_rules = []
    BatchRetryChildJob.enqueue_events = []
    BatchRetryChildJob.decent_job_concurrency_rules = []
    BatchRetryChildJob.decent_job_control_concurrency_with(key: "retry-child", enqueue_limit: 1)
    @store = DecentJob::Store.new(database)
    @batch_store = DecentJob::BatchStore.new(database)
    @performer = DecentJob::Performer.new(database: database)
  end

  it "runs retry enqueue callbacks once and persists their labels, arguments, queue, and priority" do
    batch = DecentJob::Batch.enqueue([BatchManualRetryJob.new("fail")], database: database)
    member = batch.job_instances.first
    _(@performer.perform_one.state).must_equal "discarded"
    BatchManualRetryJob.enqueue_events.clear
    batch.retry
    row = @store.find(member.provider_job_id)

    _(BatchManualRetryJob.enqueue_events).must_equal [[:before, 1], [:after, 1]]
    _(row.fetch("labels")).must_equal ["manual-retry"]
    _(row.fetch("queue_name")).must_equal "manual"
    _(row.fetch("priority")).must_equal 7
    _(row.fetch("payload").fetch("executions")).must_equal 1
    _(batch.job_instances.first.arguments).must_equal ["retried", {attempt: 1}]
    _(batch.job_instances.first.provider_job_id).must_equal member.provider_job_id
    _(@performer.perform_one.state).must_equal "succeeded"
    _(@store.executions(member.provider_job_id).map { |execution| execution.fetch("state") }).must_equal ["discarded", "succeeded"]
  end

  it "leaves an aborted retry discarded and excludes it from rescheduled identifiers" do
    batch = DecentJob::Batch.enqueue([BatchManualRetryJob.new("fail")], database: database)
    member = batch.job_instances.first
    @performer.perform_one
    original = @store.find(member.provider_job_id)
    BatchManualRetryJob.enqueue_events.clear
    BatchManualRetryJob.abort_retry = true
    retried = @batch_store.retry_members(batch.id)
    row = @store.find(member.provider_job_id)

    _(retried).must_equal []
    _(BatchManualRetryJob.enqueue_events).must_equal [[:before, 1]]
    _(row.fetch("state")).must_equal "discarded"
    _(row.fetch("payload")).must_equal original.fetch("payload")
    _(row.fetch("error")).must_equal original.fetch("error")
    _(@store.executions(member.provider_job_id).size).must_equal 1
  end

  it "wakes the async worker after a batch retry commits" do
    DecentJob.reset!
    DecentJob.configuration.max_threads = 1
    DecentJob.configuration.poll_interval = 30
    DecentJob.configuration.enable_listen_notify = false
    async = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    [BatchManualRetryJob, BatchManualSuccessfulJob].each { |klass| klass.queue_adapter = async }
    batch = DecentJob::Batch.enqueue([BatchManualRetryJob.new("fail")], database: database)
    member = batch.job_instances.first
    Timeout.timeout(5) { sleep 0.01 until @store.find(member.provider_job_id).fetch("state") == "discarded" }
    batch.retry
    Timeout.timeout(5) { sleep 0.01 until @store.find(member.provider_job_id).fetch("state") == "succeeded" }
  ensure
    async&.shutdown(timeout: 5)
    DecentJob.reset!
  end

  it "preserves successful members while retrying discarded members" do
    batch = DecentJob::Batch.enqueue([BatchManualRetryJob.new("fail"), BatchManualSuccessfulJob.new], database: database)
    2.times { @performer.perform_one }
    successful = batch.job_instances.find { |job| job.is_a?(BatchManualSuccessfulJob) }
    original = @store.find(successful.provider_job_id)
    retried = @batch_store.retry_members(batch.id)

    _(retried.size).must_equal 1
    _(retried).wont_include successful.provider_job_id
    _(@store.find(successful.provider_job_id)).must_equal original
    _(@store.executions(successful.provider_job_id).size).must_equal 1
  end

  [:external, :inline].each do |mode|
    it "retries #{mode} members without loading a successful member whose class was removed" do
      successful_class = Class.new(ActiveJob::Base) do
        def perform
        end
      end
      Object.const_set(:RemovedSuccessfulBatchMemberJob, successful_class)
      successful_class.queue_adapter = @adapter
      successful = successful_class.new
      discarded = BatchManualRetryJob.new("fail")
      batch = DecentJob::Batch.enqueue([discarded, successful], database: database)
      2.times { @performer.perform_one }
      original = @store.find(successful.provider_job_id)
      Object.send(:remove_const, :RemovedSuccessfulBatchMemberJob)
      BatchManualRetryJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: mode)
      batch.retry

      _(@store.find(discarded.provider_job_id).fetch("state")).must_equal((mode == :inline) ? "succeeded" : "ready")
      _(@store.find(successful.provider_job_id)).must_equal original
      _(@store.executions(successful.provider_job_id).size).must_equal 1
    ensure
      Object.send(:remove_const, :RemovedSuccessfulBatchMemberJob) if Object.const_defined?(:RemovedSuccessfulBatchMemberJob)
    end
  end

  it "does not load successful members when there are no discarded members to retry" do
    successful_class = Class.new(ActiveJob::Base) do
      def perform
      end
    end
    Object.const_set(:RemovedSuccessfulBatchMemberJob, successful_class)
    successful_class.queue_adapter = @adapter
    successful = successful_class.new
    batch = DecentJob::Batch.enqueue([successful], database: database)
    @performer.perform_one
    original = @store.find(successful.provider_job_id)
    Object.send(:remove_const, :RemovedSuccessfulBatchMemberJob)
    batch.retry

    _(batch.succeeded?).must_equal true
    _(@store.find(successful.provider_job_id)).must_equal original
  ensure
    Object.send(:remove_const, :RemovedSuccessfulBatchMemberJob) if Object.const_defined?(:RemovedSuccessfulBatchMemberJob)
  end

  it "instruments retry_job with the reconstructed previous error" do
    batch = DecentJob::Batch.enqueue([BatchManualRetryJob.new("fail")], database: database)
    @performer.perform_one
    errors = []
    subscriber = ->(_name, _start, _finish, _id, payload) { errors << payload[:error] }
    ActiveSupport::Notifications.subscribed(subscriber, "enqueue_retry.active_job") { batch.retry }

    _(errors.size).must_equal 1
    _(errors.first).must_be_kind_of RuntimeError
    _(errors.first.message).must_equal "manual failure"
  end

  it "retries when the original error class is no longer defined" do
    batch = DecentJob::Batch.enqueue([BatchManualRetryJob.new("fail")], database: database)
    @performer.perform_one
    member = batch.job_instances.first
    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET error = $2 WHERE id = $1", [member.provider_job_id, JSON.generate("class" => "RemovedBatchRetryError", "message" => "old failure")])
    end
    errors = []
    subscriber = ->(_name, _start, _finish, _id, payload) { errors << payload[:error] }
    ActiveSupport::Notifications.subscribed(subscriber, "enqueue_retry.active_job") { batch.retry }

    _(errors.first).must_be_kind_of StandardError
    _(errors.first.message).must_include "old failure"
    _(@store.find(member.provider_job_id).fetch("state")).must_equal "ready"
  end

  it "bypasses enqueue admission for the existing job and avoids enclosing bulk capture" do
    batch = DecentJob::Batch.enqueue([BatchManualRetryJob.new("fail")], database: database)
    member = batch.job_instances.first
    @performer.perform_one
    BatchManualRetryJob.decent_job_control_concurrency_with(total_limit: 0)
    captured = DecentJob::Bulk.capture { @batch_store.retry_members(batch.id) }

    _(captured).must_equal []
    _(@store.find(member.provider_job_id).fetch("state")).must_equal "ready"
    _(BatchManualRetryJob.enqueue_events.last).must_equal [:after, 1]
    _(DecentJob::Current.execution).must_be_nil
  end

  it "rejects a changed queue adapter without losing the discarded record" do
    batch = DecentJob::Batch.enqueue([BatchManualRetryJob.new("fail")], database: database)
    member = batch.job_instances.first
    @performer.perform_one
    BatchManualRetryJob.queue_adapter = :test

    _ { @batch_store.retry_members(batch.id) }.must_raise ArgumentError
    _(@store.find(member.provider_job_id).fetch("state")).must_equal "discarded"
    _(BatchManualRetryJob.queue_adapter.enqueued_jobs).must_equal []
  end

  it "keeps the reopened batch discarded when its member aborts retry" do
    batch = DecentJob::Batch.enqueue([BatchManualRetryJob.new("fail")], database: database)
    @performer.perform_one
    BatchManualRetryJob.abort_retry = true
    batch.retry

    _(batch.discarded?).must_equal true
    _(batch.succeeded?).must_equal false
  end

  it "prepares callback-created bulk children before taking parent row locks" do
    batch = DecentJob::Batch.enqueue([BatchManualRetryJob.new("fail")], database: database)
    member = batch.job_instances.first
    @performer.perform_one
    BatchRetryChildJob.parent_id = member.provider_job_id
    child = BatchRetryChildJob.new
    BatchManualRetryJob.retry_action = ->(_job) { ActiveJob.perform_all_later(child) }
    retried = @batch_store.retry_members(batch.id)

    _(retried).must_equal [member.provider_job_id]
    _(BatchRetryChildJob.enqueue_events).must_equal [child.job_id]
    _(child.provider_job_id).wont_be_nil
    _(@store.find(child.provider_job_id).fetch("batch_id")).must_be_nil
  end

  it "validates all captured children before modifying discarded members" do
    batch = DecentJob::Batch.enqueue([BatchManualRetryJob.new("fail")], database: database)
    member = batch.job_instances.first
    @performer.perform_one
    BatchManualSuccessfulJob.queue_adapter = :test
    BatchManualRetryJob.retry_action = ->(_job) { @adapter.enqueue_all([BatchManualSuccessfulJob.new]) }

    _ { @batch_store.retry_members(batch.id) }.must_raise ArgumentError
    _(@store.find(member.provider_job_id).fetch("state")).must_equal "discarded"
    _(BatchManualSuccessfulJob.queue_adapter.enqueued_jobs).must_equal []
  end

  it "rejects foreign callback child adapters before they can enqueue" do
    batch = DecentJob::Batch.enqueue([BatchManualRetryJob.new("fail")], database: database)
    member = batch.job_instances.first
    @performer.perform_one
    BatchManualSuccessfulJob.queue_adapter = :test
    BatchManualRetryJob.retry_action = ->(_job) { BatchManualSuccessfulJob.perform_later }

    _ { @batch_store.retry_members(batch.id) }.must_raise ArgumentError
    _(BatchManualSuccessfulJob.queue_adapter.enqueued_jobs).must_equal []
    _(@store.find(member.provider_job_id).fetch("state")).must_equal "discarded"
  end

  it "does not overwrite a member changed during callback preparation" do
    batch = DecentJob::Batch.enqueue([BatchManualRetryJob.new("fail")], database: database)
    member = batch.job_instances.first
    @performer.perform_one
    BatchManualRetryJob.retry_action = lambda do |_job|
      connection = database.connect
      connection.exec_params("UPDATE decent_jobs SET state = 'succeeded' WHERE id = $1", [member.provider_job_id])
    ensure
      connection&.finish
    end

    _(@batch_store.retry_members(batch.id)).must_equal []
    _(@store.find(member.provider_job_id).fetch("state")).must_equal "succeeded"
    _(@store.find(member.provider_job_id).fetch("queue_name")).must_equal "default"
  end
end
