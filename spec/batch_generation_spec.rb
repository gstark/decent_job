# frozen_string_literal: true

require "spec_helper"
require "timeout"

class BatchGenerationMemberJob < ActiveJob::Base
  class_attribute :started, :release

  def perform(*)
    self.class.started << true
    self.class.release.pop
    raise "failed during reopen"
  end
end

class BatchGenerationCallbackJob < ActiveJob::Base
  def perform(*)
  end
end

class BatchGenerationFailingCallbackJob < BatchGenerationMemberJob
end

describe "batch reopening during execution" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    adapter = DecentJob::Adapter.new(database: database)
    [BatchGenerationMemberJob, BatchGenerationCallbackJob, BatchGenerationFailingCallbackJob].each { |klass| klass.queue_adapter = adapter }
    BatchGenerationMemberJob.started = Queue.new
    BatchGenerationMemberJob.release = Queue.new
    @threads = []
    @reopen_release = Queue.new
  end

  after do
    @reopen_release << true
    BatchGenerationMemberJob.release << true
    @threads.each(&:value)
  end

  def fail_while_reopening(batch, job, operation: :enqueue, gate: :mark_enqueued)
    @threads << Thread.new { DecentJob::Performer.new(database: database).perform_one(job_id: job.provider_job_id) }
    Timeout.timeout(5) { BatchGenerationMemberJob.started.pop }
    entered = Queue.new
    release = @reopen_release
    batch.send(:store).define_singleton_method(gate) do |id|
      super(id)
      entered << true
      release.pop
    end
    @threads << Thread.new { batch.public_send(operation) }
    Timeout.timeout(5) { entered.pop }
    BatchGenerationMemberJob.release << true
    store = DecentJob::Store.new(database)
    Timeout.timeout(5) { sleep 0.001 until store.find(job.provider_job_id).fetch("state") == "discarded" }
    @reopen_release << true
    Timeout.timeout(5) { @threads.each(&:value) }
  end

  it "counts a member failure committed while reopening is uncommitted" do
    member = BatchGenerationMemberJob.new
    batch = DecentJob::Batch.enqueue([member], database: database,
      on_success: BatchGenerationCallbackJob, on_discard: BatchGenerationCallbackJob)
    fail_while_reopening(batch, member)

    _(batch.reload.discarded?).must_equal true
    _(batch.callback_job_instances.map { |job| job.arguments.last.fetch(:event) }).must_equal [:discard]
  end

  it "counts a callback failure committed while reopening is uncommitted" do
    batch = DecentJob::Batch.enqueue(database: database,
      on_finish: BatchGenerationFailingCallbackJob, on_discard: BatchGenerationCallbackJob)
    callback = batch.callback_job_instances.first
    batch.on_finish = nil
    batch.on_success = BatchGenerationCallbackJob
    fail_while_reopening(batch, callback)

    _(batch.reload.discarded?).must_equal true
    _(batch.finished?).must_equal false
    _(batch.callback_job_instances.map { |job| job.arguments.last.fetch(:event) }.sort).must_equal [:discard, :finish]
  end

  it "clears a concurrent failure if that member is then manually retried" do
    member = BatchGenerationMemberJob.new
    batch = DecentJob::Batch.enqueue([member], database: database, on_discard: BatchGenerationCallbackJob)
    fail_while_reopening(batch, member, operation: :retry, gate: :open)

    _(DecentJob::Store.new(database).find(member.provider_job_id).fetch("state")).must_equal "ready"
    _(batch.reload.discarded?).must_equal false
    _(batch.callback_job_instances).must_be_empty
  end

  it "ignores an earlier completed failure whose completion request is still pending" do
    member = BatchGenerationMemberJob.new
    batch = DecentJob::Batch.enqueue([member], database: database,
      on_success: BatchGenerationCallbackJob, on_discard: BatchGenerationCallbackJob)
    BatchGenerationMemberJob.release << true
    DecentJob::Performer.new(database: database).perform_one(job_id: member.provider_job_id)
    database.transaction do |connection|
      DecentJob::BatchStore.new(database).request(connection, [batch.id], job_id: member.provider_job_id, discarded: true)
    end
    batch.enqueue

    _(batch.reload.discarded?).must_equal false
    _(batch.callback_job_instances.map { |job| job.arguments.last.fetch(:event) }.sort).must_equal [:discard, :success]
  end
end
