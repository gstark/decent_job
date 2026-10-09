# frozen_string_literal: true

require "spec_helper"
require "support/jobs"

class BatchStoreCallbackJob < ActiveJob::Base
  class_attribute :events, default: Queue.new

  def perform(batch, options)
    self.class.events << [batch.id, options.fetch(:event), batch.properties]
  end
end

describe DecentJob::BatchStore do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @batches = DecentJob::BatchStore.new(database)
    @adapter = DecentJob::Adapter.new(database: database)
    [RecordingJob, FailingJob, BatchStoreCallbackJob].each { |job| job.queue_adapter = @adapter }
    RecordingJob.events = Queue.new
    BatchStoreCallbackJob.events = Queue.new
  end

  it "persists metadata without opening a batch" do
    row = @batches.save(id: nil, attributes: {description: "stored", properties: {"x" => 1}})
    _(row.fetch("id")).wont_be_nil
    _(row.fetch("enqueued_at")).must_be_nil
    _(@batches.find(row.fetch("id")).fetch("description")).must_equal "stored"
  end

  it "finishes an empty enqueued batch" do
    id = @batches.save(id: nil, attributes: {}).fetch("id")
    @batches.locked([id]) do
      @batches.open(id)
      @batches.mark_enqueued(id)
      @batches.reconcile(id)
    end
    row = @batches.find(id)
    _(row.fetch("generation")).must_equal 1
    _(row.fetch("jobs_finished_at")).must_be_kind_of Time
    _(row.fetch("finished_at")).must_be_kind_of Time
  end

  it "does not finish members added before the batch is enqueued" do
    id = @batches.save(id: nil, attributes: {}).fetch("id")
    job = RecordingJob.perform_later("member")
    @batches.attach(id, [job])
    DecentJob::Performer.new(database: database).perform_one
    _(@batches.find(id).fetch("jobs_finished_at")).must_be_nil
    @batches.mark_enqueued(id)
    @batches.reconcile(id)
    _(@batches.find(id).fetch("finished_at")).must_be_kind_of Time
  end

  it "records member completion and reconciles an enqueued batch" do
    id = @batches.save(id: nil, attributes: {}).fetch("id")
    job = RecordingJob.perform_later("member")
    @batches.attach(id, [job])
    @batches.mark_enqueued(id)
    DecentJob::Performer.new(database: database).perform_one
    _(@batches.find(id).fetch("finished_at")).must_be_kind_of Time
  end
end
