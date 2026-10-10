# frozen_string_literal: true

require "spec_helper"

class BatchUnavailableArgumentsJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::Concurrency

  class_attribute :enqueue_events, default: []
  class_attribute :abort_retry, default: false

  before_enqueue do
    enqueue_events << [executions, I18n.locale]
    throw :abort if executions.positive? && abort_retry
    self.queue_name = "retried" if executions.positive?
  end

  after_enqueue { enqueue_events << :after }

  discard_on ActiveJob::DeserializationError

  def perform(*)
    raise "initial failure"
  end
end

# Reference: GoodJob ad4be5c2, spec/app/models/good_job/job_spec.rb:222.
# The batch composition is source-derived from Batch#retry delegating to
# Job#retry_job in app/models/good_job/batch.rb:313.
describe "batch retries with unavailable serialized arguments" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    BatchUnavailableArgumentsJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    BatchUnavailableArgumentsJob.enqueue_events = []
    BatchUnavailableArgumentsJob.abort_retry = false
    BatchUnavailableArgumentsJob.decent_job_control_concurrency_with(key: nil, total_limit: 1)
    @store = DecentJob::Store.new(database)
    @performer = DecentJob::Performer.new(database: database)
  end

  after do
    BatchUnavailableArgumentsJob.decent_job_concurrency_config = {}
  end

  {
    serializer: [{"_aj_serialized" => "RemovedBatchArgumentSerializer"}],
    global_id: [{"_aj_globalid" => "gid://decent-job/RemovedBatchArgument/1"}]
  }.each do |kind, arguments|
    [:external, :inline].each do |mode|
      it "retries #{mode} members with an unavailable #{kind} under their original identity" do
        member = BatchUnavailableArgumentsJob.new("original")
        batch = DecentJob::Batch.enqueue([member], database: database)
        _(@performer.perform_one.state).must_equal "discarded"
        original_history = @store.executions(member.provider_job_id)

        database.with_connection do |connection|
          connection.exec_params("UPDATE decent_jobs SET payload = jsonb_set(payload, '{arguments}', $2::jsonb) WHERE id = $1", [member.provider_job_id, JSON.generate(arguments)])
        end

        BatchUnavailableArgumentsJob.enqueue_events = []
        BatchUnavailableArgumentsJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: mode)
        batch.retry
        row = @store.find(member.provider_job_id)
        _(row.fetch("active_job_id")).must_equal member.job_id
        _(row.fetch("batch_id")).must_equal batch.id
        _(row.fetch("payload").fetch("arguments")).must_equal arguments
        _(row.fetch("queue_name")).must_equal "retried"
        _(BatchUnavailableArgumentsJob.enqueue_events).must_equal [[1, I18n.locale], :after]
        _(@store.executions(member.provider_job_id).first).must_equal original_history.first

        if mode == :external
          _(row.fetch("state")).must_equal "ready"
          _(row.fetch("payload").fetch("executions")).must_equal 1
          _(@performer.perform_one.state).must_equal "discarded"
        else
          _(row.fetch("state")).must_equal "discarded"
        end

        _(batch.reload.finished?).must_equal true
        _(@store.executions(member.provider_job_id).size).must_equal 2
      end
    end
  end

  it "leaves unavailable arguments discarded when enqueue callbacks abort retry" do
    member = BatchUnavailableArgumentsJob.new("original")
    batch = DecentJob::Batch.enqueue([member], database: database)
    @performer.perform_one

    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET payload = jsonb_set(payload, '{arguments}', '[{\"_aj_serialized\":\"RemovedBatchArgumentSerializer\"}]') WHERE id = $1", [member.provider_job_id])
    end

    before = @store.find(member.provider_job_id)
    BatchUnavailableArgumentsJob.abort_retry = true
    BatchUnavailableArgumentsJob.enqueue_events = []
    batch.retry
    _(BatchUnavailableArgumentsJob.enqueue_events).must_equal [[1, I18n.locale]]
    _(@store.find(member.provider_job_id)).must_equal before
    _(batch.discarded?).must_equal true
    _(@store.executions(member.provider_job_id).size).must_equal 1
  end
end
