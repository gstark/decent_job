# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "decent_job/active_job_extensions/batches"
require "decent_job/batch"

class BatchFacadeJob < RecordingJob
  include DecentJob::ActiveJobExtensions::Batches
end

class ControlledBatchFacadeJob < BatchFacadeJob
  include DecentJob::ActiveJobExtensions::Concurrency

  class_attribute :enqueue_events, default: []
  before_enqueue { enqueue_events << job_id }
end

class BatchFacadeContextJob < BatchFacadeJob
  def perform(batch_id = nil)
    DecentJob::Batch.find(batch_id).add(self) if batch_id
    events << batch&.id
  end
end

class BatchFacadeRetryJob < BatchFacadeJob
  class_attribute :fail_execution, default: true

  def perform
    raise "retry batch member" if fail_execution
    events << "retried"
  end
end

class BatchFacadeUnlockedCallback < ActiveJob::Base
  class_attribute :member_id
  before_enqueue do
    connection = queue_adapter.database.connect
    connection.exec_params("SELECT id FROM decent_jobs WHERE id = $1 FOR UPDATE NOWAIT", [member_id])
  ensure
    connection&.finish
  end

  def perform(*)
  end
end

class BatchFacadeBulkObserverJob < BatchFacadeJob
  def perform(value)
    counts = DecentJob::Current.execution.fetch(:database).with_connection do |connection|
      connection.exec("SELECT (SELECT count(*) FROM decent_job_batches) AS batches, (SELECT count(*) FROM decent_jobs) AS jobs").first
    end
    events << [value, counts]
  end
end

describe DecentJob::Batch do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)
    BatchFacadeJob.queue_adapter = @adapter
    BatchFacadeRetryJob.queue_adapter = @adapter
    BatchFacadeUnlockedCallback.queue_adapter = @adapter
    BatchFacadeJob.events = Queue.new
    ControlledBatchFacadeJob.decent_job_concurrency_config = {}
    ControlledBatchFacadeJob.decent_job_concurrency_rules = []
    ControlledBatchFacadeJob.enqueue_events = []
    BatchFacadeRetryJob.fail_execution = true
  end

  it "saves configuration separately from arbitrary typed properties" do
    batch = DecentJob::Batch.new(database: database, description: "Import", properties: {"source" => "file"}, account: :example, date: Date.new(2026, 10, 9))
    _(batch.persisted?).must_equal false
    _(batch.save).must_equal true
    restored = DecentJob::Batch.find(batch.id, database: database)

    _(restored.description).must_equal "Import"
    _(restored.properties).must_equal({"source" => "file", :account => :example, :date => Date.new(2026, 10, 9)})
    _(restored.created_at).must_be_kind_of Time
    _(restored.enqueued?).must_equal false
  end

  it "rejects non-hash properties on assignment" do
    batch = DecentJob::Batch.new(database: database)

    _ { batch.properties = [] }.must_raise ArgumentError
    _ { DecentJob::Batch.new(database: database, properties: "invalid") }.must_raise ArgumentError
  end

  it "enqueues explicit and block members into the same batch" do
    explicit = BatchFacadeJob.new("explicit")
    batch = DecentJob::Batch.new(database: database)
    accepted = batch.enqueue([explicit]) { BatchFacadeJob.perform_later("captured") }

    _(accepted.map { |job| job.arguments.first }).must_equal ["explicit", "captured"]
    _(batch.enqueued?).must_equal true
    _(batch.finished?).must_equal false
    _(batch.active_jobs.map(&:provider_job_id).sort).must_equal accepted.map(&:provider_job_id).sort
    _(batch.callback_active_jobs).must_equal []
  end

  it "returns the batch from the class enqueue helper" do
    batch = DecentJob::Batch.enqueue([BatchFacadeJob.new("member")], database: database, description: "Class helper")

    _(batch).must_be_kind_of DecentJob::Batch
    _(batch.description).must_equal "Class helper"
    _(batch.active_jobs.size).must_equal 1
  end

  it "adds jobs without marking the batch enqueued" do
    batch = DecentJob::Batch.new(database: database)
    accepted = batch.add([BatchFacadeJob.new("member")])

    _(accepted.size).must_equal 1
    _(batch.persisted?).must_equal true
    _(batch.enqueued?).must_equal false
    _(batch.finished?).must_equal false
  end

  it "attaches existing jobs without inserting duplicates" do
    job = BatchFacadeJob.perform_later("existing")
    batch = DecentJob::Batch.new(database: database)
    batch.add([job])

    _(batch.active_jobs.map(&:provider_job_id)).must_equal [job.provider_job_id]
    database.with_connection { |connection| _(connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)).must_equal "1" }
  end

  it "flushes its members independently of an enclosing bulk capture" do
    batch = DecentJob::Batch.new(database: database)
    outer = DecentJob::Bulk.enqueue do
      batch.enqueue { BatchFacadeJob.perform_later("member") }
      _(batch.active_jobs.size).must_equal 1
      BatchFacadeJob.perform_later("outside")
    end

    _(outer.map { |job| job.arguments.first }).must_equal ["outside"]
    _(batch.active_jobs.map { |job| job.arguments.first }).must_equal ["member"]
  end

  it "prepares controlled member callbacks once before admission" do
    ControlledBatchFacadeJob.decent_job_control_concurrency_with(total_limit: 1)
    jobs = [ControlledBatchFacadeJob.new("first"), ControlledBatchFacadeJob.new("second")]
    batch = DecentJob::Batch.new(database: database)

    _(batch.enqueue(jobs)).must_equal [jobs.first]
    _(ControlledBatchFacadeJob.enqueue_events).must_equal jobs.map(&:job_id)
    _(batch.active_jobs.size).must_equal 1
  end

  it "rejects members from another adapter before saving the batch" do
    BatchFacadeJob.queue_adapter = :test
    batch = DecentJob::Batch.new(database: database)

    _ { batch.enqueue([BatchFacadeJob.new("invalid")]) }.must_raise ArgumentError
    _(batch.persisted?).must_equal false
    _(BatchFacadeJob.queue_adapter.enqueued_jobs).must_equal []
  end

  it "rejects members from another database before any insertion" do
    other_database = DecentJob::Database.new(url: @connection_options)
    BatchFacadeJob.queue_adapter = DecentJob::Adapter.new(database: other_database)
    batch = DecentJob::Batch.new(database: database)

    _ { batch.enqueue([BatchFacadeJob.new("invalid")]) }.must_raise ArgumentError
    _(batch.persisted?).must_equal false
  ensure
    other_database&.close
  end

  it "rejects foreign adapters used inside a capture block" do
    BatchFacadeJob.queue_adapter = :test
    batch = DecentJob::Batch.new(database: database)

    _ { batch.enqueue { BatchFacadeJob.perform_later("invalid") } }.must_raise ArgumentError
    _(BatchFacadeJob.queue_adapter.enqueued_jobs).must_equal []
    _(batch.persisted?).must_equal false
  end

  it "restores context before after-commit callbacks execute" do
    contexts = []
    DecentJob::Batch.within_thread(batch_id: "temporary", database: database) do
      _(DecentJob::Batch.context_for(database).fetch(:batch_id)).must_equal "temporary"
      database.after_commit { contexts << DecentJob::Batch.context_for(database) }
    end

    _(contexts).must_equal [nil]
    _(DecentJob::Batch.context_for(database)).must_be_nil
  end

  it "finds batches through GlobalID using the current database" do
    batch = DecentJob::Batch.new(database: database, account: 42)
    batch.save
    gid = batch.to_global_id

    _(gid.app).must_equal "decent-job"
    DecentJob::Current.with(database: database) do
      _(GlobalID::Locator.locate(gid).id).must_equal batch.id
      _(GlobalID::Locator.locate_many([gid], ignore_missing: true).map(&:id)).must_equal [batch.id]
    end
  end

  it "enqueues multiple new batches in one transaction" do
    first = DecentJob::Batch.new(database: database)
    second = DecentJob::Batch.new(database: database)
    result = DecentJob::Batch.enqueue_all([[first, [BatchFacadeJob.new("first")]], [second, [BatchFacadeJob.new("second")]]])

    _(result).must_equal [first, second]
    _(first.active_jobs.map { |job| job.arguments.first }).must_equal ["first"]
    _(second.active_jobs.map { |job| job.arguments.first }).must_equal ["second"]
  end

  it "inserts batch records and ordinary members with one statement each" do
    pairs = 3.times.map do |index|
      [DecentJob::Batch.new(database: database, sequence: index), [BatchFacadeJob.new("member-#{index}")]]
    end
    inserts = []
    database.with_connection do |connection|
      recorder = Module.new do
        define_method(:exec_params) do |sql, *parameters, &block|
          inserts << sql if sql.match?(/\A\s*INSERT INTO (decent_job_batches|decent_jobs)\b/)
          super(sql, *parameters, &block)
        end
      end
      connection.singleton_class.prepend(recorder)
      DecentJob::Batch.enqueue_all(pairs)
    end

    _(inserts.count { |sql| sql.match?(/INSERT INTO decent_job_batches\b/) }).must_equal 1
    _(inserts.count { |sql| sql.match?(/INSERT INTO decent_jobs\b/) }).must_equal 1
    _(pairs.map { |batch, _| batch.properties.fetch(:sequence) }).must_equal [0, 1, 2]
    _(pairs.map { |batch, _| batch.active_jobs.first.arguments.first }).must_equal ["member-0", "member-1", "member-2"]
  end

  it "rejects a member assigned to more than one batch before writing records" do
    first = DecentJob::Batch.new(database: database)
    second = DecentJob::Batch.new(database: database)
    member = BatchFacadeJob.new("ambiguous")

    _ { DecentJob::Batch.enqueue_all([[first, [member]], [second, [member]]]) }.must_raise ArgumentError
    _(first.persisted?).must_equal false
    _(second.persisted?).must_equal false
    database.with_connection { |connection| _(connection.exec("SELECT count(*) FROM decent_job_batches").getvalue(0, 0)).must_equal "0" }
  end

  it "persists all bulk members before the first inline execution" do
    BatchFacadeJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :inline)
    pairs = 2.times.map do |index|
      [DecentJob::Batch.new(database: database), [BatchFacadeBulkObserverJob.new(index)]]
    end
    DecentJob::Batch.enqueue_all(pairs)
    observations = 2.times.map { BatchFacadeJob.events.pop }

    _(observations.map(&:first).sort).must_equal [0, 1]
    _(observations.map(&:last)).must_equal Array.new(2) { {"batches" => "2", "jobs" => "2"} }
    _(pairs.all? { |batch, _| batch.succeeded? }).must_equal true
  end

  it "combines empty, ordinary, and controlled batches without repeating callbacks" do
    ControlledBatchFacadeJob.decent_job_control_concurrency_with(total_limit: 1)
    empty, ordinary, controlled = 3.times.map { DecentJob::Batch.new(database: database) }
    controlled_jobs = [ControlledBatchFacadeJob.new("accepted"), ControlledBatchFacadeJob.new("rejected")]
    DecentJob::Batch.enqueue_all([[empty, []], [ordinary, [BatchFacadeJob.new("ordinary")]], [controlled, controlled_jobs]])

    _(empty.succeeded?).must_equal true
    _(ordinary.active_jobs.size).must_equal 1
    _(controlled.active_jobs.map(&:provider_job_id)).must_equal [controlled_jobs.first.provider_job_id]
    _(controlled_jobs.last.successfully_enqueued?).must_equal false
    _(ControlledBatchFacadeJob.enqueue_events).must_equal controlled_jobs.map(&:job_id)
  end

  it "validates all bulk batches before persisting any batch" do
    first = DecentJob::Batch.new(database: database)
    existing = DecentJob::Batch.new(database: database)
    existing.save

    _ { DecentJob::Batch.enqueue_all([[first, []], [existing, []]]) }.must_raise ArgumentError
    _(first.persisted?).must_equal false
  end

  it "rejects duplicate batch instances before writing any bulk records" do
    batch = DecentJob::Batch.new(database: database)

    _ { DecentJob::Batch.enqueue_all([[batch, []], [batch, []]]) }.must_raise ArgumentError
    _(batch.persisted?).must_equal false
  end

  it "reports the owning batch during execution" do
    batch = DecentJob::Batch.enqueue([BatchFacadeContextJob.new], database: database)
    DecentJob::Performer.new(database: database).perform_one

    _(BatchFacadeJob.events.pop).must_equal batch.id
    _(batch.reload.succeeded?).must_equal true
  end

  it "reflects a batch attached to the currently executing job" do
    batch = DecentJob::Batch.new(database: database)
    batch.save
    BatchFacadeContextJob.perform_later(batch.id)
    DecentJob::Performer.new(database: database).perform_one

    _(BatchFacadeJob.events.pop).must_equal batch.id
    _(batch.active_jobs.size).must_equal 1
  end

  it "retries discarded members while preserving their provider identifiers" do
    batch = DecentJob::Batch.enqueue([BatchFacadeRetryJob.new], database: database)
    member_id = batch.active_jobs.first.provider_job_id
    performer = DecentJob::Performer.new(database: database)
    _(performer.perform_one.state).must_equal "discarded"
    _(batch.reload.discarded?).must_equal true
    BatchFacadeRetryJob.fail_execution = false
    batch.retry

    _(batch.discarded?).must_equal false
    _(batch.active_jobs.first.provider_job_id).must_equal member_id
    _(performer.perform_one.state).must_equal "succeeded"
    _(batch.reload.succeeded?).must_equal true
  end

  it "rolls back members and new facade state when capture raises" do
    batch = DecentJob::Batch.new(database: database)
    _ {
      batch.enqueue do
        BatchFacadeJob.perform_later("captured")
        raise "abort capture"
      end
    }.must_raise RuntimeError

    _(batch.persisted?).must_equal false
    database.with_connection { |connection| _(connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)).must_equal "0" }
  end

  it "keeps its persisted state when inline execution raises after commit" do
    BatchFacadeRetryJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :inline)
    batch = DecentJob::Batch.new(database: database)

    _ { batch.enqueue([BatchFacadeRetryJob.new]) }.must_raise RuntimeError
    _(batch.persisted?).must_equal true
    _(batch.reload.discarded?).must_equal true
  ensure
    BatchFacadeRetryJob.queue_adapter = @adapter
  end

  it "releases member row locks before preparing completion callbacks" do
    member = BatchFacadeJob.perform_later("already complete")
    DecentJob::Performer.new(database: database).perform_one
    BatchFacadeUnlockedCallback.member_id = member.provider_job_id
    batch = DecentJob::Batch.new(database: database, on_success: BatchFacadeUnlockedCallback)

    _(batch.enqueue([member])).must_equal [member]
    _(batch.callback_active_jobs.size).must_equal 1
  end

  it "keeps a durable reconciliation request when inline execution fails after commit" do
    BatchFacadeRetryJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :inline)
    batch = DecentJob::Batch.new(database: database)
    error = _ {
      database.transaction do
        # An earlier after-commit failure prevents subsequent reconciliation callbacks.
        database.after_commit { raise "earlier callback failed" }
        batch.enqueue([BatchFacadeRetryJob.new])
      end
    }.must_raise RuntimeError

    _(error.message).must_equal "earlier callback failed"
    _(batch.persisted?).must_equal true
    database.with_connection do |connection|
      count = connection.exec_params("SELECT count(*) FROM decent_job_batch_updates WHERE batch_id = $1", [batch.id]).getvalue(0, 0).to_i
      _(count).must_be :positive?
    end
  ensure
    BatchFacadeRetryJob.queue_adapter = @adapter
  end
end
