# frozen_string_literal: true

require "spec_helper"
require "support/jobs"

class BulkJob < RecordingJob
  class_attribute :callback_events, default: []
  class_attribute :after_callback_events, default: []
  before_enqueue { callback_events << job_id }
  after_enqueue { after_callback_events << job_id }
end

class ControlledBulkJob < BulkJob
  include DecentJob::ActiveJobExtensions::Concurrency
end

class ControlledBulkDescendantJob < ControlledBulkJob
  before_enqueue do
    depth = arguments.first
    ActiveJob.perform_all_later(self.class.new(depth - 1)) if depth.positive?
    self.arguments = [depth, "scope-#{depth}"]
    self.queue_name = "generation-#{depth}"
    self.decent_job_labels = ["prepared-#{depth}"]
  end
end

describe DecentJob::Bulk do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)
    BulkJob.queue_adapter = @adapter
    BulkJob.events = Queue.new
    BulkJob.callback_events = []
    BulkJob.after_callback_events = []
    ControlledBulkJob.decent_job_concurrency_config = {}
    ControlledBulkJob.decent_job_concurrency_rules = []
    ControlledBulkJob.decent_job_labels = []
    @store = DecentJob::Store.new(database)
  end

  it "captures block jobs until the block completes and returns accepted jobs" do
    result = DecentJob::Bulk.enqueue do
      first = BulkJob.perform_later("first")
      _(first.provider_job_id).must_be_nil
      BulkJob.perform_later("second")
      database.with_connection { |connection| _(connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)).must_equal "0" }
    end

    _(result.size).must_equal 2
    _(result.all?(&:successfully_enqueued?)).must_equal true
    _(result.map(&:provider_job_id).compact.uniq.size).must_equal 2
    _(BulkJob.callback_events).must_equal result.map(&:job_id)
  end

  it "accepts explicit jobs as positional arguments or an array" do
    first, second, third = 3.times.map { |index| BulkJob.new(index) }

    _(DecentJob::Bulk.enqueue(first, second)).must_equal [first, second]
    _(DecentJob::Bulk.enqueue([third])).must_equal [third]
    _([first, second, third].all?(&:provider_job_id)).must_equal true
  end

  it "rejects explicit jobs combined with a block" do
    _ { DecentJob::Bulk.enqueue(BulkJob.new) { BulkJob.perform_later("extra") } }.must_raise ArgumentError
  end

  it "validates explicit jobs before writing any of them" do
    _ { DecentJob::Bulk.enqueue(BulkJob.new("first"), Object.new) }.must_raise ArgumentError
    database.with_connection { |connection| _(connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)).must_equal "0" }
  end

  it "leaves no writes when the block raises and restores capture state" do
    job = nil

    _ {
      DecentJob::Bulk.enqueue do
        job = BulkJob.perform_later("discarded")
        raise "abort"
      end
    }.must_raise RuntimeError

    _(DecentJob::Bulk.current_buffer).must_be_nil
    _(job.provider_job_id).must_be_nil
    _(job.successfully_enqueued?).must_equal false
    database.with_connection { |connection| _(connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)).must_equal "0" }
    _(BulkJob.perform_later("outside").provider_job_id).wont_be_nil
  end

  it "joins nested enqueue blocks to the outer capture" do
    inner = nil

    result = DecentJob::Bulk.enqueue do
      BulkJob.perform_later("before")
      inner = DecentJob::Bulk.enqueue { BulkJob.perform_later("inner") }
      _(inner.first.provider_job_id).must_be_nil
      BulkJob.perform_later("after")
      database.with_connection { |connection| _(connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)).must_equal "0" }
    end

    _(result.map { |job| job.arguments.first }).must_equal ["before", "inner", "after"]
    _(inner.first.provider_job_id).wont_be_nil
  end

  it "does not persist nested jobs when the outer block raises" do
    _ {
      DecentJob::Bulk.enqueue do
        DecentJob::Bulk.enqueue { BulkJob.perform_later("inner") }
        raise "outer abort"
      end
    }.must_raise RuntimeError

    database.with_connection { |connection| _(connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)).must_equal "0" }
  end

  it "captures independently in separate threads" do
    entered = Queue.new
    release = Queue.new

    worker = Thread.new do
      DecentJob::Bulk.enqueue do
        BulkJob.perform_later("thread")
        entered << true
        release.pop
      end
    end

    entered.pop
    outside = BulkJob.perform_later("outside")
    _(outside.provider_job_id).wont_be_nil
    database.with_connection { |connection| _(connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)).must_equal "1" }
    release << true
    _(worker.value.first.provider_job_id).wont_be_nil
  ensure
    release << true if release
    worker&.join
  end

  it "preserves queue, priority, scheduled time, and provider identifier" do
    scheduled_at = Time.now.utc + 3600

    job = DecentJob::Bulk.enqueue do
      BulkJob.set(queue: "bulk", priority: 7, wait_until: scheduled_at).perform_later("later")
    end.first

    row = @store.find(job.provider_job_id)

    _(row.fetch("queue_name")).must_equal "bulk"
    _(row.fetch("priority")).must_equal 7
    _(row.fetch("scheduled_at").to_f).must_be_close_to scheduled_at.to_f, 0.001
    _(row.fetch("payload").fetch("provider_job_id")).must_equal job.provider_job_id
  end

  it "does not enqueue already persisted jobs again" do
    job = BulkJob.perform_later("already")
    provider_id = job.provider_job_id

    _(DecentJob::Bulk.enqueue(job)).must_equal [job]
    _(job.provider_job_id).must_equal provider_id
    database.with_connection { |connection| _(connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)).must_equal "1" }
  end

  it "enforces controls for captured jobs without repeating callbacks" do
    ControlledBulkJob.decent_job_control_concurrency_with(total_limit: 1)
    first = second = nil

    result = DecentJob::Bulk.enqueue do
      first = ControlledBulkJob.perform_later("accepted")
      second = ControlledBulkJob.perform_later("rejected")
    end

    _(result).must_equal [first]
    _(second.provider_job_id).must_be_nil
    _(second.successfully_enqueued?).must_equal false
    _(second.enqueue_error).must_be_kind_of ActiveJob::EnqueueError
    _(BulkJob.callback_events).must_equal [first.job_id, second.job_id]
  end

  it "enforces controls and runs callbacks for explicit controlled jobs" do
    ControlledBulkJob.decent_job_control_concurrency_with(total_limit: 1)
    jobs = [ControlledBulkJob.new("accepted"), ControlledBulkJob.new("rejected")]

    _(DecentJob::Bulk.enqueue(*jobs)).must_equal [jobs.first]
    _(BulkJob.callback_events).must_equal jobs.map(&:job_id)
  end

  it "skips after_enqueue for rejected explicit controlled jobs" do
    ControlledBulkJob.decent_job_control_concurrency_with(total_limit: 1)
    jobs = [ControlledBulkJob.new("accepted"), ControlledBulkJob.new("rejected")]

    _(DecentJob::Bulk.enqueue(*jobs)).must_equal [jobs.first]
    _(BulkJob.after_callback_events).must_equal [jobs.first.job_id]
    _(jobs.last.enqueue_error).must_be_kind_of ActiveJob::EnqueueError
  end

  it "runs captured callbacks once before reporting eventual admission" do
    ControlledBulkJob.decent_job_control_concurrency_with(total_limit: 1)
    jobs = []

    accepted = DecentJob::Bulk.enqueue do
      jobs << ControlledBulkJob.perform_later("accepted")
      jobs << ControlledBulkJob.perform_later("rejected")
      _(BulkJob.after_callback_events).must_equal jobs.map(&:job_id)
      _(jobs.all?(&:successfully_enqueued?)).must_equal true
    end

    _(accepted).must_equal [jobs.first]
    _(jobs.last.successfully_enqueued?).must_equal false
    _(jobs.last.enqueue_error).must_be_kind_of ActiveJob::EnqueueError
    _(BulkJob.after_callback_events).must_equal jobs.map(&:job_id)
  end

  it "executes inline jobs only after the capture has completed" do
    BulkJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :inline)

    result = DecentJob::Bulk.enqueue do
      BulkJob.perform_later("inline")
      _(BulkJob.events).must_be_empty
    end

    _(BulkJob.events.pop).must_equal "inline"
    _(@store.find(result.first.provider_job_id).fetch("state")).must_equal "succeeded"
  end

  it "captures direct adapter bulk enqueues" do
    jobs = [BulkJob.new("one"), BulkJob.new("two")]

    result = DecentJob::Bulk.enqueue do
      @adapter.enqueue_all(jobs)
      database.with_connection { |connection| _(connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)).must_equal "0" }
    end

    _(result).must_equal jobs
    _(jobs.all?(&:provider_job_id)).must_equal true
  end

  it "restores the previous buffer after an isolated capture" do
    captured = nil

    result = DecentJob::Bulk.enqueue do
      previous = DecentJob::Bulk.current_buffer
      captured = DecentJob::Bulk.capture { BulkJob.perform_later("captured only") }
      _(DecentJob::Bulk.current_buffer).must_be_same_as previous
      BulkJob.perform_later("enqueued")
    end

    _(captured.first.provider_job_id).must_be_nil
    _(result.map { |job| job.arguments.first }).must_equal ["enqueued"]
  end

  it "returns nil when capture has no active buffer" do
    _(DecentJob::Bulk.capture(BulkJob.new)).must_be_nil
    _(DecentJob::Bulk.enqueue).must_equal []
  end

  it "supports explicitly supplied jobs using another adapter" do
    BulkJob.queue_adapter = :test
    job = BulkJob.new("other")

    _(DecentJob::Bulk.enqueue(job)).must_equal [job]
    _(BulkJob.queue_adapter.enqueued_jobs.size).must_equal 1
  end

  it "prepares bulk-created controlled descendants before returning their scopes" do
    ControlledBulkDescendantJob.decent_job_control_concurrency_with(key: -> { arguments.last }, enqueue_limit: 10)
    buffer = DecentJob::Bulk::Buffer.new
    buffer.add(ControlledBulkDescendantJob.new(2))
    buffer.prepare

    _(buffer.jobs.size).must_equal 3
    _(buffer.jobs.map(&:queue_name).sort).must_equal ["generation-0", "generation-1", "generation-2"]
    _(buffer.jobs.flat_map(&:decent_job_labels).sort).must_equal ["prepared-0", "prepared-1", "prepared-2"]
    scopes = buffer.jobs.flat_map { |job| job.decent_job_resolved_concurrency_rules(stage: :metadata).map(&:scope) }
    _(scopes.sort).must_equal ["key:scope-0", "key:scope-1", "key:scope-2"]
    _(BulkJob.callback_events.tally.values).must_equal [1, 1, 1]
    _(BulkJob.after_callback_events.tally.values).must_equal [1, 1, 1]
    database.with_connection { |connection| _(connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)).must_equal "0" }
  end

  it "prepares a duplicated controlled job object only once" do
    ControlledBulkJob.decent_job_control_concurrency_with(total_limit: 10)
    job = ControlledBulkJob.new("same object")
    buffer = DecentJob::Bulk::Buffer.new
    buffer.add([job, job])
    buffer.prepare

    _(buffer.jobs).must_equal [job]
    _(BulkJob.callback_events).must_equal [job.job_id]
    _(BulkJob.after_callback_events).must_equal [job.job_id]
  end

  it "prefers a captured controlled entry over an unprepared duplicate" do
    job = ControlledBulkJob.new("same object")
    buffer = DecentJob::Bulk::Buffer.new
    buffer.add(job)
    buffer.capture { job.enqueue }
    buffer.prepare

    _(buffer.jobs).must_equal [job]
    _(BulkJob.callback_events).must_equal [job.job_id]
    _(BulkJob.after_callback_events).must_equal [job.job_id]
  end
end
