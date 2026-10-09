# frozen_string_literal: true

require "spec_helper"
require "support/jobs"

describe "dequeue policy" do
  include DatabaseSpec

  before do
    DecentJob.reset!
    DecentJob::Schema.new(database).migrate
    RecordingJob.queue_adapter = DecentJob::Adapter.new(database: database)
    RecordingJob.events = Queue.new
    @performer = DecentJob::Performer.new(database: database, enable_pauses: true)
  end

  after { DecentJob.reset! }

  def enqueue_candidate(value, created: 60, scheduled: 60, **options)
    job = RecordingJob.set(**options).perform_later(value)
    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET created_at = statement_timestamp() - $2::interval, scheduled_at = statement_timestamp() - $3::interval WHERE id = $1", [job.provider_job_id, "#{created} seconds", "#{scheduled} seconds"])
    end
    job
  end

  def perform_value(**options)
    return unless @performer.perform_one(**options)
    RecordingJob.events.pop(true)
  end

  ["*", "default", "-other", "+default,other"].each do |queues|
    it "uses creation order for #{queues} even when deadlines disagree" do
      enqueue_candidate("created first", created: 120, scheduled: 30)
      enqueue_candidate("scheduled first", created: 30, scheduled: 120)
      _(perform_value(queues: queues)).must_equal "created first"
      _(perform_value(queues: queues)).must_equal "scheduled first"
    end
  end

  it "reads changed sort and limit settings on an existing performer" do
    DecentJob.configuration.lock_strategy = :advisory
    enqueue_candidate("created first", created: 120, scheduled: 30)
    enqueue_candidate("scheduled first", created: 30, scheduled: 120)
    config = DecentJob.configuration
    config.queue_select_limit = 0
    _(@performer.perform_one).must_be_nil
    config.queue_select_limit = 1
    config.dequeue_query_sort = :scheduled_at
    _(perform_value).must_equal "scheduled first"
    config.dequeue_query_sort = nil
    config.queue_select_limit = nil
    _(perform_value).must_equal "created first"
  end

  [:created_at, :scheduled_at].each do |sort|
    it "orders queues then priority then #{sort} then UUID" do
      DecentJob.configuration.dequeue_query_sort = sort
      enqueue_candidate("other queue", queue: "other", priority: -100, created: 1000, scheduled: 1000)
      enqueue_candidate("lower priority", priority: 10, created: 1000, scheduled: 1000)
      enqueue_candidate("later", priority: 0, created: 20, scheduled: 20)
      tied = 2.times.map { |index| enqueue_candidate("tie #{index}", priority: 0, created: 50, scheduled: 50) }
      database.with_connection do |connection|
        connection.exec_params("UPDATE decent_jobs SET created_at = '2020-01-01', scheduled_at = '2020-01-01' WHERE id = ANY($1::uuid[])", [PG::TextEncoder::Array.new.encode(tied.map(&:provider_job_id))])
      end
      values = 5.times.map { perform_value(queues: "+default,other") }
      _(values).must_equal [*tied.sort_by(&:provider_job_id).map { |job| job.arguments.first }, "later", "lower priority", "other queue"]
    end

    it "filters paused, future, excluded and terminal jobs before the #{sort} candidate limit" do
      DecentJob.configuration.dequeue_query_sort = sort
      DecentJob.configuration.queue_select_limit = 1
      enqueue_candidate("future", created: 500, scheduled: -3600)
      enqueue_candidate("paused", queue: "paused", created: 500, scheduled: 500)
      enqueue_candidate("excluded", queue: "excluded", created: 500, scheduled: 500)
      terminal = enqueue_candidate("terminal", created: 500, scheduled: 500)
      DecentJob::Job.find(terminal.provider_job_id, database: database).discard_job("removed")
      enqueue_candidate("eligible", created: 30, scheduled: 30)
      DecentJob::Pauses.new(database).pause(queue: "paused")
      _(perform_value(queues: "-excluded")).must_equal "eligible"
      _(@performer.perform_one(queues: "-excluded")).must_be_nil
    end
  end

  it "bounds candidates before advisory locking and releases the selected lock" do
    DecentJob.configuration.lock_strategy = :advisory
    held = 2.times.map { |index| enqueue_candidate("held #{index}", created: 100 - index, scheduled: 100 - index) }
    available = enqueue_candidate("available", created: 30, scheduled: 30)
    separate = database.connect
    held.each do |job|
      separate.exec_params("SELECT pg_advisory_lock(hashtextextended(current_schema() || '.decent_jobs.' || $1, 0))", [job.provider_job_id])
    end
    DecentJob.configuration.queue_select_limit = 2
    _(@performer.perform_one).must_be_nil
    DecentJob.configuration.queue_select_limit = 3
    _(perform_value).must_equal "available"
    _(separate.exec_params("SELECT pg_try_advisory_lock(hashtextextended(current_schema() || '.decent_jobs.' || $1, 0))", [available.provider_job_id]).getvalue(0, 0)).must_equal "t"
    _(DecentJob::Store.new(database).executions(available.provider_job_id).size).must_equal 1
    DecentJob.configuration.queue_select_limit = 100_000
    _(@performer.perform_one).must_be_nil
  ensure
    separate&.finish
  end

  it "bypasses a zero ordinary limit for targeted and inline execution while preserving eligibility" do
    DecentJob.configuration.lock_strategy = :advisory
    DecentJob.configuration.queue_select_limit = 0
    direct = enqueue_candidate("direct")
    future = enqueue_candidate("future", scheduled: -3600)
    _(@performer.perform_one).must_be_nil
    _(perform_value(job_id: direct.provider_job_id)).must_equal "direct"
    _(@performer.perform_one(job_id: future.provider_job_id)).must_be_nil
    RecordingJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :inline)
    inline = RecordingJob.perform_later("inline")
    _(RecordingJob.events.pop(true)).must_equal "inline"
    _(DecentJob::Job.find(inline.provider_job_id, database: database).succeeded?).must_equal true
  end

  it "does not apply dequeue ordering or limits to deadline discovery" do
    DecentJob.configuration.queue_select_limit = 0
    enqueue_candidate("later deadline", created: 500, scheduled: -200)
    enqueue_candidate("earlier deadline", created: 30, scheduled: -100)
    claim = DecentJob::Claim.new(database)
    times = claim.next_scheduled_at(limit: 2)
    _(times.length).must_equal 2
    _(times).must_equal times.sort
    DecentJob.configuration.dequeue_query_sort = :scheduled_at
    _(claim.next_scheduled_at(limit: 2)).must_equal times
  end
end
