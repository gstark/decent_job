# frozen_string_literal: true

require "minitest/mock"
require "spec_helper"
require "support/jobs"

describe "execution aggregates" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)
    [RecordingJob, RetryingJob].each { |klass| klass.queue_adapter = @adapter }
    RecordingJob.events = Queue.new
    @origin = Time.utc(2025, 1, 1)
  end

  def metrics
    DecentJob::ExecutionMetrics.new(database: database)
  end

  def snapshot(job: nil, scheduled_at: @origin, queue: 2, duration: 3, job_class: "FirstJob", queue_name: "default", state: "succeeded")
    job ||= RecordingJob.perform_later("fixture")
    id = SecureRandom.uuid
    database.with_connection do |connection|
      connection.exec_params(<<~SQL, [id, job.provider_job_id, (@origin + queue).iso8601(6), scheduled_at&.iso8601(6), duration, job_class, queue_name, state])
        INSERT INTO decent_job_executions (id, job_id, started_at, scheduled_at, duration, job_class, queue_name, state, process_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1)
      SQL
    end
    [job, id]
  end

  def empty_summary
    {jobs_count: 0, executions_count: 0,
     avg_queue: nil, min_queue: nil, max_queue: nil,
     avg_execution: nil, min_execution: nil, max_execution: nil,
     avg_total: nil, min_total: nil, max_total: nil}
  end

  it "returns zero counts and unknown metrics when no attempts exist" do
    _(DecentJob.const_defined?(:ExecutionMetrics)).must_equal true
    _(metrics.summary).must_equal empty_summary
  end

  it "counts distinct jobs and aggregates each available metric independently" do
    job, = snapshot(queue: 2, duration: 4)
    snapshot(job: job, queue: 8, duration: nil, state: "running")
    snapshot(queue: 20, duration: 10, scheduled_at: nil)
    snapshot(queue: -4, duration: 0)
    snapshot(queue: 6, duration: nil, state: "interrupted")
    snapshot(queue: 30, duration: nil, scheduled_at: nil, job_class: nil, queue_name: nil)
    result = metrics.summary
    _(result).must_equal({jobs_count: 5, executions_count: 6,
                         avg_queue: 3.0, min_queue: -4.0, max_queue: 8.0,
                         avg_execution: 14.0 / 3, min_execution: 0.0, max_execution: 10.0,
                         avg_total: 1.0, min_total: -4.0, max_total: 6.0})
    result.except(:jobs_count, :executions_count).each_value { |value| _(value).must_be_kind_of Float }
    _(result.fetch(:jobs_count)).must_be_kind_of Integer
    _(result.fetch(:executions_count)).must_be_kind_of Integer
  end

  it "uses monotonic durations and retained snapshots without deserializing job payloads" do
    job, = snapshot(queue: 2, duration: 3)
    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET queue_name = 'moved', job_class = 'MissingJob', payload = '{}' WHERE id = $1", [job.provider_job_id])
      connection.exec("UPDATE decent_job_executions SET payload = '{\"job_class\":\"MissingJob\"}', finished_at = started_at + interval '1 hour'")
    end
    _(metrics.summary).must_equal empty_summary.merge(jobs_count: 1, executions_count: 1,
      avg_queue: 2.0, min_queue: 2.0, max_queue: 2.0,
      avg_execution: 3.0, min_execution: 3.0, max_execution: 3.0,
      avg_total: 5.0, min_total: 5.0, max_total: 5.0)
  end

  it "groups snapshots by class or queue with empty names distinct from unknown names" do
    job, = snapshot(job_class: "Zulu", queue_name: "second")
    snapshot(job: job, job_class: "Alpha", queue_name: "first", duration: 5)
    snapshot(job_class: "", queue_name: "", duration: 0)
    snapshot(job_class: nil, queue_name: nil, duration: nil, scheduled_at: nil)
    database.with_connection { |connection| connection.exec("UPDATE decent_jobs SET job_class = 'Changed', queue_name = 'changed'") }
    classes = metrics.grouped_by(:job_class)
    _(classes.map { |row| row.fetch(:job_class) }).must_equal ["", "Alpha", "Zulu", nil]
    _(classes.map { |row| row.fetch(:executions_count) }).must_equal [1, 1, 1, 1]
    _(classes.last).must_equal empty_summary.merge(job_class: nil, jobs_count: 1, executions_count: 1)
    _(classes[1].fetch(:avg_execution)).must_equal 5.0
    queues = metrics.grouped_by("queue_name")
    _(queues.map { |row| row.fetch(:queue_name) }).must_equal ["", "first", "second", nil]
    _(metrics.grouped_by("job_class")).must_equal classes
    _(metrics.grouped_by(:queue_name)).must_equal queues
  end

  it "counts retried jobs once within each group" do
    job, = snapshot
    snapshot(job: job)
    row = metrics.grouped_by(:job_class).first
    _(row.fetch(:jobs_count)).must_equal 1
    _(row.fetch(:executions_count)).must_equal 2
    _(metrics.grouped_by(:queue_name).first.fetch(:jobs_count)).must_equal 1
  end

  it "returns an empty array when no groups match" do
    _(metrics.grouped_by(:job_class)).must_equal []
    snapshot
    _(metrics.grouped_by(:queue_name, job_class: "Absent")).must_equal []
  end

  it "applies combined exact class and queue filters to snapshots" do
    snapshot(job_class: "FirstJob", queue_name: "target", duration: 1)
    snapshot(job_class: "FirstJob", queue_name: "other", duration: 3)
    snapshot(job_class: "SecondJob", queue_name: "target", duration: 7)
    snapshot(job_class: "", queue_name: "", duration: 9)
    snapshot(job_class: nil, queue_name: nil)
    database.with_connection { |connection| connection.exec("UPDATE decent_jobs SET job_class = 'Changed', queue_name = 'changed'") }
    filters = {job_class: "FirstJob", queue_name: "target", from: @origin, to: @origin + 1}
    result = metrics.summary(**filters)
    _(result.fetch(:executions_count)).must_equal 1
    _(result.fetch(:avg_execution)).must_equal 1.0
    _(metrics.grouped_by(:job_class, **filters)).must_equal [result.merge(job_class: "FirstJob")]
    _(metrics.grouped_by(:queue_name, **filters)).must_equal [result.merge(queue_name: "target")]
    _(metrics.summary(job_class: "", queue_name: "").fetch(:avg_execution)).must_equal 9.0
    _(metrics.summary(job_class: "firstjob")).must_equal empty_summary
  end

  it "uses inclusive scheduled lower bounds and exclusive upper bounds with timezone offsets" do
    snapshot(scheduled_at: @origin - 0.000001, duration: 1)
    snapshot(scheduled_at: @origin, duration: 3)
    snapshot(scheduled_at: @origin + 9.999999, duration: 5)
    snapshot(scheduled_at: @origin + 10, duration: 7)
    snapshot(scheduled_at: nil, duration: 11)
    from = Time.new(2024, 12, 31, 19, 0, 0, "-05:00")
    to = Time.new(2025, 1, 1, 1, 0, 10, "+01:00")
    result = metrics.summary(from: from, to: to)
    _(result.fetch(:executions_count)).must_equal 2
    _(result.fetch(:avg_execution)).must_equal 4.0
    _(metrics.grouped_by(:job_class, from: from, to: to)).must_equal [result.merge(job_class: "FirstJob")]
    _(metrics.summary(to: @origin).fetch(:executions_count)).must_equal 1
    _(metrics.summary(from: @origin + 10).fetch(:executions_count)).must_equal 1
    _(metrics.summary(from: from, to: from)).must_equal empty_summary
    _(metrics.grouped_by(:queue_name, from: from, to: from)).must_equal []
    _(metrics.summary.fetch(:executions_count)).must_equal 5
    zoned = from.in_time_zone("America/New_York")
    _(metrics.summary(from: zoned, to: to)).must_equal result
  end

  it "rejects invalid bounds and filter types" do
    [{from: "2025-01-01"}, {to: Date.new(2025, 1, 1)}, {from: 1}, {to: false},
      {from: @origin + 1, to: @origin}, {job_class: RecordingJob}, {job_class: ["FirstJob"]},
      {queue_name: :default}, {queue_name: ["default"]}, {queue_name: false}].each do |filters|
      _ { metrics.summary(**filters) }.must_raise ArgumentError
      _ { metrics.grouped_by(:queue_name, **filters) }.must_raise ArgumentError
    end
  end

  it "rejects nonwhitelisted group identifiers without coercion" do
    coercible = Object.new
    def coercible.to_s = "job_class"
    def coercible.to_str = "job_class"
    def coercible.==(other) = true
    [nil, 1, [], coercible, :duration, "JOB_CLASS", "job_class; DROP TABLE decent_job_executions"].each do |group|
      _ { metrics.grouped_by(group) }.must_raise ArgumentError
    end
  end

  it "treats injection-shaped filter values as exact names" do
    name = "' OR 1=1; DROP TABLE decent_job_executions; --"
    snapshot(job_class: name, queue_name: name)
    snapshot
    _(metrics.summary(job_class: name, queue_name: name).fetch(:executions_count)).must_equal 1
    _(metrics.grouped_by(:job_class, queue_name: name).first.fetch(:job_class)).must_equal name
    _(metrics.summary.fetch(:executions_count)).must_equal 2
  end

  it "captures the explicit database before current or global defaults" do
    snapshot
    explicit = nil
    current = nil
    global = nil
    DecentJob.stub(:database, -> { raise "global database should not be used" }) do
      DecentJob::Current.with(database: Object.new) { explicit = DecentJob::ExecutionMetrics.new(database: database) }
      DecentJob::Current.with(database: database) { current = DecentJob::ExecutionMetrics.new }
    end
    DecentJob.stub(:database, database) { global = DecentJob::ExecutionMetrics.new }
    DecentJob.stub(:database, -> { raise "database should have been captured" }) do
      [explicit, current, global].each { |reader| _(reader.summary.fetch(:executions_count)).must_equal 1 }
    end
  end

  it "returns detached results and reads current retained attempts on every call" do
    snapshot
    reader = metrics
    summary = reader.summary
    summary[:executions_count] = 99
    grouped = reader.grouped_by(:job_class)
    grouped.first.fetch(:job_class).replace("Changed")
    grouped.first[:executions_count] = 99
    grouped.clear
    _(reader.summary.fetch(:executions_count)).must_equal 1
    _(reader.grouped_by(:job_class).first.fetch(:job_class)).must_equal "FirstJob"
    snapshot
    _(reader.summary.fetch(:executions_count)).must_equal 2
    database.with_connection { |connection| connection.exec("DELETE FROM decent_job_executions") }
    _(reader.summary).must_equal empty_summary
    _(reader.grouped_by(:job_class)).must_equal []
  end

  it "aggregates in one SELECT per call" do
    snapshot
    database.with_connection do |connection|
      original = connection.method(:exec_params)
      calls = []
      connection.stub(:exec_params, ->(sql, binds) {
        calls << [sql, binds]
        original.call(sql, binds)
      }) do
        metrics.summary(from: @origin, job_class: "FirstJob")
        _(calls.length).must_equal 1
        _(calls.first.first.strip).must_match(/\ASELECT /)
        metrics.grouped_by(:queue_name, to: @origin + 1)
        _(calls.length).must_equal 2
        _(calls.last.first.strip).must_match(/\ASELECT /)
      end
    end
  end

  it "matches execution instance values across real retries while excluding unattempted jobs" do
    job = RetryingJob.perform_later("done")
    performer = DecentJob::Performer.new(database: database)
    2.times { performer.perform_one }
    RecordingJob.perform_later("pending")
    RecordingJob.set(wait: 3600).perform_later("future")
    attempts = DecentJob::Execution.for_job(job.provider_job_id, database: database)
    result = metrics.summary
    _(result.fetch(:jobs_count)).must_equal 1
    _(result.fetch(:executions_count)).must_equal 2
    {queue: :queue_latency, execution: :runtime_latency, total: :total_latency}.each do |metric, method|
      values = attempts.map(&method)
      _(result.fetch(:"avg_#{metric}")).must_be_close_to values.sum / values.length, 0.000001
      _(result.fetch(:"min_#{metric}")).must_be_close_to values.min, 0.000001
      _(result.fetch(:"max_#{metric}")).must_be_close_to values.max, 0.000001
    end
  end

  it "includes a live claim's queue latency without estimating its runtime" do
    RecordingJob.perform_later("running")
    DecentJob::Claim.new(database).with_next do |row|
      execution = DecentJob::Execution.find(row.fetch("execution_id"), database: database)
      result = metrics.summary
      _(result.fetch(:executions_count)).must_equal 1
      _(result.fetch(:avg_queue)).must_be_close_to execution.queue_latency, 0.000001
      _(result.fetch(:avg_execution)).must_be_nil
      _(result.fetch(:avg_total)).must_be_nil
    end
  end
end
