# frozen_string_literal: true

require "spec_helper"

class CronStoreJob < ActiveJob::Base
  def perform(*)
  end
end

class CronStoreControlledJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::Concurrency

  decent_job_control_concurrency_with enqueue_limit: 1, key: "cron-test"

  def perform(*)
  end
end

describe "durable cron entries" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    CronStoreJob.queue_adapter = DecentJob::Adapter.new(database: database)
    CronStoreControlledJob.queue_adapter = CronStoreJob.queue_adapter
    @entry = DecentJob::CronEntry.new(key: :daily, cron: "@daily UTC", class: CronStoreJob, database: database)
    @at = Time.utc(2026, 1, 1, 12, 0, 0, 123456)
  end

  it "enqueues options and records microsecond occurrences with decoded history" do
    entry = DecentJob::CronEntry.new(key: :daily, cron: "@daily", class: "CronStoreJob", args: -> { [1] }, kwargs: -> { {color: "blue"} }, set: -> { {queue: "reports", priority: 4} }, database: database)
    job = entry.enqueue(@at)
    _(job).must_be_kind_of CronStoreJob
    _(job.successfully_enqueued?).must_equal true
    row = entry.last_job
    _(row.fetch("cron_key")).must_equal "daily"
    _(row.fetch("cron_at")).must_equal @at
    _(row.fetch("queue_name")).must_equal "reports"
    _(row.fetch("priority")).must_equal 4
    _(job.arguments).must_equal [1, {color: "blue"}]
    _(entry.last_job_at).must_equal @at
    _(entry.jobs.map { |value| value.fetch("id") }).must_equal [job.provider_job_id]
  end

  it "deduplicates occurrences without poisoning an outer transaction" do
    first = @entry.enqueue(@at)
    database.transaction do |connection|
      duplicate = @entry.enqueue(@at)
      _(duplicate).must_equal false
      _(connection.exec("SELECT 42").getvalue(0, 0)).must_equal "42"
      _(@entry.enqueue(@at + Rational(1, 1_000_000))).must_be_kind_of CronStoreJob
    end
    _(@entry.jobs.size).must_equal 2
    _(first.provider_job_id).wont_be_nil
  end

  it "allows repeated manual runs and uses creation time when no occurrence exists" do
    2.times { @entry.enqueue }
    _(@entry.jobs.size).must_equal 2
    _(@entry.last_job_at).must_equal @entry.last_job.fetch("created_at")
    _(@entry.jobs.map { |row| row["cron_at"] }).must_equal [nil, nil]
  end

  it "persists enable overrides independently of configured defaults" do
    _(@entry.enabled?).must_equal true
    @entry.disable
    replacement = DecentJob::CronEntry.new(key: :daily, cron: "@daily", database: database)
    _(replacement.enabled?).must_equal false
    replacement.enable
    _(@entry.enabled?).must_equal true
    disabled = DecentJob::CronEntry.new(key: :other, cron: "@daily", enabled_by_default: -> { false }, database: database)
    _(disabled.enabled?).must_equal false
    disabled.enable
    _(disabled.enabled?).must_equal true
  end

  it "returns latest scheduled history per key and supports an occurrence cutoff" do
    @entry.enqueue(@at)
    @entry.enqueue(@at + 86400)
    @entry.enqueue
    _(@entry.last_job_at).must_equal @at + 86400
    _(DecentJob::CronEntry.last_jobs_by_key([@entry]).fetch("daily").fetch("cron_at")).must_equal @at + 86400
    _(DecentJob::CronEntry.last_jobs_by_key([@entry], before: @at).fetch("daily").fetch("cron_at")).must_equal @at
    _(DecentJob::CronEntry.last_jobs_by_key([])).must_equal({})
  end

  it "captures callable multi-job and bulk enqueues with one winner and accurate flags" do
    captured = []
    entry = DecentJob::CronEntry.new(key: :bulk, cron: "@daily", class: -> {
      captured.concat([CronStoreJob.new(1), CronStoreJob.new(2)])
      ActiveJob.perform_all_later(captured)
    }, database: database)
    accepted = entry.enqueue(@at)
    _(accepted).must_equal [captured.first]
    _(captured.map(&:successfully_enqueued?)).must_equal [true, false]
    _(captured.last.provider_job_id).must_be_nil
    _(entry.jobs.size).must_equal 1
  end

  it "flushes cron enqueues inside an outer Bulk without losing their context" do
    ordinary = nil
    DecentJob::Bulk.enqueue do
      @entry.enqueue(@at)
      ordinary = CronStoreJob.perform_later
    end
    _(@entry.jobs.size).must_equal 1
    _(DecentJob::Store.new(database).find(ordinary.provider_job_id).fetch("cron_key")).must_be_nil
  end

  it "does not consume an occurrence when concurrency admission fails" do
    CronStoreControlledJob.perform_later
    entry = DecentJob::CronEntry.new(key: :controlled, cron: "@daily", class: CronStoreControlledJob, database: database)
    _(entry.enqueue(@at)).must_equal false
    _(entry.jobs).must_equal []
    database.with_connection { |connection| connection.exec("UPDATE decent_jobs SET state = 'succeeded'") }
    _(entry.enqueue(@at)).must_be_kind_of CronStoreControlledJob
  end
end
