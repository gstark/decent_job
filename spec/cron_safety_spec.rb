# frozen_string_literal: true

require "spec_helper"

class CronSafetyJob < ActiveJob::Base
  def perform(*)
  end
end

class CronSafetyControlledJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::Concurrency

  class_attribute :callback_locales, default: []
  before_enqueue { self.class.callback_locales << I18n.locale }

  def perform(*)
  end
end

class CronSafetyForeignJob < ActiveJob::Base
  def perform(*)
  end
end

describe "cron enqueue safety" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    CronSafetyJob.queue_adapter = DecentJob::Adapter.new(database: database)
    CronSafetyControlledJob.queue_adapter = CronSafetyJob.queue_adapter
    @at = Time.new(2026, 1, 1, 12, 0, 0, "+05:00")
  end

  it "serializes under the default locale and preserves the caller's locale and occurrence timezone" do
    previous = I18n.available_locales
    I18n.available_locales = [:en, :fr]
    entry = DecentJob::CronEntry.new(key: :locale, cron: "@daily", class: CronSafetyJob, database: database)

    I18n.with_locale(:fr) do
      entry.enqueue(@at)
      _(I18n.locale).must_equal :fr
    end

    _(entry.last_job.fetch("payload").fetch("locale")).must_equal I18n.default_locale.to_s
    _(@at.utc_offset).must_equal 18000
  ensure
    I18n.available_locales = previous
  end

  it "runs deferred controlled bulk callbacks under the default locale" do
    previous = I18n.available_locales
    I18n.available_locales = [:en, :fr]
    CronSafetyControlledJob.callback_locales = []
    entry = DecentJob::CronEntry.new(key: :locale, cron: "@daily", class: -> { ActiveJob.perform_all_later(CronSafetyControlledJob.new) }, database: database)
    I18n.with_locale(:fr) { entry.enqueue(@at) }
    _(CronSafetyControlledJob.callback_locales).must_equal [I18n.default_locale]
    _(entry.last_job.fetch("payload").fetch("locale")).must_equal I18n.default_locale.to_s
  ensure
    I18n.available_locales = previous
  end

  it "rejects a foreign adapter bulk before either adapter writes" do
    CronSafetyForeignJob.queue_adapter = :test

    entry = DecentJob::CronEntry.new(key: :foreign, cron: "@daily", class: -> {
      CronSafetyJob.perform_later
      ActiveJob.perform_all_later(CronSafetyJob.new, CronSafetyForeignJob.new)
    }, database: database)

    _ { entry.enqueue(@at) }.must_raise ArgumentError
    _(entry.jobs).must_equal []
    _(CronSafetyForeignJob.queue_adapter.enqueued_jobs).must_equal []
    _(DecentJob::CronEntry.current_context).must_be_nil
  end

  it "restores nested cron contexts and each entry's identity" do
    inner = DecentJob::CronEntry.new(key: :inner, cron: "@daily", class: CronSafetyJob, database: database)

    outer = DecentJob::CronEntry.new(key: :outer, cron: "@daily", class: -> {
      inner.enqueue(@at)
      CronSafetyJob.perform_later
    }, database: database)

    outer.enqueue(@at)
    _(outer.jobs.size).must_equal 1
    _(inner.jobs.size).must_equal 1
    _(DecentJob::CronEntry.current_context).must_be_nil
  end

  it "resolves callable class names and returns directly enqueued jobs" do
    entry = DecentJob::CronEntry.new(key: :resolve, cron: "@daily", class: -> { "CronSafetyJob" }, database: database)
    _(entry.enqueue(@at)).must_be_kind_of CronSafetyJob
    direct = DecentJob::CronEntry.new(key: :direct, cron: "@daily", class: -> { CronSafetyJob.set(queue: "direct").perform_later }, database: database)
    job = direct.enqueue(@at)
    _(job).must_be_kind_of CronSafetyJob
    _(direct.last_job.fetch("queue_name")).must_equal "direct"
    _(direct.enqueue(@at)).must_equal false
  end

  it "does not enqueue when the class is blank" do
    entry = DecentJob::CronEntry.new(key: :blank, cron: "@daily", database: database)
    _(entry.enqueue(@at)).must_be_nil
    _(entry.jobs).must_equal []
  end

  it "does not suppress unrelated uniqueness errors or poison a surrounding transaction" do
    entry = DecentJob::CronEntry.new(key: :constraint, cron: "@daily", class: CronSafetyJob, database: database)
    database.with_connection { |connection| connection.exec("CREATE UNIQUE INDEX unique_cron_queue_test ON decent_jobs(queue_name)") }
    entry.enqueue(@at)

    database.transaction do |connection|
      _ { entry.enqueue(@at + 1) }.must_raise ActiveJob::EnqueueError
      _(connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)).must_equal "1"
    end

    _(entry.jobs.size).must_equal 1
  end

  it "rolls back earlier adapter groups and their returned flags after a serialization failure" do
    jobs = []
    CronSafetyControlledJob.queue_adapter = DecentJob::Adapter.new(database: database)

    entry = DecentJob::CronEntry.new(key: :serialization, cron: "@daily", class: -> {
      jobs << CronSafetyJob.perform_later
      jobs << CronSafetyControlledJob.perform_later(Object.new)
    }, database: database)

    _ { entry.enqueue }.must_raise ActiveJob::SerializationError
    _(entry.jobs).must_equal []
    _(jobs.map(&:successfully_enqueued?)).must_equal [false, false]
    _(jobs.map(&:provider_job_id)).must_equal [nil, nil]
    _(DecentJob::CronEntry.current_context).must_be_nil
  end

  it "treats blank class values as no-op definitions without resolving options" do
    entry = DecentJob::CronEntry.new(key: :blank, cron: "@daily", class: "  ", args: -> { raise "called" }, database: database)
    _(entry.valid?).must_equal true
    _(entry.enqueue(@at)).must_be_nil
    _(entry.jobs).must_equal []
  end

  it "returns only accepted jobs from callable arrays on duplicate occurrences" do
    entry = DecentJob::CronEntry.new(key: :array, cron: "@daily", class: -> { [CronSafetyJob.perform_later, CronSafetyJob.perform_later] }, database: database)
    _(entry.enqueue(@at).size).must_equal 1
    _(entry.enqueue(@at)).must_equal []
  end

  it "preserves previously persisted job identities when another captured job fails" do
    existing = CronSafetyJob.perform_later
    provider_id = existing.provider_job_id

    entry = DecentJob::CronEntry.new(key: :existing, cron: "@daily", class: -> {
      ActiveJob.perform_all_later(existing, CronSafetyJob.new(Object.new))
    }, database: database)

    _ { entry.enqueue }.must_raise ActiveJob::SerializationError
    _(existing.provider_job_id).must_equal provider_id
    _(existing.successfully_enqueued?).must_equal true
    _(DecentJob::Store.new(database).find(provider_id)).wont_be_nil
    _(entry.jobs).must_equal []
  end

  it "raises controlled job database errors without poisoning a surrounding transaction" do
    entry = DecentJob::CronEntry.new(key: :controlled_constraint, cron: "@daily", class: CronSafetyControlledJob, database: database)
    database.with_connection { |connection| connection.exec("CREATE UNIQUE INDEX unique_controlled_queue_test ON decent_jobs(queue_name)") }
    entry.enqueue(@at)

    database.transaction do |connection|
      error = _ { entry.enqueue(@at + 1) }.must_raise ActiveJob::EnqueueError
      _(error.cause).must_be_kind_of PG::UniqueViolation
      _(connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)).must_equal "1"
    end

    _(entry.enqueue(@at)).must_equal false
  end

  it "rolls back accepted plain jobs when a subsequent controlled job encounters a database error" do
    jobs = []
    database.with_connection { |connection| connection.exec("CREATE UNIQUE INDEX unique_mixed_queue_test ON decent_jobs(queue_name)") }

    entry = DecentJob::CronEntry.new(key: :mixed_constraint, cron: "@daily", class: -> {
      jobs << CronSafetyJob.perform_later
      jobs << CronSafetyControlledJob.perform_later
    }, database: database)

    error = _ { entry.enqueue }.must_raise ActiveJob::EnqueueError
    _(error.cause).must_be_kind_of PG::UniqueViolation
    _(entry.jobs).must_equal []
    _(jobs.map(&:successfully_enqueued?)).must_equal [false, false]
    _(jobs.map(&:provider_job_id)).must_equal [nil, nil]
    _(DecentJob::CronEntry.current_context).must_be_nil
  end

  it "clears returned identities of nested cron entries when the enclosing entry rolls back" do
    inner = DecentJob::CronEntry.new(key: :inner_rollback, cron: "@daily", class: CronSafetyJob, database: database)
    job = nil

    outer = DecentJob::CronEntry.new(key: :outer_rollback, cron: "@daily", class: -> {
      job = inner.enqueue(@at)
      raise "abort outer entry"
    }, database: database)

    _ { outer.enqueue(@at) }.must_raise RuntimeError
    _(inner.jobs).must_equal []
    _(job.provider_job_id).must_be_nil
    _(job.successfully_enqueued?).must_equal false
    _(inner.enqueue(@at)).must_be_kind_of CronSafetyJob
  end

  it "retains persisted cron identities when an after-commit callback raises" do
    job = nil

    entry = DecentJob::CronEntry.new(key: :after_commit, cron: "@daily", class: -> {
      job = CronSafetyJob.perform_later
      database.after_commit { raise "already committed" }
      job
    }, database: database)

    _ { entry.enqueue(@at) }.must_raise RuntimeError
    _(job.provider_job_id).wont_be_nil
    _(job.successfully_enqueued?).must_equal true
    _(entry.jobs.map { |row| row.fetch("id") }).must_equal [job.provider_job_id]
  end
end
