# frozen_string_literal: true

require "spec_helper"

class AdapterParityJob < ActiveJob::Base
  class_attribute :events, default: []

  def perform(value)
    self.class.events << [value, locale, timezone, I18n.locale, Time.zone.name, job_id, provider_job_id]
  end
end

# Reference: GoodJob ad4be5c2, spec/lib/good_job/adapter_spec.rb:130,193,211
# and spec/integration/adapter_spec.rb:31. Duplicate-input cases additionally
# verify DecentJob's documented inserted-row count adaptation: GoodJob maps a
# returned identity to every matching input object and can overcount it.
describe "adapter parity with the pinned GoodJob contract" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    AdapterParityJob.queue_adapter = @adapter
    AdapterParityJob.events = []
    @store = DecentJob::Store.new(database)
  end

  it "treats a nil enqueue_at timestamp as immediate" do
    job = AdapterParityJob.new("immediate")
    @adapter.enqueue_at(job, nil)
    _(job.successfully_enqueued?).must_equal true
    _(@store.find(job.provider_job_id).fetch("scheduled_at").to_f).must_be_close_to Time.now.to_f, 1
  end

  it "skips existing identities in a bulk insert while accepting fresh jobs" do
    existing = AdapterParityJob.perform_later("existing")
    duplicate = AdapterParityJob.deserialize(existing.serialize)
    duplicate.provider_job_id = nil
    duplicate.successfully_enqueued = true
    fresh = AdapterParityJob.new("fresh")
    fresh.enqueue_error = ActiveJob::EnqueueError.new("earlier failure")

    _(@adapter.enqueue_all([duplicate, fresh])).must_equal 1
    _(duplicate.successfully_enqueued?).must_equal false
    _(duplicate.provider_job_id).must_be_nil
    _(fresh.successfully_enqueued?).must_equal true
    _(fresh.enqueue_error).must_be_nil
    _(@store.find(existing.provider_job_id).fetch("payload").fetch("arguments")).must_equal ["existing"]
    _(@store.find(fresh.provider_job_id).fetch("payload").fetch("arguments")).must_equal ["fresh"]
  end

  it "counts inserted rows when distinct bulk instances share a job ID" do
    first = AdapterParityJob.new("first")
    duplicate = AdapterParityJob.deserialize(first.serialize)
    fresh = AdapterParityJob.new("fresh")
    _(@adapter.enqueue_all([first, duplicate, fresh])).must_equal 2
    _([first, duplicate, fresh].map(&:successfully_enqueued?)).must_equal [true, false, true]
    _(duplicate.provider_job_id).must_be_nil
    _(@store.find(first.provider_job_id).fetch("payload").fetch("arguments")).must_equal ["first"]
  end

  it "retains success for one object repeated in a bulk array" do
    job = AdapterParityJob.new("same object")
    _(@adapter.enqueue_all([job, job])).must_equal 1
    _(job.successfully_enqueued?).must_equal true
    _(@store.find(job.provider_job_id).fetch("active_job_id")).must_equal job.job_id
  end

  it "keeps single enqueue duplicate rejection" do
    job = AdapterParityJob.perform_later("original")
    _ { @adapter.enqueue(job) }.must_raise ActiveJob::EnqueueError
    _(@store.find(job.provider_job_id).fetch("payload").fetch("arguments")).must_equal ["original"]
  end

  [:single, :bulk].each do |kind|
    it "persists and restores locale and timezone through #{kind} execution" do
      original_locale = I18n.locale
      original_zone = Time.zone
      available = I18n.available_locales
      I18n.available_locales = (available + [:fr]).uniq

      job = I18n.with_locale(:fr) do
        Time.use_zone("Pacific/Honolulu") do
          if kind == :single
            AdapterParityJob.perform_later("localized")
          else
            AdapterParityJob.new("localized").tap { |value| @adapter.enqueue_all([value]) }
          end
        end
      end

      payload = @store.find(job.provider_job_id).fetch("payload")
      _(payload.values_at("locale", "timezone", "job_id", "provider_job_id")).must_equal ["fr", "Pacific/Honolulu", job.job_id, job.provider_job_id]

      I18n.with_locale(:en) do
        Time.use_zone("UTC") do
          _(DecentJob::Performer.new(database: database).perform_one.state).must_equal "succeeded"
          _(I18n.locale).must_equal :en
          _(Time.zone.name).must_equal "UTC"
        end
      end

      _(AdapterParityJob.events).must_equal [["localized", "fr", "Pacific/Honolulu", :fr, "Pacific/Honolulu", job.job_id, job.provider_job_id]]
      _(I18n.locale).must_equal original_locale
      _(Time.zone).must_be_same_as original_zone
    ensure
      I18n.available_locales = available
    end
  end
end
