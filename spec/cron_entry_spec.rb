# frozen_string_literal: true

require "spec_helper"

class CronEntryJob < ActiveJob::Base
  def perform(*)
  end
end

describe "cron entries" do
  it "validates definitions without opening the database or evaluating callable properties" do
    callable = ->(*) { raise "evaluated" }
    entry = DecentJob::CronEntry.new(key: :daily, cron: callable, class: callable, args: callable, kwargs: callable, set: callable)
    _(entry.valid?).must_equal true
    _(entry.key).must_equal :daily
    _(entry.id).must_equal :daily
    _(entry.to_param).must_equal :daily
    _(entry.display_schedule).must_equal "Lambda/Callable"
    _(entry.display_properties.values_at(:class, :args, :kwargs, :set)).must_equal ["Lambda/Callable"] * 4
    _(entry.display_properties[:description]).must_equal "None"
  end

  it "reports all malformed fields without raising" do
    entry = DecentJob::CronEntry.new(key: "invalid", cron: Object.new, class: 123, args: "bad", kwargs: [], set: 1)
    _(entry.valid?).must_equal false
    _(entry.errors.keys.sort).must_equal %i[args class cron key kwargs set]
    _(DecentJob::CronEntry.new(key: :valid, cron: "@daily", class: "MissingCronClass").valid?).must_equal false
    _(DecentJob::CronEntry.new(key: :valid, cron: "@daily", class: "CronEntryJob").valid?).must_equal true
    _(DecentJob::CronEntry.new(key: :valid, cron: "@daily").valid?).must_equal true
  end

  it "calculates schedules and displays normalized natural schedules" do
    entry = DecentJob::CronEntry.new(key: :daily, cron: "every day at noon UTC", class: CronEntryJob)
    start = Time.utc(2026, 1, 1)
    _(entry.display_schedule).must_equal "0 12 * * * UTC"
    _(entry.next_at(after: start)).must_equal Time.utc(2026, 1, 1, 12)
    _(entry.within(start..Time.utc(2026, 1, 2, 12))).must_equal [Time.utc(2026, 1, 1, 12)]
  end

  it "passes previous occurrence to callable schedules and handles expressions and times" do
    previous = Time.utc(2026, 1, 1)
    received = []

    entry = DecentJob::CronEntry.new(key: :daily, cron: ->(last) {
      received << last
      "@daily UTC"
    })

    _(entry.next_at(previously_at: previous, after: previous)).must_equal previous + 86400
    _(entry.within(previous..(previous + 172800), previously_at: previous)).must_equal [previous + 86400]
    _(received).must_equal [previous, previous]
    exact = DecentJob::CronEntry.new(key: :exact, cron: ->(_) { previous })
    _(exact.next_at(previously_at: previous)).must_equal previous
    _(exact.within(previous..(previous + 100), previously_at: previous)).must_equal []
    invalid = DecentJob::CronEntry.new(key: :invalid, cron: ->(_) { "invalid schedule" })
    _(invalid.next_at(previously_at: previous)).must_be_nil
  end

  it "builds and finds validated configuration entries without resolving a database" do
    configuration = DecentJob::Configuration.new
    configuration.cron = {daily: {cron: "@daily", class: "CronEntryJob"}}
    _(configuration.validate!).must_be_same_as configuration
    _(DecentJob::CronEntry.all(configuration: configuration).map(&:key)).must_equal [:daily]
    _(DecentJob::CronEntry.find("daily", configuration: configuration).job_class).must_equal "CronEntryJob"
    _ { DecentJob::CronEntry.find(:missing, configuration: configuration) }.must_raise KeyError
    configuration.cron = {daily: {cron: "no"}}
    _ { configuration.validate! }.must_raise ArgumentError
    configuration.cron = []
    _ { configuration.validate! }.must_raise ArgumentError
  end

  it "does not accept static Time schedules" do
    entry = DecentJob::CronEntry.new(key: :invalid, cron: Time.now)
    _(entry.valid?).must_equal false
    _(entry.next_at).must_be_nil
  end

  it "uses global configuration when an explicit nil configuration is supplied" do
    _(DecentJob::CronEntry.all(configuration: nil)).must_equal []
    _ { DecentJob::CronEntry.find(:absent, configuration: nil) }.must_raise KeyError
  end
end
