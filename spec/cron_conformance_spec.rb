# frozen_string_literal: true

require "spec_helper"
require "decent_job/cron_schedule"

describe "cron reference conformance" do
  fixture = JSON.parse(File.read(File.expand_path("fixtures/cron_reference.json", __dir__)))

  before do
    @previous_timezone = ENV["TZ"]
    ENV["TZ"] = fixture.fetch("reference").fetch("timezone")
  end

  after do
    ENV["TZ"] = @previous_timezone
  end

  fixture.fetch("next").each do |entry|
    it "matches reference occurrences for #{entry.fetch("expression").inspect} after #{entry.fetch("after")}" do
      schedule = DecentJob::CronSchedule.parse(entry.fetch("expression"))
      _(schedule).wont_be_nil
      cursor = Time.iso8601(entry.fetch("after"))

      actual = entry.fetch("next").map do
        cursor = schedule.next_at(after: cursor)
        cursor.utc.iso8601
      end

      _(actual).must_equal entry.fetch("next")
    end
  end

  fixture.fetch("within").each do |entry|
    it "matches reference range boundaries for #{entry.fetch("expression").inspect} from #{entry.fetch("begin")}" do
      schedule = DecentJob::CronSchedule.new(entry.fetch("expression"))
      first = Time.iso8601(entry.fetch("begin"))
      last = Time.iso8601(entry.fetch("end"))
      _(schedule.within(first..last).map { |time| time.utc.iso8601 }).must_equal entry.fetch("times")
      _(schedule.within(first...last).map { |time| time.utc.iso8601 }).must_equal entry.fetch("times")
    end
  end

  fixture.fetch("invalid").each do |expression|
    it "rejects #{expression.inspect}" do
      _(DecentJob::CronSchedule.parse(expression)).must_be_nil
    end
  end
end
