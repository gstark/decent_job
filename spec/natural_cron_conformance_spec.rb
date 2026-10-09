# frozen_string_literal: true

require "spec_helper"
require "decent_job/cron_schedule"

describe "natural cron reference conformance" do
  fixture = JSON.parse(File.read(File.expand_path("fixtures/natural_cron_reference.json", __dir__)))

  before do
    @previous_timezone = ENV["TZ"]
    ENV["TZ"] = fixture.fetch("reference").fetch("timezone")
  end

  after do
    ENV["TZ"] = @previous_timezone
  end

  fixture.fetch("cases").each do |entry|
    it "matches reference scheduling for #{entry.fetch("expression").inspect}" do
      schedule = DecentJob::CronSchedule.parse(entry.fetch("expression"))
      if entry.fetch("valid")
        _(schedule).wont_be_nil
        _(schedule.original).must_equal entry.fetch("original")
        cursor = Time.iso8601(entry.fetch("after"))
        actual = entry.fetch("next").map do
          cursor = schedule.next_at(after: cursor)
          cursor.utc.iso8601
        end
        _(actual).must_equal entry.fetch("next")
      else
        _(schedule).must_be_nil
      end
    end
  end
end
