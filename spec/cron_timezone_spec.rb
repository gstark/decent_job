# frozen_string_literal: true

require "minitest/mock"
require "spec_helper"

describe "implicit cron timezones" do
  before do
    @previous_environment_zone = ENV.delete("TZ")
    @previous_time_zone = Time.zone
  end

  after do
    ENV["TZ"] = @previous_environment_zone
    Time.zone = @previous_time_zone
  end

  it "uses the Active Support timezone when no environment timezone is configured" do
    Time.use_zone("Pacific/Honolulu") do
      entry = DecentJob::CronEntry.new(key: :noon, cron: "0 12 * * *")
      _(entry.next_at(after: Time.utc(2026, 10, 9))).must_equal Time.utc(2026, 10, 9, 22)
      _(entry.within(Time.utc(2026, 10, 9)..Time.utc(2026, 10, 11))).must_equal [Time.utc(2026, 10, 9, 22), Time.utc(2026, 10, 10, 22)]
    end
  end

  it "uses the same Active Support timezone for calendar jumps and DST gaps" do
    ENV["TZ"] = "XST0"
    Time.use_zone("America/New_York") do
      entry = DecentJob::CronEntry.new(key: :morning, cron: "30 2 * * *")
      _(entry.next_at(after: Time.utc(2026, 3, 8))).must_equal Time.utc(2026, 3, 9, 6, 30)
      _(entry.within(Time.utc(2026, 3, 7)..Time.utc(2026, 3, 10))).must_equal [Time.utc(2026, 3, 7, 7, 30), Time.utc(2026, 3, 9, 6, 30)]
      calendar = DecentJob::CronEntry.new(key: :calendar, cron: "0 0 9 3 *")
      _(calendar.next_at(after: Time.utc(2026, 3, 1))).must_equal Time.utc(2026, 3, 9, 4)
    end
  end

  it "prefers a recognized environment timezone over the Active Support timezone" do
    ENV["TZ"] = "Asia/Tokyo"
    Time.use_zone("Pacific/Honolulu") do
      entry = DecentJob::CronEntry.new(key: :noon, cron: "0 12 * * *")
      _(entry.next_at(after: Time.utc(2026, 10, 9))).must_equal Time.utc(2026, 10, 9, 3)
    end
  end

  it "keeps explicit schedule timezones authoritative" do
    ENV["TZ"] = "Asia/Tokyo"
    Time.use_zone("Pacific/Honolulu") do
      entry = DecentJob::CronEntry.new(key: :noon, cron: "0 12 * * * Europe/Paris")
      _(entry.next_at(after: Time.utc(2026, 10, 9))).must_equal Time.utc(2026, 10, 9, 10)
    end
  end

  it "resolves the timezone of each thread when reusing a parsed schedule" do
    schedule = DecentJob::CronSchedule.parse("0 12 * * *")
    ready = Queue.new
    proceed = Queue.new
    threads = ["Pacific/Honolulu", "Asia/Tokyo"].map do |zone|
      Thread.new do
        Time.use_zone(zone) do
          ready << true
          proceed.pop
          schedule.next_at(after: Time.utc(2026, 10, 9))
        end
      end
    end
    2.times { ready.pop }
    2.times { proceed << true }
    _(threads.map(&:value)).must_equal [Time.utc(2026, 10, 9, 22), Time.utc(2026, 10, 9, 3)]
  ensure
    threads&.each { |thread| thread.kill if thread.alive? }
    threads&.each(&:join)
  end

  it "uses the Active Support timezone when ENV TZ is not a recognized identifier" do
    ENV["TZ"] = "XST0XDT-0:30,M3.2.6/23:45,M11.1.0/1"
    Time.use_zone("Pacific/Honolulu") do
      entry = DecentJob::CronEntry.new(key: :noon, cron: "0 12 * * *")
      _(entry.next_at(after: Time.utc(2026, 10, 9))).must_equal Time.utc(2026, 10, 9, 22)
    end
  end
  [nil, "Pacific/Honolulu"].each do |application_zone|
    {"Z" => 0, "+05:30" => 19_800, "-05:30" => -19_800}.each do |environment_zone, offset|
      it "honors ENV TZ #{environment_zone} with application zone #{application_zone.inspect}" do
        ENV["TZ"] = environment_zone
        Time.zone = application_zone
        entry = DecentJob::CronEntry.new(key: :fixed, cron: "0 12 * * *")
        _(entry.next_at(after: Time.utc(2026, 1, 1))).must_equal Time.utc(2026, 1, 1, 12) - offset
        calendar = DecentJob::CronEntry.new(key: :calendar, cron: "0 0 2 1 *")
        _(calendar.next_at(after: Time.utc(2026, 1, 1))).must_equal Time.utc(2026, 1, 2) - offset
      end
    end
  end
  ["30 1 * * *", "30 1 * * * America/New_York"].each do |expression|
    it "uses the implicit local origin for a default next occurrence during a fold in #{expression}" do
      ENV["TZ"] = "XST0"
      Time.use_zone("America/New_York") do
        instant = Time.utc(2026, 11, 1, 5, 30)
        entry = DecentJob::CronEntry.new(key: :fold, cron: expression)
        Time.stub(:now, instant) do
          _(entry.next_at).must_equal Time.utc(2026, 11, 2, 6, 30)
          _(DecentJob::CronSchedule.parse(expression).next_at).must_equal Time.utc(2026, 11, 2, 6, 30)
        end
        _(entry.next_at(after: instant)).must_equal Time.utc(2026, 11, 1, 6, 30)
      end
    end
  end
  {"+14:00" => Time.utc(2026, 1, 1, 22), "-12:00" => Time.utc(2026, 1, 2), "-00:30" => Time.utc(2026, 1, 1, 12, 30), "+05:" => Time.utc(2026, 1, 1, 22)}.each do |environment_zone, expected|
    it "resolves ENV TZ #{environment_zone} consistently with fixed-offset bounds and malformed fallback" do
      ENV["TZ"] = environment_zone
      Time.use_zone("Pacific/Honolulu") do
        entry = DecentJob::CronEntry.new(key: :offset_bounds, cron: "0 12 * * *")
        _(entry.next_at(after: Time.utc(2026, 1, 1))).must_equal expected
      end
    end
  end

  it "keeps the calendar day of a positive fourteen-hour environment offset" do
    ENV["TZ"] = "+14:00"
    Time.use_zone("Pacific/Honolulu") do
      entry = DecentJob::CronEntry.new(key: :offset_day, cron: "0 12 2 1 *")
      _(entry.next_at(after: Time.utc(2026, 1, 1))).must_equal Time.utc(2026, 1, 1, 22)
    end
  end
end
