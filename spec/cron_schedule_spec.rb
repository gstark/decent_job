# frozen_string_literal: true

require "spec_helper"
require "time"
require "decent_job/cron_schedule"

describe "DecentJob::CronSchedule" do
  it "finds an exclusive next occurrence and preserves the expression" do
    cron = DecentJob::CronSchedule.new("  */15 9-17 * * mon-fri UTC  ")
    _(cron.original).must_equal "  */15 9-17 * * mon-fri UTC  "
    _(cron.next_at(after: Time.utc(2026, 10, 9, 17, 45))).must_equal Time.utc(2026, 10, 12, 9)
  end

  it "accepts capitalized weekdays without mistaking them for a timezone" do
    cron = DecentJob::CronSchedule.new("0 9 * * MON-FRI")
    _(cron.match?(Time.local(2026, 10, 9, 9))).must_equal true
    _(DecentJob::CronSchedule.new("0 0 9 * * MON-FRI").match?(Time.local(2026, 10, 9, 9))).must_equal true
  end

  it "bounds searches for contradictory day restrictions" do
    cron = DecentJob::CronSchedule.new("0 0 1 * mon#2& UTC")
    _ { cron.next_at(after: Time.utc(2026, 1, 1)) }.must_raise ArgumentError
  end

  it "calculates month days across ambiguous midnight boundaries" do
    cron = DecentJob::CronSchedule.new("0 0 8 * * America/Havana")
    _(cron.next_at(after: Time.utc(2026, 10, 1))).must_equal Time.utc(2026, 10, 8, 4)
  end

  it "chooses the first local midnight during a fold and returns UTC" do
    previous = ENV["TZ"]
    ENV["TZ"] = "America/Havana"
    cron = DecentJob::CronSchedule.new("0 0 1 * *")
    result = cron.next_at(after: Time.utc(2026, 10, 31, 12))
    _(result).must_equal Time.utc(2026, 11, 1, 4)
    _(result.utc?).must_equal true
  ensure
    ENV["TZ"] = previous
  end

  it "returns UTC after a calendar jump in a fixed-offset timezone" do
    cron = DecentJob::CronSchedule.new("0 0 1 * * +05:30")
    result = cron.next_at(after: Time.utc(2026, 10, 15))
    _(result).must_equal Time.utc(2026, 10, 31, 18, 30)
    _(result.utc?).must_equal true
  end

  it "observes process timezone changes after parsing a local schedule" do
    previous = ENV["TZ"]
    ENV["TZ"] = "UTC"
    cron = DecentJob::CronSchedule.new("0 0 1 * *")
    ENV["TZ"] = "America/Havana"
    _(cron.next_at(after: Time.utc(2026, 10, 31, 12))).must_equal Time.utc(2026, 11, 1, 4)
  ensure
    ENV["TZ"] = previous
  end

  it "finds the first valid second in a local midnight gap" do
    previous = ENV["TZ"]
    ENV["TZ"] = "Africa/Monrovia"
    cron = DecentJob::CronSchedule.new("* * * 7 jan *")
    _(cron.next_at(after: Time.utc(1972, 1, 6))).must_equal Time.utc(1972, 1, 7, 0, 44, 30)
  ensure
    ENV["TZ"] = previous
  end

  it "chooses the first midnight with a POSIX process timezone" do
    previous = ENV["TZ"]
    ENV["TZ"] = "CST5CDT,M3.2.0/0,M11.1.0/1"
    cron = DecentJob::CronSchedule.new("0 0 1 * *")
    _(cron.next_at(after: Time.utc(2026, 10, 31, 12))).must_equal Time.utc(2026, 11, 1, 4)
  ensure
    ENV["TZ"] = previous
  end

  it "finds the exact gap boundary with a POSIX process timezone" do
    previous = ENV["TZ"]
    ENV["TZ"] = "XST0XDT-0:30,M3.2.6/23:45,M11.1.0/1"
    cron = DecentJob::CronSchedule.new("* * * 15 mar *")
    _(cron.next_at(after: Time.utc(2026, 3, 14))).must_equal Time.utc(2026, 3, 14, 23, 45)
  ensure
    ENV["TZ"] = previous
  end

  it "supports seconds and excludes both range boundaries" do
    cron = DecentJob::CronSchedule.new("*/10 * * * * * UTC")
    start = Time.utc(2026, 10, 9)
    _(cron.within(start..(start + 30))).must_equal [start + 10, start + 20]
    _(cron.next_at(after: start + 0.5)).must_equal start + 10
  end

  {
    "0 9 , * * * UTC" => [Time.utc(2026, 10, 1), Time.utc(2026, 10, 1, 9)],
    "0 , 15 9 * * * UTC" => [Time.utc(2026, 10, 1), Time.utc(2026, 10, 1, 9, 15)],
    "0 9 * * * , UTC" => [Time.utc(2026, 10, 1), Time.utc(2026, 10, 1, 9)],
    "0, ,15 9 * * * UTC" => [Time.utc(2026, 10, 1, 9), Time.utc(2026, 10, 1, 9, 15)],
    "0 , , 15 9 * * * UTC" => [Time.utc(2026, 10, 1), Time.utc(2026, 10, 1, 9, 15)],
    "0 * -1 * * Australia/Lord_Howe" => [Time.utc(2003, 3, 22, 15, 37, 18), Time.utc(2003, 3, 30, 13, 30)],
    "* * * 7 jan * Africa/Monrovia" => [Time.utc(1972, 1, 6), Time.utc(1972, 1, 7, 0, 44, 30)],
    "0 1 * * 0 America/Havana" => [Time.utc(2026, 3, 6), Time.utc(2026, 3, 8, 5)],
    "0 0 29 feb * UTC" => [Time.utc(2025, 3, 1), Time.utc(2028, 2, 29)],
    "0 0 L * * UTC" => [Time.utc(2026, 2, 1), Time.utc(2026, 2, 28)],
    "0 0 -2 * * UTC" => [Time.utc(2026, 2, 1), Time.utc(2026, 2, 27)],
    "0 0 * * fri#last UTC" => [Time.utc(2026, 10, 1), Time.utc(2026, 10, 30)],
    "0 0 * * mon#2 UTC" => [Time.utc(2026, 10, 1), Time.utc(2026, 10, 12)],
    "0 0 13 * fri UTC" => [Time.utc(2026, 10, 10), Time.utc(2026, 10, 13)],
    "0 0 13 * fri& UTC" => [Time.utc(2026, 10, 10), Time.utc(2026, 11, 13)],
    "0 0 * * mon%2 UTC" => [Time.utc(2026, 10, 1), Time.utc(2026, 10, 12)],
    "0 0 * * mon%2+1 UTC" => [Time.utc(2026, 10, 1), Time.utc(2026, 10, 5)],
    "@yearly UTC" => [Time.utc(2026, 10, 1), Time.utc(2027, 1, 1)],
    "0 0 * * * -00:30" => [Time.utc(2026, 10, 1), Time.utc(2026, 10, 1, 0, 30)],
    "0 0 * * * +14:00" => [Time.utc(2026, 10, 1), Time.utc(2026, 10, 1, 10)],
    "@noon +05:30" => [Time.utc(2026, 10, 1), Time.utc(2026, 10, 1, 6, 30)],
    "0 24 * * * UTC" => [Time.utc(2026, 10, 1), Time.utc(2026, 10, 2)],
    "0 22-2 * * * UTC" => [Time.utc(2026, 10, 1, 23), Time.utc(2026, 10, 2)],
    "0 0 * * 5-1/2 UTC" => [Time.utc(2026, 10, 1), Time.utc(2026, 10, 2)],
    "30 2 * * * America/New_York" => [Time.utc(2026, 3, 8, 5), Time.utc(2026, 3, 9, 6, 30)]
  }.each do |expression, (after, expected)|
    it "calculates #{expression.inspect}" do
      _(DecentJob::CronSchedule.new(expression).next_at(after: after)).must_equal expected
    end
  end

  {"UTC" => Time.utc(2026, 11, 1, 6, 30), "America/New_York" => Time.utc(2026, 11, 2, 6, 30)}.each do |timezone, expected|
    it "matches DST repeat suppression when the process timezone is #{timezone}" do
      previous = ENV["TZ"]
      ENV["TZ"] = timezone
      cron = DecentJob::CronSchedule.new("30 1 * * * America/New_York")
      _(cron.next_at(after: Time.utc(2026, 11, 1, 5, 30))).must_equal expected
    ensure
      ENV["TZ"] = previous
    end
  end

  [nil, 42, "", "0 ,9 * * *", "0 ,15 9 * * *", "0 , ,15 9 * * *", "* * *", "61 * * * *", "0 0 30 feb *", "0 0 * * * Missing/Zone", "*/0 * * * *", "0 0 * * mon%0", "0 0 * * mon#6", "0 0 * * * +24:00", "0 0 * * * UTCextra", "0 0 * * * America/New_Yorkextra"].each do |expression|
    it "rejects #{expression.inspect}" do
      _(DecentJob::CronSchedule.parse(expression)).must_be_nil
      _ { DecentJob::CronSchedule.new(expression) }.must_raise ArgumentError
    end
  end
end
