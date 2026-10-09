# frozen_string_literal: true

require "spec_helper"
require "decent_job/cron_schedule"

describe "DecentJob natural cron schedules" do
  {
    "every day" => "0 0 * * *",
    "every 15 minutes" => "*/15 * * * *",
    "every 10 seconds" => "*/10 * * * * *",
    "every 2 hours" => "0 */2 * * *",
    "every 3 days" => "0 0 */3 * *",
    "every 2 months" => "0 0 1 */2 *",
    "every week" => "0 0 * * 0",
    "every year" => "0 0 1 1 *",
    "every weekday" => "0 0 * * 1-5",
    "every Monday and Tuesday" => "0 0 * * 1,2",
    "every Fri-Sun" => "0 0 * * 5-0",
    "every twenty-first of the month" => "0 0 21 * *",
    "every 21th of the month" => "0 0 21 * *",
    "every 22th of the month" => "0 0 22 * *",
    "every 23th of the month" => "0 0 23 * *",
    "every second of the month" => "0 0 2 * *",
    "every month on days 1,15 at 10:00" => "0 10 1,15 * *",
    "on the 2nd and the 4th" => "0 0 2,4 * *",
    "on minute 5 and minute 20" => "5,20 * * * *",
    "on the hour" => "* 0 * * *",
    "on the minute" => "* * * * *",
    "at 4 at Five" => "0 4 * * *",
    "on minute 30 at Five" => "30 * * * *",
    "at 4:30 at Five thirty" => "30 4 * * *",
    "every hour at Five" => "0 * * * *",
    "at Five on the hour" => "0 0 * * *",
    "at five twenty pm" => "20 17 * * *",
    "at oh dark thirty" => "30 0 * * *",
    "at noon" => "0 12 * * *",
    "at midnight" => "0 0 * * *",
    "at 12 midnight" => "0 24 * * *",
    "at 5 and 8 pm" => "0 5,20 * * *",
    "at seconds 10,20" => "10,20 * * * * *",
    "at hour 3 and minutes 20" => "20 * * * *",
    "every 15 minutes from 9 to 17" => "*/15 9-16 * * *",
    "from 9:30 to 17:30" => "0 9-17 * * *",
    "from the first to the fifth" => "0 0 1-5 * *",
    "every day at 09:30 in America/New_York" => "30 9 * * * America/New_York",
    "UTC" => "* * * * * UTC",
    "3 UTC" => "0 3 * * * UTC",
    "every 2 hours and 5 minutes" => "0 */2 * * *",
    "every mon-fri and sun" => "0 0 * * 1-5",
    "at 9:30 and 17:00" => "30 9 * * *"
  }.each do |expression, expected|
    it "compiles #{expression.inspect}" do
      schedule = DecentJob::CronSchedule.parse(expression)
      _(schedule).must_be_instance_of DecentJob::CronSchedule
      _(schedule.original).must_equal expected
      after = Time.utc(2026, 10, 9, 12)
      _(schedule.next_at(after: after)).must_equal DecentJob::CronSchedule.new(expected).next_at(after: after)
    end
  end

  ["at Five", "at Five pm", "at 4 at Five pm", "every minute at 5", "at 5 every 15 minutes", "on minute 30 at 5", "at 0 minutes 0", "3 h 5", "3 hours 5", "3 hours 5 h 4", "3", "3 hours", "3h", "2026-10-09", "at Noon", "at five twenty-one", "every 2 weeks", "every Monday junk", "from 9:15 to 17:30", "every day " * 30].each do |expression|
    it "rejects #{expression.inspect}" do
      _(DecentJob::CronSchedule.parse(expression)).must_be_nil
      _ { DecentJob::CronSchedule.new(expression) }.must_raise ArgumentError
    end
  end
end
