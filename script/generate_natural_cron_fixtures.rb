# frozen_string_literal: true

# Run outside Bundler with the pinned Fugit/et-orbi sources on RUBYLIB.
# The reference gems are not needed by the normal specs or runtime library.
ENV["TZ"] = "UTC"

gem "raabro", "1.4.0"
require "fugit"
require "json"
require "time"

unless [Fugit::VERSION, EtOrbi::VERSION, Raabro::VERSION] == %w[1.12.1 1.4.0 1.4.0]
  abort "Use GoodJob's pinned Fugit 1.12.1, et-orbi 1.4.0, and raabro 1.4.0"
end

expressions = [
  "every second", "every minute", "every hour", "every day", "every month", "every week", "every year",
  "every 1 week", "every 1 year", "every weekday", "every monday", "every mon", "every Fri-Sun",
  "every Monday and Tuesday", "every mon or wed and fri", "every monday through friday",
  "every 1st of the month", "every second of the month", "every last of the month",
  "every first and fifteenth of the month", "every 2nd, 15th and 31st of the month",
  "every 21th of the month", "every 22th of the month", "every 23th of the month",
  "every day at noon", "every day at midnight", "every day at 2am", "every monday at 9am",
  "every weekday at 5pm", "every weekday at five", "every day at five thirty pm",
  "every day at eleven fifty-nine pm", "every day at oh dark thirty", "every day at five o'clock",
  "every day at twelve midnight", "every day at 12 midnight", "every day at 12 noon",
  "every day at 12am", "every day at 12pm", "every day at 24:00", "every day at 14:35",
  "every day at 9 and 17", "every day at 9:30 and 17:30", "every day at 9:30 and 17:45",
  "every day at 9:30, 12:45, and 17:30", "every day from 9 to 17", "every day from 9am to 5pm",
  "every 15 minutes from 9 to 17", "every hour at minute 10", "every hour on minute 10 and 30",
  "every hour on the minute", "every day on the hour", "every day on the minute",
  "every hour at minutes 5, 10 and 15", "every minute at seconds 5 and 35",
  "every day at hours 8 and 12", "every month on day 2 at 10:00", "every month on days 1,15 at 10:00",
  "every month on the 1st and the 15th", "every month from the 5th to the 10th at noon",
  "every week on monday at 18:23", "on monday at 18:23", "on days 1 and 15 at noon",
  "from monday to friday at noon", "monday to friday at noon", "from the first to the fifth at noon",
  "at noon", "noon", "at 9:30", "3pm", "every 5m", "every 2h", "every 2M",
  "every 2 hours on minute 15", "every 15 minutes and every 2 hours", "every 15 minutes, from 9 to 17, on mon-fri",
  "every day at 12:00 America/New_York", "every day at noon in Europe/Berlin",
  "every day at noon on Asia/Kathmandu", "every day at noon UTC", "every day at noon Z",
  "every day at noon +05:30", "every day at noon +05", "Every day at noon",
  "every\tday\tat\tnoon", "  every day at noon  ",
  "2017-12-12", "12y12M", "5m", "0", "10", "every 2 weeks", "every 2 years", "every fortnight",
  "3 h 5", "3 hours 5", "every minute at 5", "on minute 30 at 5", "at Noon",
  "at 4 at Five", "on minute 30 at Five",
  "every weekend", "every zero minutes", "every 0 seconds", "every 0 minutes", "every 100 minutes",
  "every day at 25:00", "every day at 09:99", "every day from 9:15 to 17:30",
  "every day at noon Mars/Olympus", "tomorrow at noon", "every day at", "nonsense",
  "every day at noon and", "every day garbage", "every day " * 40
]

[1, 2, 5, 15].each do |count|
  %w[seconds minutes hours days months sec min s m h d M].each { |unit| expressions << "every #{count} #{unit}" }
end

%w[first second third fourth fifth last].each do |ordinal|
  expressions << "every #{ordinal} of the month at noon"
end

expressions.uniq!

after = Time.utc(2026, 1, 1)

cases = expressions.map do |expression|
  record = {"expression" => expression, "after" => after.iso8601}

  begin
    reference = Fugit.parse(expression)
    record["valid"] = reference.instance_of?(Fugit::Cron)

    if record.fetch("valid")
      record["original"] = reference.original
      cursor = after

      record["next"] = 5.times.map do
        cursor = reference.next_time(cursor)
        cursor.to_t.utc.iso8601
      end
    end
  rescue => error
    record["valid"] = false
    record["reference_error"] = error.class.name
  end

  record
end

fixture = {"reference" => {"good_job" => "ad4be5c2", "fugit" => Fugit::VERSION,
                           "et_orbi" => EtOrbi::VERSION, "raabro" => Raabro::VERSION, "timezone" => ENV.fetch("TZ")},
           "cases" => cases}
File.write(File.expand_path("../spec/fixtures/natural_cron_reference.json", __dir__), JSON.pretty_generate(fixture) + "\n")
puts "Generated #{cases.size} natural schedule fixtures (#{cases.count { |entry| entry.fetch("valid") }} accepted)"
