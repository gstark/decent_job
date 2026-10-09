# frozen_string_literal: true

# Run outside Bundler with the reference gems on RUBYLIB. These gems are oracle
# tooling only; neither DecentJob nor its normal test suite requires them.
# ruby -I/path/to/fugit-1.12.1/lib -I/path/to/et-orbi-1.4.0/lib script/generate_cron_fixtures.rb
ENV["TZ"] = "UTC"

gem "raabro", "1.4.0"
require "fugit"
require "json"
require "time"

unless [Fugit::VERSION, EtOrbi::VERSION, Raabro::VERSION] == %w[1.12.1 1.4.0 1.4.0]
  abort "Use GoodJob's pinned Fugit 1.12.1, et-orbi 1.4.0, and raabro 1.4.0"
end

expressions = [
  "* * * * *", "*/15 * * * *", "15/30 * * * *", "/20 * * * *",
  "1,5,30 2,9,17 * * *", "50-10/5 * * * *", "0 22-2 * * *", "0 24 * * *",
  "0 0 * nov-feb *", "0 0 * * fri-mon", "0 0 * * 5-1/2", "0 0 * * 5/2",
  "0 0 * * 7", "0 0 * * 0,7", "0 0 * * *,mon", "0 0 1-7 * mon", "0 0 1-7 * mon&",
  "0 0 1-7& * mon", "0 0 */2 * 1-5", "0 0 */2& * 1-5",
  "0 0 L * *", "0 0 last * *", "0 0 -2 * *", "0 0 -7-L * *", "0 0 -7--1/2 * *",
  "0 0 29 feb *", "0 0 31 * *", "0 0 * * mon#1", "0 0 * * fri#4,fri#5",
  "0 0 * * mon#-1", "0 0 * * fri#last", "0 0 * * sun#L", "0 0 * * mon#2,tue",
  "0 0 * * sun%2", "0 0 * * mon%4+1", "0 0 * * mon%2,wed%3+1",
  "*/10 * * * * *", "5,35 */2 * * * *", "0 0 0 1 jan *",
  "@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly",
  "@yearly Asia/Kuala_Lumpur", "@monthly Asia/Jakarta",
  "0 9 * * * America/New_York", "0 9 * * * Asia/Kathmandu", "0 9 * * * +05:45",
  "0 9 * * * -0330", "0 9 * * * UTC", "0 9 * * * Etc/GMT+5",
  "  0 9 * * MON-FRI  ", "0,,15, 9 * * *",
  "59 * * * * *", "59-0 * * * * *", "/99 * * * * *", ",0,,59, * * * * *",
  "0 23-24 * * *", "0 24-2 * * *", "0 23-24/2 * * *", "0 0-24/3 * * *",
  "0 0 -31 * *", "0 0 -1--7 * *", "0 0 2--10 * *", "0 0 1-31/15 * *",
  "0 0 * DEC-FEB/2 *", "0 0 * 12-1/3 *", "0 0 * * *,mon#1", "0 0 * * 0,mon#2",
  "0 0 * * mon%2+3", "0 0 * * fri#L,mon#1", "0 0 * * sun-sat/2", "@noon",
  "0 9 , * * *", "0 , 15 9 * * *", "0 9 * * * ,"
]

cases = expressions.map { |expression| {"expression" => expression, "after" => "2026-01-01T00:00:00Z"} }
[
  ["30 2 * * * America/New_York", "2026-03-07T00:00:00Z"],
  ["30 1 * * * America/New_York", "2026-10-31T00:00:00Z"],
  ["*/15 1 * * * America/New_York", "2026-11-01T04:55:00Z"],
  ["0 2 * * * Europe/Berlin", "2026-10-24T00:00:00Z"],
  ["15 2 * * * Australia/Lord_Howe", "2026-10-03T00:00:00Z"],
  ["45 1 * * * Australia/Lord_Howe", "2026-04-04T00:00:00Z"],
  ["0 0 * * * Pacific/Apia", "2011-12-29T00:00:00Z"],
  ["0 0 29 feb *", "2096-03-01T00:00:00Z"],
  ["* * * * * *", "2026-01-01T00:00:00.75Z"]
].each { |expression, after| cases << {"expression" => expression, "after" => after} }

cases.each do |entry|
  cron = Fugit::Cron.parse(entry.fetch("expression")) || abort("Invalid fixture: #{entry}")
  cursor = Time.iso8601(entry.fetch("after"))
  entry["next"] = 5.times.map do
    cursor = cron.next_time(cursor)
    cursor.to_t.utc.iso8601
  end
end

within = [
  ["* * * * *", "2026-01-01T00:00:00Z", "2026-01-01T00:03:00Z"],
  ["*/10 * * * * *", "2026-01-01T00:00:00.5Z", "2026-01-01T00:00:40Z"],
  ["30 1 * * * America/New_York", "2026-10-31T00:00:00Z", "2026-11-03T00:00:00Z"]
].map do |expression, first, last|
  cron = Fugit::Cron.parse(expression)
  {"expression" => expression, "begin" => first, "end" => last,
   "times" => cron.within(Time.iso8601(first)..Time.iso8601(last)).map { |time| time.to_t.utc.iso8601 }}
end

invalid = [nil, 42, "", "* * * *", "* * * * * * *", "60 * * * *", "0 25 * * *",
  "0 0 0 * *", "0 0 32 * *", "0 0 * 13 *", "0 0 * * 8", "0 0 * * mon#6",
  "0 0 * * mon#0", "0 0 * * mon%-2", "0 0 * * mon%2-1", "0 0 * * MONDAY",
  "0 0 * january *", "0 0 * * * Mars/Olympus", "2017-12-12", "12y12M", "@reboot",
  "0 0 31 feb *", "0 0 30 feb *"]
invalid.each { |expression| abort "Expected invalid: #{expression.inspect}" if Fugit::Cron.parse(expression) }

fixture = {"reference" => {"good_job" => "ad4be5c2", "fugit" => Fugit::VERSION,
                           "et_orbi" => EtOrbi::VERSION, "raabro" => Raabro::VERSION, "timezone" => ENV.fetch("TZ")},
           "next" => cases, "within" => within, "invalid" => invalid}
directory = File.expand_path("../spec/fixtures", __dir__)
Dir.mkdir(directory) unless Dir.exist?(directory)
File.write(File.join(directory, "cron_reference.json"), JSON.pretty_generate(fixture) + "\n")
puts "Generated #{cases.size} next-time, #{within.size} range, and #{invalid.size} invalid-expression fixtures"
