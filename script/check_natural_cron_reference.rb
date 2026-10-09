# frozen_string_literal: true

# Optional natural-language comparison. Run outside Bundler with the pinned
# reference sources on RUBYLIB; normal specs use saved fixtures instead.
gem "raabro", "1.4.0"
require "fugit"
$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "decent_job/cron_schedule"

unless [Fugit::VERSION, EtOrbi::VERSION, Raabro::VERSION] == %w[1.12.1 1.4.0 1.4.0]
  abort "Use GoodJob's pinned Fugit 1.12.1, et-orbi 1.4.0, and raabro 1.4.0"
end
ENV["TZ"] = "UTC"
expressions = []
units = %w[second seconds sec s minute minutes min m hour hours h day days d month months M week year weekday]
expressions += units.product(%w[1 2 3 05 0 60 61]).map { |unit, count| "every #{count} #{unit}" }
expressions += (1..31).flat_map { |n| %w[st nd rd th].map { |suffix| "every #{n}#{suffix} of the month" } }
expressions += %w[monday mon tuesday tue wednesday wed thursday thu friday fri saturday sat sunday sun].product(%w[monday tue fri sunday], ["-", " through ", " to ", " and ", " or ", ",", ", and "]).map { |a, b, j| "every #{a}#{j}#{b}" }
clauses = ["every minute", "every 15 minutes", "every hour", "every 2 hours", "every day", "every month", "every week", "every year", "every weekday", "on the hour", "on the minute", "on minute 30", "at 5", "at 9:30", "at minutes 5,20", "at seconds 10,20", "at hour 5", "from 9 to 17", "from 9:30 to 17:30", "on days 1,15", "on monday", "on the last", "UTC"]
expressions += clauses.product(clauses).map { |a, b| "#{a} #{b}" }
%w[Five FIVE Noon NOON Midnight MIDNIGHT].product(clauses).each do |name, clause|
  expressions << "at #{name} #{clause}"
  expressions << "#{clause} at #{name}"
end
expressions += ["3", "3h", "3 h", "3 hours", "3h5", "3 h 5", "3 hours 5", "3 and 5", "3,5", "3 and 5s", "3 h 5 h", "at 3 h 5", "3 h 5 UTC"]
expressions += %w[one two three four five six seven eight nine ten eleven twelve zero oh midnight noon].product(["", " am", " pm", " AM", " PM", " noon", " midnight", " midday"], ["", " twenty", " thirty", " o'clock", " dark thirty", " twenty-one"]).map { |a, b, c| "at #{a}#{c}#{b}" }
expressions += ["every first and last of the month", "every year and month", "every 1 year", "every day at 5, 5:30, 6", "every 2 hours at 9:15 and 17:30", "at 0 minutes 0", "3 hours 5", "at 5 on day 2 and monday"]
seed = Integer(ENV.fetch("CRON_SEED", "712"))
count = Integer(ENV.fetch("CRON_CASES", "10000"))
abort "CRON_CASES must be positive" unless count.positive?
random = Random.new(seed)
count.times do
  expressions << Array.new(random.rand(2..4)) { clauses.sample(random: random) }.join([" ", ", ", "  "].sample(random: random))
end
["on", "every"].each do |prefix|
  expressions += %w[first second third fourth last 21th 22nd].product([" and ", ",", ", or ", " or "], %w[first second last]).map { |a, j, b| (prefix == "on") ? "on the #{a}#{j}#{b}" : "every #{a}#{j}#{b} of the month" }
end
expressions += ["at seconds 0 and minutes 5", "at minutes 30 and hour 12", "at hour 12 and minutes 30", "at 5:30 and 6:30 and 5:45 and 6:45", "at 5:30 and 5:30 and 6:45", "every hour on minute 30 and 5", "at 5 every day on the hour"]
errors = []
expressions.uniq.each do |s|
  begin
    expected = Fugit.parse(s)
    expected = expected.instance_of?(Fugit::Cron) ? expected.original : nil
  rescue
    expected = nil
  end
  begin
    actual = DecentJob::CronSchedule.parse(s)&.original
  rescue => e
    actual = "ERROR #{e.class} #{e.message}"
  end
  errors << [s, expected, actual] unless expected == actual
end
errors.each { |error| warn error.inspect }
abort "#{errors.size} differences in #{expressions.uniq.size} cases (seed #{seed})" unless errors.empty?
puts "#{expressions.uniq.size} natural expressions match the pinned reference (seed #{seed})"
