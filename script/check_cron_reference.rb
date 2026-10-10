# frozen_string_literal: true

# Optional differential check, run outside Bundler with the pinned reference
# gems on RUBYLIB, as for generate_cron_fixtures.rb. No runtime dependency.
ENV["TZ"] = "UTC"

gem "raabro", "1.4.0"
require "fugit"
require "time"

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "decent_job/cron_schedule"

unless [Fugit::VERSION, EtOrbi::VERSION, Raabro::VERSION] == %w[1.12.1 1.4.0 1.4.0]
  abort "Use GoodJob's pinned Fugit 1.12.1, et-orbi 1.4.0, and raabro 1.4.0"
end

fields = [
  %w[* 0 59 */2 /15 2/7 45-12/2 50-59 0-59 1,5,42 ,1,,5, 01],
  %w[* 0 24 0-24 */27 23-2 22-24/2 24/3 01],
  %w[* 1 31 L last -31 -1 -1--5/2 28-3 1-31 1-L 3,-1 01],
  %w[* 1 12 JAN feb-dec dec-feb */4 2/5 01],
  %w[* 0 7 MON 7-0 7-7 6-2 2/2 6-2/2 */2 0-7/2 1#2 1#-2 mon%2 mon%3+1 *,1]
]
seed = Integer(ENV.fetch("CRON_SEED", "81"))
count = Integer(ENV.fetch("CRON_CASES", "1000"))
abort "CRON_CASES must be positive" unless count.positive?
random = Random.new(seed)
failures = []

count.times do
  expression = fields.map { |choices| choices.sample(random: random) }.join(" ") + " UTC"
  reference = Fugit::Cron.parse(expression)
  schedule = DecentJob::CronSchedule.parse(expression)

  if !!reference != !!schedule
    failures << ["parse", expression, !!reference, !!schedule]
    next
  end

  next unless reference

  after = Time.utc(2026, random.rand(1..12), random.rand(1..28), random.rand(0..23), random.rand(0..59), random.rand(0..59))
  expected = reference.next_time(after).to_i
  actual = schedule.next_at(after: after).to_i
  failures << ["next", expression, after, Time.at(expected).utc, Time.at(actual).utc] unless expected == actual
rescue => error
  failures << ["error", expression, error.class, error.message]
end

failures.each { |failure| warn failure.inspect }
abort "#{failures.size} differences in #{count} cases (seed #{seed})" unless failures.empty?
puts "#{count} cron cases match the pinned reference (seed #{seed})"
