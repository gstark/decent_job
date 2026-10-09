# Cron compatibility

Automatic recurring execution remains in progress. The schedule parser accepts numeric and natural-language expressions. [Cron entries](recurring-jobs.md) add callable schedules, persistent controls, and deduplicated enqueue; scheduler lifecycle and restart catch-up remain unfinished.

## Numeric schedules

`require "decent_job/cron_schedule"` loads `DecentJob::CronSchedule`. `parse(expression)` returns a schedule or `nil`; `new(expression)` raises `ArgumentError` for invalid input. `original` retains the supplied expression. `next_at(after: time)` returns the first matching instant strictly after the given time. `within(range)` excludes both endpoints, including the end of an inclusive Ruby range, matching the pinned reference's range behavior. Returned occurrences are Ruby `Time` objects representing UTC instants.

Supported expressions include five-field cron and six-field cron with seconds; lists, ranges, steps, month and weekday names; the yearly, annually, monthly, weekly, daily, midnight, noon, and hourly aliases; negative and last month days; nth and last weekdays with `#`; alternating weeks with `%`; and `&` to require both day fields to match. Week numbering follows et-orbi 1.4.0's Monday, December 31, 2018 reference.

An expression may end with an IANA timezone or a numeric offset. Without one, scheduling uses a recognized `TZ` environment setting first, then ActiveSupport's thread-local `Time.zone`, then Ruby's native local timezone. Recognized settings include IANA names, `Z`, and supported numeric offsets. This priority applies to calendar jumps as well as matching. Matching preserves the reference's day-of-month/day-of-week OR semantics and its range-expansion conventions. Search advances through calendar fields and stops with `ArgumentError` if no occurrence is reached within its bounded search.

## Natural-language schedules

The same parser accepts phrases such as `every 15 minutes`, `every weekday at 5pm`, `every month on days 1,15 at 10:00`, and `every day at noon in Europe/Berlin`. It translates intervals, weekday lists and ranges, ordinal month days, named and numeric times, and time ranges into numeric cron. For natural input, `original` returns that generated cron expression. Natural input is limited to 256 characters after surrounding whitespace is stripped.

Intervals use calendar fields. For example, `every 15 hours` runs at 00:00 and 15:00 each day. It does not measure fifteen hours from the previous run. Standalone durations such as `3 hours` are rejected; use `every 3 hours` for a recurring schedule.

Some accepted reference phrases have surprising meanings. DecentJob retains these behaviors so existing definitions keep their schedules:

- `every day at 9:30 and 17:45` selects the first resulting cron schedule, `30 9 * * *`. Times sharing the same minute can combine, so `at 9:30 and 17:30` runs twice a day.
- `every 2 hours and 5 minutes` uses the first interval, `0 */2 * * *`.
- `every 15 minutes from 9 to 17` becomes `*/15 9-16 * * *`, excluding the ending hour.
- Named times retain the reference's case quirks. `at noon` works; `at Noon` is rejected on its own. Another clause can supply a missing hour, so `at 4 at Five` becomes `0 4 * * *`.

Inspect `original` when validating a natural definition. Phrases that cause internal reference errors, such as `every minute at 5` or a range with unequal endpoint minutes, return `nil` from `parse` and raise `ArgumentError` from `new`.

## Reference evidence

GoodJob checkout `ad4be5c2` locks Fugit 1.12.1, et-orbi 1.4.0, and raabro 1.4.0. These are comparison tools only. They are not dependencies of DecentJob or its normal test suite.

`spec/fixtures/cron_reference.json` records five successive occurrences for each of 88 expressions/start times, three range cases, and 23 invalid expressions. Cases cover leap years and the year 2100, named and numeric zones, seconds, extended day syntax, comma spacing, DST gaps and folds in New York, Berlin, and Lord Howe, and Pacific/Apia's skipped day. `spec/cron_conformance_spec.rb` compares DecentJob directly with those saved results.

`spec/fixtures/natural_cron_reference.json` records 170 natural-language cases, including 137 accepted expressions with generated cron strings and five successive occurrences. The remaining cases cover invalid phrases, standalone durations, and reference exceptions. `spec/natural_cron_conformance_spec.rb` uses these results without loading Fugit.

The optional scripts require the exact reference versions to be available outside Bundler:

```sh
ruby -I/path/to/fugit-1.12.1/lib -I/path/to/et-orbi-1.4.0/lib script/generate_cron_fixtures.rb
ruby -I/path/to/fugit-1.12.1/lib -I/path/to/et-orbi-1.4.0/lib script/check_cron_reference.rb
ruby -I/path/to/fugit-1.12.1/lib -I/path/to/et-orbi-1.4.0/lib script/generate_natural_cron_fixtures.rb
ruby -I/path/to/fugit-1.12.1/lib -I/path/to/et-orbi-1.4.0/lib script/check_natural_cron_reference.rb
```

raabro 1.4.0 must also be installed for these scripts. The generators regenerate the saved fixtures. The numeric comparison checks 1,000 seeded combinations of fields, including acceptance and next occurrences. The natural comparison checks systematic grammar combinations plus randomly combined clauses, comparing acceptance and generated cron strings. Its defaults produce 9,795 distinct expressions. `CRON_SEED` and `CRON_CASES` can vary either comparison's inputs. All scripts use UTC as the process timezone for reproducibility. Separate specs explicitly exercise local-zone behavior and repeated wall-clock suppression.

## Deliberate corrections

DecentJob does not preserve these reference defects:

- Zero steps and zero weekday moduli are rejected during parsing, rather than failing with division errors.
- Numeric offsets in expressions and `TZ` retain their sign, including `-00:30`. Valid offsets beyond eleven hours, such as `+14:00`, work. Unknown expression timezone suffixes are rejected instead of resolving a recognized prefix.
- A skipped midnight does not cause an otherwise valid occurrence later that day to disappear. For example, `0 1 * * 0 America/Havana` includes March 8, 2026 at 01:00 local time; the pinned reference skips that Sunday when searching from March 6. Calendar jumps also handle ambiguous month boundaries, half-hour rollbacks in Lord Howe, and historical transitions ending at a non-hour boundary.

These corrections have explicit specs and are separate from the saved matching-reference cases. Full recurring-job parity still requires manager lifecycle and restart catch-up.
