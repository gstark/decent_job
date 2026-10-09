# Cron compatibility

Recurring-job execution remains in progress. The numeric schedule parser is a foundation for that work; it does not yet enqueue recurring jobs or accept natural-language schedules.

## Numeric schedules

`require "decent_job/cron_schedule"` loads `DecentJob::CronSchedule`. `parse(expression)` returns a schedule or `nil`; `new(expression)` raises `ArgumentError` for invalid input. `original` retains the supplied expression. `next_at(after: time)` returns the first matching instant strictly after the given time. `within(range)` excludes both endpoints, including the end of an inclusive Ruby range, matching the pinned reference's range behavior. Returned occurrences are Ruby `Time` objects representing UTC instants.

Supported expressions include five-field cron and six-field cron with seconds; lists, ranges, steps, month and weekday names; the yearly, annually, monthly, weekly, daily, midnight, noon, and hourly aliases; negative and last month days; nth and last weekdays with `#`; alternating weeks with `%`; and `&` to require both day fields to match. Week numbering follows et-orbi 1.4.0's Monday, December 31, 2018 reference.

An expression may end with an IANA timezone or a numeric offset. Without one, scheduling uses the process's local timezone. Matching preserves the reference's day-of-month/day-of-week OR semantics and its range-expansion conventions. Search advances through calendar fields and stops with `ArgumentError` if no occurrence is reached within its bounded search.

## Reference evidence

GoodJob checkout `ad4be5c2` locks Fugit 1.12.1, et-orbi 1.4.0, and raabro 1.4.0. These are comparison tools only. They are not dependencies of DecentJob or its normal test suite.

`spec/fixtures/cron_reference.json` records five successive occurrences for each of 88 expressions/start times, three range cases, and 23 invalid expressions. Cases cover leap years and the year 2100, named and numeric zones, seconds, extended day syntax, comma spacing, DST gaps and folds in New York, Berlin, and Lord Howe, and Pacific/Apia's skipped day. `spec/cron_conformance_spec.rb` compares DecentJob directly with those saved results.

The optional scripts require the exact reference versions to be available outside Bundler:

```sh
ruby -I/path/to/fugit-1.12.1/lib -I/path/to/et-orbi-1.4.0/lib script/generate_cron_fixtures.rb
ruby -I/path/to/fugit-1.12.1/lib -I/path/to/et-orbi-1.4.0/lib script/check_cron_reference.rb
```

raabro 1.4.0 must also be installed for these scripts. The first regenerates the saved fixtures. The second compares 1,000 seeded combinations of numeric fields, including acceptance and next occurrences; `CRON_SEED` and `CRON_CASES` can vary its inputs. Both use UTC as the process timezone for reproducibility. Separate specs explicitly exercise local-zone behavior and repeated wall-clock suppression.

## Deliberate corrections

DecentJob does not preserve these reference defects:

- Zero steps and zero weekday moduli are rejected during parsing, rather than failing with division errors.
- Numeric offsets retain their sign, including `-00:30`. Valid offsets beyond eleven hours, such as `+14:00`, work. Unknown timezone suffixes are rejected instead of resolving a recognized prefix.
- A skipped midnight does not cause an otherwise valid occurrence later that day to disappear. For example, `0 1 * * 0 America/Havana` includes March 8, 2026 at 01:00 local time; the pinned reference skips that Sunday when searching from March 6. Calendar jumps also handle ambiguous month boundaries, half-hour rollbacks in Lord Howe, and historical transitions ending at a non-hour boundary.

These corrections have explicit specs and are separate from the saved matching-reference cases. Full recurring-job parity also requires natural-language parsing, callable entries, durable deduplication, enable/disable overrides, manager lifecycle, and restart catch-up.
