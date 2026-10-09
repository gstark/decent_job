# Recurring job definitions

`DecentJob::CronEntry` defines, validates, and enqueues a recurring job. Automatic scheduling and restart catch-up are still in progress. Defining an entry currently does not start a scheduler.

## Configuration

Define jobs before configuring their schedules:

```ruby
DecentJob.configure do |config|
  config.cron = {
    daily_report: {
      cron: "every weekday at 9am in America/New_York",
      class: "ReportJob",
      args: -> { [Date.today.iso8601] },
      kwargs: {format: "summary"},
      set: {queue: "reports", priority: 5},
      description: "Weekday report"
    }
  }
end

entry = DecentJob::CronEntry.find(:daily_report)
entry.valid?
entry.display_schedule
entry.next_at
```

Keys must be symbols. `class` accepts a class, a class name, or a callable. `args`, `kwargs`, and `set` accept an array, hash, and hash respectively, or callables returning those values. Validation and display do not execute these callables. Enqueue evaluates them using the application's default I18n locale.

`entry.errors` contains validation errors by field. Configuration validation rejects invalid definitions before opening the database. `DecentJob::CronEntry.all` returns configured entries; `find` accepts a symbol or string and raises `KeyError` for an unknown key.

`DECENT_JOB_CRON` can supply the same definitions as JSON. JSON keys become symbols; class names remain strings. Assigning `config.cron` replaces the environment definitions. Typed options accept `nil` as an omitted value; other non-callable values must have the declared array or hash type.

See [cron compatibility](cron-compatibility.md) for numeric and natural-language grammar, timezone behavior, and reference quirks.

## Enqueue and deduplication

Pass the intended occurrence time to enqueue a scheduled run:

```ruby
occurrence = entry.next_at
job = entry.enqueue(occurrence)
```

This enqueues immediately unless `set` supplies an Active Job delay. The occurrence time identifies the run; it is not itself a delay. Competing processes inserting the same key and occurrence create one job. A duplicate returns `false`, and the surrounding database transaction remains usable.

`entry.enqueue` without an occurrence time is a manual run. It can be called repeatedly. Manual runs still carry the cron key for history.

The job row retains `cron_key` and `cron_at` through Active Job retries. Deduplication uses the retained row, so removing cron job records also removes their deduplication history. A rolled-back enqueue or rejected concurrency admission does not reserve an occurrence.

If an enclosing database transaction rolls back, newly enqueued cron jobs lose their provisional provider IDs and enqueue flags. This includes jobs from nested entries. A failure during inline execution after commit retains the persisted job and its ID for inspection and retry.

An entry uses one database. Pass `database:` when constructing an entry to bind it explicitly:

```ruby
entry = DecentJob::CronEntry.new(
  {key: :report, cron: "every hour", class: ReportJob},
  database: database
)
```

Its jobs must use a DecentJob adapter for that database. Enqueue inside a `DecentJob::Bulk.enqueue` block flushes the entry independently, sharing any enclosing database transaction. Inline execution starts after commit and after the entry's enqueue context is restored. Jobs created by a running cron job do not inherit its cron metadata.

A callable `class` may enqueue directly:

```ruby
{
  key: :report,
  cron: "every hour",
  class: -> { ReportJob.perform_later("current") }
}
```

If a callable enqueues multiple jobs with the same non-nil occurrence time, the unique key permits only the first accepted job. Manual runs can enqueue multiple jobs. Bulk calls and concurrency-controlled jobs retain the same database and uniqueness rules.

An entry resolving to a job class returns the enqueued job or `false` when rejected. A callable that directly returns an enqueued job has the same return behavior. Other callables return their accepted jobs as an array, or `nil` when they enqueue nothing. A blank class definition is a no-op.

## Callable schedules

A schedule callable receives the previous occurrence time, or the latest stored job's time when no previous time is supplied. It can return a `Time` or a supported schedule expression:

```ruby
{
  key: :refresh,
  cron: ->(previously_at) { (previously_at || Time.now) + 300 },
  class: RefreshJob
}
```

`next_at(previously_at: time)` supplies the previous time explicitly. `next_at(after: time)` sets the search origin for expression schedules. A callable returning an invalid expression yields no next occurrence. `within(range)` returns occurrences strictly inside the range for expression schedules; a callable returning a `Time` yields an empty array, leaving repeated evaluation to the scheduler.

`display_properties` shows callable values as `"Lambda/Callable"`. Natural schedules display their generated cron expression. `jobs`, `last_job`, and `last_job_at` expose stored history; job records are decoded hashes, and times are Ruby `Time` objects.

## Persistent controls

```ruby
entry.disable
entry.enabled? # false
entry.enable
entry.enabled? # true
```

Overrides persist in PostgreSQL across entry instances and processes. Without an override, `enabled_by_default` defaults to `true` and may be a callable. These controls govern whether the scheduler should enqueue an occurrence. Explicit `entry.enqueue` remains available when the entry is disabled.
