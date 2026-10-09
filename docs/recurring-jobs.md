# Recurring jobs

`DecentJob::CronEntry` defines, validates, and enqueues a recurring job. `DecentJob::CronManager` schedules entries and optionally catches up occurrences missed during a restart. Automatic scheduling is disabled by default.

## Configuration

Define jobs before configuring their schedules:

```ruby
DecentJob.configure do |config|
  config.enable_cron = true
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

## Running the scheduler

With `config.enable_cron = true`, `DecentJob.start` starts the configured schedules alongside workers. Async adapters start schedules on their first enqueue. The CLI supports the same settings:

```sh
bundle exec decent_job start --require ./jobs.rb --enable-cron --cron-graceful-restart-period 300
```

`--no-enable-cron` overrides application configuration. The environment equivalents are `DECENT_JOB_ENABLE_CRON=true` and `DECENT_JOB_CRON_GRACEFUL_RESTART_PERIOD=300`. The restart period defaults to `nil` and otherwise must be a finite positive number of seconds.

Start or stop configured schedules independently of workers:

```ruby
DecentJob.start_cron
DecentJob.cron_running?
DecentJob.stop_cron(timeout: 30)
```

Explicit `start_cron` works even when automatic scheduling is disabled. Repeated starts reuse the manager for the database. `stop_cron` prevents automatic restart until an explicit start reopens the lifecycle. Pass `database:` to start or stop only that database's configured manager. Static job classes must use a DecentJob adapter bound to that database; callable-produced jobs are checked when enqueued.

`DecentJob.shutdown(timeout: 30)` stops schedulers and workers with one shared deadline. It returns `false` if callbacks or jobs remain running; a later shutdown can wait for them. `reset!` waits for completion, clears the registry and configuration, and closes the global database pool. An async adapter's `shutdown` also stops its database's configured scheduler and prevents that adapter from restarting during draining callbacks. Use `adapter.restart(timeout: 30)` to resume it after draining. Other databases' managers remain running. See [lifecycle and restart](lifecycle.md), including the reset behavior when called from an owned callback.

Pass `force: true` to request interruption of owned cron callbacks after the shutdown deadline. Ruby cleanup may continue after that request; a later shutdown can wait for it. See [forced shutdown](shutdown.md).

For independently configured schedules, construct and own a manager directly:

```ruby
entry = DecentJob::CronEntry.new(
  {key: :report, cron: "every hour", class: ReportJob},
  database: database
)
manager = DecentJob::CronManager.new(
  [entry], graceful_restart_period: 300,
  on_thread_error: ->(error) { warn error.full_message }
)
manager.start
manager.shutdown(timeout: 30)
```

`start_on_initialize: true` starts immediately. `running?`, `shutdown?`, and `restart(timeout:)` expose lifecycle state; restart returns `false` while old callbacks remain alive. Direct managers belong to their caller and must be stopped before their database is closed.

Each entry uses a timer thread and two live delivery threads, with one pending occurrence slot. Catch-up adds one temporary thread per entry. Two live enqueue callbacks can overlap; catch-up runs serially alongside them. Slow entries do not block other entries. No thread or history array is created per missed occurrence. With automatic cron enabled, the default database pool reserves three connections per configured entry, or four with catch-up, in addition to execution threads and two application connections. Explicit pools and manually enabled schedulers need equivalent sizing for their workload.

Threads inherit the starting caller's `Time.zone` and use the Rails executor when available. The scheduler computes the next occurrence before delivering the current one. Expression schedules advance from the current wall clock, so a forward clock change does not replay the intervening history. A backward change delays delivery until its intended time. Callable schedules returning a `Time` advance from the previous intended occurrence and can replay missed times through that chain. Non-advancing times stop that entry and report an error. Enqueue errors are sent to `on_thread_error`; other entries continue.

## Restart catch-up

When a restart period is configured, each entry snapshots its latest scheduled occurrence at or before startup before delivering new work. Expression schedules replay occurrences strictly between startup minus the period (or that latest occurrence, whichever is later) and startup. Future-dated records are excluded. Catch-up proceeds oldest first and stops when shutdown is requested. Live scheduling continues independently; its delivery order relative to catch-up is not guaranteed.

Callable expression schedules use the historical snapshot when choosing the catch-up expression. Callable schedules returning a `Time` use their normal previous-occurrence chain instead of a separate range replay. Persistent enable/disable settings are checked before each delivery. Disabled entries continue advancing their live schedules, so enabling an entry does not itself replay skipped runs. Several processes may run the same schedules; PostgreSQL deduplicates each key and intended occurrence.

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

`next_at(previously_at: time)` supplies the previous time explicitly. For expression schedules, default searches stay after that occurrence even if the wall clock moves backward. `next_at(after: time)` explicitly sets the search origin. A callable returning an invalid expression yields no next occurrence. `within(range)` returns occurrences strictly inside the range for expression schedules; a callable returning a `Time` yields an empty array, leaving repeated evaluation to the scheduler. `each_within(range)` iterates without building the whole result array.

`display_properties` shows callable values as `"Lambda/Callable"`. Natural schedules display their generated cron expression. `jobs`, `last_job`, and `last_job_at` expose stored history; job records are decoded hashes, and times are Ruby `Time` objects.

## Persistent controls

```ruby
entry.disable
entry.enabled? # false
entry.enable
entry.enabled? # true
```

Overrides persist in PostgreSQL across entry instances and processes. Without an override, `enabled_by_default` defaults to `true` and may be a callable. These controls govern whether the scheduler should enqueue an occurrence. Explicit `entry.enqueue` remains available when the entry is disabled.
