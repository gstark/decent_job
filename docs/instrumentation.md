# Instrumentation and logging

DecentJob uses Active Support notifications and log subscribers supplied by Active Job. Standalone applications do not need Rails or Active Record for logging.

## Configure logging

The subscriber attaches to the `decent_job` namespace when the library loads. Its default destination follows `DecentJob.configuration.logger`, including Rails logger adoption, configuration replacement, and `DecentJob.reset!`. You can replace that destination at runtime:

```ruby
require "decent_job"

DecentJob.logger = Logger.new("log/jobs.log")
```

`DecentJob.logger` and `configuration.logger` refer to the same configured logger. Replacing the logger does not restart workers. In a Rails host, an explicitly configured DecentJob logger takes precedence over the Rails logger.

For explicit destinations, change the subscriber's logger list and rebuild its broadcast logger:

```ruby
file_logger = Logger.new("log/jobs.log")
console_logger = ActiveSupport::TaggedLogging.new(Logger.new($stdout))
console_logger.level = Logger::WARN

DecentJob::LogSubscriber.loggers.replace([file_logger, console_logger])
DecentJob::LogSubscriber.reset_logger
```

Each destination retains its own severity threshold. Explicit destination lists survive runtime configuration and logger replacement. An empty list disables subscriber output. This does not disable notifications, Active Job's own logs, or a configured `on_thread_error` callback.

The list follows configuration while it still contains only its tracked default logger. Changing its contents selects explicit destinations. `reset_logger` rebuilds the broadcast logger; it does not restore the default destination.

Tagged loggers receive `DecentJob` and available process/thread tags only for the current message. Existing application tags are preserved. Plain loggers and tagged loggers without a formatter also work. Default messages identify jobs without printing their arguments; an exception's own message and backtrace can contain application data.

## CLI output

The executable enables stdout logging by default. Embedded calls to `DecentJob::CLI` leave it disabled unless requested. Application boot code can override this behavior:

```ruby
require "decent_job/cli"
DecentJob::CLI.log_to_stdout = false
```

`CLI.log_to_stdout?` uses `CLI.within_exe?` when no override is set. When enabled, CLI setup adds a tagged stdout destination after loading the application and its configuration. It avoids adding another destination when an existing logger already writes to stdout. Repeated CLI setup does not duplicate messages.

## Subscribe to events

Use ordinary Active Support subscriptions and unsubscribe when the observer is no longer needed:

```ruby
subscription = ActiveSupport::Notifications.subscribe("cleanup_preserved_jobs.decent_job") do |event|
  puts "Removed #{event.payload.fetch(:destroyed_records_count)} records"
end

ActiveSupport::Notifications.unsubscribe(subscription)
```

Notification callbacks are synchronous application code. An event observer is separate from the built-in log subscriber and from `configuration.on_thread_error`.

These events are currently emitted by the runtime:

| Event, followed by `.decent_job` | Payload | Built-in log level |
| --- | --- | --- |
| `enqueue_job` | Original `active_job`, original `scheduled_at` argument (nil for ordinary enqueue), `job` snapshot, `accepted`, `create_with_advisory_lock: false` | No default message |
| `perform_job` | `job` and `execution` snapshots, OS `process_id`, `thread_name`, and outcome fields described below | info |
| `enqueue_concurrency_limit_exceeded` | Original Active Job as `job`, resolved `key`, `kind`, `scope`, numeric `limit`, observed `count` | info |
| `enqueue_concurrency_throttle_exceeded` | Same admission fields, plus `window` in seconds | info |
| `cleanup_preserved_jobs` | `older_than`, cutoff `timestamp`, `destroyed_jobs_count`, `destroyed_executions_count`, `destroyed_batches_count`, total `destroyed_records_count` | info |
| `scheduler_shutdown_kill` | Interrupted `active_job_ids` | warn |
| `cluster_start` | `subprocesses` | info |
| `cluster_spawn` | `pid`, slot `index`, `restart_count` | info |
| `cluster_reap` | `pid`, `index`, exit `status`, terminating `signal` | info |
| `cluster_backoff` | `index`, `restart_count`, `delay` | warn |
| `cluster_shutdown` | Supervisor `pid` | info |
| `subprocess_start`, `subprocess_shutdown_start`, `subprocess_shutdown` | `pid`, `index` | info |
| `systemd_watchdog_start` | `interval` | info |
| `systemd_watchdog_error` | `error` | error |
| `before_supervisor_fork`, `before_subprocess_boot` | Standard notification timing and exception metadata | No default message |

Cleanup events report partial deletion counts if cleanup raises. A reap log describes a terminating signal by name when available. Forced shutdown requests interruption before invoking observers, and its event does not claim that application ensure blocks have finished draining.

## Job event timing and outcomes

Single-job enqueue events surround persistence after concurrency admission, and finish before inline execution or asynchronous dispatch. Active execution retries and manual management retries emit the same event when rescheduling the existing job. Manual retry callback preparation remains silent until the prepared retry is persisted. Plain bulk insertion emits no per-job enqueue events; jobs that require individual concurrency checks use the single-job path. Rejected admission emits its rejection event without an enqueue event. Only the first rejected rule is reported, with its limit checked before its throttle.

An enqueue snapshot describes the attempted write. `accepted` is true when persistence accepted it, including a write inside a transaction that may later roll back. A duplicate cron occurrence has an attempted snapshot and `accepted: false`; unknown persisted timestamps remain nil. Rails jobs configured for enqueue after transaction commit emit these events after commit; rolled-back deferred jobs emit none. `create_with_advisory_lock` is false because DecentJob acquires execution claims separately.

Execution events begin after the attempt record exists and finish before final outcome persistence, retention deletion, and claim release. Event observers can therefore still see an unfinished attempt in PostgreSQL. `job` is a `DecentJob::Job` snapshot and `execution` is a `DecentJob::Execution` snapshot, taken from the execution start. Their stored attributes remain readable after deletion. Methods that reload or query history still require database records. Editing snapshot attributes does not change the executing job's serialized arguments.

The payload's `process_id` is the operating-system PID, distinct from the persisted process UUID. `thread_name` is the Ruby thread's name or its object ID converted to a string. Outcomes use these fields:

| Outcome | Additional payload fields |
| --- | --- |
| Successful return | `value` (including nil, false, or zero), `error: nil`, `handled_error: nil`, `retried`, `error_event: nil` |
| Active Job handles an error | `value`, actual exception in `error` and `handled_error`, `retried`, symbolic `error_event` such as `:retried`, `:discarded`, or `:retry_stopped` |
| Returned exception object | `value: nil`, exception in `error` and `handled_error`, `error_event: :handled` unless already classified by Active Job |
| Exception handled by the backend | Exception in `error` and `unhandled_error`, symbolic `error_event`; no successful-return fields |
| Exception outside configured handled classes | Standard Active Support `exception` and `exception_object` metadata; the exception propagates |
| Interrupted/nonlocal exit | `interrupted: true`; no successful-return claim |

`retried` reports an actual Active Job reenqueue, including a direct self-reenqueue. A later backend decision to retry an unhandled exception is separate. Operator force-discard remains authoritative even if the application returned successfully. Event duration includes notification overhead; persisted execution duration measures the Active Job attempt and host execution wrapper without the execution event's start/finish callbacks.

For these job and admission events, failing start/finish observers are reported through `on_thread_error` without skipping or repeating the application operation or replacing its result or exception. A failing error reporter is contained, and recursive observer-error reporting is suppressed. Observer queries run within a savepoint when the current borrowed connection has a usable transaction, so a failed observer query cannot poison that transaction. An already-aborted application transaction retains its original failure. Observers run outside runtime and pool mutexes, but are synchronous and can delay the caller.

Notification hashes are separate from the application's outcome state. If an observer freezes its notification hash, later outcome fields cannot be added to that hash; this is reported without changing the application result. An interruption during a finish observer updates the retained mutable notification hash to mark interruption and remove successful-return fields. Observers that already ran cannot observe an interruption that occurs later.

## Remaining instrumentation work

The subscriber also implements the corresponding GoodJob handlers for scheduler creation/shutdown/restart, cron startup, task errors, polling errors, and listener lifecycle/messages/errors. Their runtime event producers are the next checkpoint; these handlers alone do not establish event parity.

That checkpoint will distinguish timer polling from job attempts. The pinned reference at `ad4be5c2` emits `finished_timer_task` without its namespace; DecentJob's corresponding event will use `.decent_job`. See the [implementation plan](implementation-plan.md) and [parity audit](parity.md) for the remaining I3 requirements.
