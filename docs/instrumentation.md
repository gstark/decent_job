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
| `finished_job_task` | Worker/pool metadata, returned `result` (possibly nil), task `error`, completion `time` | error when `error` is present |
| `finished_timer_task` | Worker/pool metadata, wake `result`, `error`, completion `time` | error when `error` is present |
| `scheduler_create_pool` | Worker/pool metadata, `max_threads`, captured `run_id`, `operation_duration` | info |
| `scheduler_shutdown_start`, `scheduler_shutdown` | Captured worker/pool/run metadata, `timeout`, `force`, `stopped`, `draining`; completion includes `operation_duration` and any drain `error` | info |
| `scheduler_restart_pools` | Captured worker/pool metadata, `old_run_id`, `new_run_id`, `restarted`, `outcome`; successful allocation includes `operation_duration` | info |
| `cron_manager_start` | `cron_manager`, detached `cron_entries` array, `run_id`, OS `process_id`, `thread_name`, `operation_duration` | info |
| `notifier_listen`, `notifier_unlisten` | OS `process_id`, `thread_name`, standard exception metadata on SQL failure | info |
| `notifier_notified` | Raw `payload`, OS `process_id`, `thread_name` | debug |
| `notifier_notify_error` | Listener-task `error`, OS `process_id`, `thread_name` | error |
| `cleanup_preserved_jobs` | `older_than`, cutoff `timestamp`, `destroyed_jobs_count`, `destroyed_executions_count`, `destroyed_batches_count`, total `destroyed_records_count` | info |
| `scheduler_shutdown_kill` | Captured worker/pool/run metadata and interrupted `active_job_ids`, grouped by pool | warn |
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

## Worker tasks, polling, and listening

`finished_job_task` describes a completed call to the worker's performer, including a queue check that found no executable job. Its `result` is a `Performer::Result` or nil. Its `error` is an exception that escaped that call. An ordinary returned result can have `unhandled_error` while the event's `error` remains nil. The worker reports the task exception or returned unhandled error through its error callback before emitting the event. Activity counters are already updated; retry wakeup, cache refill, and cleanup happen afterward. Failures in that later maintenance do not create another task-completion event. Killed, unfinished calls do not emit a completed task event.

Task and polling events include `scheduler` (the `Worker`), normalized queue selector `queues`, its alias `performer_name`, zero-based `pool_index`, OS `process_id`, and `thread_name`. Their `time` is a completion `Time`, captured before any error reporter or event observer can delay delivery.

Polling remains integrated into execution threads. `finished_timer_task` is emitted when a positive polling deadline expires, with `result` equal to the number of new permits accepted by that wakeup (zero or one). A failed wake has a nil result and the actual error, reported through the worker callback before the event. It is separate from the subsequent queue check. Startup permits, notifications, cached schedule deadlines, and immediate retry work do not emit timer events. Nonpositive poll intervals disable these timer events. Unlike the reference's accidentally unnamespaced event, DecentJob consistently uses `finished_timer_task.decent_job`.

Listener events run on the listener's dedicated connection owner. `notifier_listen` surrounds the actual LISTEN statement. `notifier_notified` publishes a detached copy of the raw payload before JSON parsing and validation, including malformed messages. PostgreSQL can supply nil for an empty maintenance notification; that raw value is preserved in the event and treated as a broad wakeup afterward. Changing the event's raw string cannot change queue routing. Malformed messages are reported without reconnecting the listener or emitting a listener-task failure event.

`notifier_unlisten` surrounds UNLISTEN on a healthy session whose LISTEN completed, before the connection is closed. Unusable or never-subscribed sessions close without an UNLISTEN event. `notifier_notify_error` describes listener connection/task failures, including cleanup failure, before invoking the error callback; it does not describe outgoing NOTIFY SQL. The original task error takes precedence if cleanup also fails. Expected loop errors during shutdown remain suppressed, while fatal exceptions and cleanup failures remain visible. Existing bounded reconnect waits and immediate connection-error reporting remain, instead of the reference's delayed reporting threshold. Raised observer and reporter errors cannot prevent connection cleanup.

Worker runtime event observer errors use that worker's configured `on_thread_error`, including an explicit nil callback. Job events continue to use the global callback. All these callbacks are synchronous and run outside worker/runtime mutexes. The error callback and log subscriber are independent; if both log a failure, both messages may appear.

## Lifecycle observations

Pool creation, shutdown, and restart events identify the worker as `scheduler`, with normalized `queues`, its alias `performer_name`, zero-based `pool_index`, `max_threads`, OS `process_id`, and `thread_name`. A worker's `run_id` increases with each allocation. It identifies a run of that worker, not a globally unique process or scheduler. Cron managers maintain their own run IDs. Calling start on an already running resource does not emit another creation event.

These lifecycle events describe transitions using metadata captured during the operation. Notifications run synchronously after registry, adapter, worker, and cron mutexes have been released. Their Active Support event duration measures notification delivery. `operation_duration` is measured in monotonic seconds around worker or cron allocation, captured-run draining, or replacement startup, respectively. Shutdown draining includes the start observer's time. A restart's replacement-start duration does not include the preceding shutdown. This differs from the reference's events that surround executor operations directly.

Shutdown signals every selected resource before invoking start observers. Each completion reports the captured run's state, even if a later call already started a replacement. `stopped: false` and `draining: true` mean resources remain alive; the default log says the scheduler is still draining. `timeout` preserves the requested timeout: nil waits indefinitely, zero only signals, and a positive value allows a grace period. Synchronous observers can delay the caller and consume that shared grace period. Calling shutdown on a fully stopped resource emits no new shutdown events.

Explicit restart reports `old_run_id`, `new_run_id`, and an `outcome` of `:restarted`, `:incomplete`, `:refused`, or `:failed`. A refused or incomplete attempt does not claim a new run. Creation events precede successful restart observations. A newer stop request can refuse a stale restart; global restart preserves owners that were stopped locally.

Observers can inspect runtime state and call lifecycle methods. Lifecycle operations invoked from an observer still run, but nested lifecycle observations for the same resource are suppressed throughout that callback chain. This prevents an observer that always restarts its worker from recursively generating creation events forever. Later independent transitions remain observable. Raised observer or error-reporter exceptions are contained using the same instrumentation boundary as worker events.

Cron startup captures a separate entries array after allocation, including an empty manager started during construction. An empty manager or a manager spanning multiple databases does not open a default database for observation.
