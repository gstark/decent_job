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

Notification callbacks are synchronous application code. An event observer is separate from the built-in log subscriber and from `configuration.on_thread_error`. The logging checkpoint preserves existing error-handler behavior.

These events are currently emitted by the runtime:

| Event, followed by `.decent_job` | Payload | Built-in log level |
| --- | --- | --- |
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

## Remaining instrumentation work

The subscriber also implements the corresponding GoodJob handlers for scheduler creation/shutdown/restart, cron startup, job execution and task errors, polling errors, listener lifecycle/messages/errors, and concurrency rejection. Their runtime event producers are the next checkpoint; these handlers alone do not establish event parity.

That checkpoint will also add single-job enqueue/reschedule events, document attempt timing before final persistence, and distinguish timer polling from job attempts. The pinned reference at `ad4be5c2` emits `finished_timer_task` without its namespace; DecentJob's corresponding event will use `.decent_job`. See the [implementation plan](implementation-plan.md) and [parity audit](parity.md) for the remaining I3 requirements.
