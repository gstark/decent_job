# Worker shutdown and interruption

`DecentJob.shutdown`, `Adapter#shutdown`, `Worker#shutdown`, and `CronManager#shutdown` stop accepting work and drain the resources they own. All accept `timeout:` and `force:` keywords.

| Timeout | Default behavior | With `force: true` |
| --- | --- | --- |
| `nil` | Wait until owned work finishes | Same indefinite wait |
| `0` | Signal shutdown and return | Also request interruption of remaining owned work |
| Positive number | Wait up to that many seconds | Request interruption after the grace period expires |

Negative, infinite, and nonnumeric timeouts are invalid. Adapter and global shutdown share one monotonic deadline across their resources and signal all resources before waiting. They do not give each resource its own grace period.

```ruby
drained = worker.shutdown(timeout: 30, force: true)
warn "Interrupted jobs: #{worker.interrupted_job_ids.join(', ')}"
worker.shutdown unless drained
```

Shutdown returns true only when all owned resources have drained. A false return means some resources still run. Calling shutdown again can wait for them. A worker cannot restart while its previous threads still own resources.

## Forced interruption

Forced shutdown requests termination of execution threads and cron work owned by the selected resource. It does not kill application threads or the thread invoking shutdown. A job that shuts down its own worker observes the stopping state and can return normally. See [cooperative shutdown](worker-observability.md#cooperative-shutdown-from-a-job) for long-running jobs that can stop themselves.

Ruby runs `ensure` blocks during thread termination. Application cleanup can therefore outlive the shutdown timeout. DecentJob retains the job's PostgreSQL session lock and process registration while that cleanup runs. It retires the interrupted pool checkout after unwinding, so a subsequent job receives a clean connection. An interrupted transaction rolls back when its session closes.

If a query was still running, connection cleanup requests PostgreSQL cancellation before closing the session. With libpq 17 or newer, this request waits at most one second. Older client libraries use pg's cancellation method, which may delay cleanup. Cancellation is best effort: a network failure can leave the remote query running until PostgreSQL detects the disconnect or the query ends. The pool never reuses that connection.

`Worker#interrupted_job_ids` returns the Active Job IDs targeted by forced interruption during the current worker run. These are `job_id` values, not `provider_job_id` values. The returned array and strings are independent snapshots. Starting a new run resets this list.

The `scheduler_shutdown_kill.decent_job` notification includes `active_job_ids`. Notification failures are reported through the worker's error handler and do not prevent termination requests or sibling shutdown.

An interrupted attempt stays unfinished until another worker acquires its released lock. Recovery then records the interruption and starts a replacement attempt. The optional `InterruptErrors` extension lets Active Job handlers decide whether to retry or discard the replacement. See [execution recovery](execution-recovery.md#interrupted-attempts).

Thread interruption cannot undo external effects or terminate arbitrary native code immediately. Use cooperative cleanup where possible. The CLI keeps its existing signal behavior: SIGINT and SIGTERM request graceful shutdown, and an expired grace period produces an unsuccessful exit. The supervisor escalates processes according to its [shutdown policy](supervisor.md).
