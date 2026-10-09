# Worker activity and thread controls

Inspect registered workers in the current process without opening a database:

```ruby
DecentJob.stats
DecentJob.idle?
DecentJob.idle?(30)
```

`DecentJob.stats` aggregates the workers registered through the global runtime, CLI, or embedded adapters. For a manually owned worker, use `worker.stats` and `worker.idle?` directly, or register it with `DecentJob.start_worker(worker)`. An empty runtime reports zero counts, nil activity times, and an empty scheduler list.

## Statistics

Each worker and the global aggregate expose:

| Field | Meaning |
| --- | --- |
| `schedulers` | Per-queue-pool counters, capacity, activity, and cache statistics |
| `total_executions_count` | Successful attempts plus attempts with job errors |
| `succeeded_executions_count` | Attempts that returned without a job error |
| `errored_executions_count` | Attempts with job errors, including handled retries and discards |
| `empty_executions_count` | Dequeue attempts that found no executable job |
| `active_execution_thread_count` | Threads currently executing jobs |
| `execution_at` | Most recent execution start or completion |
| `check_queue_at` | Most recent dequeue attempt start |

`total_succeeded_executions_count` and `total_errored_executions_count` remain aliases for the corresponding aggregate counts. Existing per-pool `last_execution_at`, `last_success_at`, `last_error_at`, and `last_empty_at` fields remain available. `last_execution_at` describes completion, while `execution_at` also records job starts.

Activity times use ISO8601 strings and are nil before the corresponding activity occurs. Aggregation compares absolute instants, including timestamps with different UTC offsets. Returned arrays, hashes, and strings are independent snapshots. Modifying a snapshot cannot change a worker's counters. Global snapshots collect workers individually and are not an atomic snapshot across the whole process.

Infrastructure failures do not count as job errors or completed attempts. Pending jobs do not acquire execution timestamps merely because a worker polled their queue. Restarting a drained worker resets its counters and activity times. Process heartbeats persist the worker snapshot for inspection from another process; see [process tracking](process-tracking.md).

`idle?` requires no active execution in any inspected pool. Passing a duration also requires that much monotonic time since startup or the latest execution start or completion. A long job therefore receives a full idle period after finishing. Empty queue checks do not reset the idle clock. See [idle shutdown](daemon-and-idle.md#idle-shutdown).

## Cooperative shutdown from a job

```ruby
class ImportJob < ActiveJob::Base
  def perform
    records.each do |record|
      break if DecentJob.current_thread_shutting_down?
      import(record)
    end
  end
end
```

Inside an owned worker, `current_thread_running?` returns true until shutdown begins. It then returns false, and `current_thread_shutting_down?` returns true. Outside an owned worker they return true and nil, respectively. Nested inline work retains its enclosing worker's status. The context follows Active Support's configured thread or fiber isolation and is restored after nested scopes and exceptions.

For jobs that do not stop cooperatively, shutdown accepts `force: true` and reports the Active Job IDs targeted for interruption. See [shutdown timeouts and recovery](shutdown.md).

These helpers report the owning worker's lifecycle. The adapter's `stopping?` remains the API for job-specific force-discard and pause reasons. Jobs remain responsible for persisting any application checkpoint before returning.

## Execution thread priority

```ruby
DecentJob.configure do |config|
  config.lower_thread_priority = true
end
```

The default is false. `DECENT_JOB_LOWER_THREAD_PRIORITY=true` enables it, and CLI `--lower-thread-priority` or `--no-lower-thread-priority` overrides application configuration. Application values must be booleans.

Enabled execution threads use Ruby priority -3 before each attempt. Listener, heartbeat, cron, and application threads retain their own priorities. With the option disabled from startup, DecentJob leaves execution priority unchanged. Disabling a previously enabled option restores the thread's original priority before its next attempt. Runtime changes do not alter a job already executing:

```ruby
DecentJob.lower_thread_priority = true # Registered workers and future runtime workers
worker.lower_thread_priority = false # One explicitly owned worker
```

Ruby engines determine how priorities affect scheduling. Engines that raise `NotImplementedError` for priority assignment continue without changing it.
