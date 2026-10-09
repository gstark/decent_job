# Process tracking

Each running worker has a UUID process identity shared by its queue pools and job attempts. A standalone or inline performer registers while it executes work. The process record contains its hostname, OS PID, memory use, program title, heartbeat timestamp, worker counters, and pool statistics.

```ruby
processes = DecentJob::ProcessRecord.all
active = DecentJob::ProcessRecord.active
inactive = DecentJob::ProcessRecord.inactive

process = DecentJob::ProcessRecord.find(processes.first.id)
process.state
process.schedulers
process.stale?
process.expired?
```

These methods accept `database:` for an explicit DecentJob database. Otherwise they use the current execution's database or the global database. Look up a record again to observe its latest heartbeat and counters.

`Job#locked_by_id` identifies its current process owner. It clears when the executor finishes; a force-discarded body can retain ownership until it returns. Attempt history retains `process_uuid` after the process record is removed; the existing `process_id` field remains the OS PID. Old attempts have no process UUID because migration 009 cannot reconstruct one from a PID alone.

## Heartbeats and liveness

```ruby
DecentJob.configure do |config|
  config.advisory_lock_heartbeat = false
  config.process_heartbeat_interval = 30
end
```

These are the standalone defaults. Environment equivalents are `DECENT_JOB_ADVISORY_LOCK_HEARTBEAT` and `DECENT_JOB_PROCESS_HEARTBEAT_INTERVAL`. The interval must be finite, positive, and below 300 seconds.

Timestamp heartbeats consider a process active until its heartbeat expires after five minutes. An optional advisory heartbeat keeps a PostgreSQL session lock on the process identity; loss of that lock makes the process inactive without waiting for expiration. A record is stale after 30 seconds and expired after five minutes. An advisory-locked process can remain active despite stale timestamps.

Each tracker owns a monitor thread and a dedicated PostgreSQL connection. It does not borrow from the job connection pool or depend on LISTEN/NOTIFY. This lets heartbeats continue while every pooled connection is occupied. With [PgBouncer transaction pooling](pgbouncer.md), this is a dedicated client connection; timestamp heartbeats share server sessions between statements. The monitor reconnects after a connection error, reports the error through `on_thread_error`, and restores the same process identity. It also recreates a process record deleted while registration is still active.

Advisory and hybrid job session locks remain authoritative even when a heartbeat connection fails. SKIP LOCKED claims use persisted process ownership instead. Process cleanup rechecks process liveness and raw job session locks before releasing ownership or concurrency grants. See [lock strategies](locking.md).

## Shutdown and cleanup

Shutdown stops new work while active registrations keep heartbeats running. A bounded shutdown can return before a job completes; the tracker remains registered until that work drains. Normal completion of the final registration removes the process record and closes the monitor connection. An unfinished job can retain a record for later recovery.

Workers periodically clean inactive process records using their ordinary execution threads. Heartbeat renewal remains independent of that cleanup's pooled connections. Explicit cleanup is also available:

```ruby
deleted_count = DecentJob::ProcessRecord.cleanup(in_batches_of: 1000)
```

Cleanup skips live or contended owners, clears abandoned job ownership, and releases granted concurrency claims so waiting jobs can proceed. It retains jobs and attempt history. Another execution can then mark an abandoned attempt interrupted and run its replacement through the configured recovery behavior.

Process and owned-job scans use bounded pages. Lock waits are capped at 100 milliseconds per transaction, respecting a tighter caller setting; a contended candidate rolls back and can be retried on a later sweep. Pass `stopping: -> { ... }` to stop between records or owned jobs. Workers allow one sweep at a time and stop cleanup when shutdown begins.

A killed process using timestamp heartbeats can leave its process record visible until expiration. Advisory and hybrid job recovery can proceed when the job session lock is gone. SKIP LOCKED recovery waits for inactive-process cleanup to release the persisted claim. Process-record retention is separate from [job record preservation](record-preservation.md).

## Worker statistics and connection sizing

`Worker#process_id` exposes its UUID. `Worker#stats` returns per-pool scheduler statistics and total execution, success, and error counts. `Worker#idle?` reports whether any job is currently being executed. Counters reset on worker restart.

Scheduler statistics include configured capacity, live, active and available threads, empty polls, attempt outcomes, and recent activity timestamps. Dead execution threads reduce live and available capacity. When the final execution thread exits, the worker stops its listener and tracker; `running?` requires a live execution thread and no shutdown request. Active Job-handled retries and discards with errors count as errored attempts. Infrastructure failures do not invent job attempts. Heartbeat state copies these statistics, so database inspection reflects the most recent refresh rather than an instantaneous read of the worker.

Each scheduler also reports `max_cache`, `active_cache`, and `available_cache`. These count cached deadlines, which may represent multiple jobs. See [worker scheduling](scheduling.md) for the zero-cache notification deadline and cache refill behavior.

`DecentJob.stats` aggregates registered workers in the current process. `execution_at` records execution starts and completions, and `check_queue_at` records dequeue attempt starts. These fields also appear in persisted heartbeat state. See [worker activity and thread controls](worker-observability.md) for all aggregate fields, snapshot semantics, idle timing, and cooperative shutdown helpers.

`Database#stats` reports configured pool size, allocated connections, active checkouts, and idle connections. The dedicated heartbeat and LISTEN connections are outside those counts. Reserve one additional connection for each active tracker, plus the listener when enabled. An idle worker keeps its tracker. A standalone performer registers after acquiring an advisory candidate, or before its row-claim transaction in SKIP LOCKED and hybrid modes. An empty row claim releases that temporary registration.

With Active Support's fiber isolation enabled, concurrent fibers count as separate checkout owners. [Runtime resources](runtime-resources.md) lists the thread and session bounds and explains how scoped state and transactions follow that setting.

Claim cleanup can temporarily use an independent connection after interruption or pool exhaustion. It waits for a caller's interrupted checkout to retire before clearing persisted ownership, so cleanup cannot wait on its own abandoned transaction.

Create fresh database and worker objects after forking. Inherited pools and trackers are rejected instead of reusing the parent's connections or process identity. A child's rejected use must not remove the parent's process record.
