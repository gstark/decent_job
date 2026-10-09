# Job locking

`lock_strategy` selects how workers claim jobs. The default is `:advisory`.

```ruby
DecentJob.configure do |config|
  config.lock_strategy = :skiplocked
end
```

The setting accepts `:advisory`, `:skiplocked`, or `:hybrid`, and the corresponding strings. Other values raise `ArgumentError`. Nil clears an explicit override. Precedence is explicit configuration or CLI, Rails configuration, `DECENT_JOB_LOCK_STRATEGY`, then the default. Rails hosts can set `config.decent_job.lock_strategy`.

```sh
bundle exec decent_job start --require ./jobs.rb --lock-strategy hybrid
```

A claim reads the strategy before acquiring ownership. Configuration changes affect subsequent claims, including claims made by an existing performer. An executing job retains its original ownership until completion or cleanup.

| Strategy | Acquisition | Ownership during execution |
| --- | --- | --- |
| `advisory` | Try session advisory locks in a bounded candidate list, then recheck the selected row | A PostgreSQL session advisory lock |
| `skiplocked` | Select and update a row in one transaction using `FOR NO KEY UPDATE SKIP LOCKED` | Persisted process UUID and execution identity |
| `hybrid` | Select a row with `SKIP LOCKED` and acquire its session advisory lock in the claim transaction | A PostgreSQL session advisory lock and persisted claim metadata |

All strategies filter due times, queues, and pauses before execution. They use the configured priority and timestamp ordering and create an attempt only after claiming the job. Row-lock strategies skip a row held by another transaction. Advisory mode retains its row-lock wait and eligibility recheck after acquiring the session lock.

`queue_select_limit` applies only to advisory mode, matching GoodJob. SKIP LOCKED and hybrid mode select one eligible row directly and ignore that setting, including a value of zero. A zero advisory limit prevents ordinary claims but permits explicitly targeted execution. See [dequeue ordering](scheduling.md#dequeue-ordering-and-limits).

## Ownership and operations

`Job#lock_type`, `locked_by_id`, and `locked_at` expose claimed metadata. Normal completion clears them. `running?` reports advisory or persisted ownership; a finished status can coexist with ownership while a force-discarded job is still unwinding.

Workers using different strategies can share DecentJob's tables. Advisory claims recheck persisted SKIP LOCKED ownership before starting an attempt. Row claims check advisory ownership and persisted owners. A nested claim skips its own active job while retaining the ability to execute another eligible job.

Concurrency limits, ordinary job-management actions, batch retry, and cleanup recognize persisted SKIP LOCKED ownership. Force-discard retains its existing authority over the final outcome and does not terminate the executing Ruby code. Cleanup and retry leave a force-discarded job alone until its owner has released it.

## Recovery

Losing an advisory or hybrid execution session permits a replacement claim after PostgreSQL releases its lock. An unfinished previous attempt becomes interrupted when the replacement starts.

A crashed SKIP LOCKED process leaves persisted ownership. Process cleanup checks that the owning process is inactive before releasing its jobs and concurrency claims. Heartbeat-based processes expire after 300 seconds without an update; processes using advisory heartbeat ownership become inactive when that process lock disappears. An old timestamp alone cannot override a live advisory process lock. See [process tracking](process-tracking.md).

Ordinary exceptions and interrupted threads release their persisted claim after application cleanup unwinds. Completion and cleanup use the execution identity so they cannot clear ownership belonging to a newer attempt. Interrupted attempts remain unfinished until recovered; cleanup does not invent a successful execution.

Execution remains at least once. A lost database connection cannot prevent application code from completing external side effects. See [execution recovery](execution-recovery.md).

## Installation and connection pooling

Migration 011 adds nullable `lock_type` metadata without rewriting existing jobs. Before that migration, all requested strategies fall back to advisory mode. Run `decent_job migrate` before relying on SKIP LOCKED behavior. An existing performer detects the installed column on its next claim.

Deploy the ownership-aware implementation to every worker before enabling mixed strategies. Earlier DecentJob builds did not check persisted SKIP LOCKED ownership and cannot safely share that mode's jobs. This is not compatibility with GoodJob's tables or workers.

Advisory and hybrid execution require direct PostgreSQL connections or session pooling. SKIP LOCKED supports PgBouncer transaction pooling with LISTEN and advisory heartbeats disabled and a positive polling interval. Migration 011 must be installed before switching to that endpoint. See [PgBouncer configuration and verification](pgbouncer.md) for all required settings, schema configuration, and connection limits.
