# Worker scheduling

Each queue pool has a fixed number of execution threads and a bounded cache of future deadlines. A deadline wakes those threads to claim work from PostgreSQL. The claim query still checks queue selection, due time, pauses, concurrency limits, and ownership before execution. Cached entries contain times, not job arguments or executable job objects.

```ruby
DecentJob.configure do |config|
  config.poll_interval = 10
  config.max_cache = 10_000
end
```

The corresponding environment variables are `DECENT_JOB_POLL_INTERVAL` and `DECENT_JOB_MAX_CACHE`. CLI settings override application configuration:

```sh
bundle exec decent_job start --require ./jobs.rb --poll-interval -1 --max-cache 1000
```

## Dequeue ordering and limits

`dequeue_query_sort` defaults to `:created_at`, matching the pinned GoodJob version. This changes DecentJob's earlier scheduled-time-only ordering. Use `:scheduled_at` to retain that ordering:

```ruby
DecentJob.configure do |config|
  config.dequeue_query_sort = :scheduled_at
  config.queue_select_limit = 1000
end
```

An explicitly ordered queue list such as `+urgent,default` takes precedence over job priority. Within a queue, or across an unordered selection, smaller priority numbers come first, followed by the chosen timestamp and job ID. Job ID supplies stable ties in both modes. Creation order never makes a future-scheduled job eligible early.

An Active Job retry, direct self-reenqueue, or operator retry refreshes `created_at` in created-time mode, placing it after peers already waiting at the same priority. Scheduled-time mode preserves the original creation time. Attempt snapshots retain their original scheduling metadata. A backend retry after an unhandled error is not an Active Job reenqueue and preserves its timestamps. Management rescheduling changes the deadline without refreshing creation time; durable concurrency waiters also keep their own queue position.

`queue_select_limit` defaults to 1,000. In advisory mode it bounds the eligible, queue-filtered, unpaused candidate set before advisory locks are attempted. Locked candidates count against this bound. If all candidates within it are locked, the attempt returns no job even when an unlocked job exists beyond the bound. Set the limit above the number of concurrently running jobs across workers to reduce this contention. Each attempt still executes at most one job. SKIP LOCKED and hybrid claims select one row directly and ignore this limit; see [lock strategies](locking.md).

A limit of zero prevents ordinary advisory dequeue attempts from selecting work. Explicitly targeted execution, including inline enqueue, still checks the requested job. SKIP LOCKED and hybrid claims are unaffected. This setting is separate from polling and the future-deadline cache; it does not disable their activity or stop existing jobs.

Both controls are read when claiming work, so changes apply to an existing performer. Sort values accept the two named symbols or strings; limits accept nonnegative integers or decimal integer strings. Invalid values raise `ArgumentError` rather than silently becoming a different policy. Nil clears an explicit override.

Precedence is explicit configuration or CLI, Rails configuration, environment, then defaults. Environment variables are `DECENT_JOB_DEQUEUE_QUERY_SORT` and `DECENT_JOB_QUEUE_SELECT_LIMIT`. Rails hosts can use `config.decent_job.dequeue_query_sort` and `config.decent_job.queue_select_limit`. CLI flags override configuration loaded with `--require`:

```sh
bundle exec decent_job start --require ./jobs.rb --dequeue-query-sort scheduled_at --queue-select-limit 2000
```

Migration 010 adds global and queue-scoped creation-order indexes. Existing scheduled-order indexes remain available. The new policy can execute against schema version 9 because it uses existing columns; run `decent_job migrate` to add the indexes.

## Advisory ownership

Each execution holds a PostgreSQL session advisory lock until its claim unwinds. A nested claim using that same session skips an already-held job lock without acquiring it again or changing its depth. It can select another eligible job within the candidate limit. Targeting the currently executing job returns no result.

Claims release their own lock when execution returns or raises, or when registration or attempt creation fails. An interrupted acquisition whose result is unknown retires the connection, preventing an uncertain lock from returning to the pool. A connection failure during unlock preserves an already-propagating application exception. Recovery records an unfinished attempt as interrupted when a replacement claims it; see [execution recovery](execution-recovery.md).

Eligibility is checked again under the job row lock. A candidate deleted or finished after selection cannot start another attempt. Advisory execution requires direct PostgreSQL connections or session pooling. [Lock strategies](locking.md) describes the row-based alternatives and mixed-worker ownership.

## Polling and deadlines

A positive polling interval enables periodic database checks. Fractional intervals are supported. Zero or a negative value disables polling. Values must be finite real numbers.

The default interval is 10 seconds. Rails development applications using an async execution mode default to disabled polling, matching GoodJob. Explicit configuration and environment settings override these defaults.

Workers check for ready work at startup. With a positive cache limit, they also discover future jobs and refill the cache after draining available work. Future notifications can interrupt a wait with an earlier deadline. Existing execution threads wait on a shared condition variable; scheduled jobs do not allocate individual timer threads. Workers recheck wall time at least every half second, while polling and shutdown deadlines use monotonic time.

If the application's clock is ahead of PostgreSQL, an apparently due deadline can produce an empty claim. The worker backs off for half a second before retrying that deadline. It does not bypass PostgreSQL's due-time check.

Notifications target pools that accept the named queue, including exclusion and ordered queue expressions. Counts request bounded execution capacity. A fanout hint wakes every matching pool. Empty legacy notifications wake all pools. PostgreSQL can coalesce notifications, so neither counts nor cache entries represent an exact job inventory.

Embedded async enqueue sends local scheduling hints after transaction commit even when PostgreSQL notifications are disabled globally or for the job. Rollbacks do not schedule local work. External workers require polling, an existing cached deadline, or another wakeup to discover changes whose notifications were suppressed or missed.

## Cache limits

`max_cache` must be a nonnegative integer and defaults to 10,000 per queue pool. Equal deadlines share one cache entry. When the cache fills, it keeps earlier deadlines and discovers later work as it refills.

A limit of zero disables startup cache warming. Future notifications still schedule work. DecentJob keeps one coalesced deadline per pool in this mode and rearms it from PostgreSQL after execution. This preserves notified future execution with bounded memory. It does not provide a backlog scan of previously scheduled future jobs at startup.

`Worker#stats` and process records expose `max_cache`, `active_cache`, and `available_cache` for each scheduler. At a zero limit, `active_cache` can be one for the notified deadline, and `available_cache` remains zero. Shutdown clears cached deadlines; restarting rebuilds them according to the configured limit.

## Connection recovery

The listener uses a dedicated PostgreSQL connection, sends a keepalive query every 10 seconds, and reconnects after errors. A successful connection wakes workers to catch up on available work and rebuild positive caches. Invalid notification payloads are reported through `on_thread_error` without disconnecting the listener. Rails notification dispatch runs inside the application executor.

DecentJob reports connection errors as they occur and marks listener readiness false immediately. Reporter failures do not stop reconnection. The independent heartbeat connection keeps process registration alive while the listener reconnects or execution-pool checkouts time out. Shutdown closes the listener connection; restarting creates a fresh listener and process identity.

Polling provides recovery for missed notifications and changes made outside DecentJob. Disabling both polling and notifications leaves a running worker dependent on cached deadlines and explicit wakeups. SQL claim checks prevent stale cached deadlines from executing deleted, rescheduled, paused, or already claimed work.
