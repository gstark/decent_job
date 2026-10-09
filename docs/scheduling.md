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
