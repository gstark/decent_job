# Start, shutdown, and restart

Workers and cron managers own their execution threads. Async adapters own a worker and use the configured cron manager for their database. The global runtime tracks the workers and configured cron managers it starts.

```ruby
DecentJob.start
DecentJob.shutdown(timeout: 30)
DecentJob.restart(timeout: 30)

adapter.shutdown(timeout: 30)
adapter.restart(timeout: 30)
```

`Worker#restart`, `CronManager#restart`, `Adapter#restart`, and `DecentJob.restart` first stop accepting work, then wait for the selected old run to drain. They return the owner on success or `false` when draining is incomplete or a newer lifecycle operation has superseded the restart. A later restart can try again after work finishes.

The timeout applies to the whole drain. The defaults match [shutdown](shutdown.md): `nil` waits indefinitely, zero signals without waiting, and positive values provide a grace period. `force: true` requests interruption after that period. Ruby cleanup can outlive it, in which case restart returns false and does not create a replacement run.

## Explicit and automatic startup

Calling `Worker#start` or `CronManager#start` explicitly can restart a stopped owner. Calling start repeatedly while its run is alive does not create another set of resources. An owner does not replace its old run while old threads or its process monitor still need to finish.

Automatic enqueue wakeups and framework readiness callbacks never reopen an explicitly stopped adapter. Jobs can still be enqueued and stored while workers are stopped. Use `adapter.restart` when that adapter should resume execution.

Global restart resumes its registered workers and configured cron managers. It does not create an additional default worker when an adapter supplied the existing worker. Adapters explicitly stopped with their own `shutdown` remain stopped until their own `restart`. Global shutdown alone leaves those adapters eligible for global restart. Use `DecentJob.start` to create the default worker when no execution owners have been started.

Cron managers are shared by database. Stopping an adapter also stops that database's configured cron manager, including when other adapters use the same pool. Global restart preserves this database-specific stop. An explicit `adapter.restart` or `DecentJob.start_cron(database: database)` reopens it; `DecentJob.start` also reopens cron for the global database.

`DecentJob.start` returns false while an earlier runtime shutdown is still draining. It leaves automatic startup closed until an explicit start or restart can proceed. Global `started?` and `connected?` continue to inspect all registered workers, so a deliberately stopped registered worker makes these checks false even when another worker is running.

Existing owners keep their queue and thread recipes across restart. Use reset and configuration, or construct a new directly owned worker, to change those recipes.

## Independent owners

```ruby
producer = DecentJob::Adapter.new(database: database, execution_mode: :external)
consumer = DecentJob::Worker.new(database: database, queues: "reports:2")
consumer.start

ReportJob.queue_adapter = producer
ReportJob.set(queue: "reports").perform_later

consumer.shutdown(timeout: 30)
```

A directly constructed worker or cron manager belongs to its caller. Stop it before closing its database. Global runtime shutdown does not adopt manually owned resources. The fork preparation checks still require every live worker, cron manager, and process tracker to drain before forking.

DecentJob exposes these owners directly instead of adding GoodJob's capsule wrapper. Explicit `start` remains a restart entry point for directly owned workers and cron managers. Automatic activity cannot reopen a stopped owner, and a shutdown that captured an old run cannot later interrupt a replacement run.

## Reset and configuration

`DecentJob.reset!` drains registered resources before clearing configuration, retiring adapters, and closing its database pool. If an owned callback invokes reset and cannot finish draining itself, reset returns false and leaves the database available while that callback unwinds. Call reset again from outside the owned work after it returns.

Reset also returns false while a manually owned worker, cron manager, or standalone execution still uses the global pool. Standalone execution owns the pool throughout `perform_one`, including maintenance before claiming a job or registering a heartbeat. Stop that owner or let its execution finish before trying reset again. Unrelated pools supplied explicitly remain caller-owned.

Adapters retired by reset cannot restart. Construct new adapters after reset, including when an old adapter used an explicitly supplied database that remains open. CLI configuration preserves adapters, their explicit stop flags, and database-specific cron stops while replacing drained workers and cron managers. Subsequent eligible startup uses the new CLI recipe.

Fork preparation and CLI reconfiguration reject active standalone executions, including their preliminary maintenance. Let those calls finish before retrying the lifecycle operation.

Explicit startup during a reset or configuration transition raises `DecentJob::LifecycleInProgress`. Automatic startup remains dormant. Configuration blocks execute without holding the registry lock, so they can inspect runtime state. Publish configuration before starting workers and opening the global database.
