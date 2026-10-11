# PgBouncer

DecentJob can use PgBouncer transaction pooling with SKIP LOCKED claims, polling, and timestamp heartbeats. Configure all four settings before starting workers:

```ruby
DecentJob.configure do |config|
  config.database_url = ENV.fetch("DECENT_JOB_DATABASE_URL")
  config.lock_strategy = :skiplocked
  config.enable_listen_notify = false
  config.advisory_lock_heartbeat = false
  config.poll_interval = 5
end
```

The database URL points to PgBouncer's transaction-pooled endpoint. Environment equivalents are:

```sh
DECENT_JOB_LOCK_STRATEGY=skiplocked
DECENT_JOB_ENABLE_LISTEN_NOTIFY=false
DECENT_JOB_ADVISORY_LOCK_HEARTBEAT=false
DECENT_JOB_POLL_INTERVAL=5
```

Use a positive polling interval. Five seconds is an example; choose an interval that fits the required pickup latency and database load. Notifications are unnecessary for this configuration. Application enqueues, workers, timestamp monitors, and ownership cleanup use the same endpoint. The example sets `database_url`, so the enqueue connection is `:own`. In a Rails host with `connection = :active_record`, enqueues use the Active Record connection and workers connect with the Active Record settings; see [Rails integration](rails-integration.md#pgbouncer).

## Install before switching

Apply migration 011 before routing SKIP LOCKED workers through transaction pooling:

```sh
DECENT_JOB_DATABASE_URL="$DIRECT_DATABASE_URL" bundle exec decent_job migrate
```

Before migration 011, DecentJob falls back to advisory claims even when configured for SKIP LOCKED. That fallback requires a direct connection or session pooling. Upgrade every worker to the ownership-aware implementation before switching lock strategies. See [job locking](locking.md).

## Connection behavior

PgBouncer releases a server connection at the end of each transaction. The next query on the same Ruby `PG::Connection` can run on a different PostgreSQL backend. Do not store application state in that backend between transactions. PgBouncer's [feature map](https://www.pgbouncer.org/features.html) documents the pooling restrictions.

DecentJob's SKIP LOCKED claim persists its process UUID and attempt identity in a transaction. The job body runs after that transaction commits. Concurrency controls, management actions, batches, and cleanup use transaction-scoped locks. Timestamp heartbeats issue independent statements and do not reserve a server connection between updates.

Each worker still owns Ruby client connections, including its heartbeat client. These count against PgBouncer's client limit. The proxy's server pool must have capacity for concurrent transactions. A long application transaction can delay both execution and heartbeats; transaction pooling does not remove that capacity requirement. See [runtime resources](runtime-resources.md).

An abrupt worker exit leaves persisted ownership until its timestamp heartbeat expires and inactive-process cleanup runs. The default expiration is 300 seconds. Graceful shutdown drains running jobs and releases their ownership. Execution remains at least once; see [recovery](execution-recovery.md).

## Schema and session settings

The verified test topology uses a dedicated database and its default `public` schema. For another schema, configure a consistent PostgreSQL role/database default for every server connection rather than issuing session-level `SET search_path` through the proxy. For example, an administrator can set:

```sql
ALTER ROLE job_worker IN DATABASE jobs SET search_path = decent_job, public;
```

Reconnect existing PgBouncer server sessions after changing such defaults. The migration connection and runtime connection must resolve the same schema. Do not use `ignore_startup_parameters` to discard a requested search path. Startup-parameter tracking depends on PgBouncer and PostgreSQL versions; consult [PgBouncer configuration](https://www.pgbouncer.org/config.html#track_extra_parameters).

Advisory and hybrid execution, advisory process heartbeats, and LISTEN require direct PostgreSQL connections or PgBouncer session pooling. `Database#connect` creates an independently owned client connection, but cannot make a transaction-pooled endpoint retain a backend session. Application jobs that require session state must use an appropriate connection themselves. Statement pooling is unsupported because DecentJob requires multi-statement transactions.

## Running the integration suite

Install the `pgbouncer` executable, then run:

```sh
DECENT_JOB_TEST_DATABASE_URL=postgres:///postgres bundle exec rake spec:pgbouncer
```

Set `PGBOUNCER_BIN` if the executable is outside `PATH`. The PostgreSQL test account must be allowed to create databases. The fixture creates an isolated database, starts its own PgBouncer process with a private Unix socket, and removes both after the test. It does not start or alter a system PgBouncer service. The command fails if PgBouncer is unavailable.

Run this suite separately from the regular PostgreSQL and Rails suites. CI installs PgBouncer and invokes the suite in sequence. Local results do not establish that remote CI has run.

## Verification evidence

Local verification used PgBouncer 1.26.0, PostgreSQL 17.9, and Ruby 3.4.10 on macOS. All 13 integration specs passed, with 111 assertions. The tests force one client to change PostgreSQL backends between transactions, then exercise savepoints, scheduled jobs, retries, batches, concurrency, management, cleanup, cron deduplication, pauses, preservation, and aggregate execution statistics. Separate CLI workers verify polling, exclusive claims, renewed timestamps with only two server connections, graceful drain, force-discard, and crash recovery. Forced thread shutdown cancels a blocked SQL query and releases ownership before replacement execution. Each case checks advisory locks before disconnecting the proxy.

The built gem also passed migration and retry execution through the proxy without loading Rails or Active Record. At that checkpoint the direct runtime dependencies were `activejob` and `pg`; the gem now depends only on `pg`, and the PgBouncer suite passed again after that change. Full PostgreSQL and Rails regression suites passed; [the implementation plan](implementation-plan.md#pgbouncer-transaction-pooling-checkpoint) records counts and seeds. Remote CI and other platform/version combinations have not been executed locally.
