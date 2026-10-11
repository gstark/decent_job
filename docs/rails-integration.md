# Rails integration

DecentJob works with Rails 8.1. Rails, Active Job, and Active Record stay optional: the only runtime dependency is pg, and standalone use loads none of them.

This guide covers:

- [Setup](#setup): the Railtie, the adapter, and configuration.
- [Active Job](#active-job): the queue adapter and native jobs.
- [Active Record](#active-record): the enqueue connection, transactions, admission locks, and PgBouncer.
- [Execution modes and boot](#execution-modes-and-boot).
- [Active Record migrations](#active-record-migrations): the migration generator and schema dumps.
- [Testing the host integration](#testing-the-host-integration).

## Setup

The Railtie loads automatically when Rails is present, or when a Rails application class is defined after DecentJob was loaded. This supports the CLI loading `config/environment.rb`: before it requires application files, the CLI loads ActiveSupport when it is installed, so the Railtie hook is in place. An application can also require `decent_job/railtie` before initialization.

Configure the adapter in the application:

```ruby
config.active_job.queue_adapter = :decent_job
config.decent_job.execution_mode = :external
config.decent_job.max_threads = 5
```

With Active Record loaded, DecentJob uses the Active Record database by default. You do not need a `database_url`. See [the enqueue connection](#the-enqueue-connection).

Install the tables with a Rails migration (see [Active Record migrations](#active-record-migrations)) or with the CLI:

```sh
bundle exec decent_job migrate --require ./config/environment.rb
```

Run workers:

```sh
bundle exec decent_job start --require ./config/environment.rb
```

Rails options override environment defaults. Explicit Ruby settings keep precedence, also when Rails options change or are removed. CLI worker options override application settings. See [configuration sources](configuration.md) for environment fallbacks and mutable settings. DecentJob uses the Rails logger unless a DecentJob logger was configured.

## Active Job

The Railtie loads the Active Job adapter when Active Job loads. Rails applications can use Active Job classes, `DecentJob::Base` classes, or both. See [native jobs](native-jobs.md).

Active Job features that DecentJob supports:

- `perform_later`, `set(wait:, queue:, priority:)`, and `perform_all_later`.
- Retries with `retry_on` and `discard_on`. A job that retries itself updates its existing row.
- `enqueue_after_transaction_commit` on job classes. See [transactions](#transactions).
- Concurrency controls, labels, batches, and cron entries through the DecentJob job extensions.

Jobs and cron callbacks run inside the Rails reloader or executor.

## Active Record

### The enqueue connection

`config.decent_job.connection` selects the connection that enqueues use:

| Value | Enqueue connection | Use it when |
| --- | --- | --- |
| `:active_record` | The Active Record connection of the current thread or fiber. | DecentJob's tables are in the Active Record database. |
| `:own` | DecentJob's own pg pool. | DecentJob uses another database, or the application has no Active Record. |

The default follows these rules, in order:

1. An explicit `connection` setting wins.
2. An explicit `database_url` selects `:own`. Explicit means `DECENT_JOB_DATABASE_URL`, `config.decent_job.database_url`, or a Ruby assignment.
3. A Rails application with the Active Record Railtie selects `:active_record`.
4. Otherwise the value is `:own`.

The `DATABASE_URL` fallback of `database_url` does not count as explicit, because it usually names the Active Record database.

In `:active_record` mode, workers keep their own pg pool. The pool connects with the settings of `ActiveRecord::Base.connection_db_config`, including `schema_search_path`, so workers read the database that enqueues write to. Workers use the pool for claims, execution bookkeeping, LISTEN, heartbeats, session advisory locks, cleanup, and cron manager state. They never hold a session on an Active Record connection.

Configuration validation rejects `:active_record` together with an explicit `database_url` that names another database. It compares host, port, and database name after libpq defaults apply. A Unix socket and `localhost` count as different hosts. Validation also rejects `:active_record` when Active Record is not loaded.

DecentJob uses `ActiveRecord::Base.connection_pool`. In a multi-database application, the DecentJob tables must be in the database of `ActiveRecord::Base`, or you must use `:own` with a `database_url`.

### Transactions

In `:active_record` mode, an enqueue is atomic with the application's Active Record transaction:

- The job row and its NOTIFY wakeup commit with the transaction. A worker cannot claim the job before the commit.
- A rollback removes the job row and discards the wakeup.
- In a nested transaction with `requires_new: true`, a rollback of the inner transaction removes only the jobs that the inner transaction enqueued.
- Bulk enqueue (`perform_all_later`), batch creation and enqueue, concurrency admission, and cron enqueue behave the same way.
- Inline execution, async wakeups, and `Database#after_commit` callbacks wait for the outermost Active Record commit. `Database#after_rollback` callbacks run when the Active Record transaction rolls back.

DecentJob opens a savepoint inside the open Active Record transaction. It registers its after-commit work with `ActiveRecord::Base.current_transaction`. Without an open Active Record transaction, an enqueue commits at once, as in `:own` mode.

The enqueue path includes:

- single enqueue: `perform_later` and `enqueue_at`
- bulk enqueue: `perform_all_later` and `DecentJob::Base.perform_all_later`
- batch creation, `Batch#enqueue`, `Batch#add`, `Batch.enqueue_all`, and `Batch#retry`
- concurrency admission
- `CronEntry#enqueue`

An enqueue from inside a DecentJob transaction on the worker pool, for example a batch callback, joins that transaction instead of the Active Record transaction.

The borrowed connection follows `config.active_support.isolation_level`. Active Record and DecentJob both key connections by the Active Support isolation context, so under fiber isolation each fiber enqueues through its own Active Record connection. Configure isolation at boot; see [runtime resource ownership](runtime-resources.md).

#### The `:own` mode

In `:own` mode, DecentJob inserts jobs through its own pg connection. That connection cannot join the Active Record transaction:

- A worker can run the job before the application commits. For example, a `deliver_later` mail can run before its account row is visible.
- After a rollback, the job stays in the queue.

The workaround is Active Job's transaction deferral on your base job:

```ruby
class ApplicationJob < ActiveJob::Base
  self.enqueue_after_transaction_commit = true
end
```

With the Active Record Railtie loaded, Active Job enqueues ordinary and bulk jobs with this setting after the surrounding transactions commit. A rollback drops the deferred enqueue. A job can set it to `false` to enqueue at once. Scheduled jobs keep their enqueue-time queue, priority, and scheduled time.

Deferral is not atomic. A crash between the commit and the enqueue loses the job, and an enqueue error happens after the application data committed. Use `:active_record` mode when DecentJob shares the Active Record database.

Active Job 8.1 reads only the job-class `enqueue_after_transaction_commit`. It ignores `config.active_job.enqueue_after_transaction_commit` and the adapter's `enqueue_after_transaction_commit?` preference. See the [Rails transaction guide](https://guides.rubyonrails.org/active_job_basics.html#using-enqueue-after-transaction-commit) and [8.1 release notes](https://guides.rubyonrails.org/8_1_release_notes.html#active-job). The gem does not change this setting.

At boot, the Railtie logs a warning when all of these are true:

- Active Record is loaded.
- The Active Job adapter is DecentJob.
- The connection mode is `:own`.
- `enqueue_after_transaction_commit` is false on `ApplicationJob`, or on `ActiveJob::Base` when the application has no `ApplicationJob`.

#### Standalone transactions

Without Active Record, use `DecentJob.database.transaction { ... }`. Enqueues through that database object share the transaction. Inline execution and async wakeups wait for its outer commit. External-mode enqueues also respect explicit SQL `BEGIN`, `COMMIT`, and `ROLLBACK` on a connection borrowed from that database. Use the block transaction API when you need DecentJob's after-commit callbacks, because it cannot observe manually issued transaction boundaries.

### Admission locks and long transactions

Concurrency admission takes `pg_advisory_xact_lock` for each concurrency scope. In `:active_record` mode, the lock belongs to the application's transaction, so PostgreSQL holds it until the application commits or rolls back.

This keeps the admission result correct: a second enqueue for the same scope waits, then counts the first job. The cost is serialization. A long transaction that enqueues a limited job blocks every other enqueue for that scope until the transaction ends. Worker concurrency checks lock the same key, so they wait for that scope too, or retry later. Enqueue limited jobs near the end of long transactions, or move the enqueue out of the transaction.

Jobs without concurrency controls take no advisory lock on enqueue.

### PgBouncer

In `:active_record` mode, the enqueue uses only transaction-scoped state: a savepoint, transaction advisory locks, and NOTIFY. It works when Active Record connects through PgBouncer transaction pooling. Active Record itself must use a configuration that supports transaction pooling, for example with prepared statements and advisory locks turned off.

The worker pool connects with the Active Record settings, so it uses the same endpoint. The worker rules in [PgBouncer](pgbouncer.md) still apply: use SKIP LOCKED claims, polling, and timestamp heartbeats behind transaction pooling. To send workers to another endpoint, use `:own` mode with a `database_url`.

## Execution modes and boot

| Mode | Behavior |
| --- | --- |
| `external` | Persist jobs for a separate worker. |
| `inline` | Execute due jobs synchronously and propagate unhandled errors. |
| `async`, `async_server` | Execute in background threads when a web server is detected. Persist only in consoles, tasks, and other processes. |
| `async_all` | Execute in background threads in any process. |

Without an explicit mode, Rails development uses `async`, test uses `inline`, and other environments use `external`. Standalone applications default to `external`. Rails development also defaults to advisory process heartbeats, while other environments use timestamp heartbeats. An explicit heartbeat setting takes precedence; see [process tracking](process-tracking.md). Server detection recognizes Rails servers, Rack boot, Puma request handling and worker boot, and JRuby Rack. A Puma cluster master does not count as a worker.

Adapters created during Rails initialization wait until the framework is ready. Boot then starts eligible adapters, including workers for jobs already in the database. Active Record is not required. The CLI suppresses this automatic startup while loading the application and starts its configured workers afterward.

Standalone `async_all` starts workers on the first enqueue. Call `adapter.start_async` or `DecentJob.start` explicitly to process an existing queue. Use `DecentJob.shutdown(timeout: 30)` to drain library workers and cron managers.

Framework readiness does not reopen an explicitly stopped adapter. Use `adapter.restart(timeout: 30)` after shutdown, or `DecentJob.restart(timeout: 30)` for owners stopped by global shutdown. Adapters retired by `reset!` must be replaced. See [lifecycle and restart](lifecycle.md).

## Active Record migrations

When DecentJob's tables live in the Active Record database, you can install them with a Rails migration instead of `decent_job migrate`. Rails then records the tables in `db/schema.rb` or `db/structure.sql`:

```sh
bin/rails generate decent_job:migration
bin/rails db:migrate
```

The generator writes `db/migrate/<timestamp>_decent_job_schema_v<version>.rb`, where the version is the newest SQL migration in the gem. The migration calls `DecentJob::Schema.apply` inside the Active Record migration transaction. It applies every packaged SQL migration up to that version that `decent_job_schema_migrations` does not record yet. A later gem upgrade does not change what an older generated migration applies.

After a gem upgrade that adds SQL migrations, run the generator again. The new migration applies only the new versions. If no SQL migration was added, the generator stops because a migration with the same name exists. Pass `--database <name>` to write the migration for another database of a multi-database application.

Rails schema dumps contain table definitions but no table rows. The Railtie therefore adds the rows of `decent_job_schema_migrations` to both dump formats as one `INSERT` statement. After `db:schema:load` or `db:prepare`, `DecentJob.migrated?` returns true and `decent_job migrate` has no work to do. The dump hooks use private Active Record methods (`ActiveRecord::SchemaDumper#trailer` and the PostgreSQL adapter's `dump_schema_versions`), so check them when you upgrade Rails.

The generated migrations have no `down` method, because the SQL migrations are not reversible. A rollback raises `ActiveRecord::IrreversibleMigration`.

## Testing the host integration

The core test bundle excludes Rails. Install and run the optional host suite separately:

```sh
BUNDLE_GEMFILE=gemfiles/rails.gemfile bundle install
BUNDLE_GEMFILE=gemfiles/rails.gemfile bundle exec rake spec:rails
```

These Minitest specs boot real Rails applications in child processes and use PostgreSQL. `integration/rails_active_record_enqueue_spec.rb` covers the `:active_record` mode. Run the host suite separately from the core suite, because PostgreSQL notifications are database-wide.
