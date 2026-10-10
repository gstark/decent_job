# Rails integration

DecentJob works with Rails 8.1 while keeping Rails and Active Record optional. Its only runtime dependency is pg. The Railtie loads automatically when Rails is present, or when a Rails application class is defined after DecentJob was loaded. This supports the CLI loading `config/environment.rb`: before it requires application files, the CLI loads ActiveSupport when it is installed, so the Railtie hook is in place. An application may also explicitly require `decent_job/railtie` before initialization.

The Railtie loads the Active Job adapter when Active Job loads. Rails applications can use Active Job classes, `DecentJob::Base` classes, or both. See [native jobs](native-jobs.md).

Configure the adapter and its PostgreSQL connection in the application:

```ruby
config.active_job.queue_adapter = :decent_job
config.decent_job.database_url = ENV.fetch("DATABASE_URL")
config.decent_job.execution_mode = :external
config.decent_job.max_threads = 5
```

Run `bundle exec decent_job migrate --require ./config/environment.rb` to install DecentJob's tables, then `bundle exec decent_job start --require ./config/environment.rb` to run workers. The connection may point to a separate PostgreSQL database. DecentJob uses its own pg pool even when Active Record connects to the same database.

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

Rails options override environment defaults. Explicit Ruby settings retain precedence, including when Rails options change or are removed; CLI worker options override application settings. See [configuration sources](configuration.md) for environment fallbacks and mutable settings. The Rails logger is used unless a DecentJob logger was configured. Standalone use does not load Rails or Active Record.

## Execution modes and boot

| Mode | Behavior |
| --- | --- |
| `external` | Persist jobs for a separate worker. |
| `inline` | Execute due jobs synchronously and propagate unhandled errors. |
| `async`, `async_server` | Execute in background threads when a web server is detected. Persist only in consoles, tasks, and other processes. |
| `async_all` | Execute in background threads in any process. |

Without an explicit mode, Rails development uses `async`, test uses `inline`, and other environments use `external`. Standalone applications default to `external`. Rails development also defaults to advisory process heartbeats, while other environments use timestamp heartbeats. An explicit heartbeat setting takes precedence; see [process tracking](process-tracking.md). Server detection recognizes Rails servers, Rack boot, Puma request handling and worker boot, and JRuby Rack. A Puma cluster master does not count as a worker.

Adapters created during Rails initialization wait until the framework is ready. Boot then starts eligible adapters, including workers for jobs already in the database. Active Record is not required. The CLI suppresses this automatic startup while loading the application and starts its configured workers afterward.

Standalone `async_all` starts workers on the first enqueue. Call `adapter.start_async` or `DecentJob.start` explicitly to process an existing queue. Use `DecentJob.shutdown(timeout: 30)` to drain library workers and cron managers. Jobs and cron callbacks run in the host reloader/executor context when available.

Framework readiness does not reopen an explicitly stopped adapter. Use `adapter.restart(timeout: 30)` after shutdown, or `DecentJob.restart(timeout: 30)` for owners stopped by global shutdown. Adapters retired by `reset!` must be replaced. See [lifecycle and restart](lifecycle.md).

## Transactions

DecentJob follows `config.active_support.isolation_level` for execution metadata and its pg pool. Under fiber isolation, interleaved fibers have separate checkouts and transaction callbacks. Configure isolation at boot; see [runtime resource ownership](runtime-resources.md).

Enable Active Job 8.1's supported transaction deferral on your base job:

```ruby
class ApplicationJob < ActiveJob::Base
  self.enqueue_after_transaction_commit = true
end
```

With the Active Record Railtie loaded, ordinary and bulk jobs with this setting are enqueued after the surrounding transactions commit. Rollback drops the deferred enqueue. A job may override the setting to `false` to enqueue immediately. Scheduled jobs retain their enqueue-time queue, priority, and scheduled time.

Active Job 8.1 uses this job-class boolean; it does not consult the adapter's `enqueue_after_transaction_commit?` preference. See the [Rails transaction guide](https://guides.rubyonrails.org/active_job_basics.html#using-enqueue-after-transaction-commit) and [8.1 release notes](https://guides.rubyonrails.org/8_1_release_notes.html#active-job).

This deferral does not make Active Record's transaction and DecentJob's insertion atomic. An enqueue can fail after application data commits. Without deferral, an enqueue through DecentJob's separate connection can remain persisted even if the application transaction rolls back.

For standalone transactions, use `DecentJob.database.transaction { ... }`. Enqueues through that database object share the transaction; inline execution and async wakeups wait for its outer commit. External-mode enqueues also respect explicit SQL `BEGIN`/`COMMIT`/`ROLLBACK` on a connection borrowed from that database. Use the block transaction API when you need DecentJob's after-commit callbacks; it cannot observe manually issued transaction boundaries.

## Testing the host integration

The core test bundle excludes Rails. Install and run the optional host suite separately:

```sh
BUNDLE_GEMFILE=gemfiles/rails.gemfile bundle install
BUNDLE_GEMFILE=gemfiles/rails.gemfile bundle exec rake spec:rails
```

These Minitest specs boot real Rails applications in child processes and use PostgreSQL. Run them separately from the core suite because PostgreSQL notifications are database-wide.
