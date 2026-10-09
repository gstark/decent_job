# DecentJob

A PostgreSQL backend for Active Job with two direct runtime dependencies: `activejob` and `pg`.

The first working slice supports persistent jobs, delayed execution, priorities, queue pools, bulk enqueue, retries, discards, execution history, inline and async execution, and external workers. It uses session advisory locks, LISTEN/NOTIFY, and polling.

This is an early implementation. Full GoodJob parity remains the project goal. Batches, recurring schedules, concurrency limits, throttling, the dashboard, and several operational features are still planned. See [the parity checklist](docs/parity.md) and [implementation plan](docs/implementation-plan.md).

## Install and run

Requires Ruby 3.2 or newer and PostgreSQL. Local verification uses Ruby 3.4 and PostgreSQL 17. The lockfile targets Active Job 8.1.4 and pg 1.7.0.

Until a release is published, add the checkout to your application's Gemfile:

```ruby
gem "decent_job", path: "/path/to/decent_job"
```

Configure the database and install its tables:

```sh
export DECENT_JOB_DATABASE_URL=postgres://localhost/my_application
bundle exec decent_job migrate
```

The database must already exist. Migrations install only DecentJob's tables in the connection's current schema. Installation is transactional and repeatable; no PostgreSQL extensions are required.

Create an application file, such as `jobs.rb`:

```ruby
require "decent_job"

ActiveJob::Base.queue_adapter = :decent_job

class GreetingJob < ActiveJob::Base
  retry_on IOError, wait: 5, attempts: 3

  def perform(name)
    puts "Hello, #{name}!"
  end
end
```

Enqueue jobs from your application:

```ruby
GreetingJob.perform_later("Ada")
GreetingJob.set(queue: "mailers", priority: -10, wait: 60).perform_later("Grace")
ActiveJob.perform_all_later(GreetingJob.new("Linus"), GreetingJob.new("Matz"))
```

Run a worker that loads those job classes:

```sh
bundle exec decent_job start --require ./jobs.rb --max-threads 5
```

SIGINT or SIGTERM stops claiming jobs and allows running jobs to finish. The CLI exits unsuccessfully if its shutdown deadline expires. A later worker can recover jobs interrupted by process termination.

## Configuration

Configure before opening the database or enqueueing jobs:

```ruby
DecentJob.configure do |config|
  config.database_url = ENV.fetch("DECENT_JOB_DATABASE_URL")
  config.execution_mode = :external
  config.queues = "urgent:2;default:3"
  config.max_threads = 5
  config.poll_interval = 1
  config.enable_listen_notify = true
  config.shutdown_timeout = 30
  config.on_thread_error = ->(error) { warn "#{error.class}: #{error.message}" }
end
```

`DECENT_JOB_DATABASE_URL` falls back to `DATABASE_URL`. The other settings support `DECENT_JOB_EXECUTION_MODE`, `DECENT_JOB_QUEUES`, `DECENT_JOB_MAX_THREADS`, `DECENT_JOB_POLL_INTERVAL`, `DECENT_JOB_ENABLE_LISTEN_NOTIFY`, and `DECENT_JOB_SHUTDOWN_TIMEOUT`. Set notification configuration to `false` to disable it. CLI options override configuration in the required application file.

Queue expressions:

| Expression | Behavior |
| --- | --- |
| `*` | All queues |
| `mailers,default` | Both queues, ordered by job priority |
| `+mailers,default` | Prefer mailers over default |
| `-mailers,slow` | Exclude both queues |
| `urgent:2;default:3` | Separate pools of two and three threads |

Smaller priority numbers run first. Equal priorities use scheduled time and job ID. Running jobs are not preempted.

The default database pool reserves one connection per execution thread plus two for application operations. The listener has an additional dedicated connection. Account separately for connections used by application code, especially Active Record.

## Embedded execution and tests

Use `execution_mode = :async` to start background workers on the first enqueue. Call `DecentJob.shutdown(timeout: 30)` to stop both explicit and adapter-started workers. Enqueues after global shutdown remain persisted until `DecentJob.start` or `reset!` reopens the lifecycle. `DecentJob.start` also starts workers for jobs already in the database.

Use `execution_mode = :inline` to run due jobs before enqueue returns. Future jobs stay persisted. Inline mode records and propagates unhandled job errors; handled retries and discards retain Active Job behavior. Nested inline jobs and immediate retries drain iteratively.

`DecentJob.perform_inline(queues: "*", limit: 1000)` drains currently due jobs explicitly. It returns the number of attempted executions. `DecentJob.reset!` drains workers, closes the global pool, and resets configuration. Existing adapter objects should be replaced after a reset.

For isolated instances:

```ruby
database = DecentJob::Database.new(url: ENV.fetch("DECENT_JOB_DATABASE_URL"), pool_size: 5)
adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
GreetingJob.queue_adapter = adapter

worker = DecentJob::Worker.new(database: database, max_threads: 3).start
# At application shutdown:
worker.shutdown(timeout: 30)
database.close
```

Directly constructed workers belong to their caller. Shut them down before closing their database.

## Transactions and reliability

Enqueue within `DecentJob.database.transaction` to atomically commit application SQL and jobs on the same connection:

```ruby
DecentJob.database.transaction do |connection|
  connection.exec_params("UPDATE accounts SET notified = true WHERE id = $1", [account_id])
  GreetingJob.perform_later("Ada")
end
```

An exception or nonlocal block exit rolls back the transaction. Nested transactions use savepoints. Inline and async execution wait until the outer transaction commits. Use the transaction API rather than issuing manual BEGIN/COMMIT statements on pooled connections.

A separate Active Record connection does not share this transaction. The adapter exposes an after-commit preference, but dedicated Rails transaction integration is still planned. Do not assume an Active Record rollback also rolls back a job inserted through a separate pg connection.

Execution is **at least once**. A process can die after an external side effect and before recording success, so jobs must tolerate retries. PostgreSQL releases session locks when it detects a disconnected worker. A replacement records the abandoned attempt as interrupted. During network partitions, an old job can continue external work even after losing its database session; database ownership checks cannot fence those external effects.

Session locks require direct PostgreSQL connections or session pooling. PgBouncer transaction pooling is not supported by the current locking mode. Stop workers before forking and construct fresh database instances in child processes.

Unhandled job errors are retained as discarded jobs. Inspect records and attempt histories with:

```ruby
store = DecentJob::Store.new(DecentJob.database)
store.find(job.provider_job_id)
store.executions(job.provider_job_id)
```

Records are currently retained indefinitely. Automatic retention and management operations are tracked in the parity checklist.

## Development

```sh
bundle install
DECENT_JOB_TEST_DATABASE_URL=postgres:///postgres bundle exec rake
bundle exec standardrb
gem build decent_job.gemspec
```

All tests use Minitest spec syntax. Database specs create and remove uniquely named schemas in the test database; the account needs schema-creation permission. Separate-connection specs exercise competing workers. Process specs cover SIGTERM/SIGINT, shutdown deadlines, and SIGKILL recovery. No production job database should be used for tests.

GoodJob is the behavioral reference, pinned at `ad4be5c2`. This gem has its own API and tables and does not consume existing GoodJob records.
