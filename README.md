# DecentJob

A PostgreSQL backend for Active Job with two direct runtime dependencies: `activejob` and `pg`.

The current implementation supports persistent jobs, delayed execution, priorities, queue pools, bulk capture, batches, labels, concurrency limits and throttles, persistent pauses, recurring schedules with restart catch-up, retries, discards, execution history, inline and async execution, and external workers. It uses session advisory locks, LISTEN/NOTIFY, and polling.

This is an early implementation. Full GoodJob parity remains the project goal. The dashboard and several operational features are still planned. See [the parity checklist](docs/parity.md) and [implementation plan](docs/implementation-plan.md).

Numeric and natural-language cron parsing are implemented as a foundation for recurring schedules. Supported expressions, reference comparisons, and timezone corrections are documented in [cron compatibility](docs/cron-compatibility.md).

[Recurring jobs](docs/recurring-jobs.md) support callable schedules and arguments, persistent enable/disable overrides, history, and enqueue deduplication across processes. Set `config.enable_cron = true` to start configured schedules with workers, or call `DecentJob.start_cron` independently.

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

Add `--probe-port 7001` to expose HTTP liveness and worker readiness checks. The CLI also supports systemd readiness, stopping, and watchdog notifications. See [health probes and systemd](docs/health-services.md) for configuration and shutdown behavior.

Add `--subprocesses 3` to supervise three workers, or use pipe-separated recipes such as `--queues 'critical:2|default:4'`. Each child gets its own connections and threads. See [the supervisor](docs/supervisor.md) for lifecycle hooks, crash replacement, cluster readiness, and shutdown.

Use `--idle-timeout 60` to exit a single worker after an idle period, or `--daemonize --pidfile ./tmp/pids/worker.pid` to detach it. See [daemon mode and idle shutdown](docs/daemon-and-idle.md) for PID ownership, logging, and shutdown behavior.

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
  config.enable_pauses = true
  config.shutdown_timeout = 30
  config.on_thread_error = ->(error) { warn "#{error.class}: #{error.message}" }
end
```

`DECENT_JOB_DATABASE_URL` falls back to `DATABASE_URL`. The other settings support `DECENT_JOB_EXECUTION_MODE`, `DECENT_JOB_QUEUES`, `DECENT_JOB_MAX_THREADS`, `DECENT_JOB_POLL_INTERVAL`, `DECENT_JOB_ENABLE_LISTEN_NOTIFY`, `DECENT_JOB_ENABLE_PAUSES`, and `DECENT_JOB_SHUTDOWN_TIMEOUT`. Set notification configuration to `false` to disable it. CLI options override configuration in the required application file.

Queue expressions:

| Expression | Behavior |
| --- | --- |
| `*` | All queues |
| `mailers,default` | Both queues, ordered by job priority |
| `+mailers,default` | Prefer mailers over default |
| `-mailers,slow` | Exclude both queues |
| `urgent:2;default:3` | Separate pools of two and three threads |

Smaller priority numbers run first. Equal priorities use scheduled time and job ID. Running jobs are not preempted.

The default database pool reserves one connection per execution thread plus two for application operations. Automatic cron adds three per configured entry, or four when restart catch-up is enabled. Each worker's heartbeat monitor and optional listener have additional dedicated connections. Account separately for connections used by application code, especially Active Record. See [recurring jobs](docs/recurring-jobs.md) for scheduler costs and [process tracking](docs/process-tracking.md) for heartbeat lifecycle, worker statistics, and connection sizing.

Per-job notification controls and transactional publication are described in [notifications](docs/notifications.md). Disabling a notification does not prevent execution.

## Embedded execution and tests

Use `execution_mode = :async_all` to start background workers on the first enqueue in a standalone process. The `async` and `async_server` modes start embedded workers only in detected web servers. Rails starts eligible adapters after application boot. See [Rails integration](docs/rails-integration.md) for configuration, reloading, and transaction deferral. Call `DecentJob.shutdown(timeout: 30)` to stop registered workers and cron managers with one shared deadline. Enqueues after global shutdown remain persisted until `DecentJob.start` or `reset!` reopens the lifecycle. `DecentJob.start` also starts workers for jobs already in the database.

Use `execution_mode = :inline` to run due jobs before enqueue returns. Future jobs stay persisted. Inline mode records and propagates unhandled job errors; handled retries and discards retain Active Job behavior. Nested inline jobs and immediate retries drain iteratively.

`DecentJob.perform_inline(queues: "*", limit: 1000)` drains currently due jobs explicitly. It returns the number of attempted executions. `DecentJob.reset!` drains workers and cron callbacks, closes the global pool, and resets configuration. Existing adapter objects should be replaced after a reset.

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

## Labels, concurrency, and pauses

Include the labels extension for searchable job metadata:

```ruby
class AccountJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::Labels
  self.decent_job_labels = ["accounts", -> { "account:#{arguments.first}" }]
end

AccountJob.set(decent_job_labels: ["manual"]).perform_later(42)
```

Labels are trimmed, converted to strings, and deduplicated. Instance labels replace the class defaults. Retries persist changed labels in both the payload and indexed columns.

The concurrency extension includes labels. Use a class key for one job type, or label rules to share a limit across classes:

```ruby
class AccountSyncJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::Concurrency
  decent_job_control_concurrency_with(enqueue_limit: 100, perform_limit: 5)
  decent_job_concurrency_rule(
    label: -> { "account:#{arguments.first}" },
    apply_label: true,
    perform_limit: 1
  )
end
```

`total_limit` supplies the default for both enqueue and perform limits. An explicit `enqueue_limit` or `perform_limit` overrides that default for its phase. Legacy configuration uses the job class as its default key; `key: nil` disables it. New rules can match a key, a label, or all jobs when neither is supplied. A label takes precedence over a key. Label rules only apply to jobs carrying that label; `apply_label: true` adds it even when instance labels were explicitly supplied.

Enqueue limits count unfinished jobs, excluding executing jobs for an enqueue-only limit. Admission and insertion share a PostgreSQL transaction and scope lock. Rejected `perform_later` calls return `false`. Limits run after application enqueue callbacks. The named callback `:_decent_job_concurrency_before_enqueue` supports Active Job's `skip_callback` API.

Perform-limit rejections retry with Active Job's polynomial delay. Durable waiting claims let completion or crash recovery move a waiter forward immediately. Promotion does not reserve capacity; the job must pass its rules again. Controls are bypassed for direct `perform_now` and other queue adapters.

Both phases accept a rolling throttle, for example `enqueue_throttle: [20, 60]` or `perform_throttle: [10, 60]`. Limits, keys, and throttle arrays may be callables. Perform throttling follows the reference's execution-history semantics: it considers the earliest executions in the window and tests job identity, excluding throttle-rejected attempts. A retry of an already admitted job can remain eligible within that window.

Limits and throttle counts must be nonnegative integers; throttle periods must be positive, finite numbers of seconds. Invalid values raise configuration errors. This validation is stricter than the reference's permissive handling of malformed limits.

Pauses persist until explicitly removed. Enable dequeue filtering with `config.enable_pauses = true` or `DECENT_JOB_ENABLE_PAUSES=true`; it is disabled by default.

```ruby
DecentJob.pause(queue: "mailers")
DecentJob.pause(job_class: AccountSyncJob)
DecentJob.pause(label: "account:42")
DecentJob.paused # {queues: [...], job_classes: [...], labels: [...]}
DecentJob.paused?(queue: "mailers")
DecentJob.unpause(queue: "mailers")
```

Provide exactly one target per mutation. DecentJob deliberately accepts labels on both `pause` and `unpause`, resolving the reference's inconsistent top-level API. Pausing prevents new execution claims; it does not interrupt running jobs. Unpausing notifies sleeping workers.

## Bulk capture

```ruby
jobs = DecentJob::Bulk.enqueue do
  GreetingJob.perform_later("Ada")
  GreetingJob.set(wait: 60).perform_later("Grace")
end

DecentJob::Bulk.enqueue([GreetingJob.new("Linus"), GreetingJob.new("Matz")])
```

A block captures enqueues until it returns. An exception in the block abandons its captured jobs. Nested enqueue blocks join the outer capture; separate threads have independent buffers. The returned array contains accepted jobs after persistence, and captured enqueue callbacks run once. Concurrency-controlled jobs receive individual checks; ordinary jobs use SQL bulk insertion. Inline execution begins after capture ends.

Captured callbacks observe provisional enqueue success. Admission happens after the block, so a later rejection updates the job's status but cannot undo callback side effects. Ordinary `perform_later` and explicit controlled bulk jobs suppress `after_enqueue` when admission rejects them.

`DecentJob::Bulk.capture { ... }` captures jobs without flushing them and restores an enclosing capture afterward. Pass either explicit jobs or a block to these APIs. To make writes across multiple flush groups atomic on one database, wrap bulk enqueue in `DecentJob.database.transaction`.

## Batches

Group jobs and enqueue callbacks when the group finishes:

```ruby
class GreetingBatchFinishedJob < ActiveJob::Base
  def perform(batch, options)
    puts "#{batch.properties.fetch(:account)}: #{options.fetch(:event)}"
  end
end

batch = DecentJob::Batch.enqueue(
  description: "Account greetings",
  on_finish: GreetingBatchFinishedJob,
  callback_queue_name: "default",
  callback_priority: 10,
  account: 42
) do
  GreetingJob.perform_later("Ada")
  GreetingJob.perform_later("Grace")
end

batch.reload.jobs_finished? # All member jobs have finished.
batch.reload.finished?      # All members and callback jobs have finished.
batch.reload.succeeded?     # Finished with no discarded member or callback.
```

`on_success` fires when all members finish without a discard. `on_discard` fires on the first discarded member or callback; it can fire before the other members finish. `on_finish` fires once all members finish. Each receives the batch and `{event: :success}`, `{event: :discard}`, or `{event: :finish}`. Callback jobs can use ordinary Active Job retry handlers. Callback ordering is not guaranteed.

Batch properties use Active Job serialization, including symbols, dates, and GlobalID objects. Supply arbitrary keyword properties or an explicit `properties:` hash. Batches themselves support GlobalID serialization. Use `DecentJob::Batch.find(id)` to load a batch and `reload` to refresh an existing object's state.

To build a batch incrementally, use `Batch.new`, `add(jobs)` or an `add` block, then `enqueue`. Added jobs are persisted immediately, while finish/success callbacks wait until the batch is marked enqueued. `enqueue` accepts an array and a block together. The class method returns the batch; instance `enqueue` and `add` return accepted jobs. Existing jobs can be attached without inserting them again. A running job can access its batch through `batch` and `batch?`; ordinary jobs it enqueues do not automatically inherit membership.

Calling `enqueue` again reopens the same batch. Calling `retry` reopens it and retries only discarded members still in storage, running their Active Job enqueue callbacks and preserving IDs and execution history. Old callback jobs are not manually retried. `active_jobs` and `callback_active_jobs` return associated deserialized jobs still in storage, including preserved completed jobs.

Create several new batches in one transaction with pairs of batches and job arrays:

```ruby
first = DecentJob::Batch.new(account: 42, on_finish: GreetingBatchFinishedJob)
second = DecentJob::Batch.new(account: 43, on_finish: GreetingBatchFinishedJob)
DecentJob::Batch.enqueue_all([
  [first, [GreetingJob.new("Ada")]],
  [second, []]
])
```

This uses bulk inserts for batch records and ordinary members, with individual admission checks for controlled jobs. Empty batches also run their callbacks. All batches, members, and callback jobs must use DecentJob adapters sharing the same database object; pass `database:` to batch constructors when using an isolated database. A job can belong to only one member batch. Batch operations flush independently of an enclosing bulk capture, and captured enqueue callbacks observe the provisional success described above.

Callback enqueue is deduplicated per batch generation. Completion requests commit with job results, allowing idle worker polls to recover callbacks after a crash. Admission-rejected callbacks remain pending until a later poll admits them. Callback execution has the same at-least-once guarantee as other jobs. Age-based cleanup removes eligible finished batches and their delivery history after callbacks finish.

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

Unhandled job errors are retained as discarded jobs by default. Configure `handled_exceptions` and `retry_on_unhandled_error` to control backend error handling. The opt-in `ActiveJobExtensions::InterruptErrors` module exposes interrupted attempts to Active Job retry/discard handlers; see [execution errors and recovery](docs/execution-recovery.md). [Execution history and timing](docs/execution-history.md) describes attempt snapshots, durations, and latency. Inspect records and attempt histories with:

```ruby
store = DecentJob::Store.new(DecentJob.database)
store.find(job.provider_job_id)
store.executions(job.provider_job_id)
```

Use [job management](docs/job-management.md) to retry, reschedule, discard, or destroy persisted jobs:

```ruby
record = DecentJob::Job.find(job.provider_job_id)
record.retry_job # Only discarded jobs are eligible.
```

Ordinary actions reject worker lock conflicts. Explicit force-discard remains authoritative when running code returns or attempts to retry; it does not terminate that code. The job's adapter exposes cooperative shutdown, force-discard, and pause reasons through `stopping?`.

Use `config.preserve_job_records` to retain all completed jobs (the default), delete them after execution, preserve only unhandled failures, or apply a custom predicate. Unfinished retries and cron occurrence records remain preserved after execution. Workers clean preserved records older than 14 days by default, checking cleanup thresholds after completed attempts. Run `bundle exec decent_job cleanup --require ./jobs.rb` to clean explicitly. See [record preservation and cleanup](docs/record-preservation.md) for age settings, disabling automatic cleanup, batch behavior, and cron-history tradeoffs.

## Development

```sh
bundle install
DECENT_JOB_TEST_DATABASE_URL=postgres:///postgres bundle exec rake
bundle exec standardrb
gem build decent_job.gemspec
```

All tests use Minitest spec syntax. Database specs create and remove uniquely named schemas in the test database; the account needs schema-creation permission. Separate-connection specs exercise competing workers. Process specs cover SIGTERM/SIGINT, shutdown deadlines, and SIGKILL recovery. No production job database should be used for tests.

GoodJob is the behavioral reference, pinned at `ad4be5c2`. This gem has its own API and tables and does not consume existing GoodJob records.
