# Record preservation and cleanup

Completed jobs and execution attempts are retained by default. Configure their preservation before starting workers:

```ruby
DecentJob.configure do |config|
  config.preserve_job_records = true
end
```

| Policy | Terminal execution records retained |
| --- | --- |
| `true` | All jobs and attempts; the default. |
| `false` | None, except cron records and operator force-discards. |
| `:on_unhandled_error` | Jobs whose exception escapes Active Job's handlers. |
| Callable | Jobs for which the callable returns a truthy value. |

Unfinished retries and direct reenqueues retain their existing job and attempt records under every policy. Once a job finishes, deleting its record also deletes all of its attempts. A discarded job that has been deleted is no longer available for manual retry or batch retry. Batch job lists and callback lists contain only records that remain in storage.

Rolling enqueue and perform throttles count stored job and attempt history. Deleting records removes them from those counts, so preserve records for jobs whose throttle must account for recently completed work. The enqueue throttle counts jobs by `created_at`. In created-time dequeue mode a retry refreshes `created_at` (see [scheduling](scheduling.md)), so the throttle counts the retry as a new enqueue.

The error-only policy distinguishes a handled discard from an unhandled exception. An exhausted `retry_on` that re-raises its exception is unhandled; an exhausted retry handled by its block is handled. Inline execution still raises an unhandled exception even when its record is deleted.

## Custom policies

```ruby
DecentJob.configure do |config|
  config.preserve_job_records = lambda do |job, error, error_event|
    error || job&.queue_name == "audited"
  end
end
```

The policy receives the executed Active Job instance, the exception (or `nil`), and an error-event symbol (or `nil`). Events include `:discarded`, `:retry_stopped`, `:handled`, and `:unhandled`. An exception returned from execution, including from a `rescue_from` handler, is a handled failure. Successful jobs have no error or error event. If job deserialization fails before an instance is available, the job argument is `nil`.

Policies run only for terminal, non-cron executions. A policy exception keeps the records and is reported to `config.on_thread_error`. An exception from that error handler also leaves the records intact. Each callback runs in a savepoint so a failed database operation does not abort completion; its database changes roll back if it raises.

Keep policies short and avoid acquiring unrelated batch or row locks. They run inside the completion transaction while execution owns the job's database connection, advisory lock, and row lock.

## Batches, cron, and operator actions

Deleting a completed batch member or callback does not erase its completion outcome. The worker commits the batch completion request together with the job's deletion. If the process dies before reconciliation, a later worker recovers the callback work. Batch records and callback delivery history are retained separately from job records.

Cron occurrence records remain preserved under every execution policy because they provide enqueue deduplication and restart catch-up history. Explicitly destroying a cron job removes that history and can allow the occurrence to be enqueued again.

Operator actions do not apply the execution preservation policy. In particular, force-discard remains authoritative when running code returns, and the returning worker retains the operator's record. Use `job.destroy_job` after its execution releases the lock to delete it explicitly.

## Age-based cleanup

Clean old records explicitly with:

```ruby
DecentJob.cleanup_preserved_jobs(
  older_than: 14 * 24 * 60 * 60,
  in_batches_of: 1000,
  include_discarded: true
)
```

`older_than` is an age in seconds, measured against `finished_at`. The cutoff is fixed when the sweep begins. Jobs finished at or before it are eligible; unfinished jobs and recently finished jobs remain. A batch is eligible only after all its work and callbacks finish, using the batch's `finished_at`. Pass `database:` for an explicit DecentJob pool.

The return value counts deleted jobs, execution attempts, and batches. Associated delivery and bookkeeping records are removed as needed but do not contribute to that count. Set `include_discarded: false` to retain discarded jobs and batches. Each job's entire attempt history is deleted with its job, including attempts newer than the cutoff.

Sweeps use bounded pages and skip busy records. A locked record can be collected by a later sweep. Eligibility is rechecked under locks so concurrent retries and batch reopening retain their active records. Foreign-key updates and concurrency-waiter promotion can briefly wait for related locks; these waits are bounded. Shutdown can stop a sweep between units of work; already committed deletions remain committed.

Explicit and automatic age-based cleanup include eligible cron records. Their deletion removes occurrence deduplication and catch-up history. Choose a retention age that covers the restart catch-up period you need. Cleanup also removes history used by rolling throttles; keep the retention age longer than their windows.

An ActiveSupport notification named `cleanup_preserved_jobs.decent_job` reports `older_than`, `timestamp`, and `destroyed_jobs_count`, `destroyed_executions_count`, `destroyed_batches_count`, and `destroyed_records_count`.

## Automatic cleanup

Workers apply these defaults:

| Configuration | Default |
| --- | --- |
| `cleanup_preserved_jobs_before_seconds_ago` | `1209600` (14 days) |
| `cleanup_discarded_jobs` | `true` |
| `cleanup_interval_jobs` | `1000` |
| `cleanup_interval_seconds` | `600` |

Their environment equivalents use the `DECENT_JOB_` prefix and uppercase names, such as `DECENT_JOB_CLEANUP_INTERVAL_JOBS`. Application configuration overrides the environment.

Cleanup is checked after a completed execution attempt, including an attempt that retries. One tracker is shared by all queue pools within a worker. It runs when the count or elapsed-time threshold is exceeded. The comparison is strict: a threshold of `1000` triggers after attempt `1001`. The elapsed-time threshold is checked on job completion; an idle worker does not periodically sweep on its own.

Set either interval to `false` or `0` to disable that threshold, or `-1` to request cleanup after every completed attempt. The string values `"false"` and `"0"` also disable it. `nil`, `true`, or an empty string restores that interval's default. Disable both intervals to turn off automatic cleanup.

These conversions apply to configuration settings. Explicit interval keywords passed directly to `DecentJob::Worker.new` use `false`, `-1`, or positive numbers; the job-count interval must be an integer.

Cleanup runs on an owned worker thread and shares the worker's shutdown lifecycle. It does not add background threads. Cleanup failures go to the worker's error handler; the worker can continue processing jobs. Inline execution applies preservation policies without starting an automatic cleanup worker.

## Command line

```sh
bundle exec decent_job cleanup_preserved_jobs --require ./jobs.rb
bundle exec decent_job cleanup --require ./jobs.rb --older-than 604800 --in-batches-of 500 --no-include-discarded
```

`cleanup` is an alias. `--before-seconds-ago` is an alias for `--older-than`. CLI age and discarded-job options override settings loaded by `--require`. The command prints the number of deleted records and exits unsuccessfully for invalid options, application load failures, or database errors.
