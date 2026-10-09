# Record preservation

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

Rolling enqueue and perform throttles count stored job and attempt history. Deleting records removes them from those counts, so preserve records for jobs whose throttle must account for recently completed work.

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

Age-based cleanup and automatic cleanup scheduling are the next implementation checkpoint. Preservation policies control what remains immediately after execution; they do not currently expire old records.
