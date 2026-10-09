# Execution history and timing

Each claimed job creates one attempt record before Active Job runs. The record snapshots the serialized payload, job class, queue, and scheduled time. Retrying or rescheduling the job changes the job record; it does not rewrite earlier attempt snapshots.

Read jobs and attempts through their public APIs:

```ruby
job = DecentJob::Job.find(provider_job_id)
attempts = DecentJob::Execution.for_job(job.id)
attempt = DecentJob::Execution.find(attempts.last.id)

attempt.payload
attempt.status
attempt.queue_latency
attempt.runtime_latency
attempt.total_latency
```

Both lookup methods accept `database:` for an explicit DecentJob database. `Job#executions` and `Store#executions` expose the decoded database rows as hashes. An execution object describes the record when it was read; look it up again to observe a running attempt's completion. `attributes` returns a copy of its attributes.

Timing values are numeric seconds, or `nil` when unknown:

| Value | Job | Execution attempt |
| --- | --- | --- |
| `queue_latency` | Time from the current scheduled time to its start, finish without execution, or now. Future jobs return `nil`. | Time from the snapshotted scheduled time to the attempt's start. |
| `runtime_latency` | Wall time from the current `performed_at` to finish or now. Pending jobs and retries have no current runtime. | Persisted monotonic `duration`, available after normal attempt completion. |
| `total_latency` | Queue plus runtime when both are known. | Queue plus runtime when both are known. |

Attempt duration includes Active Job callbacks and the host executor around execution. It excludes outcome persistence and batch reconciliation. A system clock adjustment does not change this monotonic measurement. Queue latency and job runtime use wall timestamps and can reflect clock differences or adjustments.

Every completed attempt gets its own duration, including handled failures and retries. Its status records that attempt's outcome: an earlier retry remains `:retried` after a later attempt succeeds. This follows DecentJob's persisted attempt state rather than deriving an old attempt's status from the job's current completion state.

`number` reflects Active Job's execution number from the snapshotted payload. Backend retries preserve that payload, so their Active Job number can repeat even though each has a distinct execution ID. `last_status_at` is the attempt's finish time, or its start time while unfinished.

## Interrupted and discarded attempts

A killed process cannot report its final monotonic duration. Its attempt keeps `duration: nil`, including after recovery marks it interrupted. `interrupted_duration` reports wall time from its start to interruption detection, including any downtime. The replacement attempt has its own snapshot and duration.

This deliberately differs from GoodJob's reference implementation, which stores the recovery worker's short detection duration on the abandoned attempt. DecentJob leaves that runtime unknown so aggregated runtime metrics do not mistake recovery work for the killed job's runtime.

An operator can force-discard a job while its body is still running. That ends the persisted attempt without a known monotonic duration. When the original body returns, it cannot overwrite the operator's finish time or fill in a duration measured beyond the recorded finish. The job's wall runtime still describes the interval through the operator's action.

## Existing records

Migration 008 adds the snapshot and duration columns. It preserves existing attempts with unknown values. A job's current start time is recovered from its existing current attempt where possible; a pending retry cancelled before it starts does not inherit its previous attempt's runtime. Historical payloads, schedules, and monotonic durations cannot be reconstructed reliably from the current job.

Metrics that need missing data return `nil`. History is subject to the configured [record preservation and cleanup](record-preservation.md) policy. Retained attempts can be inspected even when their job class is no longer available, because reading an execution snapshot does not deserialize or run the job.
