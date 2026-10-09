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

## Aggregate statistics

`ExecutionMetrics` reads retained attempts without loading job classes or serialized arguments:

```ruby
metrics = DecentJob::ExecutionMetrics.new
summary = metrics.summary
by_class = metrics.grouped_by(:job_class)
by_queue = metrics.grouped_by(:queue_name)

metrics.summary(
  from: Time.iso8601("2026-10-01T00:00:00Z"),
  to: Time.iso8601("2026-10-02T00:00:00Z"),
  queue_name: "default"
)
```

Pass `database:` to the constructor for an explicit database. Otherwise it captures the current execution database or the global database. Each method runs one aggregate SQL query over attempt snapshots and returns fresh symbol-keyed hashes. `summary` returns one hash; `grouped_by` returns an array, sorted by the grouping field with unknown names last. Grouping accepts `:job_class`, `:queue_name`, or their exact strings.

Each result contains:

| Keys | Meaning |
| --- | --- |
| `jobs_count`, `executions_count` | Distinct retained jobs and number of attempts |
| `avg_queue`, `min_queue`, `max_queue` | Queue wait in seconds |
| `avg_execution`, `min_execution`, `max_execution` | Measured monotonic runtime in seconds |
| `avg_total`, `min_total`, `max_total` | Queue plus runtime, calculated for each attempt before aggregation |

Grouped results also include the selected `job_class` or `queue_name`. Names come from execution snapshots, so renaming, rescheduling, or changing the current job does not rewrite old groups. One job can appear in several groups if its attempts used different queues or classes; adding grouped job counts can therefore exceed the overall distinct count.

Both methods accept `from:`, `to:`, `job_class:`, and `queue_name:`. Bounds apply to the snapshotted scheduled time, with an inclusive start and exclusive end. Use Time values, including Active Support times with zones; nil means unbounded. Equal bounds produce an empty range. Reversed bounds or invalid types raise `ArgumentError`. Class and queue filters accept exact strings or nil; nil leaves that filter unrestricted. Without time bounds, attempts with unknown historical schedules remain in the counts.

Running and interrupted attempts contribute to counts and to any known queue latency. Unknown values are excluded independently from each measurement. For example, an attempt with an unknown schedule but a measured duration contributes to execution averages, but not queue or total averages. A known zero duration remains zero. Negative queue differences caused by wall-clock adjustments remain visible.

An empty summary has zero counts and nil measurements; empty grouping returns `[]`. Jobs that have never started have no attempt and contribute nothing. Preservation and cleanup can remove history, so these queries describe retained records rather than lifetime totals. Dashboard charts and histograms remain deferred.
