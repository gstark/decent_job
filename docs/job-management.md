# Job management

Load a job using the provider ID returned by Active Job:

```ruby
enqueued = ReportJob.perform_later("weekly")
job = DecentJob::Job.find(enqueued.provider_job_id)

job.id             # DecentJob provider ID
job.active_job_id  # Active Job ID
job.status
job.attributes
job.executions
job.active_job
job.reload
```

Pass `database:` to `find` when using an explicit pool. Otherwise it uses the current executing job's database, or the global database outside execution. A missing record raises `KeyError`. Attributes and execution records contain decoded hashes, Ruby times, arrays, and JSON values. `attributes` returns a detached snapshot, and the provider ID is immutable. Use management methods to change records and `reload` to refresh a job object's stored attributes.

`status` distinguishes `:queued`, `:scheduled`, `:retried`, `:running`, `:succeeded`, and `:discarded`. Due jobs are queued; a future job with a previous error is retried. Running status checks advisory ownership so an abandoned running row is not mistaken for an active worker. The corresponding predicate methods and `finished?` are available. Errors include class, message, and backtrace; `error_event` records why an attempt failed or was retried.

`active_job` normally raises when a class or argument cannot be deserialized. Pass `ignore_deserialization_errors: true` to retain an Active Job with its unavailable serialized arguments, or return `nil` for a missing class.

## Operator actions

```ruby
job.retry_job                      # Retry a discarded job now.
job.reschedule_job(Time.now + 300)  # Move an unfinished job's scheduled time.
job.reschedule_job                 # Make it due now.
job.discard_job("Cancelled")       # Discard an unfinished job.
job.force_discard_job("Cancelled") # Also permits a running job.
job.destroy_job                    # Delete a finished job and its attempts.
```

Ordinary actions acquire the same PostgreSQL advisory lock as workers. A conflicting lock raises `DecentJob::Job::LockedError`. Actions recheck the persisted state under a row lock; an ineligible state raises `DecentJob::Job::StateMismatchError`. For example, a successful job cannot be retried, and an unfinished job cannot be destroyed.

Force-discard marks the job and its unfinished attempts discarded without stopping the Ruby thread. Running code can still produce external side effects. Its eventual return or retry cannot overwrite the persisted discard. Destroy remains locked until that execution releases its session lock. Discarding a job also releases its concurrency claims and requests batch completion processing.

Batch retry leaves a force-discarded member alone while its previous execution still owns the session lock. Once that execution drains, it can be retried normally. Ordinary completed failures remain eligible for batch retry while their worker finishes batch bookkeeping.

Retry preserves both IDs, existing attempt history, cron metadata, and batch membership. It uses Active Job's retry and enqueue callbacks, with the job's stored locale, and returns the accepted Active Job or `false` when a callback aborts enqueue. A new attempt is recorded only when execution starts. Inline retries execute after the surrounding database transaction commits; async workers are also notified after commit.

A manual retry keeps the prior error with `error_event: "retried"` until the next attempt records its outcome. A successful attempt clears it. Discarding records `"discarded"`; ordinary execution distinguishes unhandled failures, retries, and exhausted retries. Job and attempt records retain the operator's discard even if running code later raises another exception.

Retry requires the job class to exist and use a DecentJob adapter for the same database. Missing classes raise `DecentJob::Job::DeserializationError`; an incompatible job adapter raises `DecentJob::Job::AdapterMismatchError`. Callback-created jobs must also use that database; invalid groups are rejected before their captured writes. Unavailable serialized arguments do not prevent a retry from being queued: Active Job handles deserialization when it executes. Discard and destroy remain available for jobs whose class or arguments can no longer be loaded.

Deleting cron records also deletes their occurrence deduplication history. Recurring scheduling can enqueue that occurrence again during catch-up. [Record preservation and cleanup](record-preservation.md) controls which jobs remain after execution and how long finished records are retained. Operator discard actions keep their records until explicit destruction or eligible age-based cleanup; cleanup skips them while their original execution still owns the lock.

## Cooperative stopping

Jobs can ask their adapter whether they should stop:

```ruby
reason = queue_adapter.stopping?
return if reason
```

The no-argument call supports Active Job 8.1. Passing the job explicitly, as `queue_adapter.stopping?(self)`, is also supported. Reasons include `:stopping` during shutdown, `:force_discarded` after an operator force-discard, and queue, class, or label pause reasons when persistent pauses are enabled. Otherwise the result is `false`. These are cooperative signals; application code decides where it can safely stop.

Manual retry callbacks have no active execution for these checks. They still observe shutdown, and passing the job explicitly checks its pause targets.
