# Execution errors and recovery

Active Job's `retry_on`, `discard_on`, and `rescue_from` handlers run first. DecentJob records the resulting retry, discard, or success. Exceptions that escape those handlers are governed by backend configuration:

```ruby
DecentJob.configure do |config|
  config.handled_exceptions = [StandardError, NotImplementedError]
  config.retry_on_unhandled_error = false
  config.on_thread_error = ->(error) { warn error.full_message }
end
```

The shown exception list and retry setting are the defaults. `handled_exceptions` accepts exception classes and class-name strings. Names are resolved when exception handling is needed, so application classes can load after configuration. Resolved classes must inherit from `Exception`.

Here, a *handled backend exception* means DecentJob can persist its outcome. It can still be an *unhandled Active Job error* because no Active Job handler consumed it. Such a failure is discarded by default, and the worker reports the exception to `on_thread_error`. Handled Active Job retries and discards are not reported as backend failures. The reporter can fail without replacing the job's original error or preventing ordinary workers from continuing.

Exceptions outside `handled_exceptions` propagate without being marked terminal. The attempt stays unfinished and can be recovered after its owner releases the session lock. Configure the list deliberately; an excluded execution error can terminate its worker thread. When every execution thread of a worker ends this way, the worker stops. Automatic starts, such as the next async enqueue, do not restart it, and its next shutdown raises the error. Call `restart` or `start` to resume execution.

Shutdown still stops the other owned threads and waits for them within the shared deadline before re-raising that thread's original failure. Global reset drains resources without a deadline, then clears the registry and database pool before re-raising, so the failure remains visible without leaving an unusable registry behind.

## Retrying unhandled errors

```ruby
DecentJob.configure do |config|
  config.retry_on_unhandled_error = true
end
```

This keeps a caught, otherwise-unhandled failure ready for immediate execution. It has no retry limit or backoff. Prefer Active Job `retry_on` when you need either. If the job already reenqueued itself before raising, its explicit schedule is retained.

A backend retry preserves the stored payload, identifiers, queue, priority, labels, and cron/batch associations. It does not invoke enqueue callbacks or rewrite Active Job's serialized execution count. Each actual attempt still gets its own DecentJob history record. The job remains unfinished, keeps its records under every preservation policy, and does not prematurely finish its batch.

Inline execution persists the outcome and raises the unhandled exception back to the caller, including when backend retry is enabled. It does not synchronously loop through unhandled failures. An operator's force-discard remains authoritative and prevents the returning body from scheduling a backend retry.

## Interrupted attempts

In advisory and hybrid modes, process termination or a lost PostgreSQL session lets another worker acquire the job's advisory lock. SKIP LOCKED claims retain persisted ownership after a process crash until process cleanup confirms the owner is inactive and releases it. See [lock strategies](locking.md#recovery) for the recovery timing.

A replacement marks the previous unfinished attempt with `DecentJob::InterruptedError` and `error_event: "interrupted"`, then starts one replacement attempt. Recovery detection does not create a separate history entry of its own.

[Forced worker shutdown](shutdown.md) uses the same recovery path. It lets Ruby cleanup unwind before retiring the interrupted connection and releasing its job lock. Until a replacement claims the job, the abandoned attempt remains unfinished.

By default, the replacement executes the job body normally. To let Active Job handlers decide what happens after interruption, include the extension:

```ruby
class ImportJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::InterruptErrors

  retry_on DecentJob::InterruptError, wait: 30, attempts: 5

  def perform(source)
    # Import work.
  end
end
```

The extension raises `DecentJob::InterruptError` before the replacement body runs. Use `retry_on`, `discard_on`, or `rescue_from` to handle it. Without a handler, DecentJob records a failure with the `interrupted` error event, subject to the backend retry and preservation settings above.

`InterruptedError` describes the abandoned attempt in storage; it is not raised. `InterruptError` is raised in the replacement attempt and includes the original start time in its message. The interruption context applies only to that replacement job. A later scheduled retry, another job, or an unrelated nested `perform_now` does not inherit it.

For a retry handler that schedules future work, history contains the abandoned attempt and the replacement that ran the handler. The future attempt is added only when it actually starts.

These mechanisms preserve at-least-once execution. If the old process performed an external side effect before losing ownership, the replacement cannot undo or detect that side effect automatically. Jobs must tolerate repeated work.
