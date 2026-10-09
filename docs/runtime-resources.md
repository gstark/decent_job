# Runtime resources and execution isolation

DecentJob uses Ruby threads and its own PostgreSQL pool. Active Job supplies Active Support's execution-isolation support. No separate thread-pool or scheduling dependency is required.

## Resource ownership

| Owner | Threads | PostgreSQL connections |
| --- | --- | --- |
| Worker execution pools | The sum of the configured queue-pool thread counts | Shared-pool checkouts; advisory and hybrid claims retain one through execution, while SKIP LOCKED returns the acquisition checkout before the body |
| Worker notification listener | One when LISTEN/NOTIFY is enabled | One dedicated session |
| Active process tracker | One monitor, shared by its registration leases | One dedicated session, including when every pooled connection is busy |
| Cron entry | One timer and two delivery threads; one additional catch-up thread when enabled | Checkouts from its database's pool |
| Record and process cleanup | Existing execution threads | Checkouts from the shared pool |
| Interrupted or exhausted-pool claim cleanup | The thread releasing the claim | A temporary independent connection after its original checkout has unwound |
| Default health probe | One server thread | None |
| systemd watchdog | One when watchdog notifications are enabled | None |

Standalone and inline performers register their tracker for a claim. Advisory mode registers after acquiring a candidate; row modes register before the claim transaction. An empty row claim releases its temporary registration. Workers keep a registration while idle, and active registrations retain the monitor through shutdown. Multiple trackers using one database each need their own dedicated session.

Scheduled jobs use bounded caches and condition-variable deadlines on existing worker threads. They do not create a thread per job. Cron keeps one pending live occurrence per entry, and catch-up proceeds serially. A slow cron entry does not consume another entry's delivery threads.

These owners provide the utility concurrency supplied by GoodJob's `SharedExecutor`. DecentJob does not expose a generic utility executor: listener, heartbeat, cron, and cleanup work have explicit owners and shutdown rules. See [lifecycle operations](lifecycle.md), [scheduling](scheduling.md), and [process tracking](process-tracking.md).

## Execution context

Current execution, worker status, bulk capture, batch membership, cron occurrence, and inline buffering follow `ActiveSupport::IsolatedExecutionState.isolation_level`.

- With `:thread`, the default, fibers on one thread share the context. Different threads remain isolated.
- With `:fiber`, each fiber has its own context, including fibers on the same thread.

Choose the setting at boot. Rails applications can use `config.active_support.isolation_level = :fiber`. Standalone applications can set it before starting DecentJob:

```ruby
ActiveSupport::IsolatedExecutionState.isolation_level = :fiber
```

Nested scopes restore their outer values on normal return, exceptions, and nonlocal exits. DecentJob stores this metadata outside `ActiveSupport::CurrentAttributes`, so Active Job's reset of application current attributes does not remove job ownership or enqueue context. Host executor/reloader wrapping preserves the surrounding DecentJob scope.

When Rails streaming copies execution state with `share_with`, DecentJob's immutable snapshots preserve the copied values. Later scope changes and restoration in either context do not change the other context's state.

DecentJob's execution-state storage belongs to one process. A child discards inherited metadata, including state on a previously suspended fiber, without clearing unrelated host keys. An outer scope unwinding after the reset cannot restore its old process's state.

## Connection and transaction scope

Pooled connections use the same isolation boundary. Nested calls in one context reuse their checkout. Under fiber isolation, two interleaved fibers use separate sessions and separate transaction callbacks, even when they run on the same Ruby thread. One fiber's commit cannot dispatch another fiber's callbacks or commit its work.

The configured pool size bounds all pooled sessions. Size it for concurrent execution contexts, including application enqueues and cron callbacks. Listener and monitor sessions are additional to that limit. Ownership cleanup after interruption or pool exhaustion can briefly open an independent connection for each affected claim. Changing the isolation setting while resources are live is unsupported; configure it before checkouts or scoped work begin.

The process monitor owns its dedicated session. Registration, inspection, and job cleanup do not operate on that connection from another thread. Heartbeat state callbacks run inside the host executor, outside registration locks. Renewal remains independent of the pooled checkouts used by jobs and cron. Interrupted pooled sessions remain owned until application cleanup unwinds, then are retired as described in [forced shutdown](shutdown.md).

## Reference audit

The reference is GoodJob at `ad4be5c2`.

| Reference behavior | DecentJob counterpart and evidence |
| --- | --- |
| `SharedExecutor`: utility work can progress while another task blocks; shutdown and restart own all tasks | Dedicated listener/monitor threads, per-entry cron delivery, and captured-run lifecycle; `listener_lifecycle_spec.rb`, `cron_manager_spec.rb`, `concurrent_lifecycle_spec.rb`, and `runtime_utilities_process_spec.rb` |
| `SafeState` and `CurrentThread`: configured isolation, nested restoration, values survive the host wrapper | Execution-state scopes and persisted attempt/error metadata; `execution_state_spec.rb`, `thread_controls_spec.rb`, `execution_recovery_spec.rb`, and Rails integration |
| `CapsuleTracker`: nested/concurrent registrations, UUID identity, renewal independent of blocked checkout, reconnect and final release | Reference-counted `ProcessTracker` with a dedicated session; `runtime_utility_spec.rb`, `process_tracking_spec.rb`, `process_tracking_process_spec.rb`, `listener_lifecycle_spec.rb`, and `ownership_interruption_spec.rb` |
| Tracker and runtime fork reset | Prepared pool closure, fresh child synchronization and process identity, invalidated execution context; `supervisor_fork_spec.rb`, `process_fork_spec.rb`, `execution_state_spec.rb`, and `fork_interruption_spec.rb` |
| Async adapter thread priority | Configurable priority on owned execution threads; `thread_controls_spec.rb` and `observability_process_spec.rb` |

The reference's Active Record connection leasing and internal counters are replaced by explicit pg session ownership and scoped registration. The observable guarantees remain: no borrowed connection can be mistaken for another owner's session, a live job retains its registration, and the last drained registration releases its monitor.
