# DecentJob implementation plan

Approved direction: independent PostgreSQL storage, DecentJob API, two direct runtime dependencies, and Minitest spec syntax exclusively. Reference: GoodJob checkout ad4be5c2, four commits after v4.21.1. No GoodJob table compatibility is promised.

Use test-first implementation and run specs against PostgreSQL. Implementation work proceeds in usable slices. The full parity checklist is in parity.md; the initial release must not claim full parity.

## First runnable slice

- [x] Package the gem with Active Job 8.1.4 and pg 1.7.0; use Minitest 5 spec syntax and Standard for development only.
- [x] Implement configuration and database connection ownership in lib/decent_job/configuration.rb and database.rb. Test transaction rollback, nested savepoints, disconnects, and thread ownership in spec/database_spec.rb.
- [x] Implement versioned SQL migrations in db/migrations and lib/decent_job/schema.rb. Installation must be repeatable, serialized, and transactional. Test with an isolated temporary database schema in spec/schema_spec.rb.
- [x] Implement parameterized job storage and Active Job adapter in lib/decent_job/store.rb and lib/active_job/queue_adapters/decent_job_adapter.rb. Test serialization, scheduled jobs, bulk enqueue, rollback, and provider IDs in spec/adapter_spec.rb.
- [x] Implement session advisory lock claims and execution records in lib/decent_job/performer.rb. Test two competing connections, failed and retried jobs, interruption recovery, and discarded jobs in spec/performer_spec.rb.
- [x] Implement worker pools, polling, notifications, shutdown, and CLI. Test external process boot, SIGTERM, and bounded shutdown separately from threaded specs.
- [x] Document installation, execution, database ownership, and remaining parity work. Run bundle exec rake, bundle exec standardrb, and gem build decent_job.gemspec.

First-slice verification: 88 specs, 284 assertions; Standard passed; gem build and unpacked executable smoke test passed. Tests ran on Ruby 3.4.10 / PostgreSQL 17.9. CI for Ruby 3.2/3.4 is configured but has not run remotely. Independent review covered transactions, pool ownership, adapter errors, worker lifecycle, and CLI packaging. This completes only the first runnable slice.

## Subsequent slices

### Job controls slice

- [x] Add normalized, serializable class/instance labels and dynamic defaults with the Active Job extension API.
- [x] Add a versioned controls migration for labels, concurrency keys, pause settings, and durable claims.
- [x] Add persistent queue/class/label pauses and dequeue filtering, with explicit enable/disable behavior.
- [x] Add enqueue/total limits and rolling enqueue throttles under transaction-scoped locks. Test concurrent enqueuers and bulk paths.
- [x] Add perform limits/throttles, durable grants/waiters, prompt promotion, and stale-grant recovery. Test simultaneous performers, later-rule rejection, retries and crash cleanup.
- [x] Add bulk block capture with rollback on block failure, nested/thread isolation, and checked jobs going through their callbacks.
- [x] Review, document and verify the complete slice against real PostgreSQL before committing.

Job-controls verification: 213 specs, 766 assertions; Standard and gem build passed. The built package includes both new migrations and the controls/bulk implementation. Real PostgreSQL specs cover competing enqueuers, simultaneous claims, promotion before retry persistence, multiple releases, changed rules between attempts, and SIGKILL recovery. Independent spec-compliance and code-quality reviews passed. This slice includes documented adaptations for captured callback timing, stricter invalid-option validation, and a consistent top-level label pause API.

### Batches slice

- [x] Persist batch metadata, Active Job properties, membership, and GlobalID references.
- [x] Add enqueue/add/reopen/retry and multi-batch enqueue APIs with one-database validation.
- [x] Deduplicate success/discard/finish callback deliveries transactionally and retain pending callbacks after admission rejection.
- [x] Record completion requests atomically with job finish; recover callback work on idle polls after process failure.
- [x] Preserve inline/nested contexts, recursive reopening, existing/running membership, callback retries, and phase-specific completion state.
- [x] Test competing final completions and opposing concurrency-scope orders; review lock ownership and rollback paths.
- [x] Document, independently review, and verify the complete slice.

Batch verification: 301 specs, 1,081 assertions; Standard and gem build passed. The package contains migration 004 and all batch implementation files. Independent spec-compliance and code-quality reviews passed. Real PostgreSQL tests cover callback deduplication and recovery after SIGKILL, concurrent discards during reconciliation and reopening, recursive callback-created controlled jobs, database isolation, retry callbacks, and inline/async multi-batch execution.

### Recurring schedules slice

- [x] Implement numeric cron expressions, extended day syntax, aliases, zones, and bounded calendar search without another runtime dependency.
- [x] Record pinned-reference conformance fixtures, including DST gaps/folds and deliberate corrections to invalid-input handling.
- [x] Add natural-language schedule translation with pinned-reference conformance fixtures.
- [x] Add callable schedule definitions with validation and display metadata.
- [x] Persist cron key/time and enable/disable overrides; deduplicate scheduled enqueue across processes in the same transaction as job insertion.
- [x] Add independent cron start/stop, per-entry error isolation, clock-drift handling, and graceful restart catch-up.
- [x] Integrate configuration, worker/application lifecycle, CLI controls, documentation, and independent reviews.

Numeric parser verification: 469 specs, 1,361 assertions across the full suite; Standard, gem build, and unpacked parser boot passed. Independent spec-compliance and code-quality reviews approved the numeric module. Saved fixtures cover 88 schedules/start times, three ranges, and 23 invalid expressions. Seeded differential checks of 1,000 and 3,000 field combinations matched the pinned reference; additional timezone probes verified the documented corrections. This completes the parser foundation, not recurring-job execution. See cron-compatibility.md for the contract and remaining scope.

Natural parser verification: 702 specs, 1,974 assertions across the full suite; Standard, gem build, and unpacked natural-parser boot passed. Independent spec-compliance and code-quality reviews approved the implementation after correcting capitalized-name composition. Saved fixtures cover 170 phrases, including 137 accepted schedules with five occurrences each. The reproducible differential script matched 9,795 expressions; independent review checked another 7,471 combinations without mismatches. Runtime dependencies remained Active Job and pg. That checkpoint left callable entries and recurring execution open.

Cron-entry verification: 765 specs, 2,186 assertions across the full suite; Standard and gem build passed. An unpacked gem installed migrations 1–5 and passed enqueue, deduplication, persistent override, and execution checks. Independent reviews approved definitions, callable schedules/options/classes, transaction-scoped metadata, cross-process uniqueness, history, and enable/disable overrides. Tests cover nested rollback cleanup, default-locale serialization, inline/async child isolation, unrelated database errors, ordered concurrency locks, and ActiveSupport/ENV timezone precedence with DST folds. At that checkpoint, C1–C3 were complete; automatic scheduling, independent start/stop, and restart catch-up remained open. See recurring-jobs.md for the current API.

Scheduler verification: 807 specs, 2,351 assertions across the full suite; Standard, gem build, and whitespace checks passed. An unpacked gem installed its schema, started automatic cron alongside a worker, executed a recurring job, retained its history, and shut down cleanly. Independent spec-compliance and code-quality reviews approved C4/C5 after regressions for clock rollback during scheduling and enable checks, and invalid timeout handling. Process specs cover two competing CLI schedulers, configuration overrides, restart catch-up excluding future records, and a blocked callback exhausting the shutdown deadline. The scheduler uses bounded threads and pending work per entry; docs explain resource sizing and lifecycle ownership. Full GoodJob parity remains open beyond this completed recurring-jobs slice.

### Job management slice

- [x] Add `DecentJob::Job.find(id, database:)`, reload, decoded attributes, execution history, Active Job deserialization, and status predicates.
- [x] Implement retry, reschedule, discard, force-discard, and destroy with explicit state errors and the worker's advisory lock for ordinary actions.
- [x] Preserve retry identity, locale, callbacks, execution history, cron/batch metadata, and transaction boundaries; tolerate unavailable arguments while rejecting unavailable job classes for retry.
- [x] Make force-discard authoritative against a running job's later return or retry, finish open attempts, release concurrency claims, and reconcile batch completion.
- [x] Expose adapter stopping reasons for shutdown, force-discard, and persistent pauses with both Active Job 8.1's no-argument call and the optional job argument.
- [x] Verify lock conflicts and force-discard with separate PostgreSQL connections/processes; independently review, document, and run the full suite.

This implements J6, A8, and the management API portion of J7. Dashboard retry locale behavior remains open with the dashboard. Retention and automatic cleanup remain the following operations work; dashboard actions will use the same public API.

Job-management verification: 857 specs, 2,602 assertions across the full PostgreSQL suite; Standard, gem build, and whitespace checks passed. An unpacked gem installed migrations 1–6 and passed retry identity, error-event history, execution, and cascading destruction checks. Independent spec and quality reviews approved the slice after regressions for callback error classification, provisional enqueue cleanup, manual-retry stopping context, and immutable record identity. Process specs cover ordinary lock conflicts, authoritative force-discard despite return or retry, and cooperative CLI shutdown. The full suite also verified that normal failed batch members remain retryable while their workers finish reconciliation; only explicitly force-discarded, still-owned attempts are excluded. All tests use Minitest spec syntax, and direct runtime dependencies remain Active Job and pg.

### Preservation and cleanup slices

- [x] Implement `configuration.preserve_job_records` with `true` (default), `false`, `:on_unhandled_error`, and a callable receiving the executed Active Job, exception, and error-event symbol. Predicate failures report through the thread error handler and retain the records.
- [x] Preserve unfinished/re-enqueued jobs and cron occurrence history regardless of the policy. Delete terminal jobs and their attempts atomically with durable batch completion requests and concurrency-claim release; preserve authoritative force-discard state while its original owner drains.
- [x] Verify successful, discarded, exhausted, unhandled, direct-reenqueue, callback, cron, inline, and batch execution paths, including interrupted reconciliation recovery and batch retry during preservation. Independently review this preservation checkpoint separately.
- [x] Add age-based, bounded cleanup with discarded-job controls, deletion counts/instrumentation, and job/batch lock rechecks. Keep unfinished batches and callbacks safe; explicit cleanup may remove cron deduplication history.
- [x] Add the cleanup tracker and worker integration with count/time thresholds, disabling semantics, error reporting, and shutdown ownership. Add a CLI cleanup command and configuration/environment overrides.
- [x] Document retention tradeoffs, independently review cleanup, run the full PostgreSQL suite, and verify the packaged gem.

Cleanup reference contract: default age 1,209,600 seconds (14 days), batch size 1,000, include discarded records by default. A batch is eligible only after `finished_at`, not merely `jobs_finished_at`. Return the number of deleted jobs, executions, and batches and instrument their individual counts. Cleanup may stop between batches during shutdown. Worker defaults are 1,000 completed attempts or 600 elapsed seconds; check the elapsed threshold on completed attempts, as the reference does. Threshold comparisons are strict (`>`); `-1` requests cleanup after each completion. Configuration `nil`, empty string, or `true` uses defaults; `0`, `"0"`, `false`, or `"false"` disables that threshold. The lower-level tracker rejects zero and defaults to disabled thresholds. Concurrent sweepers must skip locked jobs, recheck eligibility under locks, and avoid repeatedly selecting an undeletable first page. Reopening a batch must serialize with its deletion.

Preservation verification: 891 specs, 2,790 assertions across the full PostgreSQL suite; Standard, gem build, and whitespace checks passed. Independent spec-compliance and quality reviews approved J5. An unpacked gem passed predicate-instance, batch deletion/callback, and cron-history checks. Real process tests terminate workers after deleting successful or discarded members but before batch reconciliation, then recover exactly the required callbacks. Additional regressions cover predicate and error-reporter SQL failures, terminal deletion rollback, reentrant batch retry, claim promotion, force-discard retention, and executed-instance capture. No migrations or runtime dependencies were added. Age-based cleanup and its tracker remain open.

Cleanup verification: 936 specs, 3,037 assertions across the full PostgreSQL suite; Standard, gem build, and whitespace checks passed. Independent spec and quality reviews approved J8/J9. An unpacked gem passed bounded age cleanup, discarded retention, count instrumentation, batch deletion, and pending-job safety checks. Process specs exercise CLI precedence, failures, and automatic cleanup past a locked record followed by SIGTERM. Regressions cover inclusive cutoff equality, oldest-first progress, competing sweepers, retry/reopen races, same-session ownership, foreign-key and waiter-promotion contention, timeout restoration, historical batch snapshots, orphan updates, and partial-error counts. Migration 007 indexes cleanup cursors. Automatic cleanup uses existing worker threads and adds no runtime dependency.

### Execution recovery and error handling

- [x] Add validated `handled_exceptions` (default `StandardError` and `NotImplementedError`, accepting class names) and `retry_on_unhandled_error` (default false).
- [x] Persist handled backend exceptions as terminal failures or immediate retries according to configuration, preserving identity, scheduling metadata, attempt history, concurrency release, batch state, and retention behavior. Propagate exceptions outside the configured classes.
- [x] Report escaped Active Job exceptions through worker error handlers once, distinguishing them from Active Job-handled retries and discards. Preserve inline propagation; worker, cron, adapter, and global shutdown must drain their owned resources before propagating thread failures.
- [x] Expose `InterruptError` and `InterruptedError`, and an opt-in `ActiveJobExtensions::InterruptErrors` hook. Mark abandoned attempts once, carry the prior start time only into their replacement execution, and let Active Job retry/discard handlers decide its outcome.
- [x] Verify crash recovery with real SIGKILL processes, callback isolation, error classification, and no extra history records for interruption detection. Audit J1/J2 coverage alongside J3/J4; independently review, run the full suite, and verify the built gem.

Execution-recovery verification: 981 specs, 3,381 assertions across the full PostgreSQL suite; Standard, gem build, and whitespace checks passed. Independent spec and quality reviews approved J2–J4. Real SIGKILL tests verify replacement retry/discard handlers, ordinary recovery, and exactly one replacement history entry. An unpacked gem passed configured exception handling, backend-retry payload/schedule preservation, and interruption-handler/history checks. Regressions cover atomic batch/concurrency/retention behavior, authoritative force-discard, nested callback isolation, reporter failures, and excluded exceptions. Worker, cron, adapter, and global shutdown drain remaining resources before re-raising fatal thread errors; reset keeps the database open until callbacks finish. No migrations or dependencies were added. J1 remains open for durations, latency, and per-attempt payload/schedule snapshots.

### Execution history and metrics

- [x] Add migration 008 for nullable execution payload, scheduled time, job class, queue, and duration snapshots, plus the job's current `performed_at`. Preserve old attempt records with unknown snapshot/duration fields; backfill only current start times that existing execution records prove.
- [x] Snapshot before invoking Active Job, retain snapshots across retry mutations, and persist monotonic duration in seconds for completed attempts atomically with their outcome. Clear job `performed_at` when it reenqueues. Interrupted and operator-discarded attempts retain unknown monotonic durations rather than substitute recovery time.
- [x] Add a read-only `Execution` API for lookup/history and job/attempt queue, runtime, and total latency. Keep raw `Store#executions` and `Job#executions` data available. Future jobs and missing historical data must yield unknown metrics where appropriate.
- [x] Test successful, handled/unhandled retries, changed payload/schedule, callback time, clock jumps, interrupted/force-discarded attempts, migration upgrades, and retained batch/cron behavior. Independently review, run the full PostgreSQL suite, and exercise the unpacked gem.

Timing contract: attempt duration uses the monotonic clock around Active Job execution and its callbacks, excluding outcome persistence and batch reconciliation. Attempt queue latency uses its immutable scheduled/start timestamps; total latency is queue plus monotonic runtime when both are known. Job queue/runtime latency follows the current lifecycle's scheduled/performed/finished timestamps, with no runtime for a pending retry. `Execution#interrupted_duration` reports wall time through recovery and includes downtime. DecentJob intentionally does not copy the reference's near-zero recovery measurement into the abandoned attempt's runtime.

Execution-history verification: 1,010 specs, 3,690 assertions across the full PostgreSQL suite; Standard, gem build, and whitespace checks passed. Independent spec and quality reviews approved J1. An unpacked gem installed migration 008 and passed snapshot, retry numbering, duration, lookup, and interrupted-history checks. Real CLI tests verify live snapshots, SIGKILL replacement timing, and force-discard protection against the original owner returning. Upgrade tests from schema versions 1 and 7 preserve unknown data and reject stale starts from cancelled pending retries. Callback/executor tests verify monotonic timing across wall-clock jumps, while atomic rollback tests leave duration unfinished on persistence failure. No runtime dependencies were added. Aggregate metrics and process/scheduler activity remain open under I4.

### Process tracking and worker state

- [x] Add process records with UUID identities, heartbeat timestamps, advisory/heartbeat liveness, and process/worker state. Preserve the execution's OS PID and add a process UUID; persist current job ownership independently of attempt history.
- [x] Add an owned heartbeat tracker using a dedicated connection and reference-counted registrations. Track idle workers and active standalone/inline execution, survive pool exhaustion and reconnects, and retain registration while shutdown drains work.
- [x] Integrate process ownership with claims, outcome persistence, and crash recovery. Cleanup must recheck liveness and job locks, clear only abandoned ownership, release granted concurrency claims in scope order, and preserve history and live work.
- [x] Expose process inspection, worker capacity/activity and outcome counters, configuration/env controls, and database pool statistics. Keep PostgreSQL connections confined to their owner threads and reject inherited database handles after fork.
- [x] Verify real process registration, SIGKILL recovery, heartbeat renewal under a saturated pool, bounded shutdown, and connection loss. Review independently, run the full PostgreSQL suite, and verify the packaged gem.

Standalone defaults use timestamp heartbeats; optional advisory heartbeat ownership provides immediate process-liveness detection. A dedicated monitor connection keeps process registration independent of LISTEN/NOTIFY configuration and pool checkout availability. A process record may remain stale while a job still holds its own lock; cleanup must not release that live job's ownership or claims.

Process-tracking verification: 1,044 specs, 3,909 assertions across the full PostgreSQL suite; Standard, gem build, and whitespace checks passed. Independent spec and quality reviews approved P9. An unpacked gem installed migration 009 and passed worker and standalone registration, process UUID history, heartbeats with a saturated pool, bounded shutdown, worker counters, and inactive cleanup. Real CLI specs cover idle registration without LISTEN, SIGTERM drain, SIGKILL recovery and waiter promotion, and heartbeat connection loss without releasing live ownership. Regression specs cover nested leases, row recreation, lock contention and timeout restoration, paged cleanup and cancellation, one active sweep, dead-thread capacity and restart, and inherited-database rejection after fork. Listener tests identify actual LISTEN sessions independently of heartbeat connection opening order. No runtime dependencies were added. The broader W7 statistics audit and other lock strategies remain open.

### Health probes and service notifications

- [x] Expose nonblocking worker/global `started?` and `connected?` state. Started requires every configured execution thread; connected additionally requires the heartbeat connection and an enabled listener. Disabled LISTEN uses heartbeat connectivity. Shutdown makes readiness false immediately; reconnect restores it.
- [x] Add the four reference probe routes, fallback routing, custom applications, and a reusable cluster-state adapter. Implement the default HTTP server with stdlib sockets, bounded request reads/writes, client limits, prompt stop, and fork socket closure. Support an optional host-provided WEBrick handler without adding it as a dependency.
- [x] Add `probe_port`, `probe_handler`, and `probe_app` configuration and CLI port/handler flags. Start the probe with the CLI, keep it available during drain, and stop it on normal exit or failure. Report bind failures rather than silently claiming a healthy probe.
- [x] Implement systemd datagram notifications and a monotonic watchdog at half the configured interval, honoring `WATCHDOG_PID`. Notify stopping once before drain and keep watchdog coverage through drain, including exceptional shutdown. Socket/reporting failures must not strand workers or service threads.
- [x] Verify HTTP behavior and UNIX datagrams, real CLI readiness and shutdown ordering, listener/heartbeat disconnects, configuration precedence, optional handler operation, and packaged operation. Complete independent spec and quality reviews and run the full suite.

This is the health foundation for the supervisor: single-process CLI integration ships first, with an injectable cluster-state application for S6/S7 integration. Supervisor recipes, child readiness channels, restart backoff, and fork lifecycle hooks remain the following slice. S7/S8 cluster coverage stays open until that integration is verified. Direct runtime dependencies remain Active Job and pg; the built-in probe requires neither Rack nor a web server gem.

Health-services verification: the bundled full PostgreSQL suite ran 1,087 specs and 4,209 assertions with no failures or errors. Four optional WEBrick specs were skipped because Bundler does not include that host dependency; all four passed separately against installed WEBrick, with 17 assertions. Standard, gem build, and whitespace checks passed. Independent spec and quality reviews approved the slice. The unpacked gem passed real worker readiness, shutdown status, default HTTP handling, optional WEBrick GET/PATCH routing, and UNIX READY/STOPPING/WATCHDOG checks through drain. Six real CLI specs verify job and probe callback drain coverage, immediate readiness loss, disabled LISTEN, PID filtering, custom app/config precedence, bind failure cleanup, and partial-request shutdown. Regressions cover bounded reads/writes, full datagram queues, fork socket copies, callback-initiated stop, queued-client shutdown, and concurrent/reentrant watchdog restart. Native Linux systemd daemon execution was not available on this macOS host; abstract-address encoding and real filesystem UNIX datagrams were verified. Runtime dependency declarations remain Active Job and pg. S7/S8 cluster integration remains open for the supervisor slice.

### Remaining slices

1. Operations: supervisor and child readiness, cluster probe/systemd integration, daemon/PID/idle controls, Rails integration, and remaining worker lifecycle refinements.
2. Dashboard: authenticated job actions, histories, batches, schedules, processes and metrics.
3. Compatibility: additional lock strategies, PgBouncer, benchmark and final parity audit.

Each slice requires behavior specs before implementation, real PostgreSQL verification, and independent review. Completion of one slice does not imply completion of the full rebuild.
