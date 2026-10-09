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
- [ ] Add natural-language schedule translation and callable schedule definitions with validation and display metadata.
- [ ] Persist cron key/time and enable/disable overrides; deduplicate scheduled enqueue across processes in the same transaction as job insertion.
- [ ] Add independent cron start/stop, per-entry error isolation, clock-drift handling, and graceful restart catch-up.
- [ ] Integrate configuration, worker/application lifecycle, CLI controls, documentation, and independent reviews.

Numeric parser verification: 469 specs, 1,361 assertions across the full suite; Standard, gem build, and unpacked parser boot passed. Independent spec-compliance and code-quality reviews approved the numeric module. Saved fixtures cover 88 schedules/start times, three ranges, and 23 invalid expressions. Seeded differential checks of 1,000 and 3,000 field combinations matched the pinned reference; additional timezone probes verified the documented corrections. This completes the parser foundation, not recurring-job execution. See cron-compatibility.md for the contract and remaining scope.

### Remaining slices

1. Recurring schedules: cron parsing, timezone/DST behavior, deduplicated enqueue, persistent enable/disable controls, and restart catch-up.
2. Operations: retention, process heartbeats, supervisor, systemd, Rails integration.
3. Dashboard: authenticated job actions, histories, batches, schedules, processes and metrics.
4. Compatibility: additional lock strategies, PgBouncer, full scheduling grammar, benchmark and parity audit.

Each slice requires behavior specs before implementation, real PostgreSQL verification, and independent review. Completion of one slice does not imply completion of the full rebuild.
