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

1. Job controls: labels, concurrency limits and throttles, bulk capture, pauses.
2. Workflows: batches and transactionally deduplicated callbacks, recurring schedules and catch-up.
3. Operations: retention, process heartbeats, supervisor, systemd, Rails integration.
4. Dashboard: authenticated job actions, histories, batches, schedules, processes and metrics.
5. Compatibility: additional lock strategies, PgBouncer, full scheduling grammar, benchmark and parity audit.

Each slice requires behavior specs before implementation, real PostgreSQL verification, and independent review. Completion of one slice does not imply completion of the full rebuild.
