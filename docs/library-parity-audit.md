# Final library parity audit

Audit date: October 9, 2026. Reference: GoodJob `ad4be5c2e10316354cb3d3d65a480b5450c7fd77` (`v4.21.1-4-gad4be5c2`). DecentJob audit baseline: `ac7eecb`, followed by the fixes and specs in this checkpoint.

All 80 library requirements in [the parity checklist](parity.md) were reviewed against the pinned source, relevant reference assertions, and DecentJob implementation/specs. The checklist contains the individual upstream source locations. Applicable examples were translated into Minitest; reference-specific Active Record, Rails engine, RSpec, and internal executor details were mapped to DecentJob's documented APIs. We did not execute GoodJob's complete suite or mechanically copy every example.

The review found adapter, concurrency, batch retry, and scheduling gaps. The changes below address them and add persistent regression coverage. Dashboard requirements D1–D15 remain deferred. Benchmarks, remote CI execution, and native platform checks described below remain open.

## Reference cases added or strengthened

Paths in the reference column are relative to the pinned GoodJob checkout. “Source-derived” means an edge case inferred from reference code, rather than an assertion copied from an existing example.

| Behavior | Reference evidence | DecentJob regression and result |
| --- | --- | --- |
| Nil `enqueue_at` timestamp | `spec/lib/good_job/adapter_spec.rb:130` | `spec/adapter_parity_spec.rb`: nil schedules immediately instead of raising from `Time.at(nil)`. |
| Bulk partial insert and success flags | `spec/lib/good_job/adapter_spec.rb:193,211`; `Adapter#enqueue_all` | `spec/adapter_parity_spec.rb`: an existing identity no longer aborts unrelated fresh inserts; rejected flags and accepted stale errors are reset. Single enqueue still rejects duplicates. |
| Duplicate objects inside one bulk input | Source-derived from `lib/good_job/adapter.rb:86–92,160` | The same spec checks the documented inserted-row count adaptation below, rather than claiming exact reference results. |
| Locale and timezone through persistence/execution | `spec/integration/adapter_spec.rb:31`; Active Job serialization delegated by both adapters | `spec/adapter_parity_spec.rb`: single and bulk jobs retain French locale and Honolulu timezone, then restore the caller's locale/zone. This closed an evidence gap without changing production behavior. |
| Batch retry with unavailable arguments | `spec/app/models/good_job/job_spec.rb:222`; source-derived composition through `Batch#retry` delegating to `Job#retry_job` | `spec/batch_retry_deserialization_spec.rb`: missing serializer and GlobalID cases, inline/external execution, original identity/history, callback metadata, and aborted retry. Batch retry now reuses the individual retry preparation and avoids eager argument loading during inline routing. |
| Callable blank keys disable a rule | Source-derived from `Concurrency::Rule#evaluate`; legacy nil example at `spec/lib/good_job/active_job_extensions/concurrency_spec.rb:488` | `spec/concurrency_rule_spec.rb` and `spec/concurrency_parity_spec.rb`: nil, false, empty and whitespace callable results disable enqueue/perform checks. A statically omitted key still means an all-job rule. |
| Legacy key preparation, retention, and default scope | `spec/lib/good_job/active_job_extensions/concurrency_spec.rb:721,766`; source-derived callback and `Job.enqueue_args` behavior | The same concurrency specs verify reevaluation before initial enqueue, callback argument changes, retained serialized keys, default class keys, cross-class matching, and retry-context bypass when arguments cannot deserialize. |
| Bulk applied labels and nested direct execution | `spec/lib/good_job/active_job_extensions/concurrency_rule_spec.rb:166` and `concurrency_spec.rb:625` | `spec/concurrency_parity_spec.rb`: perform-only rules apply dynamic labels in bulk; nested `perform_now` bypasses controls even inside another job’s execution context. Existing implementation passed. |
| Callable enqueue limit returning nil | Source-derived from `Concurrency::Rule#check_enqueue` | `spec/concurrency_parity_spec.rb`: fallback to total limit retains enqueue-only exclusion of running jobs in advisory, SKIP LOCKED, and hybrid modes. The implementation now distinguishes a configured callable from its nil result. |
| Durable grant reuse and oldest valid waiter promotion | `spec/app/models/good_job/concurrency_claim_spec.rb:33,50,64,83`; source-derived terminal-waiter composition | `spec/concurrency_claim_parity_spec.rb`: real claim ownership refresh, stale release protection, preserved waiting age/queue position, timestamp updates and promotion notification. |
| JSON-quoted scheduled hints | `spec/lib/good_job/scheduler_spec.rb:161` | `spec/scheduler_parity_spec.rb`: Time, numeric and quoted ISO hints populate a running worker's future cache. Exactly one JSON string layer is accepted; malformed/non-time/nested values report errors. |
| Ruby clock travel versus database time | `spec/lib/good_job_spec.rb:173` | `spec/global_controls_spec.rb`: explicitly records the approved difference in all three lock modes. Real elapsed-time eligibility remains tested separately. |
| Standalone packaged loading | DecentJob-specific X1/X3 requirement from `good_job.gemspec`, loading code, and the post-release demo change | `spec/packaging_spec.rb`: builds/extracts the gem, checks SQL/executable/dependencies, loads that artifact without Rails/Active Record or git in PATH, and runs CLI help. This verifies package boot/help; database execution is covered by the runtime/process suites. |

The production fixes were exercised with failing regressions before their corresponding changes. Review also found and covered the interaction between early legacy-key preparation and retries with unavailable arguments. The full suite also caught ordinary bulk conflict handling leaking into cron capture; the existing cron uniqueness regression guards its narrower contract. No runtime dependency or migration was added.

## Library coverage map

Each row below covers all checklist IDs in its first column. These are representative executable evidence paths; the detailed requirements and pinned upstream locations remain in [parity.md](parity.md).

| Requirements | Evidence in DecentJob | Scope or adaptation |
| --- | --- | --- |
| A1–A3 | `spec/configuration_spec.rb`, `spec/adapter_spec.rb`, `spec/adapter_parity_spec.rb`, `spec/packaging_spec.rb` | Named/explicit adapters, metadata, serialization, schedule and flags. |
| A4–A9 | `spec/execution_modes_spec.rb`, `spec/inline_spec.rb`, `spec/global_controls_spec.rb`, `spec/adapter_transaction_spec.rb`, `spec/job_stopping_spec.rb`, `spec/host_lifecycle_spec.rb`, `integration/rails_spec.rb` | Five modes, iterative retries, transactions, stopping, executor/reloader; PostgreSQL clock policy below. |
| P1–P3 | `spec/schema_spec.rb`, `spec/execution_spec.rb`, `spec/dequeue_policy_spec.rb`, `spec/dequeue_retry_spec.rb`, `spec/dequeue_schema_spec.rb`, `spec/dequeue_process_spec.rb` | Equivalent persisted data in an independent schema; stable UUID ties, bounded candidate selection and indexes. |
| P4–P10 | `spec/advisory_claim_spec.rb`, `spec/advisory_claim_process_spec.rb`, `spec/lock_strategy_spec.rb`, `spec/lock_strategy_process_spec.rb`, `spec/database_interruption_spec.rb`, `spec/process_tracking_process_spec.rb`, `spec/schema_readiness_spec.rb`, `spec/dequeue_schema_spec.rb`, `integration/pgbouncer/pgbouncer_spec.rb` | Session/row/hybrid ownership, recovery, process leases, upgrades and transaction pooling use real connections/processes. |
| J1–J4 | `spec/performer_spec.rb`, `spec/execution_recovery_spec.rb`, `spec/execution_metrics_spec.rb`, `spec/interrupt_errors_spec.rb`, `spec/interruption_process_spec.rb` | Retry/discard outcomes, history, timings, configured exceptions and interrupted attempts. |
| J5–J9 | `spec/preservation_spec.rb`, `spec/job_management_spec.rb`, `spec/job_management_process_spec.rb`, `spec/cleaner_spec.rb`, `spec/cleanup_tracker_spec.rb`, `spec/cleanup_process_spec.rb`, `spec/worker_cleanup_spec.rb` | Library management, missing arguments/classes, retention and cleanup; dashboard actions remain deferred. |
| W1–W3 | `spec/concurrent_lifecycle_spec.rb`, `spec/lifecycle_admission_spec.rb`, `spec/scheduler_cache_spec.rb`, `spec/scheduler_parity_spec.rb`, `spec/scheduler_process_spec.rb` | Owned runtime lifecycle, bounded deadlines, polling/discovery, explicit restart. |
| W4–W8 | `spec/notification_publication_spec.rb`, `spec/notification_process_spec.rb`, `spec/listener_lifecycle_spec.rb`, `spec/forced_shutdown_spec.rb`, `spec/worker_observability_spec.rb`, `spec/runtime_utility_spec.rb`, `spec/runtime_utilities_process_spec.rb`, `spec/execution_state_spec.rb` | Routing, reconnect, shutdown ownership, statistics and thread/fiber context; owned threads replace the shared executor. |
| L1–L5 | `spec/labels_spec.rb`, `spec/concurrency_rule_spec.rb`, `spec/concurrency_parity_spec.rb`, `spec/bulk_spec.rb` | Normalized/dynamic labels, apply_label, rule resolution and callback order. |
| L6–L9 | `spec/concurrency_spec.rb`, `spec/concurrency_parity_spec.rb`, `spec/perform_concurrency_spec.rb`, `spec/concurrency_rule_spec.rb` | Admission, throttling, cross-class scopes, legacy keys and label precedence. |
| L10–L13 | `spec/concurrency_claim_parity_spec.rb`, `spec/perform_concurrency_spec.rb`, `spec/dequeue_retry_spec.rb`, `spec/recovery_spec.rb`, `spec/process_tracking_process_spec.rb` | Durable grants, pending promotion, waiting order, multi-rule release and orphan recovery. |
| B1–B3 | `spec/adapter_parity_spec.rb`, `spec/bulk_spec.rb`, `spec/batch_spec.rb` | Partial acceptance, capture, real SQL grouping and controlled admission. |
| B4–B8 | `spec/batch_properties_spec.rb`, `spec/batch_workflow_spec.rb`, `spec/batch_retry_spec.rb`, `spec/batch_retry_deserialization_spec.rb`, `spec/batch_isolation_spec.rb`, `spec/batch_generation_spec.rb`, `spec/batch_recovery_spec.rb` | Properties, callbacks, recursive membership, retry, generations and crash recovery. |
| C1–C5 | Numeric/natural cron fixture specs; `spec/cron_entry_spec.rb`, `spec/cron_timezone_spec.rb`, `spec/cron_safety_spec.rb`, `spec/cron_manager_spec.rb`, `spec/cron_lifecycle_spec.rb`, `spec/cron_process_spec.rb` | Documented grammar, timezones/DST, unique occurrences, controls and catch-up. This is not a claim of arbitrary Fugit grammar equivalence. |
| C6–C7 | `spec/pauses_spec.rb`, `spec/global_controls_spec.rb` | Persisted queue/class/label pauses and consistent global mutation APIs. |
| S1–S6 | `spec/cli_spec.rb`, `spec/daemon_cli_spec.rb`, `spec/daemon_idle_process_spec.rb`, `spec/supervisor_configuration_spec.rb`, `spec/supervisor_fork_spec.rb`, `spec/supervisor_safety_spec.rb`, `spec/supervisor_process_spec.rb` | CLI, native local fork/daemon, backoff, hooks, signals, child readiness; platform limits below. |
| S7–S8 | `spec/health_services_spec.rb`, `spec/health_services_process_spec.rb`, `spec/health_status_spec.rb`, `spec/probe_webrick_spec.rb` | HTTP health, real local UNIX datagrams, service/watchdog protocol; native Linux remains unverified. |
| I1–I2 | Configuration/environment/CLI suites, `spec/global_controls_spec.rb`, `integration/rails_spec.rb`; [configuration mapping](configuration-reference.md) | Reference option precedence, names, validation, ownership and host adaptations are mapped explicitly. |
| I3–I4 | `spec/instrumentation_spec.rb`, job/worker/listener/runtime event suites, `spec/log_subscriber_spec.rb`, `spec/execution_aggregate_spec.rb`, Rails/PgBouncer integration | The 26 reference log-event handlers were compared with the documented event inventory; aggregate library statistics are implemented, dashboard presentation deferred. |
| X1–X4 | `spec/packaging_spec.rb`, all above suites, [configuration mapping](configuration-reference.md), [Rails integration](rails-integration.md) | Two direct dependencies, independent schema/API, Minitest conformance and post-release commit accounting. |

## Explicit compatibility differences

- **PostgreSQL decides whether persisted jobs are due.** The user chose to retain this policy. Ruby `travel_to`, Timecop, or stubbing `Time.now` does not advance PostgreSQL's clock. GoodJob's Ruby-time example therefore has a deliberately different result. See [scheduling](scheduling.md).
- **Ordinary bulk results count inserted rows.** GoodJob maps returned identities to all matching input objects and can count duplicate objects more than once. DecentJob accepts/counts one winner per inserted row; one repeated object keeps its successful result. Existing-database duplicates are skipped while unrelated fresh jobs proceed. Cron capture retains its narrower occurrence-conflict handling and raises unrelated uniqueness errors.
- **Queue recipes reject empty groups**, including trailing pipe or semicolon separators. Some reference recipes tolerate them. See [configuration](configuration-reference.md).
- **Batch callback ordering is unspecified.** Both success and finish callbacks are delivered as appropriate, but applications must not rely on the reference's inline success-before-finish order. Capture callbacks run once with provisional acceptance; batch generations, database-object identity and reopening semantics remain documented in the README.
- **Storage and runtime ownership are independent.** DecentJob uses its own tables, separate provider IDs, JSON error records, second-based duration fields, dedicated pause/cron settings, owned threads and explicit connection pools. GoodJob tables, Active Record relations, generic executors and migrations are not compatibility APIs. See [runtime resources](runtime-resources.md), [job management](job-management.md), and [locking](locking.md).
- **Lifecycle and host APIs have documented differences.** Direct explicit starts can reopen owners; listener failure changes readiness immediately; shutdown uses an explicit force option and shared deadline. Idle time resets after completion. Rails integration is optional and Active Job 8.1's job-class after-commit setting applies. See [lifecycle](lifecycle.md), [shutdown](shutdown.md), and [Rails integration](rails-integration.md).
- **Cron history survives execution preservation policies**, allowing startup catch-up even when ordinary successful jobs are deleted. Explicit destruction and age cleanup can still remove it. Parser grammar and deliberate corrections to reference parser defects are documented in [cron compatibility](cron-compatibility.md). Optional differential scripts were not rerun in this audit; saved fixtures were exercised by the core suite.
- **Label plus legacy key uses the label**, without evaluating the ignored key. GoodJob deprecates that combination; DecentJob documents label-only configuration as preferred without emitting its warning.

The two non-feature commits after v4.21.1 are accounted for: `53c87fdb` concerns JSON 3 dependency compatibility (the current bundle uses JSON 3.0.2 with Active Job 8.1.4); `ad4be5c2` adds a demo-only missing-git fallback. DecentJob has no corresponding demo bootstrap, and its extracted-package spec boots with git unavailable. Neither is presented as a new worker feature.

## Verification and remaining limits

The final core run includes 41 more specs than the audit baseline. The first full run exposed the cron conflict regression; after restoring the narrow cron contract, the complete suite passed with the additional claim cases included.

| Check | Result |
| --- | --- |
| `bundle exec rake` | 1,819 specs / 8,560 assertions; 0 failures/errors; seed 3089. Four optional WEBrick cases skipped under Bundler and passed separately below. |
| `BUNDLE_GEMFILE=gemfiles/rails.gemfile bundle exec rake spec:rails` | 34 specs / 231 assertions; 0 failures/errors/skips; seed 22720. |
| `bundle exec rake spec:pgbouncer` | 14 specs / 122 assertions; 0 failures/errors/skips; seed 23682, PgBouncer 1.26.0. |
| `ruby -Ilib -Ispec spec/probe_webrick_spec.rb` | 4 specs / 17 assertions; 0 failures/errors/skips; seed 41474. |
| `bundle exec standardrb` and `git diff --check` | Passed. |
| `gem build decent_job.gemspec --output /tmp/decent-job-final-parity.gem` | Passed; extracted-gem boot, manifest and CLI checks also passed within the core suite. |

Existing pending-promotion tests exercise DecentJob’s shared scope-lock protocol; they do not reproduce the reference’s exact blocked claim-row transaction interleaving. Equivalent observable race outcomes are the target.

Independent reviews covered adapter/batch behavior, concurrency interactions, scheduler parsing, clock-policy tests and extracted-package coverage. Reviews identified the retry-key interaction and duplicate-input wording, both addressed in this checkpoint.

The validation host is macOS with Ruby 3.4.10 and PostgreSQL 17.9. The checked-in CI matrix is not evidence that remote Linux CI ran. Native Linux abstract sockets and an actual systemd service manager, as well as native no-fork platforms, remain unverified. Local socket/process and simulated fallback tests cover the implemented protocols. Performance benchmarks remain open. Passing these cases does not establish exactly-once external side effects across process crashes.
