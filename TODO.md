# TODO

Findings from the code quality review of commit `ed4d3a1` ("Document runtime
behavior and codebase conventions"). Each item is independent unless noted.
Check an item when its change is committed and `bundle exec rake spec` and
`bundle exec standardrb` pass.

## 1. Split the commit

AGENTS.md says "Do not change code when you add documentation." Commit
`ed4d3a1` adds about 6,600 comment lines and about 350 lines of behavior
change under a message that says "Document".

- [x] ~~If `main` is not pushed, `git reset --soft HEAD~1` and recommit as one
      documentation commit plus one commit per behavior change below.~~
      Not applicable: `origin/main` contains `ed4d3a1`.
- [x] If `main` is pushed, amend nothing. Add a note to the commit list in
      this file so the behavior changes are findable. The table below is
      that note.

Behavior changes in the commit:

| Change | Where |
|---|---|
| `Worker#start` and `CronManager#start` return `false` while a stopped run still drains | `lib/decent_job/worker.rb`, `lib/decent_job/cron_manager.rb` |
| Supervisor tells a drain timeout from a child crash by exit status 3 | `lib/decent_job/supervisor.rb` `DRAIN_TIMEOUT_STATUS` |
| `ProcessRecord#stale?` and `#expired?` use the database clock | `lib/decent_job/process_record.rb` |
| Batch callback retargeting on `save`; error collection in `reconcile` | `lib/decent_job/batch_store.rb` |
| `Batch#retry` wakes async workers through `adapter.after_enqueue` | `lib/decent_job/batch.rb` |
| Bulk enqueue uses `ConcurrencyControls.controlled?` instead of `respond_to?` | `lib/decent_job/adapter.rb`, `lib/decent_job/bulk.rb`, `lib/decent_job/base.rb` |
| Dashboard answers HEAD, returns 413 for an oversized body, drops `params` and `ajax?` | `lib/decent_job/dashboard.rb`, `lib/decent_job/dashboard/request.rb` |
| `Claim#next_scheduled_at` drops a duplicate `pg_locks` predicate | `lib/decent_job/claim.rb` |
| Validator checks `enable_listen_notify` and `enable_pauses`; `Errors#[]` returns a copy; CLI restores traps; cleanup log format | several |

## 2. Fix wrong comments

Each line states something the code does not do.

- [x] `lib/decent_job/cron_manager.rb:95-98`. `start` never returns nil. A
      skipped automatic start falls through to `self` at the end of the
      method. Remove the nil case from `@return`.
- [x] `lib/decent_job/cron_entry.rb:104`. `validate_jobs` raises
      `NoMethodError`, not `KeyError`, when there is no cron context
      (`current_context` is nil). Fix the tag or add a guard that raises a
      named error.
- [x] `lib/decent_job/base/exceptions.rb:10-11` and `lib/decent_job/base.rb:62`.
      `rescue_from` does not freeze the new array (`exceptions.rb:60`). Remove
      "frozen" or freeze the array.
- [x] `lib/decent_job/batch_store.rb:364`. `reconcile_pending` can raise. The
      candidate SELECT at 366-373 runs outside the per-id rescue.
- [x] `lib/decent_job/dashboard/queries.rb:8-9`. Only `jobs` and `batches`
      use keyset pagination. `processes` has no limit. Rewrite the header.
- [x] `lib/decent_job/process_tracker.rb:79-80`. Add the runtime-transition
      case to the nil meaning of `register`.
- [x] `lib/decent_job/cli.rb:81-82`. The rescue list includes `LoadError`,
      which is not a `StandardError`. Fix the exception list.
- [x] `lib/decent_job/inline_buffer.rb:6-7`. `{Batch}` does not call
      `perform`. It calls `Adapter#after_enqueue`.
- [x] `lib/decent_job/concurrency_controls.rb:9-10`. `{Batch}` calls
      `BatchStore#lock_scopes`, not `ConcurrencyControls#lock_scopes`.
- [x] `lib/decent_job/worker.rb:17-18`. The CLI does not create a worker.
      `Worker.new` is called from `DecentJob.start` and `Adapter#start_async`.
- [x] `lib/decent_job/notifications.rb:112,125`. `{#unsubscribe}` and
      `{#subscribe}` must be `{.unsubscribe}` and `{.subscribe}`.

Resolution notes:

- `rescue_from` and `after_discard` now freeze the new array, so the
  "frozen" comments are true.
- `validate_jobs` now has a `@note` that callers must check
  `current_context` first. Every caller already does.

## 3. Add missing `@raise` tags

- [x] `lib/decent_job.rb` `shutdown`, `stop_cron`, `restart`.
      `ResourceShutdown.wait_all` re-raises the first thread error. `reset!`
      documents this; these three do not.
- [x] `lib/decent_job/adapter.rb` `enqueue`, `enqueue_at`, `enqueue_all`,
      `reschedule`. `ConcurrencyControls::AdmissionRejected` escapes to a
      direct adapter caller.
- [x] `lib/decent_job/worker.rb:330` `restart`. `ArgumentError` for a bad
      timeout. `shutdown` lists it, `restart` does not.
- [x] `lib/decent_job/store.rb:68` `enqueue_all`. `PG::UniqueViolation` when
      `skip_duplicates: false`.
- [x] `lib/decent_job/batch_store.rb:320` `reconcile`. `KeyError` from
      `Batch.find` inside `dispatch` when the batch row vanishes.
- [x] `lib/decent_job/base/exceptions.rb:69` `retry_on`. `ArgumentError` for
      any Symbol `wait` other than `:polynomially_longer`.

Resolution notes:

- `AdmissionRejected` escapes only `enqueue` and `enqueue_at`.
  `enqueue_all` and `reschedule` now list their own `@raise` tags instead
  of `(see #enqueue)`, so they do not inherit it.
- `retry_on` checks `wait` only when the handler runs, so the
  `ArgumentError` is a `@note`, not a `@raise` on `retry_on`.
- `DecentJob.restart` also got `ForkInProgress` and `LifecycleInProgress`.

## 4. Remove padding

Delete tags that restate a one-line body.

- [ ] `lib/decent_job/queue_selection.rb:80-88` (`exclude?`, `ordered?`)
- [ ] `lib/decent_job/configuration.rb:83-89` (`validate_execution_mode!`)
      and the one-line setter tags near 546-560
- [ ] `lib/decent_job/logging.rb:20,33` (`@return [String] the formatted line`)
- [ ] `lib/decent_job/log_subscriber.rb:197-205` (`cluster_shutdown`)
- [ ] `lib/decent_job/cleanup_tracker.rb:51-56` (`increment`)
- [ ] `lib/decent_job/probe_server/webrick_handler.rb:62-65` (`service`)
- [ ] `lib/decent_job.rb:86-87` (`# The gem version.`)

Not done. AGENTS.md says public methods get a summary line and the tags
that apply. Most of these tags also state a fact that the body does not show
(for example, `exclude?` is true for a "-" expression). Deleting them trades
one convention break for another. Revisit only if AGENTS.md changes.

## 5. Dashboard: one rescue for all HTTP errors

`lib/decent_job/dashboard.rb` has three error classes and two near-identical
rescue lines. The `BadRequest` rescue uses `is_a?(PayloadTooLarge)` to pick
413 or 400. A type check stands in for a missing attribute.

- [x] Add an `HttpError` base class with a `status` method. `NotFound` returns
      404, `BadRequest` 400, `PayloadTooLarge` 413.
- [x] Replace both rescue clauses with one `rescue HttpError => error` that
      uses `error.status` and `error.message`.
- [x] Keep the `NotFound` message text ("There is no page at ...").

Resolution note: the `NotFound` message needs the request path, which the
raise sites do not pass. `HttpError#page_message(request)` returns the
message, and `NotFound` overrides it. That keeps one rescue clause with no
type check.

## 6. Define `decent_job_concurrency_controlled?` once

Three definitions exist. `JobExtensions::Concurrency` returns `true`. `Base`
overrides it with a config and rules check. `ConcurrencyControls.controlled?`
adds a `respond_to?` guard. The guard is needed: plain Active Job classes lack
the method. The other two are not.

- [x] Move the config and rules check into `JobExtensions::Concurrency`. The
      extension already owns `decent_job_concurrency_config` and
      `decent_job_concurrency_rules`.
- [x] Delete the `Base` override.
- [x] Decide the Active Job consequence. An Active Job class that includes the
      extension but sets no limits will then go through the SQL bulk insert
      without enqueue callbacks on `perform_all_later`. That matches Active
      Job. If the old "including opts in" behavior must stay, keep the `true`
      and write a comment that says why a job with no rules needs the slow
      path. The current comment at `concurrency.rb:111-114` describes the
      override; it does not justify the `true`.
- [x] Add or update a spec for the chosen behavior.

Resolution note: chose the Active Job behavior. A class that includes the
extension but sets no limits or rules now goes through the SQL bulk insert.
The extension callbacks do nothing for such a class. New spec in
`spec/concurrency_spec.rb`. Two specs used a class with no rules to reach the
controlled path (`bulk_spec.rb` "prepares a duplicated controlled job object
only once", `cron_safety_spec.rb` "runs deferred controlled bulk callbacks").
They now set a `total_limit: 10` first.

## 7. `BatchStore#reconcile`: use an accumulator for errors

`dispatch` returns an errors array. The `locked` block returns it as
`failures`. `reconcile` wraps it in `Array()` to absorb the `next` nil, then
reports. The goal (never call `on_thread_error` under the batch advisory
lock) is right. The plumbing is not.

- [x] Pass `errors = []` into `dispatch(connection, id, errors)`.
- [x] Report after `locked` returns: `errors.each { |error| report(error) }`.
- [x] Remove `Array()`, `failures`, and `return [] if events.empty?`.
- [x] In `retarget_events`, replace the `sum` with a `0` branch and a
      `cmd_tuples` branch by a plain `deleted` flag set in the delete branch.

## 8. `ProcessRecord`: name the `database_now` dependency

`ProcessRecord#initialize` does `row.fetch("database_now")`. Both callers add
`, clock_timestamp() AS database_now` to their SELECT by hand. A future
`new(row)` from a plain `SELECT *` fails with a `KeyError` for a column that
does not exist in `decent_job_processes`.

- [x] Either pass the offset in explicitly, `new(row, clock_offset:)`,
      computed once per query instead of once per row.
- [x] Or put the aliased SELECT in one private query helper so one place
      knows the alias.

Resolution note: took the second option. The private constant
`ProcessRecord::SELECT_SQL` holds the aliased SELECT, and both queries use it.

## 9. Split `lib/decent_job/worker.rb`

The file is 1,008 lines, 766 of them before the documentation commit. It has
four jobs: lifecycle, the execution loop (`run`, `run_registered`), the
LISTEN thread (`listen`, `receive_notification`, `normalize_hint`, `wake`),
and process cleanup. The LISTEN side has one entry point (`wake`) and its own
connection.

- [ ] Extract the LISTEN thread into `Worker::Listener` before the next
      feature lands in this file.
- [ ] Keep `Worker#wake` as the public entry point and delegate.

Not done in this pass. The LISTEN state is shared with the rest of the
worker: `start_run` creates `@listener_reader` and `@listener_writer`, `run`
and `begin_shutdown` write to the pipe, and `connected?` reads
`@listener_connected` under the worker mutex. The extraction must define who
owns the pipe and the connected flag across start, restart, and shutdown. Do
it as its own change, with its own review, before the next feature lands in
`worker.rb`.

## Verified at review time

- `bundle exec standardrb`: clean.
- `bundle exec rake spec`: 1887 runs, 8935 assertions, 0 failures, 4 skips.
- About 870 added comments checked against code, callers, specs, and
  `db/migrations/`. About a dozen are wrong (section 2). Table and column
  names all match the migrations.

## Verified after the fixes

- `bundle exec standardrb`: clean.
- `bundle exec rake spec`: 1900 runs, 8954 assertions, 0 failures, 4 skips.
