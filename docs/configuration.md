# Configuration sources

Configure DecentJob before opening its database or enqueueing jobs:

```ruby
DecentJob.configure do |config|
  config.database_url = ENV.fetch("DATABASE_URL")
  config.execution_mode = :external
  config.max_threads = 5
  config.enable_listen_notify = false
end
```

Settings resolve in this order:

1. Explicit Ruby assignments, including CLI options applied after the required application loads.
2. Current `config.decent_job` options in a Rails host.
3. Environment values captured when the configuration was constructed, then library defaults.

Explicit assignments survive initial Rails boot and subsequent changes to host options. Removing a host option restores its captured environment/default value unless it has an explicit assignment. `false` and `0` remain explicit values. Rails option names may be symbols or strings; unknown names raise `ArgumentError`.

Cron definitions replace the lower source's whole hash; entries are not merged. Blank Rails cron values, including `{}`, fall through to the captured environment/default definitions. An explicit Ruby `cron = {}` suppresses all lower sources. Reapplying unchanged Rails options preserves edits made through mutable configuration getters. Changing or removing a host value replaces those edits unless the setting was explicitly assigned. Prefer assigning the complete value when an application override must survive later host changes.

`nil` retains each setter's meaning. It clears explicit `execution_mode`, `logger`, `lock_strategy`, `dequeue_query_sort`, and `queue_select_limit` overrides. It is an explicit value for settings that permit it, such as `idle_timeout` or `database_url`. It does not clear every setting: `poll_interval = nil` is invalid, and `max_threads = nil` fails validation.

Configuration copies own their nested hashes, arrays, and strings. A rejected `DecentJob.configure`, Rails, or CLI candidate cannot change those containers on the active configuration. Application objects such as job classes, callables, and loggers retain their identity; changing those objects' own internal state is outside configuration rollback.

## Environment values

Environment variables are read when a configuration is constructed. Changing `ENV` later does not reconfigure an existing instance. `DecentJob.reset!` constructs a fresh configuration after draining the runtime.

`DECENT_JOB_DATABASE_URL` falls back to `DATABASE_URL`. `DECENT_JOB_MAX_THREADS` falls back to `RAILS_MAX_THREADS`, then `5`. Explicit Ruby or Rails thread settings take precedence over either environment variable.

The following environment variables use the same boolean conversion:

- `DECENT_JOB_ENABLE_CRON`
- `DECENT_JOB_ENABLE_PAUSES`
- `DECENT_JOB_ENABLE_LISTEN_NOTIFY`
- `DECENT_JOB_LOWER_THREAD_PRIORITY`
- `DECENT_JOB_ADVISORY_LOCK_HEARTBEAT`
- `DECENT_JOB_DAEMONIZE`
- `DECENT_JOB_CLEANUP_DISCARDED_JOBS`

An absent variable uses its default. Empty strings, `0`, `f`, `F`, `false`, `FALSE`, `off`, and `OFF` disable the option. Other nonempty strings enable it. This conversion applies to environment strings; Ruby setters retain their existing validation. Empty strings become `false`, including where GoodJob's boolean cast returns a false-like `nil`.

Blank `DECENT_JOB_CRON` values supply no definitions. Nonblank values must be valid JSON containing a definition hash. `enable_cron` and `enable_cron?` report false without definitions, even when the enable flag is true. Adding or removing definitions updates that result; disabling the flag keeps cron off. Invalid flags or definition containers still fail validation.

Rails development defaults `advisory_lock_heartbeat` to true; standalone and other Rails environments default false. Explicit Ruby, Rails, and captured environment settings take precedence, including false. See [process tracking](process-tracking.md) and the required overrides for [PgBouncer transaction pooling](pgbouncer.md).

Numeric settings reject malformed values instead of silently truncating them. Settings expressed in seconds retain supported fractional values. See [scheduling](scheduling.md), [shutdown](shutdown.md), [worker controls](worker-observability.md), [process tracking](process-tracking.md), and [Rails integration](rails-integration.md) for individual defaults and runtime behavior.

## Validation and diagnostics

Use `valid?` to inspect a configuration without raising for ordinary validation failures:

```ruby
config = DecentJob.configuration
unless config.valid?
  warn config.errors.full_messages.join("\n")
  warn config.errors[:cron].join("\n")
end
```

`errors` contains symbol keys and arrays of messages, supports `empty?`, `to_h`, and `full_messages`, and starts empty. Reading an absent field returns an empty array. Each validation replaces the previous diagnostics. Correct the settings and call `valid?` again to clear resolved errors. Copies have independent diagnostics and validate their own settings.

Validation collects independent setting errors and all invalid cron entries. Cron diagnostics identify the entry and, for an unresolved job class, its configured name. It does not open a database, enqueue jobs, or run application cron, preservation, or error callbacks. Resolving a named cron job class can invoke the application's autoloader. Fatal exceptions propagate normally.

`validate!` runs the same checks, raises `ArgumentError` with the diagnostics on failure, and returns the configuration on success. `DecentJob.configure`, Rails initialization, and CLI configuration use raising validation before accepting a candidate. Some setters reject invalid values immediately; `valid?` does not defer those setter checks. `cron_entries` still raises if definitions cannot produce valid entries.

The diagnostic collection is a small library API. It does not require Active Model or implement its validation framework.

## Error reporting

The default `on_thread_error` callback logs the exception class and message through the configured logger. Set a callable to send operational errors to an application reporter, or assign `nil` to disable this callback:

```ruby
DecentJob.configure do |config|
  config.on_thread_error = nil
end
```

Nil is an explicit setting and retains precedence over Rails options. Disabling the callback does not change job retry/discard outcomes, retention, batch recovery, or shutdown. Normal job logging and instrumentation remain available. Failures raised by a configured reporter remain contained by the operation's existing reporting boundary.

See the [reference configuration map](configuration-reference.md) for GoodJob-to-DecentJob API names, defaults, environment variables, CLI options, intentional differences, and remaining control gaps.
