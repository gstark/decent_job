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

Cron definitions replace the lower source's whole hash; entries are not merged. Reapplying unchanged Rails options preserves edits made through mutable configuration getters. Changing or removing a host value replaces those edits unless the setting was explicitly assigned. Prefer assigning the complete value when an application override must survive later host changes.

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

Numeric settings reject malformed values instead of silently truncating them. Settings expressed in seconds retain supported fractional values. See [scheduling](scheduling.md), [shutdown](shutdown.md), [worker controls](worker-observability.md), [process tracking](process-tracking.md), and [Rails integration](rails-integration.md) for individual defaults and runtime behavior.
