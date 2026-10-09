# Supervising worker processes

The CLI can run several worker processes under one supervisor. Each worker owns its PostgreSQL connections, execution threads, listener, heartbeat, and enabled cron schedules. PostgreSQL deduplicates cron occurrences across workers.

```sh
bundle exec decent_job start --require ./jobs.rb --subprocesses 3
bundle exec decent_job start --require ./jobs.rb --queues 'critical:2;default:3|reports:2'
```

`DECENT_JOB_SUBPROCESSES` and `configuration.subprocesses` set the count. The default is zero, which runs one worker in the CLI process. Command-line options override application configuration, which overrides environment defaults.

A pipe separates subprocess recipes. A semicolon separates thread pools within one process. In the second example, one child runs two critical threads and three default threads; another runs two reports threads. Multiple pipe groups determine the process count and produce a warning if a positive explicit count is ignored. Empty groups are invalid. On platforms without `fork`, the CLI warns and combines the recipes into one process.

The supervisor retains a stable slot for each recipe. It replaces an exited child, including a child sent SIGTERM directly. Repeated exits within ten seconds introduce exponential restart delays capped at thirty seconds. Delays apply per slot, so other children can report readiness while a replacement waits.

## Fork lifecycle

Register application cleanup before a fork and application setup in each child:

```ruby
DecentJob.before_supervisor_fork do
  # Stop application-owned threads and disconnect application-owned clients.
end

DecentJob.before_subprocess_boot do
  # Establish application-owned resources in this child.
end
```

Hooks run in registration order for every initial or replacement fork. A hook raising `StandardError` reports through `configuration.on_thread_error`; remaining hooks still run. `DecentJob.clear_lifecycle_hooks` clears application hooks.

Library cleanup is separate from these hooks. Before forking, DecentJob drains its workers and cron managers, disconnects its pooled PostgreSQL sessions, and prevents checkouts across the fork boundary. Preparation fails if a pool still has a checked-out or connecting session. Cached adapter, store, and database references continue to work in the child with fresh pool state and connections. Async adapters enqueue externally in supervised children so they do not create extra execution pools. Explicit inline adapters retain synchronous behavior. A normal fork outside the supervisor still rejects inherited database use.

Directly constructed workers, cron managers, and active process-tracker registrations must be drained before fork, for example in a before-fork hook. Preparation rejects any that remain alive. Connections returned directly by `Database#connect` belong to their caller. Close them in application hooks, along with other application-owned connections and threads. The supervisor cannot make arbitrary application libraries safe to fork.

## Readiness and shutdown

Use `--probe-port PORT` to expose the health routes described in [health services](health-services.md). The parent serves the probe. `/status/started` requires all configured child slots to be present, and `/status/connected` requires a recent readiness report from every child. Children send reports only when their workers and enabled database services are ready. Missing reports expire after fifteen seconds. Shutdown immediately makes both readiness routes return 503; `/status` continues to return 200 during drain.

The supervisor closes its inherited probe socket in every child, including replacements. If the parent dies, a surviving child cannot retain the probe port. After boot hooks return, children check their parent every second and drain when orphaned. An application boot hook that blocks forever also prevents that child from reaching its orphan check.

SIGINT and SIGTERM stop the cluster. The supervisor sends TERM to its children, allows one shared `shutdown_timeout`, then kills and reaps survivors. SIGQUIT requests immediate termination. A signal sent while a child is inside its boot hook still terminates that child. Startup failures also clean up children already forked.

Systemd READY, STOPPING, and watchdog notifications come from the supervisor. STOPPING precedes child drain, and watchdog coverage continues through child and probe cleanup. No child starts a copy of the parent's watchdog. See [health services](health-services.md) for environment configuration.

A timeout means an executing job may be interrupted and later recovered. Jobs should tolerate retry after process loss, as with any abrupt worker termination.

Cluster mode warns and ignores `idle_timeout`; single-process fallback honors it. A daemonized supervisor owns the PID file, and its children close inherited PID-lock descriptors. See [daemon mode and idle shutdown](daemon-and-idle.md).
