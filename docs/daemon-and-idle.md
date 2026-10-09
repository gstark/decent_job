# Daemon mode and idle shutdown

The CLI runs in the foreground by default. Use daemon mode to detach it while preserving the current working directory:

```sh
mkdir -p tmp/pids
bundle exec decent_job start --require ./jobs.rb --daemonize
```

The default PID file is `tmp/pids/decent_job.pid`, resolved against the working directory when configuration is created. Override it with `--pidfile PATH`, `DECENT_JOB_PIDFILE`, or `configuration.pidfile`. The directory must already exist. The file identifies the detached worker, or the supervisor when using `--subprocesses` or pipe-separated queue recipes. Foreground execution does not create a PID file.

`configuration.daemonize` and `DECENT_JOB_DAEMONIZE=true` also enable daemon mode. `--no-daemonize` overrides either setting. As with other CLI options, command-line values override application configuration, which overrides environment defaults.

Daemonization redirects standard input, output, and error to `/dev/null`. Configure a file logger or another application logging destination before detaching if you need persistent logs. The launching process exits before the detached worker finishes starting. A successful launcher exit confirms detachment; use the [health probe](health-services.md) to confirm worker readiness. Native daemonization must be supported by the Ruby platform.

## PID ownership

DecentJob holds an exclusive file lock for the daemon's lifetime. A second launch fails without changing the first daemon's PID file. A valid PID that exists, including one the current user cannot signal, also prevents startup. Invalid contents and dead PIDs can be reclaimed. PID files must be regular files with one link; symbolic links are rejected.

Normal shutdown removes the file after worker, probe, and service cleanup. The CLI also removes it before a forced exit following a shutdown timeout. Cleanup checks the owning PID and file identity so a replacement file remains untouched. SIGKILL cannot run cleanup and may leave a stale file; a later launch can reclaim it once the recorded process is gone.

Send SIGTERM to the PID in the file to stop a daemon gracefully. Supervised children close inherited PID-lock descriptors without unlocking or deleting the supervisor's file. They cannot retain the lock after the supervisor dies.

Daemonization uses the same resource preparation as [supervised forks](supervisor.md). Registered workers and cron managers drain first, and cached database pools reopen in the detached process. Application-owned resources belong in `before_supervisor_fork` cleanup hooks. `before_subprocess_boot` remains specific to supervised worker children; it does not run in the detached supervisor or single-process daemon.

## Idle shutdown

```sh
bundle exec decent_job start --require ./jobs.rb --idle-timeout 60
```

Configure the same setting with `configuration.idle_timeout` or `DECENT_JOB_IDLE_TIMEOUT`. The default is `nil`, which disables idle shutdown. Values must be finite, nonnegative seconds; fractional seconds are accepted. Zero permits shutdown as soon as there is no active execution.

Idle time begins at worker startup and resets when an actual job execution starts. Empty polls, future jobs, and pending cron schedules do not reset it. The clock is monotonic, so wall-clock adjustments do not alter the timeout. No pool may have an active job when the worker exits. A job that runs longer than the timeout can therefore be followed by immediate idle shutdown once it finishes.

`worker.idle?` reports whether any jobs are active. `worker.idle?(seconds)` additionally checks elapsed time. An unstarted worker is idle. Restarting a worker resets its idle clock.

The single-process CLI checks idleness while waiting for shutdown signals and uses its usual graceful shutdown path when the timeout expires. Probes and systemd watchdog coverage continue through drain. Idle shutdown works in either foreground or daemon mode.

Cluster mode logs a warning and ignores `idle_timeout`. Otherwise the supervisor would continually replace children that exited from idleness. When the platform cannot fork and the CLI falls back to a single worker, idle shutdown remains enabled.
