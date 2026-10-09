# Health probes and systemd

The CLI can serve health checks without Rack or a web-server dependency:

```sh
decent_job start --require ./jobs.rb --probe-port 7001
```

The probe binds to all IPv4 interfaces. `DECENT_JOB_PROBE_PORT` supplies a default; application configuration can override it, and `--probe-port` takes precedence over both. A nil port disables the probe. The programmatic API also accepts port 0 and exposes the assigned port through `ProbeServer#port`.

| Path | Success | Unavailable |
| --- | --- | --- |
| `/`, `/status` | 200, `OK` | These report the HTTP server's liveness independently of workers. |
| `/status/started` | 200, `Started` | 503, `Not started` |
| `/status/connected` | 200, `Connected` | 503, `Not connected` |
| Other paths | 404, `Not found` | A custom application can provide its own routes. |

Started means every registered worker has its configured execution capacity and shutdown has not begun. Connected additionally requires the process heartbeat connection and, when enabled, the LISTEN connection. With LISTEN disabled, the dedicated heartbeat supplies connectivity state. These checks read in-memory state; they do not borrow a database connection or execute a query for each HTTP request. Connection loss becomes visible when its owning thread detects it, and reconnect restores readiness.

`DecentJob.started?` and `DecentJob.connected?` inspect workers registered through DecentJob. Independently constructed workers expose the same predicates directly. With no registered workers, global readiness is false.

The CLI keeps the probe available while jobs drain. Shutdown makes both readiness endpoints return 503 while `/status` remains 200. A bind failure makes CLI startup fail. All probe sockets close when shutdown completes.

## Custom applications and manual operation

```ruby
DecentJob.configure do |configuration|
  configuration.probe_port = 7001
  configuration.probe_app = lambda do |env|
    [200, {"Content-Type" => "text/plain"}, ["Path: #{env.fetch("PATH_INFO")}"]]
  end
end
```

A custom application replaces the entire default application, including health routes. It receives an environment hash and returns `[status, headers, body]`; the body must respond to `each`. It provides method, path, query, and request-header information, suppresses HEAD response bodies, and closes response bodies that support `close`.

The built-in server has one serving thread and a bounded connection backlog. It accepts at most 16 KiB of request headers and buffers at most 1 MiB of response data. Network reads and writes have one-second deadlines. It supports small probe requests without request bodies; it does not implement streaming or Rack's full environment and extensions. Custom callbacks must return promptly.

To retain health routes while adding a fallback application, use `ProbeServer::HealthcheckMiddleware.new(fallback, state: DecentJob)`. The state object must respond to `started?` and `connected?`. `ProbeServer.cluster_app(supervisor)` uses that interface for a cluster controller.

```ruby
probe = DecentJob::ProbeServer.new(port: 0).start
puts probe.port
probe.stop
```

`stop(timeout: seconds)` closes network sockets and waits up to that deadline for the serving thread, returning false if an application callback is still running. It leaves the callback alone; call `stop` again after it returns. The CLI shares its shutdown deadline across job and probe drain and exits unsuccessfully when either exceeds it.

The default handler uses Ruby sockets. `probe_handler = :webrick`, `DECENT_JOB_PROBE_HANDLER=webrick`, or `--probe-handler webrick` selects WEBrick when the host application provides it. It is not a DecentJob dependency. Missing WEBrick or an unknown handler produces a warning and falls back to the built-in handler.

WEBrick uses its own HTTP request parser and request limits, so it can accept requests the built-in parser rejects. Both handlers use DecentJob's response buffering limit and bounded shutdown interface.

## systemd notifications

The CLI automatically uses `NOTIFY_SOCKET` when systemd supplies it. It sends `READY=1` after startup and sends `STOPPING=1` once, before blocking on job drain. Readiness notifications describe CLI startup; use `/status/connected` to observe database connectivity.

When `WATCHDOG_USEC` is a positive integer and `WATCHDOG_PID` is absent or matches the current process, an owned thread sends `WATCHDOG=1` every half interval. It continues during graceful drain and stops afterward, including when shutdown raises. The notifier uses UNIX datagrams and supports filesystem socket paths and Linux abstract socket names. These follow the systemd [notification protocol](https://raw.githubusercontent.com/systemd/systemd/main/man/sd_notify.xml) and [watchdog interval contract](https://raw.githubusercontent.com/systemd/systemd/main/man/sd_watchdog_enabled.xml).

Applications managing their own lifecycle can use `DecentJob::SystemdService`:

```ruby
service = DecentJob::SystemdService.new
service.start
service.stop { DecentJob.shutdown(timeout: 30) }
```

`stopping` announces drain without ending watchdog coverage, and calling it again does not duplicate the announcement. `notifying?` reports whether the watchdog is running. Without systemd environment variables, no notification socket or watchdog thread is needed. Notification failures are reported through `on_thread_error`; they do not prevent the service from draining its jobs.

A service unit can use these settings, with paths and Ruby's executable environment adjusted for the host:

```ini
[Service]
Type=notify
NotifyAccess=main
WorkingDirectory=/srv/my-application
EnvironmentFile=/etc/my-application.env
ExecStart=/usr/bin/env bundle exec decent_job start --require ./jobs.rb --shutdown-timeout 30
Restart=on-failure
WatchdogSec=30
TimeoutStopSec=45
```

Keep the service manager's stop timeout longer than DecentJob's drain deadline so it can finish cleanup and report its exit status. `systemd_watchdog_start.decent_job` includes the watchdog interval; `systemd_watchdog_error.decent_job` includes notification errors.

Supervisor process management and child readiness channels remain separate work. The cluster application interface is available, but this checkpoint's CLI integration runs one process.
