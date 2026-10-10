# Dashboard

DecentJob ships an operations dashboard as a Rack-compatible application. It has no runtime dependencies beyond `pg`. Rack itself is not required: the application responds to `call(env)` and returns `[status, headers, body]`, so any Rack server or framework can host it.

The dashboard shows jobs, job detail with attempts and backtraces, batches, recurring schedules, worker processes, pauses, a cleaner for discarded jobs, and latency summaries. Operators can retry, reschedule, discard, force-discard, and delete jobs, singly or in bulk; retry discarded batch members; run, enable, and disable schedules; pause and resume queues, classes, and labels; and delete groups of discarded jobs.

## Mount in Rails

```ruby
# config/routes.rb
Rails.application.routes.draw do
  mount DecentJob::Dashboard.new => "/decent_job"
end
```

The dashboard performs no authentication. Protect it with the host application's own mechanism:

```ruby
# config/routes.rb
authenticate :user, ->(user) { user.admin? } do
  mount DecentJob::Dashboard.new => "/decent_job"
end
```

Any Rack middleware or route constraint works. `Rack::Auth::Basic` is a common choice outside Devise:

```ruby
dashboard = Rack::Builder.new do
  use Rack::Auth::Basic do |username, password|
    ActiveSupport::SecurityUtils.secure_compare(password, ENV.fetch("DECENT_JOB_DASHBOARD_PASSWORD"))
  end
  run DecentJob::Dashboard.new
end
mount dashboard => "/decent_job"
```

## Mount in any Rack application

```ruby
# config.ru
require "decent_job"
require "./jobs"
DecentJob.configure { |config| config.database_url = ENV.fetch("DECENT_JOB_DATABASE_URL") }
run DecentJob::Dashboard.new
```

Pass `database:` to use an explicit pool and `configuration:` to use explicit recurring schedules. Without them the dashboard uses the global database and configuration, so the job classes and `config.cron` entries must be loaded in the process that serves the dashboard.

## Run it standalone

```sh
bundle exec decent_job dashboard --require ./jobs.rb --port 7002 --host 127.0.0.1
```

The standalone server uses WEBrick, which is not a dependency of the gem. Add `gem "webrick"` to the application's Gemfile to use it. The command is intended for development and for operators on a private network; in production, mount the application in the web server described above.

## Security

- Every state change is a POST with a form token. The token is a random value in a `SameSite=Lax; HttpOnly` cookie scoped to the mount path and repeated in each form. A mismatch returns 400.
- Responses set a content security policy that allows only the dashboard's own script and stylesheet plus inline `style` attributes, which the charts use for bar sizes. Scripts can never run inline. `X-Content-Type-Options`, `X-Frame-Options`, and `Referrer-Policy` are also set.
- Only two asset paths are served, from an allowlist. Path traversal returns 404.
- All job data is escaped before it reaches markup. Serialized arguments and errors never become executable content.
- Redirects after actions stay inside the mount path. A foreign `Referer` falls back to the dashboard root.
- Live polling calls `metrics.json` under the same mount, with the same cookies, so the host's authentication applies.

## Pages

| Path | Content |
| --- | --- |
| `/` | State ribbon, queues, workers, jobs due in the next hour, finished attempts per hour, recent discards |
| `/jobs` | Filter by state, queue, class, label, schedule, or batch; search; keyset pagination; bulk actions |
| `/jobs/:id` | Arguments, payload, timing, ownership, concurrency claims, every attempt with error and backtrace |
| `/batches`, `/batches/:id` | Progress per batch, members, callback jobs, retry of discarded members |
| `/schedules`, `/schedules/:key` | Definitions, last and next run, run now, enable, disable, history |
| `/processes` | Registered workers, queues, busy threads, running jobs, heartbeat |
| `/pauses` | Pause or resume a queue, job class, or label |
| `/cleaner` | Discarded jobs grouped by class and error, with deletion per group |
| `/performance` | Queue wait, run time, and total latency summaries, a log-scale histogram, breakdowns by class and queue |
| `/metrics.json` | Counts for live polling |

The state ribbon on the overview and jobs pages shows every job's state in proportion. Each legend entry is a filter: selecting one narrows the job list, and selecting several combines them.

Times are UTC. Relative labels refresh in the browser; hover a time for the exact value. The theme follows the operating system and can be switched from the masthead. Live polling can be switched off from the masthead; the choice is kept in the browser.

## How the UI is built

The dashboard uses only the Ruby standard library:

- `DecentJob::Dashboard` routes requests and performs actions through the same public APIs as the Ruby management methods (`Job#retry_job`, `Batch#retry`, `CronEntry#enqueue`, `Pauses`).
- `DecentJob::Dashboard::Queries` holds every read query. Lists use keyset pagination on `(created_at, id)` so ties never repeat or skip rows.
- `DecentJob::Dashboard::View` renders ERB templates from `lib/decent_job/dashboard/views` and escapes every value.
- One stylesheet and one script under `lib/decent_job/dashboard/assets`. The script adds confirmations, bulk selection, backtrace tabs, relative-time refresh, and live polling of the ribbon. The pages work without it.

Charts are plain HTML and CSS: the ribbon is a flex row, the hourly chart and the histogram are CSS bars, and the hour-ahead timeline is a grid whose cell colour encodes the count. No canvas library is needed.

## Proposals for production use

These are recommendations, not requirements. Each is a separate decision.

1. **Serve it from the application's web server.** Mount the dashboard in Rails or in `config.ru` so the application's authentication, TLS termination, logging, and process supervision apply. Do not expose the standalone WEBrick server to the public internet.
2. **Put it behind an admin constraint.** Use the host's existing admin role. For API-only Rails applications, wrap the mount in `Rack::Auth::Basic` with a secret from the environment.
3. **Give it a read-only database role where possible.** Actions need write access, so use a role that can write only to the `decent_job_*` tables. Pass an explicit `database:` pool built from that role's URL.
4. **Cache assets at the proxy.** The two assets carry `Cache-Control: public, max-age=3600`. Append a version query in a future release when the files change.
5. **Keep polling cheap.** The ribbon polls `metrics.json` every five seconds while the tab is visible. The query scans job states with one `GROUP BY`. If the jobs table grows beyond a few million unfinished rows, add a partial index on `(state) WHERE finished_at IS NULL` or lengthen the interval in `dashboard.js`.
6. **Search scales to moderate tables.** Search uses `ILIKE` over class, queue, payload, error, labels, and cron key, which is exact and locale-independent but reads matching pages sequentially. For very large tables, a later release can add a generated `tsvector` column and index; the interface will not change.
7. **Automate cleanup.** The cleaner page deletes discarded jobs by group. Routine retention should still run through `decent_job cleanup_preserved_jobs` or the worker's automatic cleanup.
8. **Extend by composition.** Host-specific links or panels belong in a wrapper that renders around the dashboard, or in a future `custom_head` hook. The templates are internal and may change between releases.
