# Example application

A small application that enqueues every kind of job DecentJob supports, so you can watch the worker and the dashboard handle them. It lives in the source tree and is not part of the gem.

## Requirements

- Ruby 3.2 or newer
- PostgreSQL with a role that can create a database

## Quick start

With [just](https://just.systems) installed, one command does the setup and starts everything:

```sh
cd example
just
```

Ctrl-C stops the worker, the dashboard, and the seeder. `just setup` runs only the setup, and `just console` opens IRB with the jobs loaded.

## Set up by hand

```sh
cd example
bundle install
bin/setup
```

`bin/setup` creates the `decent_job_example` database and installs the tables. Set `DECENT_JOB_DATABASE_URL` to use another database.

## Run

Open three terminals in `example/`:

```sh
bin/worker       # workers, recurring jobs, health probe on port 7001
bin/dashboard    # dashboard on http://127.0.0.1:7002/
bin/seed --loop  # a new round of jobs every 20 seconds
```

Or, with a Procfile runner such as `foreman` or `overmind`, run `foreman start`.

Run `bin/seed` without `--loop` for a single round.

## What the seeder enqueues

| Job | Feature |
| --- | --- |
| `WelcomeEmailJob` | Fast success, labels, delayed jobs, bulk capture, a paused queue |
| `GenerateReportJob` | Slow jobs with keyword arguments, priorities, tenant labels, far-future scheduling |
| `FlakyWebhookJob` | Random failures with polynomial and custom retry waits |
| `BrokenImportJob` | Exhausted retries and `discard_on`, deep backtraces |
| `AccountSyncJob` | Concurrency key with a perform limit, enqueue limit, and a throttled label rule |
| `LongRunningJob` | Cooperative stopping; try force-discard from the dashboard |
| `ExactlyOnceJob` | `InterruptErrors` so a crashed attempt fails instead of rerunning |
| `InvoiceJob` and `BatchSummaryJob` | Batches with finish, success, and discard callbacks |
| `HeartbeatJob`, `DigestJob`, `CleanupJob` | Recurring schedules, including one disabled by default |
| `NotificationJob` | Fixed-wait retries and a label built from a keyword argument |

## Things to try

- Stop the worker with Ctrl-C while `LongRunningJob` runs, then start it again and watch recovery.
- Kill the worker with `kill -9` and see `ExactlyOnceJob` report an interrupt error.
- Pause the `webhooks` queue from the dashboard, then resume it.
- Enable the `nightly_cleanup` schedule and run it now.
- Retry a discarded batch member from the batch page.
- Use `bin/console` to call `DecentJob.stats`, `DecentJob::Job.find`, or `DecentJob.paused`.
