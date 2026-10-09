# Notification controls

DecentJob uses PostgreSQL notifications to tell workers that work may be available. Job records remain the source of truth. A notification is a scheduling hint: disabling it does not prevent a worker from finding and executing the job through polling, startup, or another wakeup.

Enqueue and retry notifications are enabled by default. Set `configuration.enable_listen_notify = false` or `DECENT_JOB_ENABLE_LISTEN_NOTIFY=false` to disable publication from enqueue/retry operations and listening in workers configured that way. Embedded async execution still wakes its local worker after enqueue. Maintenance actions such as operator rescheduling and unpausing retain their broad wakeups, as do explicit low-level notification calls. Operator retries follow the enqueue policy.

## Per-job settings

Include the optional concern to configure a job class:

```ruby
class ImportJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::NotifyOptions
  self.decent_job_notify = false

  def perform(account_id)
    # Import work.
  end
end
```

An individual enqueue can override the class default:

```ruby
ImportJob.set(decent_job_notify: true).perform_later(account_id)
ImportJob.set(decent_job_notify: false).perform_later(account_id)
```

A `nil` instance setting uses the class default. Explicit `true` and `false` values are serialized with the job and survive deserialization and Active Job retries. A class setting is inherited by subclasses, which may override it independently. Global disabling takes precedence over a per-job `true` value.

Bulk enqueue applies the same policy to each accepted job. This includes Active Job bulk enqueue and DecentJob bulk capture. A rejected duplicate cron occurrence does not publish a notification.

## Publication and payloads

Enqueue notifications carry a queue name, a count of notifying jobs, and an ISO8601 scheduled time when the work is in the future. Bulk jobs are grouped by queue and scheduled time. Job arguments are not included.

Publication runs on the same PostgreSQL transaction as insertion or rescheduling. PostgreSQL delivers it at the outer commit; a rollback publishes nothing. Identical channel/payload messages within a transaction may be coalesced by PostgreSQL, so counts are hints rather than an execution ledger.

An unusually long queue name can exceed PostgreSQL's notification payload limit. DecentJob falls back to a broad empty notification so a valid job can still be persisted. Maintenance actions also use broad notifications.

The listener routes queue-specific hints to matching pools and bounds wakeups by execution capacity. A `fanout: true` hint wakes every matching pool. Future scheduled times update the pool's deadline cache. Empty notifications retain their broad wakeup behavior. Malformed messages are reported without disconnecting the listener. See [worker scheduling](scheduling.md) for cache limits and execution with polling disabled.

For explicit maintenance integration, `DecentJob::Notifier.notify(connection, state = nil)` publishes on a caller-owned pg connection. It does not apply job notification policy; `nil` publishes a broad wakeup. `Store#notify(connection)` remains the broad maintenance entry point.
