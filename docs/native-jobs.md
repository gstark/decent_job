# Native jobs

`DecentJob::Base` is DecentJob's own job class. It needs no gem other than `pg`. Its API, payload keys, and argument encoding follow Active Job. To move a job between the two, you change its superclass.

## Define and enqueue a job

```ruby
require "decent_job"

class ReportJob < DecentJob::Base
  queue_as :reports
  queue_with_priority 10

  def perform(account_id, period:)
    # ...
  end
end

ReportJob.perform_later(42, period: "2026-09")
ReportJob.set(wait: 300, queue: :urgent, priority: 1).perform_later(42, period: "2026-09")
ReportJob.set(wait_until: Time.now + 3600).perform_later(42, period: "2026-09")
ReportJob.perform_now(42, period: "2026-09")
DecentJob.perform_all_later(ReportJob.new(1, period: "2026-09"), ReportJob.new(2, period: "2026-09"))
```

`perform_later` returns the job when the adapter accepts it. It returns `false` when a callback aborts or the adapter rejects the job. A rejection by the adapter or a concurrency limit sets `enqueue_error`. A callback abort leaves it `nil`, as in Active Job. `perform_all_later` enqueues in bulk and does not run enqueue callbacks, as in Active Job. A class with concurrency limits or rules is the exception: each of its jobs is enqueued on its own, with callbacks, so admission sees the final key and labels.

Durations are numbers of seconds. Any object with `to_f`, such as `ActiveSupport::Duration`, also works.

## Choose the adapter

Native jobs without their own adapter share `DecentJob.default_adapter`, which uses the global database. Set an adapter on a class to use a different database or execution mode. Subclasses inherit it.

```ruby
database = DecentJob::Database.new(url: ENV.fetch("DECENT_JOB_DATABASE_URL"), pool_size: 5)
DecentJob::Base.queue_adapter = DecentJob::Adapter.new(database: database)
```

`DecentJob.reset!` clears the default adapter.

## Callbacks

`before_enqueue`, `after_enqueue`, `around_enqueue`, `before_perform`, `after_perform`, and `around_perform` take a block or a method name. Each callback can take `if:` and `unless:`.

```ruby
class AuditJob < DecentJob::Base
  before_perform :load_account, if: -> { arguments.first }
  around_perform do |job, block|
    started = Time.now
    block.call
    Audit.record(job.job_id, Time.now - started)
  end
end
```

The order matches Active Job. Each callback wraps the callbacks defined after it. A `before` callback that throws `:abort` stops the chain, and later callbacks do not run.

## Errors and retries

```ruby
class SyncJob < DecentJob::Base
  retry_on Net::ReadTimeout, wait: :polynomially_longer, attempts: 5
  retry_on RateLimited, wait: ->(executions) { executions * 30 }, attempts: :unlimited
  discard_on ActiveRecord::RecordNotFound
  rescue_from(PaymentDeclined) { |error| notify(error) }
  after_discard { |job, error| Alerts.discarded(job, error) }
end
```

- `retry_on` accepts `wait:` (seconds, a proc, or `:polynomially_longer`), `attempts:`, `queue:`, `priority:`, `jitter:`, and a block for when attempts run out.
- The last matching handler wins. When no handler matches an error, the handlers are checked against its `cause`.
- A handled error is returned from `perform_now`, as in Active Job.

## Arguments

Arguments are stored as JSON with Active Job's `_aj_*` markers. Supported values:

- `nil`, booleans, numbers, strings, and symbols.
- Arrays, and hashes with string or symbol keys.
- `Time`, `Date`, `DateTime`, `BigDecimal`, ranges, and named classes or modules.
- `ActiveSupport::Duration`, `ActiveSupport::TimeWithZone`, and `HashWithIndifferentAccess`, when the application has loaded ActiveSupport.
- GlobalID records, when the application has loaded the `globalid` gem.

Keyword arguments reach `perform` as keywords. To add a type, register a serializer with `DecentJob::Arguments.add_serializers`. A serializer responds to `name`, `serialize?(value)`, `serialize(value)`, and `deserialize(hash)`. When the Active Job bridge is loaded, values that no DecentJob serializer handles go to `ActiveJob::Serializers`.

## DecentJob features

Native jobs include labels, notify options, concurrency controls, batches, and cron validation. These use the same methods as the Active Job extensions:

```ruby
class ImportJob < DecentJob::Base
  self.decent_job_labels = ["imports"]
  decent_job_control_concurrency_with(key: -> { "account-#{arguments.first}" }, total_limit: 1)
end
```

`DecentJob::JobExtensions::InterruptErrors` is opt-in. With it, a job whose earlier execution was interrupted fails with `DecentJob::InterruptError` instead of running again.

See [labels, concurrency, and pauses](../README.md#labels-concurrency-and-pauses), [batches](../README.md#batches), and [recurring jobs](recurring-jobs.md). Every example there works with native jobs.

## Not supported

- `locale` and `timezone` payload keys.
- The `report:` option of `retry_on` and `discard_on`.
- Callback objects. Use a block or a method name instead.

## Active Job

Active Job support is an optional bridge. Load it in one of these ways:

- In Rails, the Railtie loads it when Active Job loads.
- Set `queue_adapter = :decent_job` after you require `active_job` and `decent_job`.
- Call `require "decent_job/active_job"`.

Native jobs and Active Job jobs can share a database and workers.
