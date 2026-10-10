# frozen_string_literal: true

# Job classes that exercise every DecentJob feature. The worker and the
# dashboard both load this file.
require "decent_job"
require "net/http"

module Example
  class UpstreamTimeout < StandardError; end
  class RateLimited < StandardError; end
  class InvalidPayload < StandardError; end

  # Sleeps in small steps so a shutdown or force-discard can interrupt it.
  def self.work(seconds, job: nil)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds

    while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      return :stopped if job && job.class.queue_adapter.stopping?(job)
      sleep 0.2
    end

    :done
  end
end

# Fast, always succeeds. Shows labels and a mailers queue pool.
class WelcomeEmailJob < DecentJob::Base
  queue_as :mailers
  self.decent_job_labels = ["email", -> { "user:#{arguments.first}" }]

  def perform(user_id, template: "welcome")
    Example.work(rand(0.05..0.3))
  end
end

# Slow with keyword arguments and a tenant label. Good for latency charts.
class GenerateReportJob < DecentJob::Base
  queue_as :reports
  queue_with_priority 10
  self.decent_job_labels = [-> { "tenant:#{arguments.first}" }]

  before_perform { |job| job.arguments.first }

  def perform(tenant, period:, format: "pdf")
    Example.work(rand(0.5..3.0), job: self)
  end
end

# Fails about half the time and retries with growing waits. Fills the
# "Retrying" state and the attempt history.
class FlakyWebhookJob < DecentJob::Base
  queue_as :webhooks
  retry_on Example::UpstreamTimeout, wait: :polynomially_longer, attempts: 4
  retry_on Example::RateLimited, wait: ->(executions) { executions * 15 }, attempts: :unlimited

  def perform(url, event:)
    Example.work(rand(0.1..0.6))
    raise Example::UpstreamTimeout, "#{url} did not answer within 5s" if rand < 0.4
    raise Example::RateLimited, "429 from #{URI(url).host}" if rand < 0.15
  end
end

# Always fails. Two attempts, then discarded, with a long backtrace.
class BrokenImportJob < DecentJob::Base
  retry_on RuntimeError, wait: 20, attempts: 2
  discard_on Example::InvalidPayload

  def perform(source)
    raise Example::InvalidPayload, "#{source} has no header row" if source.end_with?(".bad")
    parse(source)
  end

  private

  def parse(source)
    load_rows(source)
  end

  def load_rows(source)
    raise "S3 returned 503 for #{source}"
  end
end

# One at a time per account, at most twenty queued per account. Shows
# concurrency claims on job detail and waiting labels in lists.
class AccountSyncJob < DecentJob::Base
  queue_as :sync
  decent_job_control_concurrency_with(key: -> { "account-#{arguments.first}" }, perform_limit: 1, enqueue_limit: 20)
  decent_job_concurrency_rule(label: "sync-global", apply_label: true, perform_throttle: [10, 30])

  def perform(account_id)
    Example.work(rand(1.0..4.0), job: self)
  end
end

# Runs for a long time and stops cooperatively. Use it to try force-discard.
class LongRunningJob < DecentJob::Base
  queue_as :slow

  def perform(minutes: 2)
    Example.work(minutes * 60, job: self)
  end
end

# A job that was interrupted by a crash fails instead of running again.
class ExactlyOnceJob < DecentJob::Base
  include DecentJob::JobExtensions::InterruptErrors

  def perform(payment_id)
    Example.work(0.2)
  end
end

# Batch members and callbacks.
class InvoiceJob < DecentJob::Base
  queue_as :mailers

  def perform(customer_id)
    Example.work(rand(0.1..0.5))
    raise "customer #{customer_id} has no billing address" if customer_id % 7 == 0
  end
end

class BatchSummaryJob < DecentJob::Base
  def perform(batch, options)
    Example.work(0.2)
    DecentJob.logger.info "batch #{batch.properties.fetch(:month)} #{options.fetch(:event)}"
  end
end

# Recurring jobs.
class HeartbeatJob < DecentJob::Base
  def perform
  end
end

class DigestJob < DecentJob::Base
  queue_as :reports

  def perform(hour)
    Example.work(rand(0.3..1.0))
  end
end

class CleanupJob < DecentJob::Base
  def perform(older_than_hours:)
    DecentJob.cleanup_preserved_jobs(older_than: older_than_hours * 3600)
  end
end

# Fixed-wait retries with a channel label.
class NotificationJob < DecentJob::Base
  queue_as :mailers
  self.decent_job_labels = [-> { "channel:#{arguments.last.fetch(:channel, "push")}" }]
  retry_on Example::UpstreamTimeout, wait: 10, attempts: 3

  def perform(user_id, channel: "push")
    Example.work(rand(0.05..0.2))
    raise Example::UpstreamTimeout, "#{channel} gateway timed out" if rand < 0.2
  end
end

# Cron entries name these classes, so configuration loads after them.
require_relative "config"
