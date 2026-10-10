# frozen_string_literal: true

# Enqueues a varied workload. Run once for a snapshot, or with --loop to keep
# the dashboard moving.
#
#   bundle exec ruby seed.rb          # one round
#   bundle exec ruby seed.rb --loop   # a new round every 20 seconds
require_relative "jobs"

module Seed
  module_function

  def round(number)
    DecentJob.logger.info "seed round #{number}"
    emails
    reports
    webhooks
    imports
    account_syncs
    batches
    notifications
    exactly_once
    long_running if number == 1
    pauses if number == 1
    bulk
    DecentJob.logger.info "seed round #{number} done"
  end

  def emails
    12.times { |i| WelcomeEmailJob.perform_later(rand(1000..1049), template: %w[welcome reminder digest][i % 3]) }
    3.times { |i| WelcomeEmailJob.set(wait: 60 * (i + 1)).perform_later(2000 + i) }
  end

  def reports
    %w[acme globex initech umbrella].each_with_index do |tenant, i|
      GenerateReportJob.perform_later(tenant, period: Time.now.utc.strftime("%Y-%m"))
      GenerateReportJob.set(wait: 300 + i * 120, priority: 20).perform_later(tenant, period: "2026-09", format: "csv")
    end
    GenerateReportJob.set(wait_until: Time.now + 3600 * 6).perform_later("acme", period: "2026-Q3")
  end

  def webhooks
    8.times do |i|
      FlakyWebhookJob.perform_later("https://hooks.example.com/customer/#{i}", event: %w[order.paid order.refunded invoice.sent][i % 3])
    end
  end

  def imports
    BrokenImportJob.perform_later("s3://exports/users-#{rand(100)}.csv")
    BrokenImportJob.perform_later("s3://exports/orders.bad")
  end

  def account_syncs
    [42, 42, 42, 42, 7, 7, 99].each { |account| AccountSyncJob.perform_later(account) }
  end

  def batches
    DecentJob::Batch.enqueue(description: "Monthly invoices", on_finish: BatchSummaryJob, on_discard: BatchSummaryJob, month: Time.now.utc.strftime("%Y-%m")) do
      (1..10).each { |customer| InvoiceJob.perform_later(customer) }
    end
    DecentJob::Batch.enqueue(description: "Reindex tenants", on_success: BatchSummaryJob, month: "n/a") do
      %w[acme globex].each { |tenant| GenerateReportJob.set(priority: 0).perform_later(tenant, period: "reindex") }
    end
  end

  def notifications
    5.times { |i| NotificationJob.perform_later(3000 + i, channel: i.even? ? "push" : "sms") }
  end

  def exactly_once
    2.times { |i| ExactlyOnceJob.perform_later("pay_#{SecureRandom.hex(4)}") }
  end

  def long_running
    LongRunningJob.perform_later(minutes: 3)
  end

  def pauses
    DecentJob.pause(queue: "paused")
    3.times { |i| WelcomeEmailJob.set(queue: "paused").perform_later(4000 + i) }
  end

  def bulk
    DecentJob::Bulk.enqueue do
      5.times { |i| WelcomeEmailJob.perform_later(5000 + i) }
    end
    DecentJob.perform_all_later(*4.times.map { |i| DigestJob.new("bulk-#{i}") })
  end
end

loop_mode = ARGV.include?("--loop")
number = 1
begin
  loop do
    Seed.round(number)
    break unless loop_mode
    number += 1
    sleep 20
  end
rescue SignalException
  nil
end
