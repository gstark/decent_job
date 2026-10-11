# frozen_string_literal: true

require "active_job/railtie"
require "active_record/railtie"
require "decent_job"
require "json"
require "stringio"
require "timeout"

HOST_OPTIONS = JSON.parse(ENV.fetch("HOST_CONNECTION"), symbolize_names: true)
HOST_SCENARIO = ENV.fetch("HOST_SCENARIO")
HOST_LOG = StringIO.new
PERFORMED = Queue.new

# Active Job 8.1 reads enqueue_after_transaction_commit only from job classes.
class ApplicationJob < ActiveJob::Base
  self.enqueue_after_transaction_commit = ENV["HOST_DEFER"] == "true"
end

class AtomicHostJob < ActiveJob::Base
  self.queue_adapter = :decent_job

  def perform(value)
    PERFORMED << value
  end
end

class LimitedHostJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::Concurrency

  self.queue_adapter = :decent_job
  decent_job_control_concurrency_with(enqueue_limit: 1)

  def perform
  end
end

class DecentJobActiveRecordApplication < Rails::Application
  config.root = ENV.fetch("HOST_ROOT")
  config.eager_load = false
  config.secret_key_base = "host active record secret" * 4
  config.logger = Logger.new(HOST_LOG)
  config.active_support.isolation_level = ENV.fetch("HOST_ISOLATION", "thread").to_sym
  config.active_job.queue_adapter = :decent_job
  config.decent_job.max_threads = 2
  config.decent_job.poll_interval = 0.02
  config.decent_job.execution_mode = ENV.fetch("HOST_MODE", "external").to_sym
  config.decent_job.connection = ENV["HOST_CONNECTION_MODE"].to_sym if ENV["HOST_CONNECTION_MODE"]

  case ENV["HOST_DATABASE_URL"]
  when "same" then config.decent_job.database_url = HOST_OPTIONS
  when "other" then config.decent_job.database_url = HOST_OPTIONS.merge(dbname: "decent_job_elsewhere")
  end

  def config.database_configuration
    options = HOST_OPTIONS.except(:options, :dbname)
    options[:database] = HOST_OPTIONS.fetch(:dbname)
    options[:schema_search_path] = HOST_OPTIONS.fetch(:options).split("search_path=").last
    options[:pool] = 5
    {Rails.env => options.merge(adapter: "postgresql")}
  end
end

def report(result)
  puts "HOST_RESULT:#{JSON.generate(result)}"
end

begin
  DecentJobActiveRecordApplication.initialize!
rescue => error
  report(boot_error: "#{error.class}: #{error.message}")
  exit
end

# A separate session, outside both pools, observes what other connections see.
observer = PG.connect(HOST_OPTIONS)
observer.exec("LISTEN decent_job")
job_count = -> { Integer(observer.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)) }
batch_count = -> { Integer(observer.exec("SELECT count(*) FROM decent_job_batches").getvalue(0, 0)) }

# Notifications are database-wide. Count only those from the Active Record
# session of this thread, which runs the enqueues.
notified = lambda do
  pid = ActiveRecord::Base.connection_pool.with_connection { |connection| connection.raw_connection.backend_pid }
  found = false

  loop do
    break unless observer.wait_for_notify(0.2) { |_channel, sender| found ||= sender == pid }
  end

  found
end

performed = lambda do |timeout|
  values = []
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout

  loop do
    values << PERFORMED.pop(true)
  rescue ThreadError
    break if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

    sleep 0.02
  end

  values
end

result = {connection: DecentJob.configuration.connection, warned: HOST_LOG.string.include?("enqueue_after_transaction_commit is false")}

case HOST_SCENARIO
when "boot"
  nil
when "rollback"
  ActiveRecord::Base.transaction do
    AtomicHostJob.perform_later("rolled back")
    DecentJob.database.after_commit { result[:commit_callback] = true }
    DecentJob.database.after_rollback { result[:rollback_callback] = true }
    result[:inside] = job_count.call
    raise ActiveRecord::Rollback
  end

  result[:after] = job_count.call
  result[:notified] = notified.call
when "commit"
  ActiveRecord::Base.transaction do
    AtomicHostJob.perform_later("committed")
    DecentJob.database.after_commit { result[:commit_callback_count] = job_count.call }
    result[:inside] = job_count.call
    result[:notified_inside] = notified.call
    result[:pool_inside] = DecentJob.database.with_connection { |connection| Integer(connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)) }
  end

  result[:after] = job_count.call
  result[:notified_after] = notified.call
when "nested"
  ActiveRecord::Base.transaction do
    AtomicHostJob.perform_later("outer")

    ActiveRecord::Base.transaction(requires_new: true) do
      AtomicHostJob.perform_later("inner")
      raise ActiveRecord::Rollback
    end
  end

  result[:values] = observer.exec("SELECT payload->'arguments'->>0 AS value FROM decent_jobs").column_values(0)
when "bulk"
  ActiveRecord::Base.transaction do
    ActiveJob.perform_all_later([AtomicHostJob.new("a"), AtomicHostJob.new("b")])
    raise ActiveRecord::Rollback
  end

  result[:rolled_back] = job_count.call

  ActiveRecord::Base.transaction do
    ActiveJob.perform_all_later([AtomicHostJob.new("a"), AtomicHostJob.new("b")])
    result[:inside] = job_count.call
  end

  result[:committed] = job_count.call
when "batch"
  ActiveRecord::Base.transaction do
    DecentJob::Batch.new(description: "rolled back").enqueue([AtomicHostJob.new("a")])
    raise ActiveRecord::Rollback
  end

  result[:rolled_back] = [batch_count.call, job_count.call]

  ActiveRecord::Base.transaction do
    batch = DecentJob::Batch.new(description: "committed")
    batch.enqueue([AtomicHostJob.new("a"), AtomicHostJob.new("b")])
    result[:reloaded] = batch.enqueued_at ? true : false
    result[:inside] = [batch_count.call, job_count.call]
  end

  result[:committed] = [batch_count.call, job_count.call]

  ActiveRecord::Base.transaction do
    DecentJob::Batch.enqueue_all([[DecentJob::Batch.new, [AtomicHostJob.new("c")]]])
    raise ActiveRecord::Rollback
  end

  result[:enqueue_all_rolled_back] = [batch_count.call, job_count.call]
when "concurrency"
  holding = Queue.new
  release = Queue.new

  first = Thread.new do
    ActiveRecord::Base.transaction do
      job = LimitedHostJob.perform_later
      holding << true
      release.pop
      job ? true : false
    end
  end

  holding.pop
  second_started = Queue.new

  second = Thread.new do
    second_started << true
    LimitedHostJob.perform_later ? true : false
  end

  second_started.pop
  sleep 0.5
  result[:second_waiting] = second.alive?
  release << true
  result[:first] = first.value
  result[:second] = Timeout.timeout(5) { second.value }
  result[:after] = job_count.call
when "worker"
  DecentJob.start

  ActiveRecord::Base.transaction do
    AtomicHostJob.perform_later("worker")
    result[:before_commit] = performed.call(0.5)
  end

  result[:after_commit] = performed.call(3).first(1)
  DecentJob.shutdown(timeout: 5)
when "inline", "async"
  ActiveRecord::Base.transaction do
    AtomicHostJob.perform_later("outer")

    ActiveRecord::Base.transaction(requires_new: true) do
      AtomicHostJob.perform_later("inner")
    end

    result[:before_commit] = performed.call(0.3)
  end

  result[:after_commit] = performed.call(3).sort

  result[:no_transaction] = begin
    AtomicHostJob.perform_later("plain")
    performed.call(3)
  end

  DecentJob.shutdown(timeout: 5)
end

report(result)
