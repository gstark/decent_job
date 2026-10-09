# frozen_string_literal: true

require "decent_job" if ENV["HOST_LOAD_EARLY"] == "true"
require "active_job/railtie"
require "fileutils"
require "json"
require "stringio"
require "timeout"

require "active_record/railtie" if ENV["HOST_ACTIVE_RECORD"] == "true"
require "decent_job"
require "decent_job/railtie" if ENV["HOST_LOAD_EARLY"] == "true"

HOST_OPTIONS = JSON.parse(ENV.fetch("HOST_CONNECTION"), symbolize_names: true)
HOST_SCENARIO = ENV.fetch("HOST_SCENARIO")
HOST_EVENTS = []
HOST_ROOT = ENV.fetch("HOST_ROOT")
HOST_LOG = StringIO.new
HOST_EXPLICIT_LOG = StringIO.new

if HOST_SCENARIO == "logging"
  DecentJob::LogSubscriber.logger
elsif HOST_SCENARIO == "logging_explicit"
  DecentJob::LogSubscriber.loggers.replace([Logger.new(HOST_EXPLICIT_LOG)])
  DecentJob::LogSubscriber.reset_logger
end

module Rails
  class Server; end if ENV["HOST_SERVER"] == "true"
end

class RailsHostJob < ActiveJob::Base
  self.queue_adapter = :decent_job
  self.enqueue_after_transaction_commit = true

  def perform(value)
    HOST_EVENTS << ["perform", value, Rails.application.executor.active?]
    File.write(ENV.fetch("HOST_RESULT"), value) if ENV["HOST_RESULT"]
  end
end

class RailsImmediateJob < RailsHostJob
  self.enqueue_after_transaction_commit = false
end

class DecentJobHostApplication < Rails::Application
  config.root = HOST_ROOT
  config.eager_load = false
  config.enable_reloading = true
  config.secret_key_base = "host integration test secret" * 4
  config.logger = Logger.new(HOST_LOG)
  config.active_support.deprecation = :stderr
  config.active_support.isolation_level = ENV.fetch("HOST_ISOLATION", "thread").to_sym
  config.active_job.queue_adapter = :decent_job
  config.decent_job.database_url = HOST_OPTIONS
  config.decent_job.max_threads = 1
  config.decent_job.poll_interval = 0.02
  config.decent_job.execution_mode = ENV.fetch("HOST_MODE").to_sym if ENV["HOST_MODE"]
  config.decent_job.logger = Logger.new(StringIO.new) if HOST_SCENARIO == "custom_logger"
  if HOST_SCENARIO == "cron_boot"
    config.decent_job.enable_cron = true
    config.decent_job.cron = {tick: {cron: "* * * * * *", class: "ScheduledHostJob"}}
  end

  if ENV["HOST_ACTIVE_RECORD"] == "true"
    def config.database_configuration
      options = HOST_OPTIONS.except(:options, :dbname)
      options[:database] = HOST_OPTIONS.fetch(:dbname)
      options[:schema_search_path] = HOST_OPTIONS.fetch(:options).split("search_path=").last
      {Rails.env => options.merge(adapter: "postgresql")}
    end
  end

  initializer "host.early_adapter", before: :finisher_hook do
    HOST_EVENTS << ["early", RailsHostJob.queue_adapter.async_started?, DecentJob.started?]
  end
end

if HOST_SCENARIO == "reload"
  FileUtils.mkdir_p(File.join(HOST_ROOT, "app/jobs"))
  File.write(File.join(HOST_ROOT, "app/jobs/host_reload_job.rb"), <<~JOB)
    class HostReloadJob < ActiveJob::Base
      def perform
        HOST_EVENTS << ["version", 1, Rails.application.executor.active?]
      end
    end
  JOB
end

if HOST_SCENARIO == "cron_boot"
  FileUtils.mkdir_p(File.join(HOST_ROOT, "app/jobs"))
  File.write(File.join(HOST_ROOT, "app/jobs/scheduled_host_job.rb"), <<~JOB)
    class ScheduledHostJob < ActiveJob::Base
      def perform
        File.write(ENV.fetch("HOST_RESULT"), "cron")
      end
    end
  JOB
end

application = DecentJobHostApplication.initialize!
application.executor.to_run { HOST_EVENTS << ["executor_run"] }
application.executor.to_complete { HOST_EVENTS << ["executor_complete"] }
store = DecentJob::Store.new(DecentJob.database)
result = {events: HOST_EVENTS, mode: RailsHostJob.queue_adapter.execution_mode,
          logger: DecentJob.configuration.logger.equal?(Rails.logger),
          custom_logger: DecentJob.configuration.logger.equal?(application.config.decent_job[:logger]),
          active_record_loaded: defined?(ActiveRecord::Base) ? true : false}

case HOST_SCENARIO
when "logging", "logging_explicit"
  DecentJob.cleanup_preserved_jobs(older_than: 0)
  result[:host_messages] = HOST_LOG.string.scan("destroyed 0 preserved").size
  result[:explicit_messages] = HOST_EXPLICIT_LOG.string.scan("destroyed 0 preserved").size
  replacement = StringIO.new
  DecentJob.logger = Logger.new(replacement)
  DecentJob.cleanup_preserved_jobs(older_than: 0)
  result[:host_messages_after] = HOST_LOG.string.scan("destroyed 0 preserved").size
  result[:explicit_messages_after] = HOST_EXPLICIT_LOG.string.scan("destroyed 0 preserved").size
  result[:replacement_messages] = replacement.string.scan("destroyed 0 preserved").size
when "boot", "custom_logger", "cron_boot"
  result[:started] = DecentJob.started?
  result[:async] = RailsHostJob.queue_adapter.execute_async?
  if ENV["HOST_RESULT"]
    Timeout.timeout(8) { sleep 0.01 until File.exist?(ENV.fetch("HOST_RESULT")) }
    result[:performed] = File.read(ENV.fetch("HOST_RESULT"))
  end
when "transactions"
  job = nil
  ActiveRecord::Base.transaction do
    job = RailsHostJob.perform_later("commit")
    result[:commit_before_id] = job.provider_job_id
    result[:commit_before_count] = DecentJob.database.with_connection { |c| c.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0).to_i }
  end
  result[:commit_after] = store.find(job.provider_job_id).fetch("payload").fetch("arguments")
  rolled_back = nil
  ActiveRecord::Base.transaction do
    rolled_back = RailsHostJob.perform_later("rollback")
    raise ActiveRecord::Rollback
  end
  result[:rollback_id] = rolled_back.provider_job_id
  result[:count] = DecentJob.database.with_connection { |c| c.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0).to_i }
when "nested"
  outer = nil
  inner = nil
  discarded = nil
  ActiveRecord::Base.transaction do
    outer = RailsHostJob.perform_later("outer")
    ActiveRecord::Base.transaction(requires_new: true) { inner = RailsHostJob.perform_later("inner") }
    ActiveRecord::Base.transaction(requires_new: true) do
      discarded = RailsHostJob.perform_later("discarded")
      raise ActiveRecord::Rollback
    end
    result[:before_ids] = [outer, inner, discarded].map(&:provider_job_id)
  end
  result[:after_arguments] = [outer, inner].map { |job| store.find(job.provider_job_id).fetch("payload").fetch("arguments") }
  result[:discarded_id] = discarded.provider_job_id
when "bulk"
  deferred = RailsHostJob.new("deferred")
  immediate = RailsImmediateJob.new("immediate")
  ActiveRecord::Base.transaction do
    ActiveJob.perform_all_later(deferred, immediate)
    result[:deferred_before] = deferred.provider_job_id
    result[:immediate_before] = !!store.find(immediate.provider_job_id)
  end
  result[:deferred_after] = store.find(deferred.provider_job_id).fetch("payload").fetch("arguments")
  rolled_back = RailsHostJob.new("rollback")
  ActiveRecord::Base.transaction do
    ActiveJob.perform_all_later(rolled_back)
    raise ActiveRecord::Rollback
  end
  result[:rollback_id] = rolled_back.provider_job_id
when "scheduled"
  time = Time.now.utc + 3600
  job = nil
  ActiveRecord::Base.transaction do
    job = RailsHostJob.set(wait_until: time, queue: "scheduled", priority: 9).perform_later("later")
    result[:before_id] = job.provider_job_id
    job.scheduled_at = time + 600
    job.queue_name = "changed"
    job.priority = 100
  end
  row = store.find(job.provider_job_id)
  result[:schedule_delta] = row.fetch("scheduled_at").to_f - time.to_f
  result[:queue] = row.fetch("queue_name")
  result[:priority] = row.fetch("priority")
when "inline"
  ActiveRecord::Base.transaction do
    RailsHostJob.perform_later("after commit")
    result[:before_perform] = HOST_EVENTS.any? { |event| event.first == "perform" }
  end
  result[:after_perform] = HOST_EVENTS.select { |event| event.first == "perform" }
when "reload"
  HostReloadJob.perform_later
  DecentJob.perform_inline
  path = File.join(HOST_ROOT, "app/jobs/host_reload_job.rb")
  File.write(path, File.read(path).sub('["version", 1,', '["version", 2,'))
  HostReloadJob.perform_later
  DecentJob.perform_inline
  result[:versions] = HOST_EVENTS.select { |event| event.first == "version" }
when "runtime_isolation"
  pool = DecentJob.database
  callbacks = []
  backend_pids = []
  buffer = DecentJob::Bulk::Buffer.new
  first = Fiber.new do
    application.executor.wrap do
      pool.transaction do |connection|
        backend_pids << connection.backend_pid
        pool.after_commit { callbacks << "first_commit" }
        pool.after_rollback { callbacks << "first_rollback" }
        DecentJob::Bulk.with_buffer(buffer) do
          RailsHostJob.perform_later("captured")
          Fiber.yield
        end
        raise "rollback first fiber"
      end
    end
  rescue RuntimeError => error
    raise unless error.message == "rollback first fiber"
  end
  first.resume
  independent = Fiber.new do
    application.executor.wrap do
      pool.transaction do |connection|
        backend_pids << connection.backend_pid
        pool.after_commit { callbacks << "second_commit" }
        RailsHostJob.perform_later("independent")
      end
    end
  end.resume
  first.resume
  observed = Queue.new
  tracker = DecentJob::ProcessTracker.new(database: pool, state: -> {
    observed << application.executor.active?
    {}
  })
  application.executor.wrap do
    tracker.register { result[:monitor_executor] = Timeout.timeout(3) { observed.pop } }
  end
  result[:isolation] = ActiveSupport::IsolatedExecutionState.isolation_level
  result[:backend_pids] = backend_pids
  result[:captured_arguments] = buffer.active_jobs.map(&:arguments)
  result[:committed_arguments] = store.find(independent.provider_job_id).fetch("payload").fetch("arguments")
  result[:callbacks] = callbacks
  result[:job_count] = pool.with_connection { |connection| connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0).to_i }
  result[:bulk_context] = DecentJob::Bulk.current_buffer
  result[:pool_active] = pool.stats.fetch(:active)
  result[:process_count] = DecentJob::ProcessRecord.all(database: pool).size
else
  raise "Unknown scenario: #{HOST_SCENARIO}"
end

result[:stopped] = DecentJob.shutdown(timeout: 5)
puts "HOST_RESULT:#{JSON.generate(result)}"
