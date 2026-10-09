# frozen_string_literal: true

require "rbconfig"
require "spec_helper"
require "timeout"

class CronIntegrationJob < ActiveJob::Base
  class_attribute :events, default: Queue.new

  def perform(value = "root")
    self.class.events << value
    self.class.perform_later("child") if value == "spawn"
  end
end

class CronIntegrationRetryJob < ActiveJob::Base
  retry_on RuntimeError, wait: 0, attempts: 2

  def perform
    raise "retry cron job" if executions == 1
  end
end

class CronIntegrationLimitedJob < CronIntegrationJob
  include DecentJob::ActiveJobExtensions::Concurrency
end

describe "recurring enqueue integration" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)
    [CronIntegrationJob, CronIntegrationRetryJob, CronIntegrationLimitedJob].each { |job_class| job_class.queue_adapter = @adapter }
    CronIntegrationJob.events = Queue.new
    CronIntegrationLimitedJob.decent_job_concurrency_config = {}
    CronIntegrationLimitedJob.decent_job_concurrency_rules = []
    @at = Time.utc(2026, 10, 9, 12, 30, 0, 123456)
  end

  after { DecentJob.reset! }

  def cron_entry(**options)
    DecentJob::CronEntry.new({key: :integration, cron: "every minute", class: CronIntegrationJob}.merge(options), database: database)
  end

  def rows
    database.with_connection do |connection|
      connection.exec("SELECT * FROM decent_jobs ORDER BY created_at, id").map { |row| DecentJob::Store.new(database).decode(row) }
    end
  end

  it "deduplicates the same occurrence from separate Ruby processes" do
    cron_entry.disable
    script = <<~RUBY
      require "decent_job/active_job"
      class CronIntegrationJob < ActiveJob::Base
        def perform(*)
        end
      end
      ActiveJob::Base.logger = Logger.new(File::NULL)
      database = DecentJob::Database.new(url: JSON.parse(ENV.fetch("CRON_DATABASE")), pool_size: 1)
      CronIntegrationJob.queue_adapter = DecentJob::Adapter.new(database: database)
      entry = DecentJob::CronEntry.new({key: :integration, cron: "every minute", class: CronIntegrationJob}, database: database)
      gate = IO.for_fd(3)
      result = IO.for_fd(4)
      result.sync = true
      result.puts "ready"
      gate.read(1)
      job = entry.enqueue(Time.iso8601(ENV.fetch("CRON_AT")))
      result.puts JSON.generate(accepted: !!job, provider_id: job && job.provider_job_id, enabled: entry.enabled?)
      database.close
    RUBY
    processes = []
    reaped = []
    2.times do
      gate_read, gate_write = IO.pipe
      result_read, result_write = IO.pipe
      pid = Process.spawn({"CRON_DATABASE" => JSON.generate(@connection_options), "CRON_AT" => @at.iso8601(6)},
        RbConfig.ruby, "-I", File.expand_path("../lib", __dir__), "-e", script,
        3 => gate_read, 4 => result_write)
      gate_read.close
      result_write.close
      processes << [pid, gate_write, result_read]
    end
    Timeout.timeout(10) do
      processes.each { |_, _, reader| _(reader.gets&.strip).must_equal "ready" }
      processes.each { |_, writer, _| writer.write("x") }
      results = processes.map { |_, _, reader| JSON.parse(reader.gets) }
      processes.each do |pid, _, _|
        status = Process.wait2(pid).last
        reaped << pid
        _(status.success?).must_equal true
      end
      _(results.count { |result| result.fetch("accepted") }).must_equal 1
      _(results.map { |result| result.fetch("enabled") }).must_equal [false, false]
      _(results.filter_map { |result| result.fetch("provider_id") }).must_equal rows.map { |row| row.fetch("id") }
    end
    _(rows.size).must_equal 1
    _(rows.first.fetch("cron_at")).must_equal @at
  ensure
    Array(processes).each do |pid, writer, reader|
      writer.close unless writer.closed?
      reader.close unless reader.closed?
      next if reaped.include?(pid)

      begin
        Process.kill("KILL", pid)
        Process.wait(pid)
      rescue Errno::ESRCH, Errno::ECHILD
        nil
      end
    end
  end

  it "rolls back occurrence ownership with the enclosing transaction" do
    entry = cron_entry
    rolled_back = nil
    _ {
      database.transaction do
        rolled_back = entry.enqueue(@at)
        raise "rollback"
      end
    }.must_raise RuntimeError
    _(rows).must_be_empty
    _(rolled_back.provider_job_id).must_be_nil
    _(rolled_back.successfully_enqueued?).must_equal false
    accepted = entry.enqueue(@at)
    _(accepted.provider_job_id).must_equal rows.first.fetch("id")
    _(rows.first.fetch("cron_at")).must_equal @at
  end

  it "keeps the surrounding transaction usable after a duplicate" do
    entry = cron_entry
    entry.enqueue(@at)
    ordinary = nil
    database.transaction do
      _(entry.enqueue(@at)).must_equal false
      ordinary = CronIntegrationJob.perform_later("ordinary")
    end
    _(rows.size).must_equal 2
    _(DecentJob::Store.new(database).find(ordinary.provider_job_id).fetch("cron_key")).must_be_nil
  end

  it "clears cron context before inline execution enqueues children" do
    CronIntegrationJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :inline)
    cron_entry(args: ["spawn"]).enqueue(@at)
    _(rows.size).must_equal 2
    _(rows.map { |row| row.fetch("state") }).must_equal ["succeeded", "succeeded"]
    _(rows.map { |row| row.fetch("cron_key") }).must_equal ["integration", nil]
    _(rows.map { |row| row.fetch("cron_at") }).must_equal [@at, nil]
    _(2.times.map { CronIntegrationJob.events.pop }).must_equal ["spawn", "child"]
  end

  it "executes cron and child jobs in async mode without sharing cron metadata" do
    adapter = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    CronIntegrationJob.queue_adapter = adapter
    cron_entry(args: ["spawn"]).enqueue(@at)
    values = Timeout.timeout(10) { 2.times.map { CronIntegrationJob.events.pop } }
    _(values).must_equal ["spawn", "child"]
    _(adapter.shutdown(timeout: 10)).must_equal true
    _(rows.map { |row| row.fetch("cron_key") }).must_equal ["integration", nil]
    _(rows.map { |row| row.fetch("state") }).must_equal ["succeeded", "succeeded"]
  ensure
    adapter&.shutdown(timeout: 10)
  end

  it "retains occurrence identity through Active Job retries" do
    entry = cron_entry(class: CronIntegrationRetryJob)
    original = entry.enqueue(@at)
    performer = DecentJob::Performer.new(database: database)
    _(performer.perform_one.state).must_equal "retried"
    _(performer.perform_one.state).must_equal "succeeded"
    _(rows.size).must_equal 1
    _(rows.first.fetch("id")).must_equal original.provider_job_id
    _(rows.first.fetch("cron_at")).must_equal @at
    _(rows.first.fetch("cron_key")).must_equal "integration"
    _(rows.first.fetch("state")).must_equal "succeeded"
    _(entry.enqueue(@at)).must_equal false
    _(DecentJob::Store.new(database).executions(original.provider_job_id).map { |row| row.fetch("state") }).must_equal ["retried", "succeeded"]
  end

  it "can retry an occurrence after concurrency admission rejects it" do
    CronIntegrationLimitedJob.decent_job_control_concurrency_with(key: "cron-limit", total_limit: 0)
    entry = cron_entry(class: CronIntegrationLimitedJob)
    _(entry.enqueue(@at)).must_equal false
    _(rows).must_be_empty
    CronIntegrationLimitedJob.decent_job_control_concurrency_with(key: "cron-limit", total_limit: 1)
    _(entry.enqueue(@at).provider_job_id).must_equal rows.first.fetch("id")
    _(rows.first.fetch("cron_at")).must_equal @at
  end

  it "keeps an entry's identity when invoked inside a bulk capture" do
    accepted = nil
    captured = DecentJob::Bulk.enqueue do
      CronIntegrationJob.perform_later("ordinary")
      accepted = cron_entry.enqueue(@at)
    end
    _(accepted.provider_job_id).wont_be_nil
    _(captured.size).must_equal 1
    _(rows.size).must_equal 2
    _(rows.map { |row| row.fetch("cron_key") }).must_equal ["integration", nil]
  end

  it "orders concurrency locks consistently for callable bulk enqueues" do
    CronIntegrationLimitedJob.decent_job_control_concurrency_with(key: -> { arguments.first }, total_limit: 10)
    arrived = Queue.new
    release = Queue.new
    entries = [["a", "b"], ["b", "a"]].each_with_index.map do |keys, index|
      cron_entry(key: :"integration_#{index}", class: -> {
        ActiveJob.perform_all_later(*keys.map { |key| CronIntegrationLimitedJob.new(key) })
        arrived << true
        release.pop
      })
    end
    threads = entries.map { |entry| Thread.new { entry.enqueue } }
    Timeout.timeout(10) do
      2.times { arrived.pop }
      2.times { release << true }
      threads.each(&:value)
    end
    _(rows.size).must_equal 4
    _(rows.map { |row| row.fetch("cron_key") }.tally).must_equal({"integration_0" => 2, "integration_1" => 2})
  ensure
    2.times { release << true } if release
    Array(threads).each do |thread|
      thread.kill if thread.alive?
      thread.join
    end
  end
end
