# frozen_string_literal: true

require "rbconfig"
require "spec_helper"
require "support/jobs"
require "timeout"

class CrashRecoveryJob < RecordingJob
end

class ControlledCrashRecoveryJob < RecordingJob
  include DecentJob::ActiveJobExtensions::Concurrency

  decent_job_control_concurrency_with(perform_limit: 1)
end

describe "process failure recovery" do
  include DatabaseSpec

  it "retries a job after SIGKILL releases its worker session" do
    DecentJob::Schema.new(database).migrate
    CrashRecoveryJob.queue_adapter = DecentJob::Adapter.new(database: database)
    CrashRecoveryJob.events = Queue.new
    job = CrashRecoveryJob.perform_later("recovered")
    reader, writer = IO.pipe
    code = <<~RUBY
      require "decent_job"
      ActiveJob::Base.logger = Logger.new(File::NULL)
      DecentJob.configure { |config| config.database_url = JSON.parse(ENV.fetch("DECENT_JOB_TEST_CONNECTION")) }
      class CrashRecoveryJob < ActiveJob::Base
        def perform(*)
          STDOUT.puts "started"
          STDOUT.flush
          sleep 60
        end
      end
      DecentJob::Performer.new.perform_one
    RUBY
    pid = Process.spawn({"DECENT_JOB_TEST_CONNECTION" => JSON.generate(@connection_options)},
      RbConfig.ruby, "-Ilib", "-e", code, out: writer, err: writer)
    writer.close
    _(Timeout.timeout(10) { reader.gets }&.strip).must_equal "started"
    Process.kill("KILL", pid)
    Process.wait(pid)
    pid = nil
    performer = DecentJob::Performer.new(database: database)
    _(performer.perform_one.state).must_equal "succeeded"
    _(CrashRecoveryJob.events.pop).must_equal "recovered"
    states = DecentJob::Store.new(database).executions(job.provider_job_id).map { |row| row.fetch("state") }
    _(states).must_equal ["interrupted", "succeeded"]
  ensure
    if pid
      begin
        Process.kill("KILL", pid)
        Process.wait(pid)
      rescue Errno::ESRCH, Errno::ECHILD
        nil
      end
    end
    reader&.close
    writer&.close unless writer&.closed?
  end

  it "releases a killed process's grant and promotes a waiter during an idle poll" do
    DecentJob::Schema.new(database).migrate
    ControlledCrashRecoveryJob.queue_adapter = DecentJob::Adapter.new(database: database)
    ControlledCrashRecoveryJob.events = Queue.new
    first = ControlledCrashRecoveryJob.perform_later("killed")
    second = ControlledCrashRecoveryJob.perform_later("promoted")
    reader, writer = IO.pipe
    code = <<~RUBY
      require "decent_job"
      ActiveJob::Base.logger = Logger.new(File::NULL)
      DecentJob.configure { |config| config.database_url = JSON.parse(ENV.fetch("DECENT_JOB_TEST_CONNECTION")) }
      class ControlledCrashRecoveryJob < ActiveJob::Base
        include DecentJob::ActiveJobExtensions::Concurrency
        self.queue_adapter = :decent_job
        decent_job_control_concurrency_with(perform_limit: 1)
        def perform(*)
          STDOUT.puts "started"
          STDOUT.flush
          sleep 60
        end
      end
      DecentJob::Performer.new.perform_one(job_id: ENV.fetch("JOB_ID"))
    RUBY
    pid = Process.spawn({"DECENT_JOB_TEST_CONNECTION" => JSON.generate(@connection_options), "JOB_ID" => first.provider_job_id},
      RbConfig.ruby, "-Ilib", "-e", code, out: writer, err: writer)
    writer.close
    _(Timeout.timeout(10) { reader.gets }&.strip).must_equal "started"
    performer = DecentJob::Performer.new(database: database)
    _(performer.perform_one(job_id: second.provider_job_id).state).must_equal "retried"
    Process.kill("KILL", pid)
    Process.wait(pid)
    pid = nil
    _(performer.perform_one(job_id: SecureRandom.uuid)).must_be_nil
    _(DecentJob::Store.new(database).find(second.provider_job_id).fetch("scheduled_at")).must_be :<=, Time.now
    _(performer.perform_one(job_id: second.provider_job_id).state).must_equal "succeeded"
    _(ControlledCrashRecoveryJob.events.pop).must_equal "promoted"
  ensure
    if pid
      begin
        Process.kill("KILL", pid)
        Process.wait(pid)
      rescue Errno::ESRCH, Errno::ECHILD
        nil
      end
    end
    reader&.close
    writer&.close unless writer&.closed?
  end
end
