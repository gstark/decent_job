# frozen_string_literal: true

require "rbconfig"
require "spec_helper"
require "support/jobs"
require "timeout"

class CrashRecoveryJob < RecordingJob
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
end
