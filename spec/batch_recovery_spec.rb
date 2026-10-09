# frozen_string_literal: true

require "rbconfig"
require "spec_helper"
require "support/jobs"
require "timeout"

class BatchRecoveryMemberJob < RecordingJob
end

class BatchRecoveryCallbackJob < ActiveJob::Base
  class_attribute :events, default: Queue.new

  def perform(batch, options)
    self.class.events << [batch.id, options.fetch(:event)]
  end
end

describe "batch completion recovery" do
  include DatabaseSpec

  it "recovers callback delivery after SIGKILL between job commit and reconciliation" do
    DecentJob::Schema.new(database).migrate
    adapter = DecentJob::Adapter.new(database: database)
    [BatchRecoveryMemberJob, BatchRecoveryCallbackJob].each { |klass| klass.queue_adapter = adapter }
    BatchRecoveryCallbackJob.events = Queue.new
    job = BatchRecoveryMemberJob.new("member")
    batch = DecentJob::Batch.enqueue([job], database: database, on_finish: BatchRecoveryCallbackJob)
    reader, writer = IO.pipe
    code = <<~RUBY
      require "decent_job/active_job"
      ActiveJob::Base.logger = Logger.new(File::NULL)
      DecentJob.logger = Logger.new(File::NULL)
      DecentJob.configure { |config| config.database_url = JSON.parse(ENV.fetch("DECENT_JOB_TEST_CONNECTION")) }
      class BatchRecoveryMemberJob < ActiveJob::Base
        def perform(*)
        end
      end
      module StopBeforeBatchReconciliation
        def reconcile(id)
          STDOUT.puts "job committed"
          STDOUT.flush
          sleep 60
        end
      end
      DecentJob::BatchStore.prepend(StopBeforeBatchReconciliation)
      DecentJob::Performer.new.perform_one(job_id: ENV.fetch("JOB_ID"))
    RUBY
    pid = Process.spawn({"DECENT_JOB_TEST_CONNECTION" => JSON.generate(@connection_options), "JOB_ID" => job.provider_job_id},
      RbConfig.ruby, "-Ilib", "-e", code, out: writer, err: writer)
    writer.close
    _(Timeout.timeout(10) { reader.gets }&.strip).must_equal "job committed"
    _(DecentJob::Store.new(database).find(job.provider_job_id).fetch("state")).must_equal "succeeded"
    _(batch.reload.jobs_finished?).must_equal false
    Process.kill("KILL", pid)
    Process.wait(pid)
    pid = nil

    performer = DecentJob::Performer.new(database: database)
    _(performer.perform_one(job_id: SecureRandom.uuid)).must_be_nil
    _(batch.reload.jobs_finished?).must_equal true
    _(batch.callback_job_instances.size).must_equal 1
    _(performer.perform_one.state).must_equal "succeeded"
    2.times { _(performer.perform_one).must_be_nil }
    _(batch.reload.succeeded?).must_equal true
    _(BatchRecoveryCallbackJob.events.pop).must_equal [batch.id, :finish]
    _(BatchRecoveryCallbackJob.events.size).must_equal 0
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
