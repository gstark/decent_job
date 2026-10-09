# frozen_string_literal: true

require "rbconfig"
require "spec_helper"
require "timeout"

class PreservationRecoveryMemberJob < ActiveJob::Base
  def perform(fail_execution)
    raise "member failed" if fail_execution
  end
end

class PreservationRecoveryCallbackJob < ActiveJob::Base
  class_attribute :events, default: Queue.new

  def perform(batch, options)
    self.class.events << [batch.id, options.fetch(:event)]
  end
end

describe "batch recovery after record deletion" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    adapter = DecentJob::Adapter.new(database: database)
    [PreservationRecoveryMemberJob, PreservationRecoveryCallbackJob].each { |klass| klass.queue_adapter = adapter }
    PreservationRecoveryCallbackJob.events = Queue.new
    @previous_policy = DecentJob.configuration.preserve_job_records
    DecentJob.configuration.preserve_job_records = false
  end

  after do
    DecentJob.configuration.preserve_job_records = @previous_policy
  end

  [false, true].each do |fail_execution|
    it "recovers #{fail_execution ? "discard" : "success"} callbacks after SIGKILL between deletion and reconciliation" do
      job = PreservationRecoveryMemberJob.new(fail_execution)
      batch = DecentJob::Batch.enqueue([job], database: database,
        on_success: PreservationRecoveryCallbackJob, on_discard: PreservationRecoveryCallbackJob,
        on_finish: PreservationRecoveryCallbackJob)
      reader, writer = IO.pipe
      code = <<~RUBY
        require "decent_job"
        ActiveJob::Base.logger = Logger.new(File::NULL)
        DecentJob.logger = Logger.new(File::NULL)
        DecentJob.configure do |config|
          config.database_url = JSON.parse(ENV.fetch("DECENT_JOB_TEST_CONNECTION"))
          config.preserve_job_records = false
        end
        class PreservationRecoveryMemberJob < ActiveJob::Base
          def perform(fail_execution)
            raise "member failed" if fail_execution
          end
        end
        module StopBeforePreservationReconciliation
          def reconcile(id)
            STDOUT.puts "completion committed"
            STDOUT.flush
            sleep 60
          end
        end
        DecentJob::BatchStore.prepend(StopBeforePreservationReconciliation)
        DecentJob::Performer.new.perform_one(job_id: ENV.fetch("JOB_ID"))
      RUBY
      pid = Process.spawn({"DECENT_JOB_TEST_CONNECTION" => JSON.generate(@connection_options), "JOB_ID" => job.provider_job_id},
        RbConfig.ruby, "-Ilib", "-e", code, out: writer, err: writer)
      writer.close
      _(Timeout.timeout(10) { reader.gets }&.strip).must_equal "completion committed"
      store = DecentJob::Store.new(database)
      _(store.find(job.provider_job_id)).must_be_nil
      _(store.executions(job.provider_job_id)).must_be_empty
      _(batch.reload.jobs_finished?).must_equal false
      Process.kill("KILL", pid)
      Process.wait(pid)
      pid = nil

      performer = DecentJob::Performer.new(database: database)
      _(performer.perform_one(job_id: SecureRandom.uuid)).must_be_nil
      _(batch.reload.jobs_finished?).must_equal true
      callbacks = batch.callback_active_jobs
      _(callbacks.size).must_equal 2
      2.times { _(performer.perform_one.state).must_equal "succeeded" }
      2.times { _(performer.perform_one).must_be_nil }
      _(batch.reload.finished?).must_equal true
      _(batch.discarded?).must_equal fail_execution
      _(batch.succeeded?).must_equal !fail_execution
      _(batch.callback_active_jobs).must_be_empty
      callbacks.each do |callback|
        _(store.find(callback.provider_job_id)).must_be_nil
        _(store.executions(callback.provider_job_id)).must_be_empty
      end
      expected = [:finish, fail_execution ? :discard : :success].sort
      actual = 2.times.map { PreservationRecoveryCallbackJob.events.pop }
      _(actual.map(&:first)).must_equal [batch.id, batch.id]
      _(actual.map(&:last).sort).must_equal expected
      _(PreservationRecoveryCallbackJob.events.size).must_equal 0
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
end
