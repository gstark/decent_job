# frozen_string_literal: true

require "minitest/mock"
require "spec_helper"
require "support/jobs"

class MetricsRetryJob < ActiveJob::Base
  def perform(data)
    if executions == 1
      data["value"].replace("changed")
      retry_job(wait: 300, queue: "later", priority: 17)
    end
  end
end

class MetricsNotImplementedJob < ActiveJob::Base
  def perform
    raise NotImplementedError, "not implemented"
  end
end

class MetricsHandledErrorJob < FailingJob
  rescue_from(RuntimeError) { |error| error }
end

class MetricsExhaustedJob < FailingJob
  retry_on RuntimeError, attempts: 1
end

class MetricsCallbackJob < ActiveJob::Base
  class_attribute :tick, default: ->(_seconds) {}
  before_perform { tick.call(2) }
  after_perform { tick.call(5) }

  def perform
    tick.call(3)
  end
end

describe "execution metrics persistence" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)
    [RecordingJob, RetryingJob, DiscardingJob, FailingJob, MetricsRetryJob, MetricsNotImplementedJob, MetricsCallbackJob, MetricsHandledErrorJob, MetricsExhaustedJob].each { |klass| klass.queue_adapter = @adapter }
    RecordingJob.events = Queue.new
    @store = DecentJob::Store.new(database)
    @performer = DecentJob::Performer.new(database: database)
  end

  after do
    MetricsCallbackJob.tick = ->(_seconds) {}
    DecentJob.reset!
  end

  it "captures the original payload and routing metadata before retry mutates the job" do
    job = MetricsRetryJob.perform_later({"value" => "original"})
    original = @store.find(job.provider_job_id)
    _(@performer.perform_one.state).must_equal "retried"
    first = @store.executions(job.provider_job_id).first
    _(first.fetch("payload")).must_equal original.fetch("payload")
    _(first.fetch("scheduled_at")).must_equal original.fetch("scheduled_at")
    _(first.fetch("queue_name")).must_equal "default"
    _(first.fetch("job_class")).must_equal "MetricsRetryJob"
    _(first.fetch("duration")).must_be_kind_of Float
    updated = @store.find(job.provider_job_id)
    _(updated.fetch("performed_at")).must_be_nil
    _(updated.fetch("queue_name")).must_equal "later"
    _(updated.fetch("payload").fetch("arguments").first.fetch("value")).must_equal "changed"
    DecentJob::Job.find(job.provider_job_id, database: database).reschedule_job
    second_payload = @store.find(job.provider_job_id).fetch("payload")
    _(@performer.perform_one.state).must_equal "succeeded"
    second = @store.executions(job.provider_job_id).last
    _(second.fetch("payload")).must_equal second_payload
    _(second.fetch("queue_name")).must_equal "later"
    _(second.fetch("started_at")).must_equal @store.find(job.provider_job_id).fetch("performed_at")
    _(@store.executions(job.provider_job_id).first.fetch("payload")).must_equal original.fetch("payload")
  end

  it "persists snapshots and the exact shared start time while an attempt is running" do
    job = RecordingJob.perform_later("snapshot")
    original = @store.find(job.provider_job_id)

    DecentJob::Claim.new(database).with_next do |row|
      execution = @store.executions(job.provider_job_id).first
      _(execution.fetch("payload")).must_equal original.fetch("payload")
      _(execution.fetch("started_at")).must_equal row.fetch("performed_at")
      _(execution.fetch("duration")).must_be_nil
      _(execution.fetch("finished_at")).must_be_nil
      row.fetch("payload").fetch("arguments").first.replace("mutated")
      _(@store.executions(job.provider_job_id).first.fetch("payload")).must_equal original.fetch("payload")
    end
  end

  [RecordingJob, DiscardingJob, FailingJob, MetricsNotImplementedJob, RetryingJob, MetricsHandledErrorJob, MetricsExhaustedJob].each do |klass|
    it "records a finite nonnegative duration for #{klass}" do
      arguments = [RecordingJob, RetryingJob].include?(klass) ? ["ok"] : []
      job = klass.perform_later(*arguments)
      @performer.perform_one
      execution = @store.executions(job.provider_job_id).first
      _(execution.fetch("duration")).must_be_kind_of Float
      _(execution.fetch("duration")).must_be :>=, 0
      _(execution.fetch("duration").finite?).must_equal true
      _(execution.fetch("finished_at")).wont_be_nil
    end
  end

  it "measures callbacks and executor monotonically across a backwards wall clock jump and excludes retention work" do
    original_rails = Object.const_get(:Rails) if Object.const_defined?(:Rails)
    Object.send(:remove_const, :Rails) if original_rails
    executor = Object.new

    executor.define_singleton_method(:wrap) do |&block|
      MetricsCallbackJob.tick.call(7)
      block.call
    ensure
      MetricsCallbackJob.tick.call(11)
    end

    host = Module.new
    application = Struct.new(:executor, :reloader).new(Class.new(ActiveSupport::Executor), executor)
    host.define_singleton_method(:application) { application }
    Object.const_set(:Rails, host)
    job = MetricsCallbackJob.perform_later
    monotonic = 10.0
    wall = Time.now

    MetricsCallbackJob.tick = ->(seconds) {
      monotonic += seconds
      wall -= 3600
    }

    DecentJob.configuration.preserve_job_records = ->(*) {
      monotonic += 100
      true
    }

    Process.stub(:clock_gettime, ->(*) { monotonic }) do
      Time.stub(:now, -> { wall }) { @performer.perform_one }
    end

    _(@store.executions(job.provider_job_id).first.fetch("duration")).must_equal 28.0
  ensure
    Object.send(:remove_const, :Rails) if Object.const_defined?(:Rails)
    Object.const_set(:Rails, original_rails) if original_rails
  end

  it "clears the current start on backend retry and manual retry" do
    job = FailingJob.perform_later
    DecentJob.configuration.retry_on_unhandled_error = true
    @performer.perform_one
    _(@store.find(job.provider_job_id).fetch("performed_at")).must_be_nil
    @performer.perform_one
    executions = DecentJob::Execution.for_job(job.provider_job_id, database: database)
    _(executions.map(&:number)).must_equal [1, 1]
    _(executions.map(&:payload).uniq.size).must_equal 1
    _(executions.map(&:duration)).must_be :all?, Float
    DecentJob.configuration.retry_on_unhandled_error = false
    @performer.perform_one
    _(@store.find(job.provider_job_id).fetch("performed_at")).wont_be_nil
    DecentJob::Job.find(job.provider_job_id, database: database).retry_job
    _(@store.find(job.provider_job_id).fetch("performed_at")).must_be_nil
  end

  it "leaves an excluded error unfinished and never invents duration during recovery" do
    job = FailingJob.perform_later
    DecentJob.configuration.handled_exceptions = []
    _ { @performer.perform_one }.must_raise RuntimeError
    first = @store.executions(job.provider_job_id).first
    _(first.fetch("duration")).must_be_nil
    _(first.fetch("finished_at")).must_be_nil
    DecentJob.configuration.handled_exceptions = [StandardError]
    @performer.perform_one
    first, second = @store.executions(job.provider_job_id)
    _(first.fetch("state")).must_equal "interrupted"
    _(first.fetch("duration")).must_be_nil
    _(second.fetch("duration")).must_be_kind_of Float
  end

  it "rejects negative and nonfinite duration values" do
    job = RecordingJob.perform_later("done")
    @performer.perform_one

    database.with_connection do |connection|
      %w[-1 NaN Infinity -Infinity].each do |invalid|
        _ { connection.exec_params("UPDATE decent_job_executions SET duration = $1 WHERE job_id = $2", [invalid, job.provider_job_id]) }.must_raise PG::CheckViolation
      end
    end
  end
end
