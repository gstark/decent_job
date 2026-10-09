# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

class ControlledJob < RecordingJob
  include DecentJob::ActiveJobExtensions::Concurrency
end

class ControlledBlockingJob < BlockingJob
  include DecentJob::ActiveJobExtensions::Concurrency
end

class CallbackLabeledJob < ControlledJob
  before_enqueue { self.decent_job_labels = ["account"] }
end

class ControlledRetryingJob < ControlledJob
  retry_on RuntimeError, wait: 0, attempts: 2

  def perform(*)
    self.decent_job_labels = ["after-retry"]
    raise "retry" if executions == 1
  end
end

describe "concurrency controls" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)
    [ControlledJob, ControlledBlockingJob, CallbackLabeledJob, ControlledRetryingJob].each do |job|
      job.queue_adapter = @adapter
      job.decent_job_concurrency_config = {}
      job.decent_job_concurrency_rules = []
      job.decent_job_labels = []
    end
    ControlledJob.events = Queue.new
    ControlledBlockingJob.started = Queue.new
    ControlledBlockingJob.release = Queue.new
    @store = DecentJob::Store.new(database)
    @performer = DecentJob::Performer.new(database: database)
  end

  it "rejects jobs beyond a total limit" do
    ControlledJob.decent_job_control_concurrency_with(total_limit: 1)
    first = ControlledJob.perform_later("one")
    _(ControlledJob.perform_later("two")).must_equal false
    _(@store.find(first.provider_job_id).fetch("concurrency_keys")).must_equal ["ControlledJob"]
    @performer.perform_one
    _(ControlledJob.perform_later("after completion")).must_be_kind_of ControlledJob
  end

  it "runs after_enqueue only for jobs accepted by concurrency admission" do
    events = []
    klass = Class.new(ControlledJob)
    Object.const_set(:AfterEnqueueControlledJob, klass)
    klass.decent_job_control_concurrency_with(total_limit: 1)
    klass.after_enqueue { events << arguments.first }
    accepted = klass.perform_later("accepted")
    rejected = klass.new("rejected")

    _(accepted).must_be_kind_of klass
    _(rejected.enqueue).must_equal false
    _(rejected.successfully_enqueued?).must_equal false
    _(rejected.enqueue_error).must_be_kind_of ActiveJob::EnqueueError
    _(events).must_equal ["accepted"]
  ensure
    Object.send(:remove_const, :AfterEnqueueControlledJob) if Object.const_defined?(:AfterEnqueueControlledJob)
  end

  it "serializes simultaneous enqueuers across connections" do
    ControlledJob.decent_job_control_concurrency_with(total_limit: 1)
    release = Queue.new
    threads = 8.times.map do |index|
      Thread.new do
        release.pop
        ControlledJob.perform_later(index)
      end
    end
    8.times { release << true }
    _(threads.map(&:value).count { |job| job }).must_equal 1
  end

  it "excludes performing jobs from enqueue-only limits" do
    ControlledBlockingJob.decent_job_control_concurrency_with(enqueue_limit: 1)
    ControlledBlockingJob.perform_later
    worker = Thread.new { @performer.perform_one }
    Timeout.timeout(5) { ControlledBlockingJob.started.pop }
    _(ControlledBlockingJob.perform_later).must_be_kind_of ControlledBlockingJob
    _(ControlledBlockingJob.perform_later).must_equal false
  ensure
    ControlledBlockingJob.release << true
    worker&.value
  end

  it "enforces label rules across classes after enqueue callbacks" do
    CallbackLabeledJob.decent_job_concurrency_rule(label: "account", total_limit: 1)
    ControlledJob.decent_job_concurrency_rule(label: "account", apply_label: true, total_limit: 1)
    first = CallbackLabeledJob.perform_later("first")
    _(first).must_be_kind_of CallbackLabeledJob
    _(ControlledJob.perform_later("second")).must_equal false
    _(@store.find(first.provider_job_id).fetch("labels")).must_equal ["account"]
  end

  it "skips label rules for jobs without that label" do
    ControlledJob.decent_job_concurrency_rule(label: "account", total_limit: 0)
    _(ControlledJob.perform_later("unlabeled")).must_be_kind_of ControlledJob
    _(ControlledJob.set(decent_job_labels: ["account"]).perform_later("labeled")).must_equal false
  end

  it "checks each controlled job in bulk and preserves individual outcomes" do
    ControlledJob.decent_job_control_concurrency_with(enqueue_limit: 1)
    jobs = [ControlledJob.new("first"), ControlledJob.new("second")]
    _(@adapter.enqueue_all(jobs)).must_equal 1
    _(jobs.first.successfully_enqueued?).must_equal true
    _(jobs.last.successfully_enqueued?).must_equal false
    _(jobs.last.provider_job_id).must_be_nil
  end

  it "throttles enqueues across a rolling time window" do
    ControlledJob.decent_job_control_concurrency_with(enqueue_throttle: [1, 60])
    job = ControlledJob.perform_later("first")
    @performer.perform_one
    _(ControlledJob.perform_later("too soon")).must_equal false
    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET created_at = clock_timestamp() - interval '61 seconds' WHERE id = $1", [job.provider_job_id])
    end
    _(ControlledJob.perform_later("next window")).must_be_kind_of ControlledJob
  end

  it "disables legacy rules with an explicitly nil key" do
    ControlledJob.decent_job_control_concurrency_with(key: nil, total_limit: 0)
    _(ControlledJob.perform_later("allowed")).must_be_kind_of ControlledJob
  end

  it "disables legacy rules with false and blank keys" do
    [false, "", "  ", -> { false }, -> { " " }].each do |key|
      ControlledJob.decent_job_control_concurrency_with(key: key, total_limit: 0)
      _(ControlledJob.perform_later("allowed")).must_be_kind_of ControlledJob
    end
  end

  it "supports dynamic keys and limits" do
    ControlledJob.decent_job_control_concurrency_with(key: -> { arguments.first }, total_limit: -> { 1 })
    _(ControlledJob.perform_later("A")).must_be_kind_of ControlledJob
    _(ControlledJob.perform_later("B")).must_be_kind_of ControlledJob
    _(ControlledJob.perform_later("A")).must_equal false
  end

  it "does not enforce DecentJob controls with another adapter" do
    ControlledJob.decent_job_control_concurrency_with(total_limit: 0)
    ControlledJob.queue_adapter = :test
    _(ControlledJob.perform_later("test adapter")).must_be_kind_of ControlledJob
  end

  it "retains the serialized legacy key when its default changes" do
    ControlledJob.decent_job_control_concurrency_with(key: -> { arguments.first }, total_limit: 1)
    payload = ControlledJob.new("original").serialize
    ControlledJob.decent_job_control_concurrency_with(key: "new", total_limit: 1)
    restored = ActiveJob::Base.deserialize(payload)

    _(payload.fetch("decent_job_concurrency_key")).must_equal "original"
    _(restored.decent_job_concurrency_key).must_equal "original"
    _(restored.decent_job_resolved_concurrency_rules.first.value).must_equal "original"
  end

  it "persists an instance concurrency key override across retries" do
    ControlledRetryingJob.decent_job_control_concurrency_with(key: "class-key", total_limit: 1)
    job = ControlledRetryingJob.new
    job.decent_job_concurrency_key = "instance-key"
    job.enqueue

    _(job.serialize.fetch("decent_job_concurrency_key")).must_equal "instance-key"
    _(@store.find(job.provider_job_id).fetch("concurrency_keys")).must_equal ["instance-key"]
    _(@performer.perform_one.state).must_equal "retried"
    row = @store.find(job.provider_job_id)
    _(row.fetch("concurrency_keys")).must_equal ["instance-key"]
    _(row.fetch("payload").fetch("decent_job_concurrency_key")).must_equal "instance-key"
  end

  it "validates an instance concurrency key override when resolving rules" do
    ControlledJob.decent_job_control_concurrency_with(total_limit: 1)
    job = ControlledJob.new("invalid")
    job.decent_job_concurrency_key = []

    _ { job.serialize }.must_raise TypeError
  end

  it "updates labels when retrying a controlled job without rejecting itself" do
    ControlledRetryingJob.decent_job_control_concurrency_with(total_limit: 1)
    job = ControlledRetryingJob.set(decent_job_labels: ["original"]).perform_later

    _(@performer.perform_one.state).must_equal "retried"
    row = @store.find(job.provider_job_id)
    _(row.fetch("labels")).must_equal ["after-retry"]
    _(row.fetch("payload").fetch("decent_job_labels")).must_equal ["after-retry"]
    _(row.fetch("concurrency_keys")).must_equal ["ControlledRetryingJob"]
    _(@performer.perform_one.state).must_equal "succeeded"
  end

  it "runs enqueue callbacks for controlled jobs submitted in bulk" do
    CallbackLabeledJob.decent_job_concurrency_rule(label: "account", total_limit: 1)
    jobs = [CallbackLabeledJob.new("first"), CallbackLabeledJob.new("second")]

    _(@adapter.enqueue_all(jobs)).must_equal 1
    _(jobs.first.decent_job_labels).must_equal ["account"]
    _(jobs.last.successfully_enqueued?).must_equal false
  end
  it "lets an explicit enqueue limit override the total limit at enqueue" do
    ControlledJob.decent_job_control_concurrency_with(total_limit: 0, enqueue_limit: 1)
    _(ControlledJob.perform_later("allowed to queue")).must_be_kind_of ControlledJob
    _(ControlledJob.perform_later("over enqueue limit")).must_equal false
  end

  it "supports conditionally skipping enqueue concurrency checks" do
    klass = Class.new(ControlledJob)
    Object.const_set(:SkippedConcurrencyJob, klass)
    klass.decent_job_control_concurrency_with(total_limit: 0)
    klass.skip_callback(:enqueue, :before, :_decent_job_concurrency_before_enqueue, if: -> { arguments.first == "skip" })
    _(klass.perform_later("skip")).must_be_kind_of klass
    _(klass.perform_later("check")).must_equal false
  ensure
    Object.send(:remove_const, :SkippedConcurrencyJob) if Object.const_defined?(:SkippedConcurrencyJob)
  end

  it "supports unconditionally skipping enqueue concurrency checks" do
    klass = Class.new(ControlledJob)
    Object.const_set(:UnconditionallySkippedConcurrencyJob, klass)
    klass.decent_job_control_concurrency_with(total_limit: -> { raise "skipped" })
    klass.skip_callback(:enqueue, :before, :_decent_job_concurrency_before_enqueue)

    _(klass.perform_later("skip")).must_be_kind_of klass
  ensure
    Object.send(:remove_const, :UnconditionallySkippedConcurrencyJob) if Object.const_defined?(:UnconditionallySkippedConcurrencyJob)
  end

  it "evaluates conditional skips after later subclass enqueue callbacks" do
    klass = Class.new(ControlledJob)
    Object.const_set(:LateLabelSkippedConcurrencyJob, klass)
    klass.decent_job_control_concurrency_with(total_limit: 0)
    klass.skip_callback(:enqueue, :before, :_decent_job_concurrency_before_enqueue, if: -> { decent_job_labels.include?("skip") })
    klass.before_enqueue { decent_job_labels << "skip" if arguments.first == "skip" }

    _(klass.perform_later("skip")).must_be_kind_of klass
    _(klass.perform_later("check")).must_equal false
  ensure
    Object.send(:remove_const, :LateLabelSkippedConcurrencyJob) if Object.const_defined?(:LateLabelSkippedConcurrencyJob)
  end

  it "preserves descendant conditional skips when parent enqueue callbacks are added" do
    parent = Class.new(ControlledJob)
    Object.const_set(:LateCallbackParentJob, parent)
    parent.decent_job_control_concurrency_with(total_limit: 0)
    child = Class.new(parent)
    Object.const_set(:LateCallbackChildJob, child)
    child.skip_callback(:enqueue, :before, :_decent_job_concurrency_before_enqueue, if: -> { decent_job_labels.include?("skip") })
    parent.before_enqueue { decent_job_labels << "skip" if arguments.first == "skip" }

    _(child.perform_later("skip")).must_be_kind_of child
    _(child.perform_later("check")).must_equal false
    _(parent.perform_later("skip")).must_equal false
  ensure
    [:LateCallbackChildJob, :LateCallbackParentJob].each do |name|
      Object.send(:remove_const, name) if Object.const_defined?(name)
    end
  end

  it "does not restore an unconditionally skipped marker when a parent adds callbacks" do
    parent = Class.new(ControlledJob)
    Object.const_set(:SkippedMarkerParentJob, parent)
    parent.decent_job_control_concurrency_with(total_limit: 0)
    child = Class.new(parent)
    Object.const_set(:SkippedMarkerChildJob, child)
    child.skip_callback(:enqueue, :before, :_decent_job_concurrency_before_enqueue)
    parent.before_enqueue { decent_job_labels << "later" }

    _(child.perform_later("allowed")).must_be_kind_of child
    _(parent.perform_later("denied")).must_equal false
  ensure
    [:SkippedMarkerChildJob, :SkippedMarkerParentJob].each do |name|
      Object.send(:remove_const, name) if Object.const_defined?(name)
    end
  end

  it "applies dynamic rule labels when a deserialized job is enqueued" do
    ControlledJob.decent_job_concurrency_rule(label: -> { "user-#{arguments.first.fetch(:id)}" }, apply_label: true, total_limit: 1)
    payload = ControlledJob.new({id: 42}).serialize.merge("decent_job_labels" => [])
    restored = ActiveJob::Base.deserialize(payload)

    _(restored.enqueue).must_be_same_as restored
    _(@store.find(restored.provider_job_id).fetch("labels")).must_equal ["user-42"]
    _(ControlledJob.perform_later({id: 42})).must_equal false
  end

  it "never evaluates the total fallback when explicit phase limits apply" do
    ControlledJob.decent_job_control_concurrency_with(total_limit: -> { raise "unused total" }, enqueue_limit: 2, perform_limit: 1)
    job = ControlledJob.perform_later("overridden")

    _(job).must_be_kind_of ControlledJob
    _(@performer.perform_one.state).must_equal "succeeded"
  end

  it "resets enqueue admission intent on every enqueue attempt" do
    klass = Class.new(ControlledJob)
    Object.const_set(:RepeatedSkippedConcurrencyJob, klass)
    klass.decent_job_control_concurrency_with(total_limit: 0)
    klass.skip_callback(:enqueue, :before, :_decent_job_concurrency_before_enqueue, if: -> { arguments.first == "skip" })
    job = klass.new("check")
    _(job.enqueue).must_equal false
    job.arguments = ["skip"]

    _(job.enqueue).must_be_same_as job
  ensure
    Object.send(:remove_const, :RepeatedSkippedConcurrencyJob) if Object.const_defined?(:RepeatedSkippedConcurrencyJob)
  end

  it "does not count a rescheduled job until its execution releases the lock" do
    ControlledJob.decent_job_control_concurrency_with(enqueue_limit: 1)
    job = ControlledJob.perform_later("retrying")
    DecentJob::Claim.new(database).with_next(job_id: job.provider_job_id) do |row|
      context = {id: row.fetch("id"), execution_id: row.fetch("execution_id")}
      @store.reschedule(job, context)
      _(ControlledJob.perform_later("next")).must_be_kind_of ControlledJob
      _(ControlledJob.perform_later("full")).must_equal false
    end
  end

  it "evaluates limit callables only in their execution phase" do
    ControlledJob.decent_job_control_concurrency_with(
      enqueue_limit: -> {
        raise "enqueue only" unless executions.zero?
        2
      },
      perform_limit: -> {
        raise "perform only" if executions.zero?
        1
      }
    )
    job = ControlledJob.perform_later("phases")
    _(job).must_be_kind_of ControlledJob
    _(@performer.perform_one.state).must_equal "succeeded"
  end
end
