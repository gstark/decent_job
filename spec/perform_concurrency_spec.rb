# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

class LimitedBlockingJob < BlockingJob
  include DecentJob::ActiveJobExtensions::Concurrency
end

class LimitedRecordingJob < RecordingJob
  include DecentJob::ActiveJobExtensions::Concurrency
end

class SimultaneousLimitedJob < LimitedBlockingJob
  class_attribute :entered, :proceed

  before_perform(prepend: true) do
    self.class.entered << true
    self.class.proceed.pop
  end
end

class PendingPromotionJob < LimitedRecordingJob
  class_attribute :retry_entered, :retry_release

  before_enqueue do
    if executions.positive?
      self.class.retry_entered << true
      self.class.retry_release.pop
    end
  end
end

describe "perform concurrency controls" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)

    [LimitedBlockingJob, LimitedRecordingJob, SimultaneousLimitedJob, PendingPromotionJob].each do |job|
      job.queue_adapter = @adapter
      job.decent_job_concurrency_config = {}
      job.decent_job_concurrency_rules = []
      job.decent_job_labels = []
    end

    LimitedBlockingJob.started = Queue.new
    LimitedBlockingJob.release = Queue.new
    LimitedRecordingJob.events = Queue.new
    SimultaneousLimitedJob.entered = Queue.new
    SimultaneousLimitedJob.proceed = Queue.new
    PendingPromotionJob.retry_entered = Queue.new
    PendingPromotionJob.retry_release = Queue.new
    @store = DecentJob::Store.new(database)
    @performer = DecentJob::Performer.new(database: database)
    @threads = []
  end

  after do
    10.times { LimitedBlockingJob.release << true }
    10.times { SimultaneousLimitedJob.proceed << true }
    10.times { PendingPromotionJob.retry_release << true }
    @threads.each(&:value)
  end

  def start_blocking(job)
    @threads << Thread.new { @performer.perform_one(job_id: job.provider_job_id) }
    Timeout.timeout(5) { LimitedBlockingJob.started.pop }
  end

  def claims(job)
    database.with_connection do |connection|
      connection.exec_params("SELECT scope, state FROM decent_job_concurrency_claims WHERE job_id = $1 ORDER BY scope", [job.provider_job_id]).to_a
    end
  end

  it "retries a contender and promotes it when a running job finishes" do
    LimitedBlockingJob.decent_job_control_concurrency_with(key: "account", perform_limit: 1)
    first = LimitedBlockingJob.perform_later
    second = LimitedBlockingJob.perform_later
    start_blocking(first)
    rejected = Timeout.timeout(5) { @performer.perform_one(job_id: second.provider_job_id) }
    _(rejected.state).must_equal "retried"
    _(rejected.error).must_be_kind_of DecentJob::ActiveJobExtensions::Concurrency::ConcurrencyExceededError
    _(claims(second)).must_equal [{"scope" => "key:account", "state" => "waiting"}]
    _(@store.find(second.provider_job_id).fetch("scheduled_at")).must_be :>, Time.now
    LimitedBlockingJob.release << true
    @threads.last.value
    _(claims(first)).must_be_empty
    _(claims(second)).must_equal [{"scope" => "key:account", "state" => "promoted"}]
    _(@store.find(second.provider_job_id).fetch("scheduled_at")).must_be :<=, Time.now
    LimitedBlockingJob.release << true
    _(@performer.perform_one(job_id: second.provider_job_id).state).must_equal "succeeded"
    _(claims(second)).must_be_empty
  end

  it "enforces a label limit across job classes" do
    [LimitedBlockingJob, LimitedRecordingJob].each do |job|
      job.decent_job_concurrency_rule(label: "account", apply_label: true, perform_limit: 1)
    end

    first = LimitedBlockingJob.perform_later
    second = LimitedRecordingJob.perform_later("next")
    start_blocking(first)
    _(@performer.perform_one(job_id: second.provider_job_id).state).must_equal "retried"
    _(LimitedRecordingJob.events.size).must_equal 0
    LimitedBlockingJob.release << true
    @threads.last.value
    _(@performer.perform_one(job_id: second.provider_job_id).state).must_equal "succeeded"
    _(LimitedRecordingJob.events.pop).must_equal "next"
  end

  it "does not retain earlier grants when a later rule rejects" do
    LimitedRecordingJob.decent_job_concurrency_rule(key: "first", perform_limit: 1)
    LimitedRecordingJob.decent_job_concurrency_rule(key: "blocked", perform_limit: 0)
    job = LimitedRecordingJob.perform_later("blocked")
    _(@performer.perform_one.state).must_equal "retried"
    _(claims(job)).must_equal [{"scope" => "key:blocked", "state" => "waiting"}]
  end

  it "does not check perform limits for direct perform_now" do
    LimitedRecordingJob.decent_job_control_concurrency_with(perform_limit: 0)
    LimitedRecordingJob.perform_now("direct")
    _(LimitedRecordingJob.events.pop).must_equal "direct"
  end

  it "throttles executions and excludes rejected attempts from future quota" do
    LimitedRecordingJob.decent_job_control_concurrency_with(perform_throttle: [1, 60])
    first = LimitedRecordingJob.perform_later("first")
    second = LimitedRecordingJob.perform_later("second")
    _(@performer.perform_one(job_id: first.provider_job_id).state).must_equal "succeeded"
    rejected = @performer.perform_one(job_id: second.provider_job_id)
    _(rejected.state).must_equal "retried"
    _(rejected.error).must_be_kind_of DecentJob::ActiveJobExtensions::Concurrency::ThrottleExceededError

    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_job_executions SET started_at = clock_timestamp() - interval '61 seconds' WHERE job_id = $1", [first.provider_job_id])
      connection.exec_params("UPDATE decent_jobs SET scheduled_at = clock_timestamp() WHERE id = $1", [second.provider_job_id])
    end

    _(@performer.perform_one(job_id: second.provider_job_id).state).must_equal "succeeded"
    _(LimitedRecordingJob.events.size).must_equal 2
  end

  it "admits exactly one of two jobs claimed before either check" do
    SimultaneousLimitedJob.decent_job_control_concurrency_with(perform_limit: 1)
    jobs = 2.times.map { SimultaneousLimitedJob.perform_later }
    results = Queue.new

    jobs.each do |job|
      @threads << Thread.new { results << @performer.perform_one(job_id: job.provider_job_id) }
    end

    2.times { Timeout.timeout(5) { SimultaneousLimitedJob.entered.pop } }
    2.times { SimultaneousLimitedJob.proceed << true }
    _(Timeout.timeout(5) { results.pop }.state).must_equal "retried"
    Timeout.timeout(5) { LimitedBlockingJob.started.pop }
    _(LimitedBlockingJob.started.size).must_equal 0
    LimitedBlockingJob.release << true
    _(Timeout.timeout(5) { results.pop }.state).must_equal "succeeded"
  end

  it "counts running jobs without grants against a matching label limit" do
    LimitedBlockingJob.decent_job_labels = ["shared"]
    LimitedRecordingJob.decent_job_concurrency_rule(label: "shared", apply_label: true, perform_limit: 1)
    first = LimitedBlockingJob.perform_later
    second = LimitedRecordingJob.perform_later("waiting")
    start_blocking(first)
    _(claims(first)).must_be_empty
    _(@performer.perform_one(job_id: second.provider_job_id).state).must_equal "retried"
    _(LimitedRecordingJob.events.size).must_equal 0
  end

  it "preserves the reference throttle allowance for retries of the same job" do
    LimitedRecordingJob.decent_job_control_concurrency_with(perform_throttle: [1, 60])
    job = LimitedRecordingJob.perform_later("first")
    _(@performer.perform_one.state).must_equal "succeeded"

    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET state = 'ready', finished_at = NULL WHERE id = $1", [job.provider_job_id])
    end

    _(@performer.perform_one.state).must_equal "succeeded"
    _(LimitedRecordingJob.events.size).must_equal 2
  end

  it "includes concurrency-limit rejections in the reference throttle history" do
    LimitedRecordingJob.decent_job_control_concurrency_with(perform_limit: 0, perform_throttle: [1, 60])
    LimitedRecordingJob.perform_later("limit rejected")
    _(@performer.perform_one.state).must_equal "retried"
    LimitedRecordingJob.decent_job_control_concurrency_with(perform_throttle: [1, 60])
    second = LimitedRecordingJob.perform_later("throttle rejected")
    result = @performer.perform_one(job_id: second.provider_job_id)
    _(result.state).must_equal "retried"
    _(result.error).must_be_kind_of DecentJob::ActiveJobExtensions::Concurrency::ThrottleExceededError
    _(LimitedRecordingJob.events.size).must_equal 0
  end

  it "applies a promotion that arrives before retry persistence" do
    [LimitedBlockingJob, PendingPromotionJob].each do |job|
      job.decent_job_concurrency_rule(label: "shared", apply_label: true, perform_limit: 1)
    end

    first = LimitedBlockingJob.perform_later
    second = PendingPromotionJob.perform_later("waiting")
    start_blocking(first)
    @threads << Thread.new { @performer.perform_one(job_id: second.provider_job_id) }
    Timeout.timeout(5) { PendingPromotionJob.retry_entered.pop }
    LimitedBlockingJob.release << true
    @threads.first.value
    _(claims(second).first.fetch("state")).must_equal "promotion_pending"
    PendingPromotionJob.retry_release << true
    _(@threads.last.value.state).must_equal "retried"
    _(claims(second).first.fetch("state")).must_equal "promoted"
    _(@store.find(second.provider_job_id).fetch("scheduled_at")).must_be :<=, Time.now
  end

  it "repairs a pending promotion left after retry persistence and loss of ownership" do
    LimitedRecordingJob.decent_job_control_concurrency_with(perform_limit: 0)
    job = LimitedRecordingJob.perform_later("waiting")
    _(@performer.perform_one.state).must_equal "retried"

    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_job_concurrency_claims SET state = 'promotion_pending' WHERE job_id = $1", [job.provider_job_id])
    end

    _(@performer.perform_one(job_id: SecureRandom.uuid)).must_be_nil
    _(claims(job).first.fetch("state")).must_equal "promoted"
    _(@store.find(job.provider_job_id).fetch("scheduled_at")).must_be :<=, Time.now
  end

  it "promotes distinct waiters when two executing jobs finish together" do
    [LimitedBlockingJob, LimitedRecordingJob].each do |job|
      job.decent_job_concurrency_rule(label: "shared", apply_label: true, perform_limit: 2)
    end

    2.times { start_blocking(LimitedBlockingJob.perform_later) }
    waiters = 2.times.map { |index| LimitedRecordingJob.perform_later(index) }
    waiters.each { |job| _(@performer.perform_one(job_id: job.provider_job_id).state).must_equal "retried" }
    2.times { LimitedBlockingJob.release << true }
    @threads.each(&:value)

    waiters.each do |job|
      _(claims(job).first.fetch("state")).must_equal "promoted"
      _(@store.find(job.provider_job_id).fetch("scheduled_at")).must_be :<=, Time.now
    end
  end

  it "does not release a newer execution's grant from an old execution" do
    LimitedBlockingJob.decent_job_control_concurrency_with(perform_limit: 1)
    job = LimitedBlockingJob.perform_later
    start_blocking(job)
    registry = DecentJob::ConcurrencyClaims.new(database)

    registry.synchronize(job.provider_job_id) do |connection|
      registry.release(connection, id: job.provider_job_id, execution_id: SecureRandom.uuid)
    end

    _(claims(job).first.fetch("state")).must_equal "granted"
  end

  it "rechecks promoted jobs and preserves their original waiting position" do
    [LimitedBlockingJob, LimitedRecordingJob].each do |job|
      job.decent_job_concurrency_rule(label: "shared", apply_label: true, perform_limit: 1)
    end

    first = LimitedBlockingJob.perform_later
    waiter = LimitedRecordingJob.perform_later("waiting")
    start_blocking(first)
    _(@performer.perform_one(job_id: waiter.provider_job_id).state).must_equal "retried"

    created_at = database.with_connection do |connection|
      connection.exec_params("SELECT created_at FROM decent_job_concurrency_claims WHERE job_id = $1", [waiter.provider_job_id]).getvalue(0, 0)
    end

    LimitedBlockingJob.release << true
    @threads.first.value
    start_blocking(LimitedBlockingJob.perform_later)
    _(@performer.perform_one(job_id: waiter.provider_job_id).state).must_equal "retried"
    _(claims(waiter).first.fetch("state")).must_equal "waiting"

    database.with_connection do |connection|
      _(connection.exec_params("SELECT created_at FROM decent_job_concurrency_claims WHERE job_id = $1", [waiter.provider_job_id]).getvalue(0, 0)).must_equal created_at
    end
  end

  it "removes old waiting claims when a later attempt succeeds with changed rules" do
    LimitedRecordingJob.decent_job_concurrency_rule(key: "old", perform_limit: 0)
    job = LimitedRecordingJob.perform_later("changed rules")
    _(@performer.perform_one.state).must_equal "retried"
    LimitedRecordingJob.decent_job_concurrency_rules = []
    LimitedRecordingJob.decent_job_concurrency_rule(key: "new", perform_limit: 1)

    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET scheduled_at = clock_timestamp() WHERE id = $1", [job.provider_job_id])
    end

    _(@performer.perform_one.state).must_equal "succeeded"
    _(claims(job)).must_be_empty
  end
end
