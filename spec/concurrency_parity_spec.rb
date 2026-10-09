# frozen_string_literal: true

require "spec_helper"

class ConcurrencyParityJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::Concurrency

  class_attribute :performed

  def perform(value = nil)
    performed << value
  end
end

class CallbackConcurrencyParityJob < ConcurrencyParityJob
  before_enqueue do
    decent_job_concurrency_key
    self.arguments = ["after-callback"]
  end
end

class ConcurrencyKeyTargetJob < ConcurrencyParityJob
end

class NestedConcurrencyParityJob < ActiveJob::Base
  def perform
    ConcurrencyParityJob.perform_now("nested")
  end
end

describe "pinned concurrency behavior" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    [ConcurrencyParityJob, CallbackConcurrencyParityJob, ConcurrencyKeyTargetJob, NestedConcurrencyParityJob].each do |job_class|
      job_class.queue_adapter = @adapter
    end
    [ConcurrencyParityJob, CallbackConcurrencyParityJob, ConcurrencyKeyTargetJob].each do |job_class|
      job_class.decent_job_concurrency_config = {}
      job_class.decent_job_concurrency_rules = []
      job_class.decent_job_labels = []
    end
    ConcurrencyParityJob.performed = []
    @performer = DecentJob::Performer.new(database: database)
    @previous_strategy = DecentJob.configuration.lock_strategy
  end

  after do
    DecentJob.configuration.lock_strategy = @previous_strategy
  end

  # GoodJob ad4be5c2 concurrency_spec.rb:766, default key without legacy rules.
  it "retains the default class key when a job has no concurrency configuration" do
    job = ConcurrencyParityJob.perform_later("default")

    _(job.decent_job_concurrency_key).must_equal "ConcurrencyParityJob"
    _(DecentJob::Job.find(job.provider_job_id, database: database).job_instance.decent_job_concurrency_key).must_equal "ConcurrencyParityJob"
  end

  # Source-derived from GoodJob ad4be5c2 Job.enqueue_args:368 persisting the
  # default key, and Concurrency::Rule#query_scope matching that column.
  it "counts a job's default class key against another class's explicit key rule" do
    ConcurrencyKeyTargetJob.decent_job_concurrency_rule(key: "ConcurrencyParityJob", total_limit: 1)
    first = ConcurrencyParityJob.perform_later("default key")

    _(ConcurrencyKeyTargetJob.perform_later("matching key")).must_equal false
    _(@performer.perform_one(job_id: first.provider_job_id).state).must_equal "succeeded"
    _(ConcurrencyKeyTargetJob.perform_later("after completion")).must_be_kind_of ConcurrencyKeyTargetJob
  end

  # Source-derived from GoodJob ad4be5c2 Concurrency::Rule#evaluate and
  # #resolve_key; the pinned suite covers nil bypass only for legacy keys.
  [nil, false, "", "  "].each do |key|
    it "bypasses enqueue and perform limits for a dynamic #{key.inspect} rule key" do
      ConcurrencyParityJob.decent_job_concurrency_rule(key: -> { arguments.first }, total_limit: 0)
      jobs = 2.times.map { ConcurrencyParityJob.perform_later(key) }

      jobs.each do |job|
        _(job).must_be_kind_of ConcurrencyParityJob
        _(DecentJob::Job.find(job.provider_job_id, database: database).concurrency_keys).must_equal ["ConcurrencyParityJob"]
        _(@performer.perform_one(job_id: job.provider_job_id).state).must_equal "succeeded"
      end
      _(ConcurrencyParityJob.performed).must_equal [key, key]
    end
  end

  # GoodJob ad4be5c2 concurrency_spec.rb:488, legacy key returns nil.
  it "does not limit legacy jobs whose dynamic key returns nil" do
    ConcurrencyParityJob.decent_job_control_concurrency_with(key: -> {}, total_limit: 0)
    jobs = 2.times.map { ConcurrencyParityJob.perform_later("legacy") }

    jobs.each do |job|
      _(job).must_be_kind_of ConcurrencyParityJob
      _(@performer.perform_one(job_id: job.provider_job_id).state).must_equal "succeeded"
    end
    _(ConcurrencyParityJob.performed).must_equal ["legacy", "legacy"]
  end

  # Source-derived from the reference's uncached getter and final enqueue
  # callback; complements concurrency_rule_spec.rb:68's label callback cases.
  it "checks the legacy key after an enqueue callback reads it and changes arguments" do
    [ConcurrencyParityJob, CallbackConcurrencyParityJob].each do |job_class|
      job_class.decent_job_control_concurrency_with(key: -> { arguments.first }, total_limit: 1)
    end
    _(ConcurrencyParityJob.perform_later("after-callback")).must_be_kind_of ConcurrencyParityJob
    job = CallbackConcurrencyParityJob.new("before-callback")

    _(job.enqueue).must_equal false
    _(job.decent_job_concurrency_key).must_equal "after-callback"
    _(job.provider_job_id).must_be_nil
  end

  # Source-derived from GoodJob ad4be5c2 Rule#evaluate's enqueue_limit_flag:
  # the configured callable makes this enqueue-only even when total is used.
  [:advisory, :skiplocked, :hybrid].each do |strategy|
    it "excludes a claimed #{strategy} job when a nil enqueue limit uses the total fallback" do
      DecentJob.configuration.lock_strategy = strategy
      ConcurrencyParityJob.decent_job_concurrency_rule(key: "shared", enqueue_limit: -> {}, total_limit: 1)
      first = ConcurrencyParityJob.perform_later("claimed")

      DecentJob::Claim.new(database).with_next(job_id: first.provider_job_id) do
        _(ConcurrencyParityJob.perform_later("queued")).must_be_kind_of ConcurrencyParityJob
        _(ConcurrencyParityJob.perform_later("over limit")).must_equal false
      end
    end
  end

  # GoodJob ad4be5c2 concurrency_rule_spec.rb:166, applies a perform-only
  # rule label when enqueuing in bulk rather than only via perform_later.
  it "applies dynamic labels to perform-only rules in bulk" do
    ConcurrencyParityJob.decent_job_concurrency_rule(label: -> { "account-#{arguments.first}" }, apply_label: true, perform_limit: 1)
    jobs = [ConcurrencyParityJob.new("alice"), ConcurrencyParityJob.new("bob")]

    ActiveJob.perform_all_later(jobs)

    _(jobs.map(&:successfully_enqueued?)).must_equal [true, true]
    _(jobs.map { |job| DecentJob::Job.find(job.provider_job_id, database: database).labels }).must_equal [["account-alice"], ["account-bob"]]
    jobs.each { |job| _(@performer.perform_one(job_id: job.provider_job_id).state).must_equal "succeeded" }
    _(ConcurrencyParityJob.performed).must_equal ["alice", "bob"]
  end

  # GoodJob ad4be5c2 concurrency_spec.rb:625, perform_now inside another job
  # bypasses concurrency even while Current contains the outer execution.
  it "bypasses perform controls for a nested perform_now" do
    ConcurrencyParityJob.decent_job_control_concurrency_with(perform_limit: 0)
    job = NestedConcurrencyParityJob.perform_later

    _(@performer.perform_one(job_id: job.provider_job_id).state).must_equal "succeeded"
    _(ConcurrencyParityJob.performed).must_equal ["nested"]
  end
end
