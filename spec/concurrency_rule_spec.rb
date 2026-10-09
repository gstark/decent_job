# frozen_string_literal: true

require "spec_helper"
require "decent_job/concurrency_rule"

class ConcurrencyRuleJob < ActiveJob::Base
  attr_accessor :decent_job_labels

  def initialize(*)
    super
    @decent_job_labels = []
  end
end

describe DecentJob::ConcurrencyRule do
  it "resolves an unrestricted rule to the all-jobs scope" do
    resolved = DecentJob::ConcurrencyRule.new.resolve(ConcurrencyRuleJob.new)

    _(resolved).must_be_kind_of DecentJob::ConcurrencyRule::Resolved
    _(resolved.scope).must_equal "all"
    _(resolved.kind).must_equal "all"
    _(resolved.value).must_be_nil
    _(resolved.total_limit).must_be_nil
    _(resolved.enqueue_limit).must_be_nil
    _(resolved.perform_limit).must_be_nil
    _(resolved.enqueue_throttle).must_be_nil
    _(resolved.perform_throttle).must_be_nil
  end

  it "resolves keys in the job context" do
    rule = DecentJob::ConcurrencyRule.new(key: -> { "user-#{arguments.first}" }, total_limit: 2)
    resolved = rule.resolve(ConcurrencyRuleJob.new(42))

    _(resolved.scope).must_equal "key:user-42"
    _(resolved.kind).must_equal "key"
    _(resolved.value).must_equal "user-42"
    _(resolved.total_limit).must_equal 2
  end

  ["name", :name, 5, 1.5, Date.new(2026, 10, 9), Time.utc(2026, 10, 9), true].each do |key|
    it "accepts #{key.inspect} as a scalar key" do
      resolved = DecentJob::ConcurrencyRule.new(key: key).resolve(ConcurrencyRuleJob.new)

      _(resolved.value).must_equal key.to_s
      _(resolved.scope).must_equal "key:#{key}"
    end
  end

  it "uses the all-jobs scope for a nil key" do
    job = ConcurrencyRuleJob.new

    _(DecentJob::ConcurrencyRule.new(key: nil).resolve(job).scope).must_equal "all"
  end

  # GoodJob ad4be5c2, Concurrency::Rule#evaluate/#resolve_key. These callable
  # cases extend the reference's legacy nil-key examples to its Rule behavior.
  [nil, false, "", "  "].each do |key|
    it "skips a rule whose dynamic key resolves to #{key.inspect}" do
      rule = DecentJob::ConcurrencyRule.new(key: -> { key }, total_limit: 0)

      [:metadata, :enqueue, :perform].each do |stage|
        _(rule.resolve(ConcurrencyRuleJob.new, stage: stage)).must_be_nil
      end
    end
  end

  [false, "", "  "].each do |key|
    it "uses the all-jobs scope for a blank #{key.inspect} key" do
      _(DecentJob::ConcurrencyRule.new(key: key).resolve(ConcurrencyRuleJob.new).scope).must_equal "all"
    end
  end

  it "resolves metadata without evaluating limits or throttles" do
    skipped = -> { raise "must not evaluate metadata limits" }
    rule = DecentJob::ConcurrencyRule.new(key: "metadata", total_limit: skipped, enqueue_limit: skipped,
      perform_limit: skipped, enqueue_throttle: skipped, perform_throttle: skipped)
    resolved = rule.resolve(ConcurrencyRuleJob.new, stage: :metadata)

    _(resolved.scope).must_equal "key:metadata"
    _(resolved.to_h.values_at(:total_limit, :enqueue_limit, :perform_limit, :enqueue_throttle, :perform_throttle)).must_equal [nil, nil, nil, nil, nil]
  end

  it "resolves only enqueue limits and throttles at enqueue time" do
    skipped = -> { raise "perform only" }
    rule = DecentJob::ConcurrencyRule.new(total_limit: 3, enqueue_limit: 2, enqueue_throttle: [1, 60],
      perform_limit: skipped, perform_throttle: skipped)
    resolved = rule.resolve(ConcurrencyRuleJob.new, stage: :enqueue)

    _(resolved.to_h.values_at(:total_limit, :enqueue_limit, :enqueue_throttle)).must_equal [nil, 2, [1, 60]]
    _(resolved.perform_limit).must_be_nil
    _(resolved.perform_throttle).must_be_nil
  end

  it "resolves only perform limits and throttles at perform time" do
    skipped = -> { raise "enqueue only" }
    rule = DecentJob::ConcurrencyRule.new(total_limit: 3, perform_limit: 2, perform_throttle: [1, 60],
      enqueue_limit: skipped, enqueue_throttle: skipped)
    resolved = rule.resolve(ConcurrencyRuleJob.new, stage: :perform)

    _(resolved.to_h.values_at(:total_limit, :perform_limit, :perform_throttle)).must_equal [nil, 2, [1, 60]]
    _(resolved.enqueue_limit).must_be_nil
    _(resolved.enqueue_throttle).must_be_nil
  end

  [[], {}, Object.new].each do |key|
    it "rejects #{key.class} keys with a useful error" do
      error = _ { DecentJob::ConcurrencyRule.new(key: key).resolve(ConcurrencyRuleJob.new) }.must_raise TypeError

      _(error.message).must_match(/key/i)
      _(error.message).must_include key.class.name
    end
  end

  it "matches normalized labels and ignores a conflicting key callable" do
    job = ConcurrencyRuleJob.new
    job.decent_job_labels = ["email"]
    rule = DecentJob::ConcurrencyRule.new(label: " email ", key: -> { raise "ignored key" }, enqueue_limit: 3)
    resolved = rule.resolve(job)

    _(resolved.scope).must_equal "label:email"
    _(resolved.kind).must_equal "label"
    _(resolved.value).must_equal "email"
    _(resolved.enqueue_limit).must_equal 3
  end

  it "skips unmatched labels before resolving keys or limits" do
    rule = DecentJob::ConcurrencyRule.new(label: "email", key: -> { raise "ignored key" }, total_limit: -> { raise "ignored limit" })

    _(rule.resolve(ConcurrencyRuleJob.new)).must_be_nil
    _(rule.resolve(ActiveJob::Base.new)).must_be_nil
  end

  it "resolves dynamic labels in the job context" do
    job = ConcurrencyRuleJob.new(42)
    job.decent_job_labels = ["user-42"]
    rule = DecentJob::ConcurrencyRule.new(label: -> { " user-#{arguments.first} " })

    _(rule.resolve(job).scope).must_equal "label:user-42"
  end

  it "falls back to the key when a label resolves to blank" do
    rule = DecentJob::ConcurrencyRule.new(label: -> { "  " }, key: "email")

    _(rule.resolve(ConcurrencyRuleJob.new).scope).must_equal "key:email"
  end

  it "does not evaluate a label for application unless enabled" do
    rule = DecentJob::ConcurrencyRule.new(label: -> { raise "not applied" })

    _(rule.applied_label(ConcurrencyRuleJob.new)).must_be_nil
  end

  it "applies a normalized dynamic label without evaluating limits or keys" do
    rule = DecentJob::ConcurrencyRule.new(label: -> { " user-#{arguments.first} " }, apply_label: true,
      key: -> { raise "ignored key" }, total_limit: -> { raise "ignored limit" })

    _(rule.applied_label(ConcurrencyRuleJob.new(42))).must_equal "user-42"
  end

  [nil, "", "  "].each do |label|
    it "rejects apply_label with an unconfigured #{label.inspect} label" do
      error = _ { DecentJob::ConcurrencyRule.new(label: label, apply_label: true) }.must_raise ArgumentError

      _(error.message).must_include "label"
    end
  end

  it "ignores nil and blank labels returned by a callable" do
    job = ConcurrencyRuleJob.new

    _(DecentJob::ConcurrencyRule.new(label: -> {}, apply_label: true).applied_label(job)).must_be_nil
    _(DecentJob::ConcurrencyRule.new(label: -> { " " }, apply_label: true).applied_label(job)).must_be_nil
  end

  it "resolves dynamic limits and throttles in the job context" do
    rule = DecentJob::ConcurrencyRule.new(total_limit: -> { arguments.first }, enqueue_limit: 0,
      perform_limit: -> {}, enqueue_throttle: -> { [arguments.first, 0.5] }, perform_throttle: [0, 60])
    resolved = rule.resolve(ConcurrencyRuleJob.new(3))

    _(resolved.total_limit).must_equal 3
    _(resolved.enqueue_limit).must_equal 0
    _(resolved.perform_limit).must_be_nil
    _(resolved.enqueue_throttle).must_equal [3, 0.5]
    _(resolved.perform_throttle).must_equal [0, 60]
  end

  [:total_limit, :enqueue_limit, :perform_limit].each do |name|
    it "rejects malformed #{name} values" do
      [-1, 1.5, "2", true, Float::INFINITY, []].each do |value|
        error = _ { DecentJob::ConcurrencyRule.new(**{name => value}).resolve(ConcurrencyRuleJob.new) }.must_raise ArgumentError
        _(error.message).must_include name.to_s
      end
    end
  end

  [:enqueue_throttle, :perform_throttle].each do |name|
    it "rejects malformed #{name} values" do
      [[], [1], [1, 2, 3], "1,2", [-1, 1], [1.5, 1], [1, 0], [1, -1], [1, "2"], [1, Float::INFINITY], [1, Float::NAN], [1, Complex(1, 2)]].each do |value|
        error = _ { DecentJob::ConcurrencyRule.new(**{name => value}).resolve(ConcurrencyRuleJob.new) }.must_raise ArgumentError
        _(error.message).must_include name.to_s
      end
    end
  end

  it "validates callable limits and throttles after evaluation" do
    job = ConcurrencyRuleJob.new

    _ { DecentJob::ConcurrencyRule.new(total_limit: -> { -1 }).resolve(job) }.must_raise ArgumentError
    _ { DecentJob::ConcurrencyRule.new(enqueue_throttle: -> { [1, 0] }).resolve(job) }.must_raise ArgumentError
  end

  it "copies mutable configuration so inherited rules can be shared" do
    key = +"email"
    throttle = [2, 60]
    rule = DecentJob::ConcurrencyRule.new(key: key, enqueue_throttle: throttle)
    key.replace("changed")
    throttle[0] = 100
    resolved = rule.resolve(ConcurrencyRuleJob.new)

    _(resolved.scope).must_equal "key:email"
    _(resolved.enqueue_throttle).must_equal [2, 60]
    _(rule).must_be :frozen?
    _(resolved).must_be :frozen?
    _(resolved.value).must_be :frozen?
    _(resolved.scope).must_be :frozen?
    _(resolved.enqueue_throttle).must_be :frozen?
  end
end

class ConcurrencyKeyJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::Concurrency
end

describe "legacy concurrency key resolution" do
  before do
    ConcurrencyKeyJob.decent_job_concurrency_config = {}
    ConcurrencyKeyJob.queue_adapter = DecentJob::Adapter.new(execution_mode: :external)
  end

  # GoodJob ad4be5c2 concurrency_spec.rb, "when no key is specified".
  it "uses the class name without any legacy configuration" do
    _(ConcurrencyKeyJob.new.decent_job_concurrency_key).must_equal "ConcurrencyKeyJob"
  end

  # Source-derived regression: GoodJob's getter does not cache; its last
  # before_enqueue callback captures the initial key for retry retention.
  [nil, "first"].each do |initial|
    it "resolves a previously read #{initial.inspect} key again before initial enqueue" do
      ConcurrencyKeyJob.decent_job_control_concurrency_with(key: -> { arguments.first }, total_limit: 1)
      job = ConcurrencyKeyJob.new(initial)
      _(job.decent_job_concurrency_key).must_equal initial if initial
      _(job.decent_job_concurrency_key).must_be_nil unless initial
      job.arguments = ["second"]

      _(job.decent_job_concurrency_key).must_equal "second"
    end
  end

  it "captures the current legacy key at initial enqueue and serializes it for retries" do
    ConcurrencyKeyJob.decent_job_control_concurrency_with(key: -> { arguments.first }, total_limit: 1)
    job = ConcurrencyKeyJob.new("first")
    _(job.decent_job_concurrency_key).must_equal "first"
    job.arguments = ["second"]
    captured = DecentJob::Bulk.capture { job.enqueue }
    job.arguments = ["third"]

    _(captured).must_equal [job]
    _(job.decent_job_concurrency_key).must_equal "second"
    _(ConcurrencyKeyJob.deserialize(job.serialize).decent_job_concurrency_key).must_equal "second"
  end

  # GoodJob ad4be5c2 Concurrency#_good_job_concurrency_before_enqueue returns
  # before key evaluation for a retry of the current job, including a nil key.
  it "does not deserialize unavailable arguments to prepare a retry's nil legacy key" do
    ConcurrencyKeyJob.decent_job_control_concurrency_with(key: nil, total_limit: 1)
    payload = ConcurrencyKeyJob.new("original").serialize
    payload["arguments"] = [{"_aj_serialized" => "RemovedConcurrencyArgumentSerializer"}]
    job = ConcurrencyKeyJob.deserialize(payload)
    captured = DecentJob::Current.with(job_id: job.job_id) do
      DecentJob::Bulk.capture { job.enqueue }
    end

    _(captured).must_equal [job]
    _(job.successfully_enqueued?).must_equal true
  end
end
