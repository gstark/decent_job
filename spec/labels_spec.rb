# frozen_string_literal: true

require "spec_helper"
require "decent_job/active_job_extensions/labels"

class LabelledJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::Labels

  def perform(*)
  end
end

class RetryingLabelledJob < LabelledJob
  retry_on RuntimeError, wait: 0, attempts: 3

  def perform
    decent_job_labels << "retried"
    raise "retry" if executions < 3
  end
end

class AppliedLabelledJob < LabelledJob
  def decent_job_applied_labels
    [" applied ", nil, "applied", "user-#{arguments.first.fetch(:user_id)}"]
  end
end

describe DecentJob::ActiveJobExtensions::Labels do
  before do
    LabelledJob.decent_job_labels = []
    LabelledJob.queue_adapter = :test
  end

  it "has independent empty labels on each instance" do
    first = LabelledJob.new
    first.decent_job_labels << "first"

    _(first.decent_job_labels).must_equal ["first"]
    _(LabelledJob.new.decent_job_labels).must_equal []
    _(LabelledJob.decent_job_labels).must_equal []
  end

  it "normalizes class defaults without modifying the class array" do
    defaults = [" email ", :email, 1, true, false, nil, "", "  ", "1"]
    LabelledJob.decent_job_labels = defaults

    _(LabelledJob.new.decent_job_labels).must_equal ["email", "1", "true", "false"]
    _(LabelledJob.decent_job_labels).must_equal defaults
  end

  it "resolves callable defaults in the instance context only on first read" do
    calls = 0
    dynamic_label = -> {
      calls += 1
      "user-#{arguments.first.fetch(:user_id)}"
    }
    LabelledJob.decent_job_labels = ["email", dynamic_label, -> {}]
    job = LabelledJob.new(user_id: 42)

    _(calls).must_equal 0
    _(job.decent_job_labels).must_equal ["email", "user-42"]
    _(job.decent_job_labels).must_equal ["email", "user-42"]
    _(calls).must_equal 1
  end

  it "copies class default arrays for each instance" do
    LabelledJob.decent_job_labels = ["email"]
    first = LabelledJob.new
    first.decent_job_labels << "first"

    _(LabelledJob.new.decent_job_labels).must_equal ["email"]
    _(LabelledJob.decent_job_labels).must_equal ["email"]
  end

  it "normalizes and copies labels assigned to an instance" do
    labels = [" email ", nil, :email, ""]
    job = LabelledJob.new
    job.decent_job_labels = labels
    job.decent_job_labels << "extra"

    _(job.decent_job_labels).must_equal ["email", "extra"]
    _(labels).must_equal [" email ", nil, :email, ""]
  end

  it "keeps the instance label array mutable across reads" do
    job = LabelledJob.new
    labels = job.decent_job_labels
    job.decent_job_labels
    labels << "added"

    _(job.decent_job_labels).must_equal ["added"]
  end

  it "replaces class defaults with labels passed through set" do
    LabelledJob.decent_job_labels = [-> { raise "defaults should not run" }]
    job = LabelledJob.set(decent_job_labels: [" custom ", :custom]).perform_later

    _(job.decent_job_labels).must_equal ["custom"]
    _(LabelledJob.queue_adapter.enqueued_jobs.last.fetch("decent_job_labels")).must_equal ["custom"]
  end

  it "allows an explicit empty or nil label list to suppress defaults" do
    LabelledJob.decent_job_labels = ["email"]

    _(LabelledJob.set(decent_job_labels: []).perform_later.decent_job_labels).must_equal []
    _(LabelledJob.set(decent_job_labels: nil).perform_later.decent_job_labels).must_equal []
  end

  it "serializes normalized labels including in-place changes" do
    job = LabelledJob.new
    job.decent_job_labels << " email " << :email << nil
    payload = job.serialize

    _(payload.fetch("decent_job_labels")).must_equal ["email"]
    payload.fetch("decent_job_labels") << "payload-only"
    _(job.decent_job_labels).must_equal ["email"]
  end

  it "restores stored labels without evaluating argument-dependent defaults" do
    payload = LabelledJob.new(user_id: 42).serialize.merge("decent_job_labels" => [" stored ", "stored"])
    LabelledJob.decent_job_labels = [-> { raise "defaults should not run" }]
    original = Marshal.load(Marshal.dump(payload))
    restored = ActiveJob::Base.deserialize(payload)

    _(restored.decent_job_labels).must_equal ["stored"]
    restored.decent_job_labels << "changed"
    _(payload).must_equal original
  end

  it "restores nil and missing serialized labels as empty arrays" do
    payload = LabelledJob.new.serialize
    LabelledJob.decent_job_labels = [-> { raise "defaults should not run" }]

    _(ActiveJob::Base.deserialize(payload.merge("decent_job_labels" => nil)).decent_job_labels).must_equal []
    _(ActiveJob::Base.deserialize(payload.except("decent_job_labels")).decent_job_labels).must_equal []
  end

  it "round trips argument-dependent defaults through serialization" do
    LabelledJob.decent_job_labels = [-> { "user-#{arguments.first.fetch(:user_id)}" }]
    payload = LabelledJob.new(user_id: 42).serialize
    restored = ActiveJob::Base.deserialize(payload)

    _(restored.decent_job_labels).must_equal ["user-42"]
    _(restored.serialize.fetch("decent_job_labels")).must_equal ["user-42"]
  end

  it "merges and normalizes applied labels with class defaults" do
    LabelledJob.decent_job_labels = ["email", "applied"]

    _(AppliedLabelledJob.new(user_id: 42).decent_job_labels).must_equal ["email", "applied", "user-42"]
  end

  it "merges applied labels with explicit enqueue labels" do
    job = AppliedLabelledJob.set(decent_job_labels: [" custom ", "applied"]).perform_later(user_id: 42)

    _(job.decent_job_labels).must_equal ["custom", "applied", "user-42"]
  end

  it "restores applied labels after Active Job has serialized the arguments" do
    payload = AppliedLabelledJob.new(user_id: 42).serialize
    original = Marshal.load(Marshal.dump(payload))
    restored = ActiveJob::Base.deserialize(payload)

    _(restored.decent_job_labels).must_equal ["applied", "user-42"]
    _(payload).must_equal original
  end

  it "preserves labels added during execution across retries" do
    RetryingLabelledJob.set(decent_job_labels: ["original"]).perform_later
    adapter = RetryingLabelledJob.queue_adapter
    3.times do |attempt|
      payload = adapter.enqueued_jobs.shift
      _(payload.fetch("decent_job_labels")).must_equal(attempt.zero? ? ["original"] : ["original", "retried"])
      ActiveJob::Base.execute(payload)
    end

    _(adapter.enqueued_jobs).must_equal []
  end
end
