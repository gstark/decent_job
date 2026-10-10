# frozen_string_literal: true

require "spec_helper"

describe "notification options" do
  it "provides an opt-in Active Job concern" do
    _(DecentJob::ActiveJobExtensions.const_defined?(:NotifyOptions)).must_equal true
    _(ActiveJob::Base.respond_to?(:decent_job_notify)).must_equal false
  end
end

class NotifyOptionsJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::NotifyOptions

  def perform(*)
  end
end

describe DecentJob::ActiveJobExtensions::NotifyOptions do
  before do
    NotifyOptionsJob.queue_adapter = :test
  end

  it "keeps class defaults separate from instance overrides and isolates inheritance" do
    parent = Class.new(NotifyOptionsJob)
    child = Class.new(parent)
    sibling = Class.new(parent)
    _(parent).must_respond_to :decent_job_notify
    _(parent.decent_job_notify).must_be_nil
    parent.decent_job_notify = false
    _(child.decent_job_notify).must_equal false
    _(child.new.decent_job_notify).must_be_nil
    child.decent_job_notify = true
    _(parent.decent_job_notify).must_equal false
    _(sibling.decent_job_notify).must_equal false
  end

  it "serializes only explicit instance values and preserves both boolean values" do
    job = NotifyOptionsJob.new
    _(job.serialize.key?("decent_job_notify")).must_equal false

    [false, true].each do |value|
      job = NotifyOptionsJob.set(decent_job_notify: value).perform_later
      _(job.serialize["decent_job_notify"]).must_equal value
      _(ActiveJob::Base.deserialize(job.serialize).decent_job_notify).must_equal value
    end
  end

  it "retains an instance override unless enqueue explicitly supplies a replacement" do
    job = NotifyOptionsJob.set(decent_job_notify: false).perform_later
    job.enqueue
    _(job.serialize["decent_job_notify"]).must_equal false
    job.enqueue(decent_job_notify: true)
    _(job.serialize["decent_job_notify"]).must_equal true
    job.enqueue(decent_job_notify: nil)
    _(job.serialize.key?("decent_job_notify")).must_equal false
  end
end
