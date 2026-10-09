# frozen_string_literal: true

require "bigdecimal"
require "spec_helper"

class ExampleJob < ActiveJob::Base
  def perform(*)
  end
end

describe "Active Job adapter" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @store = DecentJob::Store.new(database)
    @adapter = DecentJob::Adapter.new(database: database)
    ExampleJob.queue_adapter = @adapter
  end

  it "persists Active Job arguments and returns a provider identifier" do
    job = ExampleJob.set(queue: "mailers", priority: -5).perform_later("O'Reilly", amount: BigDecimal("1.23"))
    row = @store.find(job.provider_job_id)
    _(row.fetch("queue_name")).must_equal "mailers"
    _(row.fetch("priority")).must_equal(-5)
    restored = ActiveJob::Base.deserialize(row.fetch("payload"))
    restored.send(:deserialize_arguments_if_needed)
    _(restored.arguments).must_equal ["O'Reilly", {amount: BigDecimal("1.23")}]
    _(row.fetch("state")).must_equal "ready"
  end

  it "preserves the scheduled time" do
    at = Time.now.utc + 3600
    job = ExampleJob.set(wait_until: at).perform_later
    _(@store.find(job.provider_job_id).fetch("scheduled_at").to_f).must_be_close_to at.to_f, 0.001
  end

  it "enqueues a group atomically and assigns every provider identifier" do
    jobs = [ExampleJob.new("a"), ExampleJob.new("b")]
    _(@adapter.enqueue_all(jobs)).must_equal 2
    _(jobs.all?(&:successfully_enqueued?)).must_equal true
    _(jobs.map(&:provider_job_id).uniq.length).must_equal 2
    _(jobs.map { |job| @store.find(job.provider_job_id).fetch("active_job_id") }).must_equal jobs.map(&:job_id)
  end

  it "shares explicit database transactions" do
    id = nil
    _ {
      database.transaction do
        id = ExampleJob.perform_later.provider_job_id
        raise "rollback application transaction"
      end
    }.must_raise RuntimeError
    _(@store.find(id)).must_be_nil
  end

  it "does not publish a notification for rolled back jobs" do
    listener = PG.connect(@connection_options)
    listener.exec("LISTEN decent_job")
    _ {
      database.transaction do
        ExampleJob.perform_later
        raise "rollback"
      end
    }.must_raise RuntimeError
    _(listener.wait_for_notify(0.02)).must_be_nil
  ensure
    listener&.finish
  end

  it "does not silently replace an already enqueued job" do
    job = ExampleJob.perform_later("first")
    _ { @adapter.enqueue(job) }.must_raise ActiveJob::EnqueueError
    _(@store.find(job.provider_job_id).fetch("payload").fetch("arguments")).must_equal ["first"]
  end

  it "rolls back earlier bulk chunks when a later serialization fails" do
    jobs = 500.times.map { ExampleJob.new("valid") } + [ExampleJob.new(Object.new)]
    _ { @adapter.enqueue_all(jobs) }.must_raise ActiveJob::SerializationError
    database.with_connection { |connection| _(connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)).must_equal "0" }
    _(jobs.any?(&:provider_job_id)).must_equal false
  end
end
