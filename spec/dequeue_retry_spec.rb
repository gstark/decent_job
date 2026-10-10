# frozen_string_literal: true

require "spec_helper"
require "support/jobs"

class DequeueReenqueueJob < RecordingJob
  def perform(value)
    enqueue(wait: 60) if executions == 1
    super
  end
end

class DequeueLimitedJob < RecordingJob
  include DecentJob::ActiveJobExtensions::Concurrency
end

describe "dequeue retry timestamps" do
  include DatabaseSpec

  before do
    DecentJob.reset!
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)
    [RecordingJob, RetryingJob, DequeueReenqueueJob, FailingJob, DequeueLimitedJob].each { |klass| klass.queue_adapter = @adapter }
    DequeueLimitedJob.decent_job_concurrency_config = {}
    DequeueLimitedJob.decent_job_concurrency_rules = []
    RecordingJob.events = Queue.new
    @store = DecentJob::Store.new(database)
    @performer = DecentJob::Performer.new(database: database)
  end

  after { DecentJob.reset! }

  def backdate(job)
    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET created_at = '2020-01-01' WHERE id = $1", [job.provider_job_id])
    end

    @store.find(job.provider_job_id)
  end

  def database_now
    database.with_connection { |connection| Time.parse(connection.exec("SELECT clock_timestamp()").getvalue(0, 0)) }
  end

  [:created_at, :scheduled_at].each do |sort|
    [RetryingJob, DequeueReenqueueJob].each do |klass|
      it "applies #{sort} to #{klass} reenqueues while preserving identity and attempt snapshots" do
        job = klass.perform_later("retry")
        original = backdate(job)
        DecentJob.configuration.dequeue_query_sort = sort
        before = database_now
        events = []
        observer = ->(event) { events << event.payload[:job] }

        ActiveSupport::Notifications.subscribed(observer, "enqueue_job.decent_job") do
          _(@performer.perform_one.state).must_equal "retried"
        end

        current = @store.find(job.provider_job_id)

        if sort == :created_at
          _(current.fetch("created_at")).must_be :>=, before
          _(current.fetch("created_at")).must_be :<=, database_now
        else
          _(current.fetch("created_at")).must_equal original.fetch("created_at")
        end

        _(current.values_at("id", "active_job_id")).must_equal original.values_at("id", "active_job_id")
        _(current.fetch("payload").fetch("provider_job_id")).must_equal job.provider_job_id
        _(current.fetch("payload").fetch("executions")).must_equal 1
        attempt = @store.executions(job.provider_job_id).first
        _(attempt.fetch("payload")).must_equal original.fetch("payload")
        _(attempt.fetch("scheduled_at")).must_equal original.fetch("scheduled_at")
        _(events.size).must_equal 1
        _(events.first.created_at).must_equal current.fetch("created_at")
      end
    end

    it "applies #{sort} to operator retry while preserving metadata and historical attempts" do
      job = RetryingJob.set(decent_job_labels: ["preserved"]).perform_later("operator retry")
      original = backdate(job)
      DecentJob.configuration.dequeue_query_sort = sort
      facade = DecentJob::Job.find(job.provider_job_id, database: database)
      facade.discard_job("retry manually")
      before = database_now
      events = []
      ActiveSupport::Notifications.subscribed(->(event) { events << event.payload[:job] }, "enqueue_job.decent_job") { facade.retry_job }
      current = facade.reload.attributes

      if sort == :created_at
        _(facade.created_at).must_be :>=, before
        _(facade.created_at).must_be :<=, database_now
      else
        _(facade.created_at).must_equal original.fetch("created_at")
      end

      keys = %w[id active_job_id labels concurrency_keys batch_id batch_callback_id cron_key cron_at]
      _(current.slice(*keys)).must_equal original.slice(*keys)
      _(facade.executions).must_equal []
      _(events.first.created_at).must_equal facade.created_at
      _(current.fetch("payload").fetch("locale")).must_equal original.fetch("payload").fetch("locale")
    end

    it "preserves #{sort} creation time for backend retry and management deadline edits" do
      DecentJob.configuration.dequeue_query_sort = sort
      DecentJob.configuration.retry_on_unhandled_error = true
      job = FailingJob.perform_later
      original = backdate(job)
      2.times { _(@performer.perform_one.state).must_equal "retried" }
      current = @store.find(job.provider_job_id)
      keys = %w[created_at scheduled_at payload active_job_id id]
      _(current.slice(*keys)).must_equal original.slice(*keys)
      _(current.fetch("executions_count")).must_equal 2
      _(current.fetch("payload").fetch("executions")).must_equal 0
      facade = DecentJob::Job.find(job.provider_job_id, database: database)
      facade.reschedule_job(Time.now + 300)
      _(facade.created_at).must_equal original.fetch("created_at")
      _(facade.scheduled_at).must_be :>, current.fetch("scheduled_at")
    end
  end

  it "rolls back a creation timestamp reset with an operator retry transaction" do
    job = RecordingJob.perform_later("rollback")
    backdate(job)
    facade = DecentJob::Job.find(job.provider_job_id, database: database)
    facade.discard_job("discarded")
    original = facade.attributes

    _ do
      database.transaction do
        facade.retry_job
        _(facade.created_at).must_be :>, original.fetch("created_at")
        raise "rollback"
      end
    end.must_raise RuntimeError

    _(facade.reload.attributes).must_equal original
  end

  it "preserves concurrency waiter age when retries reset the job creation timestamp" do
    DequeueLimitedJob.decent_job_control_concurrency_with(key: "blocked", perform_limit: 0)
    job = DequeueLimitedJob.perform_later("wait")
    backdate(job)
    _(@performer.perform_one.state).must_equal "retried"

    first = database.with_connection do |connection|
      connection.exec_params("SELECT created_at FROM decent_job_concurrency_claims WHERE job_id = $1", [job.provider_job_id]).getvalue(0, 0)
    end

    creation = @store.find(job.provider_job_id).fetch("created_at")
    DecentJob::Job.find(job.provider_job_id, database: database).reschedule_job
    _(@performer.perform_one.state).must_equal "retried"

    second = database.with_connection do |connection|
      connection.exec_params("SELECT created_at FROM decent_job_concurrency_claims WHERE job_id = $1", [job.provider_job_id]).getvalue(0, 0)
    end

    _(second).must_equal first
    _(@store.find(job.provider_job_id).fetch("created_at")).must_be :>, creation
  end
end
