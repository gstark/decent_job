# frozen_string_literal: true

require "spec_helper"
require "timeout"

class PublicationJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::NotifyOptions

  class_attribute :performed, default: []

  def perform(value = nil)
    self.class.performed << value
  end
end

class PlainPublicationJob < ActiveJob::Base
  def perform(*)
  end
end

class PublicationRetryJob < PublicationJob
  retry_on RuntimeError, wait: 3600, attempts: 2

  def perform(*)
    raise "retry"
  end
end

class PublicationReenqueueJob < PublicationJob
  def perform(*)
    enqueue(wait: 3600)
  end
end

describe "transactional notification publication" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    PublicationJob.queue_adapter = @adapter
    PlainPublicationJob.queue_adapter = @adapter
    PublicationJob.decent_job_notify = nil
    PublicationJob.performed = []
    @previous_notify = DecentJob.configuration.enable_listen_notify
    DecentJob.configuration.enable_listen_notify = true
    @store = DecentJob::Store.new(database)
    @listener = PG.connect(@connection_options)
    @listener.exec("LISTEN decent_job")
  end

  after do
    @adapter.shutdown
    @listener.finish
    PublicationJob.decent_job_notify = nil
    DecentJob.configuration.enable_listen_notify = @previous_notify
  end

  def notifications
    messages = []
    while @listener.wait_for_notify(0.02) { |_channel, _pid, payload| messages << (payload.to_s.empty? ? nil : JSON.parse(payload)) }
    end
    messages
  end

  def new_job(queue: "default", at: nil, notify: nil)
    PublicationJob.new("private argument").tap do |job|
      job.queue_name = queue
      job.scheduled_at = at
      job.decent_job_notify = notify
    end
  end

  it "publishes only a queue and count for ordinary work, without arguments" do
    PublicationJob.set(queue: "O'Reilly").perform_later("sensitive")
    _(notifications).must_equal [{"queue_name" => "O'Reilly", "count" => 1}]
  end

  it "does not publish before commit or after rollback" do
    database.transaction do
      PublicationJob.perform_later
      _(notifications).must_equal []
    end
    _(notifications).must_equal [{"queue_name" => "default", "count" => 1}]
    _ {
      database.transaction do
        PublicationJob.perform_later
        raise "rollback"
      end
    }.must_raise RuntimeError
    _(notifications).must_equal []
  end

  it "globally disables enqueue publication including jobs explicitly enabling it" do
    DecentJob.configuration.enable_listen_notify = false
    job = PublicationJob.set(decent_job_notify: true).perform_later
    _(@store.find(job.provider_job_id)).wont_be_nil
    _(notifications).must_equal []
  end

  it "uses instance overrides before inherited class defaults" do
    PublicationJob.decent_job_notify = false
    PublicationJob.perform_later
    PublicationJob.set(decent_job_notify: nil).perform_later
    PublicationJob.set(decent_job_notify: false).perform_later
    _(notifications).must_equal []
    PublicationJob.set(decent_job_notify: true).perform_later
    _(notifications).must_equal [{"queue_name" => "default", "count" => 1}]
    PublicationJob.decent_job_notify = true
    PublicationJob.set(decent_job_notify: false).perform_later
    _(notifications).must_equal []
  end

  it "groups enabled bulk jobs by queue and future scheduled time" do
    at = Time.now.utc + 3600
    jobs = [new_job, new_job, new_job(notify: false), new_job(queue: "mailers"),
      new_job(at: at), new_job(at: at), new_job(at: at + 60), new_job(at: Time.now.utc - 60)]
    _(@adapter.enqueue_all(jobs)).must_equal 8
    _(notifications).must_equal [
      {"queue_name" => "default", "count" => 3},
      {"queue_name" => "mailers", "count" => 1},
      {"queue_name" => "default", "scheduled_at" => at.iso8601(6), "count" => 2},
      {"queue_name" => "default", "scheduled_at" => (at + 60).iso8601(6), "count" => 1}
    ]
  end

  it "groups across insert chunks and filters raw Store callers" do
    jobs = 501.times.map { new_job } + [new_job(notify: false)]
    _(@store.enqueue_all(jobs).compact.size).must_equal 502
    _(notifications).must_equal [{"queue_name" => "default", "count" => 501}]
  end

  it "preserves options through nested captured bulk and Active Job bulk enqueue" do
    DecentJob::Bulk.enqueue do
      PublicationJob.set(decent_job_notify: false).perform_later
      DecentJob::Bulk.enqueue { PublicationJob.perform_later }
      ActiveJob.perform_all_later(new_job, new_job(notify: false))
    end
    _(notifications).must_equal Array.new(2) { {"queue_name" => "default", "count" => 1} }
  end

  it "counts accepted cron jobs and emits nothing for deduplicated occurrences" do
    context = {database: database, key: :notification_test, at: Time.now.utc}
    DecentJob::CronEntry.with_context(context) do
      _(@store.enqueue_all([new_job, new_job]).compact.size).must_equal 1
      _(notifications).must_equal [{"queue_name" => "default", "count" => 1}]
      _(@store.enqueue_all([new_job]).compact.size).must_equal 0
      _(notifications).must_equal []
    end
  end

  it "does not count a suppressed accepted cron job when enabled duplicates are rejected" do
    context = {database: database, key: :notification_test, at: Time.now.utc}
    DecentJob::CronEntry.with_context(context) do
      _(@store.enqueue_all([new_job(notify: false), new_job]).compact.size).must_equal 1
    end
    _(notifications).must_equal []
  end

  it "falls back to a broad hint when a multibyte queue exceeds PostgreSQL payload limits" do
    queue = "界" * 3000
    job = PublicationJob.set(queue: queue).perform_later
    _(@store.find(job.provider_job_id).fetch("queue_name")).must_equal queue
    _(notifications).must_equal [nil]
  end

  it "preserves serialized false options during Active Job retries and self reenqueues" do
    [PublicationRetryJob, PublicationReenqueueJob].each do |klass|
      job = klass.set(decent_job_notify: false).perform_later
      result = DecentJob::Performer.new(database: database).perform_one(job_id: job.provider_job_id)
      _(result.state).must_equal "retried"
      _(@store.find(job.provider_job_id).fetch("payload")["decent_job_notify"]).must_equal false
      _(notifications).must_equal []
    end
  end

  it "publishes future queue hints after an enabled Active Job retry" do
    job = PublicationRetryJob.perform_later
    notifications
    DecentJob::Performer.new(database: database).perform_one(job_id: job.provider_job_id)
    row = @store.find(job.provider_job_id)
    _(notifications).must_equal [{"queue_name" => "default", "scheduled_at" => row.fetch("scheduled_at").utc.iso8601(6), "count" => 1}]
  end

  it "globally suppresses retries even when a stored job enables notifications" do
    job = PublicationRetryJob.set(decent_job_notify: true).perform_later
    notifications
    DecentJob.configuration.enable_listen_notify = false
    DecentJob::Performer.new(database: database).perform_one(job_id: job.provider_job_id)
    _(notifications).must_equal []
  end

  it "preserves suppression on operator retry" do
    job = PublicationJob.set(decent_job_notify: false).perform_later
    record = DecentJob::Job.find(job.provider_job_id, database: database)
    record.discard_job("operator discard")
    notifications
    record.retry_job
    _(record.payload["decent_job_notify"]).must_equal false
    _(notifications).must_equal []
  end

  it "never notifies a reschedule that has lost execution ownership" do
    job = PublicationJob.perform_later
    notifications
    _ {
      @store.reschedule(job, {id: job.provider_job_id, execution_id: SecureRandom.uuid})
    }.must_raise DecentJob::EnqueueError
    _(notifications).must_equal []
  end

  it "does not turn notification suppression into execution suppression in inline mode" do
    PublicationJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :inline)
    PublicationJob.set(decent_job_notify: false).perform_later("inline")
    _(PublicationJob.performed).must_equal ["inline"]
    _(notifications).must_equal []
  end

  it "notifies jobs that do not opt into the concern" do
    PlainPublicationJob.perform_later
    _(notifications).must_equal [{"queue_name" => "default", "count" => 1}]
  end

  it "rolls back all insert chunks without notifying when later serialization fails" do
    jobs = 500.times.map { new_job } + [PublicationJob.new(Object.new)]
    _ { @adapter.enqueue_all(jobs) }.must_raise ActiveJob::SerializationError
    _(notifications).must_equal []
    database.with_connection do |connection|
      _(connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)).must_equal "0"
    end
  end

  it "does not notify retries rolled back by their enclosing transaction" do
    job = PublicationJob.perform_later
    notifications
    _ {
      database.transaction do
        @store.reschedule(job, {id: job.provider_job_id, execution_id: nil})
        raise "rollback retry"
      end
    }.must_raise RuntimeError
    _(notifications).must_equal []
  end

  it "publishes an enabled operator retry" do
    job = PublicationJob.perform_later
    record = DecentJob::Job.find(job.provider_job_id, database: database)
    record.discard_job("operator discard")
    notifications
    record.retry_job
    _(notifications).must_equal [{"queue_name" => "default", "count" => 1}]
  end

  it "executes notification-suppressed jobs on a local asynchronous worker" do
    PublicationJob.performed = Queue.new
    async_adapter = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    PublicationJob.queue_adapter = async_adapter
    PublicationJob.set(decent_job_notify: false).perform_later("async")
    _(Timeout.timeout(5) { PublicationJob.performed.pop }).must_equal "async"
    _(notifications).must_equal []
  ensure
    async_adapter&.shutdown(timeout: 5)
  end

  it "keeps explicit maintenance wakes unconditional when automatic notification is disabled" do
    DecentJob.configuration.enable_listen_notify = false
    database.transaction { |connection| @store.notify(connection) }
    _(notifications).must_equal [nil]
  end
end
