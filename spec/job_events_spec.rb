# frozen_string_literal: true

require "spec_helper"
require "support/jobs"

class EventValueJob < ActiveJob::Base
  def perform(value = nil)
    value
  end
end

class EventReenqueueJob < ActiveJob::Base
  def perform
    enqueue(wait: 60)
    :again
  end
end

class EventReturnedErrorJob < ActiveJob::Base
  def perform
    ArgumentError.new("returned")
  end
end

class EventFatalError < Exception; end # standard:disable Lint/InheritException

class EventFatalJob < ActiveJob::Base
  def perform
    raise EventFatalError, "excluded"
  end
end

class EventRetryStoppedJob < ActiveJob::Base
  retry_on ArgumentError, attempts: 1

  def perform
    raise ArgumentError, "exhausted"
  end
end

class EventRollbackRetryJob < ActiveJob::Base
  def perform
    DecentJob::Current.execution.fetch(:database).transaction do
      2.times { enqueue(wait: 60) }
      raise "rollback retries"
    end
  rescue RuntimeError
    :rolled_back
  end
end

class EventControlledJob < EventValueJob
  include DecentJob::ActiveJobExtensions::Concurrency
end

describe "job notifications" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    [EventRollbackRetryJob, EventReturnedErrorJob, EventFatalJob, EventRetryStoppedJob, EventValueJob, EventReenqueueJob, EventControlledJob, RetryingJob, DiscardingJob, FailingJob, BlockingJob].each { |klass| klass.queue_adapter = @adapter }
    EventControlledJob.decent_job_concurrency_config = {}
    EventControlledJob.decent_job_concurrency_rules = []
    @store = DecentJob::Store.new(database)
    @performer = DecentJob::Performer.new(database: database)
    @events = []
    @subscription = ActiveSupport::Notifications.subscribe(/\.decent_job\z/) { |event| @events << event }
    @preserve = DecentJob.configuration.preserve_job_records
    @reporter = DecentJob.configuration.on_thread_error
    DecentJob.configuration.on_thread_error = ->(*) {}
  end

  after do
    ActiveSupport::Notifications.unsubscribe(@subscription)
    DecentJob.configuration.preserve_job_records = @preserve
    DecentJob.configuration.on_thread_error = @reporter
  end

  def events(name)
    @events.select { |event| event.name == "#{name}.decent_job" }
  end

  it "finishes enqueue before inline perform and snapshots metadata after retention" do
    DecentJob.configuration.preserve_job_records = false
    EventValueJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :inline)
    job = EventValueJob.perform_later(false)
    _(@events.map(&:name)).must_equal ["enqueue_job.decent_job", "perform_job.decent_job"]
    payload = events(:enqueue_job).first.payload
    _(payload[:job_instance]).must_be_same_as job
    _(payload[:accepted]).must_equal true
    _(payload[:scheduled_at]).must_be_nil
    _(payload[:create_with_advisory_lock]).must_equal false
    _(payload[:job].id).must_equal job.provider_job_id
    _(payload[:job].job_class).must_equal "EventValueJob"
    _(payload[:job].created_at).must_be_kind_of Time
    _(@store.find(job.provider_job_id)).must_be_nil
    _(events(:perform_job).first.payload[:execution].job_class).must_equal "EventValueJob"
  end

  it "reports original enqueue_at timestamps and truthful duplicate cron attempts" do
    timestamp = Time.now.to_f + 60
    job = EventValueJob.new
    @adapter.enqueue_at(job, timestamp)
    _(events(:enqueue_job).last.payload[:scheduled_at]).must_equal timestamp
    at = Time.utc(2026, 1, 1)

    DecentJob::CronEntry.with_context(key: :event, at: at, database: database) do
      EventValueJob.perform_later
      _(EventValueJob.perform_later).must_equal false
    end

    payload = events(:enqueue_job).last.payload
    _(payload[:accepted]).must_equal false
    _(payload[:job_instance].provider_job_id).must_be_nil
    _(payload[:job].cron_key).must_equal "event"
    _(payload[:job].created_at).must_be_nil
  end

  it "finishes perform while the attempt is still running and keeps observer mutations detached" do
    job = EventValueJob.perform_later({"nested" => [1]})
    observed = nil

    callback = ActiveSupport::Notifications.subscribe("perform_job.decent_job") do |event|
      observed = @store.executions(job.provider_job_id).first
      event.payload[:job].payload["arguments"].clear
      event.payload[:execution].payload["arguments"].clear
      event.payload[:error] = RuntimeError.new("invented")
      event.payload[:retried] = true
    end

    result = @performer.perform_one
    _(observed["state"]).must_equal "running"
    _(observed["duration"]).must_be_nil
    _(result.state).must_equal "succeeded"
    _(result.error).must_be_nil
    _(@store.find(job.provider_job_id)["payload"]["arguments"]).wont_be_empty
  ensure
    ActiveSupport::Notifications.unsubscribe(callback) if callback
  end

  it "preserves false nil and zero successful values" do
    [false, nil, 0].each do |value|
      EventValueJob.perform_later(value)
      @performer.perform_one
      payload = events(:perform_job).last.payload
      value.nil? ? assert_nil(payload[:value]) : assert_equal(value, payload[:value])
      _(payload[:error]).must_be_nil
      _(payload[:handled_error]).must_be_nil
      _(payload[:retried]).must_equal false
      _(payload[:error_event]).must_be_nil
      _(payload[:process_id]).must_equal Process.pid
      _(payload[:thread_name]).must_equal (Thread.current.name || Thread.current.object_id).to_s
    end
  end

  it "classifies handled retry discard and unhandled exceptions before persistence" do
    [[RetryingJob, :retried, true], [DiscardingJob, :discarded, false], [FailingJob, :unhandled, false]].each do |klass, event, retried|
      job = (klass == RetryingJob) ? klass.perform_later("retry") : klass.perform_later
      @performer.perform_one(job_id: job.provider_job_id)
      payload = events(:perform_job).last.payload
      _(payload[:error]).must_be_kind_of Exception
      _(payload[:error_event]).must_equal event

      if event == :unhandled
        _(payload[:unhandled_error]).must_be_same_as payload[:error]
        _(payload).wont_include :value
        _(payload).wont_include :retried
      else
        _(payload[:handled_error]).must_be_same_as payload[:error]
        _(payload[:retried]).must_equal retried
      end
    end
  end

  it "reports direct reenqueue and emits its enqueue event during perform" do
    EventReenqueueJob.perform_later
    @performer.perform_one
    _(events(:enqueue_job).size).must_equal 2
    _(events(:perform_job).last.payload[:retried]).must_equal true
  end

  it "emits only the first concurrency rejection before returning false" do
    EventControlledJob.decent_job_control_concurrency_with(key: "events", enqueue_limit: 1, enqueue_throttle: [1, 60])
    EventControlledJob.perform_later
    rejected = EventControlledJob.new
    _(rejected.enqueue).must_equal false
    _(events(:enqueue_job).size).must_equal 1
    payload = events(:enqueue_concurrency_limit_exceeded).last.payload
    _(payload.slice(:job, :key, :limit, :count, :kind, :scope)).must_equal(job: rejected, key: "events", limit: 1, count: 1, kind: "key", scope: "key:events")
    _(events(:enqueue_concurrency_throttle_exceeded)).must_be_empty
  end

  it "normalizes returned exceptions and identifies exhausted retries" do
    [[EventReturnedErrorJob, :handled], [EventRetryStoppedJob, :retry_stopped]].each do |klass, kind|
      job = klass.perform_later
      result = @performer.perform_one(job_id: job.provider_job_id)
      payload = events(:perform_job).last.payload
      _(payload[:error_event]).must_equal kind
      _(payload[:error]).must_be_same_as result.error

      if kind == :handled
        _(payload[:value]).must_be_nil
        _(payload[:handled_error]).must_be_same_as result.error
      else
        _(payload[:unhandled_error]).must_be_same_as result.error
        _(payload).wont_include :value
      end
    end
  end

  it "propagates excluded exceptions with standard event metadata and an unfinished attempt" do
    job = EventFatalJob.perform_later
    error = assert_raises(EventFatalError) { @performer.perform_one }
    payload = events(:perform_job).last.payload
    _(payload[:exception_object]).must_be_same_as error
    _(payload[:exception]).must_equal ["EventFatalError", "excluded"]
    _(payload).wont_include :value
    _(@store.executions(job.provider_job_id).first["finished_at"]).must_be_nil
  end

  it "does not describe backend retry policy as an Active Job reenqueue" do
    previous = DecentJob.configuration.retry_on_unhandled_error
    DecentJob.configuration.retry_on_unhandled_error = true
    FailingJob.perform_later
    _(@performer.perform_one.state).must_equal "retried"
    _(events(:perform_job).last.payload).wont_include :retried
  ensure
    DecentJob.configuration.retry_on_unhandled_error = previous
  end

  it "retains forced discard from a finish observer" do
    job = EventValueJob.perform_later

    observer = ActiveSupport::Notifications.subscribe("perform_job.decent_job") do |event|
      event.payload[:job].force_discard_job("observer stopped it")
    end

    result = @performer.perform_one
    _(result.state).must_equal "discarded"
    _(result.error).must_be_kind_of DecentJob::Job::DiscardJobError
    _(@store.find(job.provider_job_id)["state"]).must_equal "discarded"
  ensure
    ActiveSupport::Notifications.unsubscribe(observer) if observer
  end

  it "keeps killed attempts unfinished and reports interruption without success" do
    BlockingJob.started = Queue.new
    BlockingJob.release = Queue.new
    job = BlockingJob.perform_later
    thread = Thread.new { @performer.perform_one }
    BlockingJob.started.pop
    thread.kill.join
    payload = events(:perform_job).last.payload
    _(payload[:interrupted]).must_equal true
    _(payload).wont_include :value
    attempt = @store.executions(job.provider_job_id).first
    _(attempt["finished_at"]).must_be_nil
    _(attempt["duration"]).must_be_nil
    _(@performer.active?).must_equal false
  ensure
    thread&.kill&.join
  end

  it "suppresses individual events for plain bulk and capture but emits checked bulk events" do
    ActiveJob.perform_all_later(EventValueJob.new, EventValueJob.new)

    DecentJob::Bulk.enqueue do
      EventValueJob.perform_later
      _(events(:enqueue_job)).must_be_empty
    end

    _(events(:enqueue_job)).must_be_empty
    EventControlledJob.decent_job_control_concurrency_with(key: "bulk", total_limit: 1)
    ActiveJob.perform_all_later(EventControlledJob.new, EventControlledJob.new)
    _(events(:enqueue_job).size).must_equal 1
    _(events(:enqueue_concurrency_limit_exceeded).size).must_equal 1
  end

  it "reports SQL enqueue failure and leaves an outer transaction usable" do
    job = EventValueJob.perform_later
    previous_id = job.provider_job_id

    database.transaction do |connection|
      _(job.enqueue).must_equal false
      _(connection.exec("SELECT 42").getvalue(0, 0)).must_equal "42"
    end

    payload = events(:enqueue_job).last.payload
    _(payload[:accepted]).must_equal false
    _(payload[:exception_object]).must_be_kind_of DecentJob::EnqueueError
    _(payload[:job].created_at).must_be_nil
    _(job.provider_job_id).must_equal previous_id
  end

  it "keeps admission rejected when a subscriber and its reporter fail with SQL" do
    EventControlledJob.decent_job_control_concurrency_with(key: "reject", total_limit: 0)
    poison = ->(*) { database.with_connection { |connection| connection.exec("SELECT missing_observer_column") } }
    observer = ActiveSupport::Notifications.subscribe("enqueue_concurrency_limit_exceeded.decent_job", &poison)
    DecentJob.configuration.on_thread_error = poison

    database.transaction do |connection|
      _(EventControlledJob.perform_later).must_equal false
      _(connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)).must_equal "0"
    end

    _(events(:enqueue_job)).must_be_empty
  ensure
    ActiveSupport::Notifications.unsubscribe(observer) if observer
  end

  it "publishes label and global throttle scopes with the measured count and window" do
    EventControlledJob.decent_job_concurrency_rule(label: "accounts", apply_label: true, enqueue_throttle: [0, 45])
    _(EventControlledJob.perform_later).must_equal false
    payload = events(:enqueue_concurrency_throttle_exceeded).last.payload
    _(payload.slice(:key, :kind, :scope, :limit, :count, :window)).must_equal(key: "accounts", kind: "label", scope: "label:accounts", limit: 0, count: 0, window: 45)
    EventControlledJob.decent_job_concurrency_rules = []
    EventControlledJob.decent_job_concurrency_rule(total_limit: 0)
    _(EventControlledJob.perform_later).must_equal false
    payload = events(:enqueue_concurrency_limit_exceeded).last.payload
    _(payload.slice(:key, :kind, :scope)).must_equal(key: nil, kind: "all", scope: "all")
  end

  it "excludes notification callbacks from persisted execution duration" do
    observer = Object.new
    observer.define_singleton_method(:start) { |*| sleep 0.08 }
    observer.define_singleton_method(:finish) { |*| sleep 0.08 }
    subscription = ActiveSupport::Notifications.subscribe("perform_job.decent_job", observer)
    job = EventValueJob.perform_later
    @performer.perform_one
    _(events(:perform_job).last.duration / 1000).must_be :>=, 0.08
    _(@store.executions(job.provider_job_id).first["duration"]).must_be :<, 0.08
  ensure
    ActiveSupport::Notifications.unsubscribe(subscription) if subscription
  end

  it "protects enqueue writes from failing start and finish observers without changing rollback" do
    phases = []
    pool = database
    observer = Object.new

    [:start, :finish].each do |phase|
      observer.define_singleton_method(phase) do |*|
        phases << phase
        pool.with_connection { |connection| connection.exec("SELECT missing_observer_column") }
      end
    end

    subscription = ActiveSupport::Notifications.subscribe("enqueue_job.decent_job", observer)
    job = nil

    assert_raises(RuntimeError) do
      database.transaction do
        job = EventValueJob.perform_later
        _(job.successfully_enqueued?).must_equal true
        _(events(:enqueue_job).last.payload[:accepted]).must_equal true
        raise "rollback"
      end
    end

    _(phases).must_equal [:start, :finish]
    _(@store.find(job.provider_job_id)).must_be_nil
  ensure
    ActiveSupport::Notifications.unsubscribe(subscription) if subscription
  end

  it "uses INSERT and UPDATE returned metadata without event-only SELECTs" do
    statements = []

    database.with_connection do |connection|
      original = connection.method(:exec_params)

      connection.define_singleton_method(:exec_params) do |sql, *args|
        statements << sql
        original.call(sql, *args)
      end

      EventValueJob.perform_later
      _(events(:enqueue_job).last.payload[:job].created_at).must_be_kind_of Time
      _(statements.grep(/SELECT/).reject { |sql| sql.include?("pg_notify") }).must_be_empty
      statements.clear
      @performer.perform_one
      _(events(:perform_job).last.payload[:execution].started_at).must_be_kind_of Time
      _(statements.grep(/SELECT \* FROM decent_job_executions/)).must_be_empty
      _(statements.grep(/SELECT \* FROM decent_jobs/).size).must_equal 1
    ensure
      connection.singleton_class.remove_method(:exec_params)
    end
  end

  it "does not count multiple reenqueues rolled back by the application" do
    job = EventRollbackRetryJob.perform_later
    result = @performer.perform_one
    _(result.state).must_equal "succeeded"
    _(events(:perform_job).last.payload[:retried]).must_equal false
    _(@store.find(job.provider_job_id)["state"]).must_equal "succeeded"
  end

  it "performs successfully when its start observer freezes the notification payload" do
    observer = Object.new

    observer.define_singleton_method(:start) do |name, id, payload|
      payload.freeze
      raise "frozen observer"
    end

    observer.define_singleton_method(:finish) { |*| }
    subscription = ActiveSupport::Notifications.subscribe("perform_job.decent_job", observer)
    job = EventValueJob.perform_later(42)
    _(@performer.perform_one.state).must_equal "succeeded"
    _(@store.find(job.provider_job_id)["state"]).must_equal "succeeded"
  ensure
    ActiveSupport::Notifications.unsubscribe(subscription) if subscription
  end

  it "clears successful event fields if a finish observer is interrupted" do
    entered = Queue.new
    observed = nil
    observer = Object.new
    observer.define_singleton_method(:start) { |*| }

    observer.define_singleton_method(:finish) do |name, id, payload|
      observed = payload
      entered << true
      Queue.new.pop
    end

    subscription = ActiveSupport::Notifications.subscribe("perform_job.decent_job", observer)
    job = EventValueJob.perform_later(42)
    thread = Thread.new { @performer.perform_one }
    entered.pop
    thread.kill.join
    _(observed[:interrupted]).must_equal true
    _(observed).wont_include :value
    attempt = @store.executions(job.provider_job_id).first
    _(attempt["duration"]).must_be_nil
    _(attempt["finished_at"]).must_be_nil
    _(@performer.active?).must_equal false
  ensure
    thread&.kill&.join
    ActiveSupport::Notifications.unsubscribe(subscription) if subscription
  end
end
