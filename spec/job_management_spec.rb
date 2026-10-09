# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

class ManagementRetryJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::Concurrency

  class_attribute :on_enqueue, :on_perform, :before_perform_action
  before_enqueue { on_enqueue&.call(self) }
  before_perform(prepend: true) { before_perform_action&.call(self) }
  def perform(*)
    on_perform&.call(self)
  end
end

class ManagementChildJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::Concurrency

  def perform
  end
end

describe "job management callbacks and transactions" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)
    [ManagementRetryJob, ManagementChildJob, RecordingJob].each { |klass| klass.queue_adapter = @adapter }
    ManagementRetryJob.on_enqueue = nil
    ManagementRetryJob.on_perform = nil
    ManagementRetryJob.before_perform_action = nil
    ManagementRetryJob.decent_job_concurrency_config = {}
    ManagementRetryJob.decent_job_concurrency_rules = []
    ManagementChildJob.decent_job_concurrency_config = {}
    ManagementChildJob.decent_job_concurrency_rules = []
    @performer = DecentJob::Performer.new(database: database)
    @store = DecentJob::Store.new(database)
  end

  def new_job
    aj = ManagementRetryJob.perform_later("original")
    DecentJob::Job.find(aj.provider_job_id, database: database)
  end

  def discarded_job
    new_job.tap { |job| job.discard_job("previous failure") }
  end

  it "captures callback queue, priority, labels, arguments and locale before Active Job restores them" do
    job = discarded_job
    database.with_connection { |connection| connection.exec_params("UPDATE decent_jobs SET payload = jsonb_set(payload, '{locale}', '\"fr\"') WHERE id = $1", [job.id]) }
    available = I18n.available_locales
    I18n.available_locales = [:en, :fr]
    calls = []
    errors = []
    ManagementRetryJob.on_enqueue = lambda do |aj|
      calls << I18n.locale
      aj.queue_name = "manual"
      aj.priority = 21
      aj.arguments = ["changed"]
      aj.decent_job_labels = ["retried"]
    end
    subscriber = ->(_name, _start, _finish, _id, event) { errors << event[:error] }
    ActiveSupport::Notifications.subscribed(subscriber, "enqueue_retry.active_job") { job.retry_job }
    _(calls).must_equal [:fr]
    _(I18n.locale).must_equal :en
    _(errors.first).must_be_kind_of DecentJob::Job::DiscardJobError
    _(errors.first.message).must_equal "previous failure"
    _(job.queue_name).must_equal "manual"
    _(job.priority).must_equal 21
    _(job.labels).must_equal ["retried"]
    _(job.job_instance.arguments).must_equal ["changed"]
  ensure
    I18n.available_locales = available
  end

  it "returns false and preserves discarded state when an enqueue callback aborts" do
    job = discarded_job
    original = job.attributes
    ManagementRetryJob.on_enqueue = ->(_) { throw :abort }
    _(job.retry_job).must_equal false
    _(job.reload.attributes).must_equal original
  end

  it "rejects missing classes on retry but discards and destroys them" do
    job = new_job
    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET job_class = 'RemovedManagementJob', payload = jsonb_set(payload, '{job_class}', '\"RemovedManagementJob\"') WHERE id = $1", [job.id])
    end
    job.reload
    _(job.job_instance(ignore_deserialization_errors: true)).must_be_nil
    _(proc { job.job_instance }).must_raise DecentJob::UnknownJobClassError
    job.discard_job("removed")
    _(proc { job.retry_job }).must_raise DecentJob::Job::DeserializationError
    _(job.reload.discarded?).must_equal true
    job.destroy_job
  end

  it "retries invalid arguments from the stored serialization even with labels and concurrency extensions" do
    job = new_job
    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET payload = jsonb_set(payload, '{arguments}', '[{\"_aj_serialized\":\"RemovedArgumentSerializer\"}]') WHERE id = $1", [job.id])
    end
    job.reload
    _(proc { job.job_instance }).must_raise ActiveJob::DeserializationError
    _(job.job_instance(ignore_deserialization_errors: true)).must_be_kind_of ManagementRetryJob
    job.discard_job("invalid arguments")
    _(job.retry_job.successfully_enqueued?).must_equal true
    _(job.payload.fetch("arguments")).must_equal [{"_aj_serialized" => "RemovedArgumentSerializer"}]
    _(job.executions).must_equal []
  end

  [:non_decent_job, :different_database].each do |kind|
    it "rejects a #{kind} adapter before enqueue callbacks or writes" do
      job = discarded_job
      calls = []
      ManagementRetryJob.on_enqueue = ->(_) { calls << true }
      other = DecentJob::Database.new(url: @connection_options)
      ManagementRetryJob.queue_adapter = (kind == :non_decent_job) ? :test : DecentJob::Adapter.new(database: other)
      _(proc { job.retry_job }).must_raise DecentJob::Job::AdapterMismatchError
      _(calls).must_equal []
      _(job.reload.discarded?).must_equal true
    ensure
      other&.close
    end
  end

  it "bypasses same-job admission while admitting callback-created children normally" do
    job = discarded_job
    ManagementRetryJob.decent_job_control_concurrency_with(key: "parent", enqueue_limit: 0)
    ManagementChildJob.decent_job_control_concurrency_with(key: "child", enqueue_limit: 0)
    child = nil
    ManagementRetryJob.on_enqueue = ->(_) { child = ManagementChildJob.perform_later }
    _(job.retry_job.successfully_enqueued?).must_equal true
    _(child.successfully_enqueued?).must_equal false
    _(child.provider_job_id).must_be_nil
    _(job.reload.finished?).must_equal false
    _(database.with_connection { |connection| connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0) }).must_equal "1"
  end

  it "flushes independently of ambient bulk and excludes ambient cron and batch metadata from children" do
    job = discarded_job
    batch = DecentJob::Batch.new(database: database).tap(&:save)
    child = nil
    ManagementRetryJob.on_enqueue = ->(_) { child = ManagementChildJob.perform_later }
    captured = DecentJob::Bulk.capture do
      DecentJob::CronEntry.with_context(database: database, key: "ambient", at: Time.now) do
        DecentJob::Batch.within_thread(database: database, batch_id: batch.id) { job.retry_job }
      end
      _(job.reload.finished?).must_equal false
      _(child.provider_job_id).wont_be_nil
    end
    _(captured).must_equal []
    child_row = @store.find(child.provider_job_id)
    _(child_row.values_at("cron_key", "cron_at", "batch_id", "batch_callback_id")).must_equal [nil, nil, nil, nil]
  end

  it "rolls parent and callback child writes back with the outer transaction" do
    job = discarded_job
    ManagementRetryJob.on_enqueue = ->(_) { ManagementChildJob.perform_later }
    _(proc do
      database.transaction do
        job.retry_job
        raise "rollback"
      end
    end).must_raise RuntimeError
    _(job.reload.discarded?).must_equal true
    _(database.with_connection { |connection| connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0) }).must_equal "1"
    _(job.executions).must_equal []
  end

  it "does not write captured callback children into another database" do
    job = discarded_job
    other = DecentJob::Database.new(url: @connection_options)
    ManagementChildJob.queue_adapter = DecentJob::Adapter.new(database: other)
    ManagementRetryJob.on_enqueue = ->(_) { ManagementChildJob.perform_later }
    _(proc { job.retry_job }).must_raise ArgumentError
    _(job.reload.discarded?).must_equal true
    _(database.with_connection { |connection| connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0) }).must_equal "1"
  ensure
    other&.close
  end

  it "executes inline retries only after commit and restores ambient context" do
    job = discarded_job
    events = []
    ManagementRetryJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :inline)
    ManagementRetryJob.on_perform = ->(_) { events << [DecentJob::Current.execution[:manual_retry], DecentJob::Bulk.current_buffer, DecentJob::CronEntry.current_context, DecentJob::Batch.current_context] }
    outer = {database: database, job_id: "ambient"}
    DecentJob::Current.with(outer) do
      database.transaction do
        job.retry_job
        _(events).must_equal []
        _(DecentJob::Current.execution).must_equal outer
      end
      _(DecentJob::Current.execution).must_equal outer
    end
    _(events).must_equal [[nil, nil, nil, nil]]
    _(job.reload.succeeded?).must_equal true
  end

  it "does not release an advisory lock already held by the same connection" do
    previous = DecentJob.configuration.lock_strategy
    DecentJob.configuration.lock_strategy = :advisory
    job = new_job
    DecentJob::Claim.new(database).with_next do
      _(proc { job.discard_job("ordinary") }).must_raise DecentJob::Job::LockedError
      separate = database.connect
      key = separate.exec_params("SELECT hashtextextended(current_schema() || '.decent_jobs.' || $1, 0)", [job.id]).getvalue(0, 0)
      _(separate.exec_params("SELECT pg_try_advisory_lock($1::bigint)", [key]).getvalue(0, 0)).must_equal "f"
      separate.finish
    end
  ensure
    DecentJob.configuration.lock_strategy = previous
  end
  it "clears accepted return values and child identifiers if the surrounding transaction rolls back" do
    job = discarded_job
    child = nil
    result = nil
    ManagementRetryJob.on_enqueue = ->(_) { child = ManagementChildJob.perform_later }
    _(proc do
      database.transaction do
        result = job.retry_job
        raise "rollback"
      end
    end).must_raise RuntimeError
    _(result.successfully_enqueued?).must_equal false
    _(result.provider_job_id).must_equal job.id
    _(child.successfully_enqueued?).must_equal false
    _(child.provider_job_id).must_be_nil
  end

  it "rejects a row lock conflict without waiting or releasing the conflicting transaction" do
    job = new_job
    separate = database.connect
    separate.exec("BEGIN")
    separate.exec_params("SELECT id FROM decent_jobs WHERE id = $1 FOR UPDATE", [job.id])
    release = Thread.new {
      sleep 0.2
      separate.exec("ROLLBACK")
    }
    _(proc { job.reschedule_job }).must_raise DecentJob::Job::LockedError
  ensure
    release&.join
    separate&.finish
  end

  it "does not let a batch retry resurrect a force-discarded attempt still executing" do
    started = Queue.new
    release = Queue.new
    ManagementRetryJob.on_perform = ->(_) {
      started << true
      release.pop
    }
    batch = DecentJob::Batch.enqueue([ManagementRetryJob.new], database: database)
    member = batch.job_instances.first
    job = DecentJob::Job.find(member.provider_job_id, database: database)
    worker = Thread.new { @performer.perform_one }
    Timeout.timeout(3) { started.pop }
    job.force_discard_job("stop")
    _(job.reload.error.fetch("force_discarded", false)).must_equal true
    _(job.executions.first.fetch("error").fetch("force_discarded", false)).must_equal true
    _(DecentJob::BatchStore.new(database).retry_members(batch.id)).must_equal []
    _(job.reload.discarded?).must_equal true
    release << true
    _(worker.value.state).must_equal "discarded"
    _(DecentJob::BatchStore.new(database).retry_members(batch.id)).must_equal [job.id]
  ensure
    release << true
    worker&.value
  end

  it "releases a stale facade's current grants and promotes waiters during force-discard" do
    started = Queue.new
    release = Queue.new
    ManagementRetryJob.decent_job_control_concurrency_with(key: "force-scope", perform_limit: 1)
    ManagementRetryJob.on_perform = ->(_) {
      started << true
      release.pop
    }
    first = new_job
    second = new_job
    worker = Thread.new { @performer.perform_one(job_id: first.id) }
    Timeout.timeout(3) { started.pop }
    _(@performer.perform_one(job_id: second.id).state).must_equal "retried"
    _(second.reload.retried?).must_equal true
    _(second.scheduled?).must_equal false
    first.force_discard_job("release capacity")
    rows = database.with_connection { |connection| connection.exec("SELECT job_id, state FROM decent_job_concurrency_claims").to_a }
    _(rows).must_equal [{"job_id" => second.id, "state" => "promoted"}]
    _(second.reload.status).must_equal :queued
    release << true
    _(worker.value.state).must_equal "discarded"
    release << true
    _(@performer.perform_one(job_id: second.id).state).must_equal "succeeded"
  ensure
    release << true
    worker&.value
  end

  it "does not acquire new concurrency grants after a force-discard before perform callbacks complete" do
    before = Queue.new
    proceed = Queue.new
    started = Queue.new
    release = Queue.new
    ManagementRetryJob.decent_job_control_concurrency_with(key: "force-scope", perform_limit: 1)
    ManagementRetryJob.before_perform_action = ->(_) {
      before << true
      proceed.pop
    }
    ManagementRetryJob.on_perform = ->(_) {
      started << true
      release.pop
    }
    job = new_job
    worker = Thread.new { @performer.perform_one(job_id: job.id) }
    Timeout.timeout(3) { before.pop }
    job.force_discard_job("stop before admission")
    proceed << true
    Timeout.timeout(3) { started.pop }
    count = database.with_connection { |connection| connection.exec("SELECT count(*) FROM decent_job_concurrency_claims").getvalue(0, 0) }
    _(count).must_equal "0"
  ensure
    proceed << true
    release << true
    worker&.value
  end

  it "requests durable batch completion in the discard transaction and reconciles only after commit" do
    batch = DecentJob::Batch.enqueue([ManagementRetryJob.new], database: database)
    job = DecentJob::Job.find(batch.job_instances.first.provider_job_id, database: database)
    database.transaction do |connection|
      job.discard_job("batch member discarded")
      _(batch.reload.finished?).must_equal false
      _(connection.exec("SELECT count(*) FROM decent_job_batch_updates WHERE discarded").getvalue(0, 0)).must_equal "1"
    end
    _(batch.reload.finished?).must_equal true
    _(batch.discarded?).must_equal true
    _(job.executions).must_equal []
  end

  it "rechecks new claim scopes after waiting for a force-discard row lock" do
    job = new_job
    execution_id = SecureRandom.uuid
    separate = database.connect
    separate.exec("BEGIN")
    separate.exec_params("UPDATE decent_jobs SET state = 'running', execution_id = $2 WHERE id = $1", [job.id, execution_id])
    separate.exec_params("INSERT INTO decent_job_executions (id, job_id, process_id) VALUES ($1, $2, $3)", [execution_id, job.id, Process.pid])
    database.with_connection do |connection|
      connection.exec(<<~SQL)
        CREATE FUNCTION require_claim_scope_lock() RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE key bigint := hashtextextended(current_schema() || '.decent_job.concurrency.' || OLD.scope, 0);
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND granted
            AND pid = pg_backend_pid() AND objsubid = 1
            AND classid = ((key >> 32) & 4294967295)::oid AND objid = (key & 4294967295)::oid) THEN
            RAISE EXCEPTION 'Claim deleted without owning its scope lock';
          END IF;
          RETURN OLD;
        END $$;
        CREATE TRIGGER require_claim_scope_lock BEFORE DELETE ON decent_job_concurrency_claims
        FOR EACH ROW EXECUTE FUNCTION require_claim_scope_lock();
      SQL
    end
    worker = Thread.new { job.force_discard_job("concurrent grant") }
    Timeout.timeout(3) do
      loop do
        waiting = @admin.exec_params("SELECT 1 FROM pg_stat_activity WHERE wait_event = 'transactionid' AND query LIKE $1", ["%decent_jobs WHERE id =%FOR UPDATE%"]).ntuples.positive?
        break if waiting
        sleep 0.01
      end
    end
    separate.exec("SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || '.decent_job.concurrency.key:new-scope', 0))")
    separate.exec_params("INSERT INTO decent_job_concurrency_claims (scope, job_id, execution_id, state) VALUES ('key:new-scope', $1, $2, 'granted')", [job.id, execution_id])
    separate.exec("COMMIT")
    Timeout.timeout(3) { worker.value }
    _(job.reload.discarded?).must_equal true
    _(database.with_connection { |connection| connection.exec("SELECT count(*) FROM decent_job_concurrency_claims").getvalue(0, 0) }).must_equal "0"
  ensure
    separate&.exec("ROLLBACK") if separate && separate.transaction_status != PG::PQTRANS_IDLE
    separate&.finish
    worker&.value
  end
  it "starts async retry execution after the surrounding transaction commits" do
    job = discarded_job
    events = Queue.new
    adapter = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    ManagementRetryJob.queue_adapter = adapter
    ManagementRetryJob.on_perform = ->(_) { events << true }
    database.transaction do
      job.retry_job
      _(events.empty?).must_equal true
      _(job.executions).must_equal []
    end
    _(Timeout.timeout(3) { events.pop }).must_equal true
    _(adapter.shutdown(timeout: 3)).must_equal true
    _(job.reload.succeeded?).must_equal true
  ensure
    adapter&.shutdown(timeout: 3)
  end
  it "clears captured child success when retry cannot acquire the parent row lock" do
    job = discarded_job
    child = nil
    ManagementRetryJob.on_enqueue = ->(_) { child = ManagementChildJob.perform_later }
    separate = database.connect
    separate.exec("BEGIN")
    separate.exec_params("SELECT id FROM decent_jobs WHERE id = $1 FOR UPDATE", [job.id])
    _(proc { job.retry_job }).must_raise DecentJob::Job::LockedError
    _(child.successfully_enqueued?).must_equal false
    _(child.provider_job_id).must_be_nil
    _(job.reload.discarded?).must_equal true
    _(database.with_connection { |connection| connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0) }).must_equal "1"
  ensure
    separate&.exec("ROLLBACK")
    separate&.finish
  end

  it "clears provisional success on metadata failure without changing already persisted children" do
    job = discarded_job
    existing = ManagementChildJob.perform_later
    existing_id = existing.provider_job_id
    child = nil
    ManagementRetryJob.on_enqueue = lambda do |_|
      DecentJob::Bulk.capture(existing)
      child = ManagementChildJob.perform_later
    end
    # Rule metadata resolves after bulk capture; legacy keys resolve in the
    # initial enqueue callback, before provisional success can be recorded.
    ManagementChildJob.decent_job_concurrency_rule(key: -> { raise "metadata failed" }, enqueue_limit: 1)
    _(proc { job.retry_job }).must_raise RuntimeError
    _(child.successfully_enqueued?).must_equal false
    _(child.provider_job_id).must_be_nil
    _(existing.successfully_enqueued?).must_equal true
    _(existing.provider_job_id).must_equal existing_id
    _(job.reload.discarded?).must_equal true
  end

  it "clears provisional success when preparation rejects a later callback child" do
    job = discarded_job
    child = nil
    other = DecentJob::Database.new(url: @connection_options)
    RecordingJob.queue_adapter = DecentJob::Adapter.new(database: other)
    ManagementRetryJob.on_enqueue = lambda do |_|
      child = ManagementChildJob.perform_later
      RecordingJob.perform_later("wrong database")
    end
    _(proc { job.retry_job }).must_raise ArgumentError
    _(child.successfully_enqueued?).must_equal false
    _(child.provider_job_id).must_be_nil
    _(job.reload.discarded?).must_equal true
  ensure
    other&.close
  end

  it "emits one enqueue event for the final management retry write after callbacks" do
    job = discarded_job
    order = []
    ManagementRetryJob.on_enqueue = ->(_) { order << :callback }
    observed = nil
    observer = lambda do |event|
      order << :event
      observed = event.payload
    end
    retried = nil
    ActiveSupport::Notifications.subscribed(observer, "enqueue_job.decent_job") { retried = job.retry_job }
    _(order).must_equal [:callback, :event]
    _(observed[:job].id).must_equal job.id
    _(observed[:job].state).must_equal "ready"
    _(observed[:job_instance]).must_be_kind_of ManagementRetryJob
    _(observed[:accepted]).must_equal true
    _(retried.provider_job_id).must_equal job.id
  end

  it "emits management retry snapshots from prepared payloads with missing arguments" do
    job = discarded_job
    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET payload = jsonb_set(payload, '{arguments}', '[{\"_aj_serialized\":\"RemovedArgumentSerializer\"}]') WHERE id = $1", [job.id])
    end
    events = []
    observer = ->(event) { events << event }
    ActiveSupport::Notifications.subscribed(observer, "enqueue_job.decent_job") { job.retry_job }
    _(events.size).must_equal 1
    _(events.first.payload[:job].payload.fetch("arguments")).must_equal [{"_aj_serialized" => "RemovedArgumentSerializer"}]
    _(events.first.payload[:accepted]).must_equal true
  end

  it "omits management retry events when callbacks veto and retains events for rolled back attempts" do
    job = discarded_job
    events = []
    observer = ->(event) { events << event }
    ActiveSupport::Notifications.subscribed(observer, "enqueue_job.decent_job") do
      ManagementRetryJob.on_enqueue = ->(_) { throw :abort }
      _(job.retry_job).must_equal false
      _(events).must_be_empty
      ManagementRetryJob.on_enqueue = nil
      assert_raises(RuntimeError) do
        database.transaction do
          job.retry_job
          raise "rollback retry"
        end
      end
    end
    _(events.size).must_equal 1
    _(events.first.payload[:accepted]).must_equal true
    _(job.reload.discarded?).must_equal true
  end
end
