# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

describe "age cleanup" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)
    [RecordingJob, DiscardingJob].each { |klass| klass.queue_adapter = @adapter }
    RecordingJob.events = Queue.new
    @performer = DecentJob::Performer.new(database: database)
    @store = DecentJob::Store.new(database)
  end

  after { DecentJob.reset! }

  def finished_job(klass = RecordingJob, at: Time.now - 3600)
    job = (klass == RecordingJob) ? klass.perform_later("cleanup") : klass.perform_later
    @performer.perform_one(job_id: job.provider_job_id)
    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET finished_at = $2 WHERE id = $1", [job.provider_job_id, at.utc.iso8601(6)])
    end
    job
  end

  def clean(**options)
    DecentJob.cleanup_preserved_jobs(database: database, **options)
  end

  def age_batch(batch, discarded: false, finished: true)
    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_job_batches SET jobs_finished_at = now() - interval '1 hour', finished_at = CASE WHEN $2 THEN now() - interval '1 hour' END, discarded_at = CASE WHEN $3 THEN now() END WHERE id = $1", [batch.id, finished, discarded])
    end
  end

  it "deletes old finished jobs and their attempts and reports exact totals" do
    old = finished_job
    discarded = finished_job(DiscardingJob)
    recent = finished_job(at: Time.now + 60)
    unfinished = RecordingJob.perform_later
    payloads = []
    subscriber = ActiveSupport::Notifications.subscribe("cleanup_preserved_jobs.decent_job") { |event| payloads << event.payload }
    begin
      _(clean(older_than: 60, in_batches_of: 1)).must_equal 4
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber)
    end
    [old, discarded].each { |job| _(@store.find(job.provider_job_id)).must_be_nil }
    [recent, unfinished].each { |job| _(@store.find(job.provider_job_id)).wont_be_nil }
    payload = payloads.fetch(0)
    _(payload.slice(:destroyed_jobs_count, :destroyed_executions_count, :destroyed_batches_count, :destroyed_records_count)).must_equal({destroyed_jobs_count: 2, destroyed_executions_count: 2, destroyed_batches_count: 0, destroyed_records_count: 4})
    _(payload[:older_than]).must_equal 60
    _(payload[:timestamp]).must_be_kind_of Time
  end

  it "uses the configured age and discard policy" do
    successful = finished_job
    discarded = finished_job(DiscardingJob)
    _(clean).must_equal 0
    DecentJob.configuration.cleanup_preserved_jobs_before_seconds_ago = 60
    DecentJob.configuration.cleanup_discarded_jobs = false
    _(clean).must_equal 2
    _(@store.find(successful.provider_job_id)).must_be_nil
    _(@store.find(discarded.provider_job_id)).wont_be_nil
    _(clean(include_discarded: true)).must_equal 2
  end

  it "validates all cleanup arguments before obtaining a connection" do
    [-1, Float::NAN, Float::INFINITY, Complex(1, 1), "1", false].each { |value| _ { clean(older_than: value) }.must_raise ArgumentError }
    [0, -1, 1.5, "2", nil].each { |value| _ { clean(in_batches_of: value) }.must_raise ArgumentError }
    [nil, 1, "false"].each { |value| _ { clean(include_discarded: value) }.must_raise ArgumentError }
    _ { clean(stopping: false) }.must_raise ArgumentError
  end

  it "uses one cutoff and includes records exactly at it" do
    timestamp = Time.at(Time.now.to_i).utc
    job = finished_job(at: timestamp - 60)
    calls = []
    subscriber = ActiveSupport::Notifications.subscribe("cleanup_preserved_jobs.decent_job") { |event| calls << event.payload }
    original = Time.method(:now)
    Time.define_singleton_method(:now) { timestamp }
    begin
      _(clean(older_than: 60)).must_equal 2
      _(calls.fetch(0)[:timestamp]).must_equal timestamp - 60
    ensure
      Time.define_singleton_method(:now, original)
      ActiveSupport::Notifications.unsubscribe(subscriber)
    end
    _(@store.find(job.provider_job_id)).must_be_nil
  end

  it "progresses past a locked first page and skips ownership on the same connection" do
    jobs = 3.times.map { finished_job(at: Time.at(Time.now.to_i - 3600)) }.sort_by(&:provider_job_id)
    database.with_connection do |connection|
      key = connection.exec_params("SELECT #{DecentJob::ConcurrencyClaims::JOB_KEY_SQL} FROM decent_jobs WHERE id = $1", [jobs.first.provider_job_id]).getvalue(0, 0)
      connection.exec_params("SELECT pg_advisory_lock($1::bigint)", [key])
      begin
        _(Timeout.timeout(2) { clean(older_than: 60, in_batches_of: 1) }).must_equal 4
        _(@store.find(jobs.first.provider_job_id)).wont_be_nil
      ensure
        connection.exec_params("SELECT pg_advisory_unlock($1::bigint)", [key])
      end
    end
    _(clean(older_than: 60)).must_equal 2
  end

  it "skips row and scope contention without waiting and retries on a later sweep" do
    job = finished_job
    execution_id = @store.executions(job.provider_job_id).first.fetch("id")
    database.with_connection do |connection|
      connection.exec_params("INSERT INTO decent_job_concurrency_claims (scope, job_id, execution_id, state) VALUES ('busy', $1, $2, 'waiting')", [job.provider_job_id, execution_id])
    end
    other = database.connect
    begin
      other.exec("BEGIN")
      other.exec("SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || '.decent_job.concurrency.busy', 0))")
      _(Timeout.timeout(2) { clean(older_than: 60) }).must_equal 0
      other.exec("ROLLBACK")
      other.exec("BEGIN")
      other.exec_params("SELECT id FROM decent_jobs WHERE id = $1 FOR UPDATE", [job.provider_job_id])
      _(Timeout.timeout(2) { clean(older_than: 60) }).must_equal 0
      other.exec("ROLLBACK")
      _(clean(older_than: 60)).must_equal 2
    ensure
      other.finish
    end
  end

  it "checks stopping between records" do
    3.times { finished_job }
    calls = 0
    clean(older_than: 60, in_batches_of: 3, stopping: -> {
      calls += 1
      calls > 3
    })
    remaining = database.with_connection { |connection| connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0).to_i }
    _(remaining).must_be :>, 0
  end

  it "allows explicit cleanup to release cron deduplication history" do
    entry = DecentJob::CronEntry.new(key: "cleanup", cron: "@daily", class: RecordingJob, database: database)
    time = Time.utc(2026, 1, 1)
    job = entry.enqueue(time)
    @performer.perform_one(job_id: job.provider_job_id)
    _(entry.enqueue(time)).must_equal false
    _(clean(older_than: 0)).must_equal 2
    _(entry.enqueue(time)).must_be_kind_of RecordingJob
  end

  it "deletes completed batches and ancillary rows but retains incomplete and discarded batches when excluded" do
    completed = DecentJob::Batch.enqueue([], database: database)
    discarded = DecentJob::Batch.enqueue([], database: database)
    partial = DecentJob::Batch.enqueue([], database: database)
    age_batch(completed)
    age_batch(discarded, discarded: true)
    age_batch(partial, finished: false)
    database.with_connection do |connection|
      connection.exec_params("INSERT INTO decent_job_batch_updates (batch_id, generation) VALUES ($1, 0)", [completed.id])
    end
    _(clean(older_than: 60, include_discarded: false)).must_equal 1
    _(DecentJob::BatchStore.new(database).find(completed.id)).must_be_nil
    _(DecentJob::BatchStore.new(database).find(discarded.id)).wont_be_nil
    _(DecentJob::BatchStore.new(database).find(partial.id)).wont_be_nil
    database.with_connection { |connection| _(connection.exec("SELECT count(*) FROM decent_job_batch_updates").getvalue(0, 0)).must_equal "0" }
    _(clean(older_than: 60)).must_equal 1
  end

  it "retains batches with unfinished work or undispatched callbacks" do
    pending = DecentJob::Batch.enqueue([RecordingJob.new], database: database)
    callback = DecentJob::Batch.enqueue([], database: database)
    [pending, callback].each { |batch| age_batch(batch) }
    database.with_connection do |connection|
      connection.exec_params("INSERT INTO decent_job_batch_events (batch_id, generation, event, job_class) VALUES ($1, 0, 'finish', 'RecordingJob')", [callback.id])
    end
    _(clean(older_than: 60)).must_equal 0
  end

  it "skips a batch held by a generation change" do
    batch = DecentJob::Batch.enqueue([], database: database)
    age_batch(batch)
    other = database.connect
    begin
      other.exec("BEGIN")
      other.exec_params("SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || '.decent_job.batch.' || $1, 0))", [batch.id])
      _(Timeout.timeout(2) { clean(older_than: 60) }).must_equal 0
      other.exec_params("UPDATE decent_job_batches SET finished_at = NULL WHERE id = $1", [batch.id])
      other.exec("COMMIT")
      _(clean(older_than: 60)).must_equal 0
    ensure
      other.finish
    end
  end

  it "counts each record once with competing sweepers" do
    10.times { finished_job }
    threads = 2.times.map { Thread.new { clean(older_than: 60, in_batches_of: 2) } }
    _(threads.sum(&:value)).must_equal 20
    _(clean(older_than: 60)).must_equal 0
  end
  it "processes the oldest eligible jobs first when stopped early" do
    jobs = 2.times.map { finished_job }.sort_by(&:provider_job_id)
    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET finished_at = now() - interval '2 hours' WHERE id = $1", [jobs.last.provider_job_id])
    end
    _(clean(older_than: 60, in_batches_of: 1, stopping: -> { @store.find(jobs.last.provider_job_id).nil? })).must_equal 2
    _(@store.find(jobs.first.provider_job_id)).wont_be_nil
  end

  it "restores the caller's lock timeout and respects a tighter timeout" do
    job = finished_job
    database.transaction do |connection|
      connection.exec("SET LOCAL lock_timeout = '5ms'")
      connection.exec("CREATE TABLE observed_cleanup_timeout (value text)")
      connection.exec(<<~SQL)
        CREATE FUNCTION observe_cleanup_timeout() RETURNS trigger LANGUAGE plpgsql AS $$
          BEGIN INSERT INTO observed_cleanup_timeout VALUES (current_setting('lock_timeout')); RETURN OLD; END;
        $$;
        CREATE TRIGGER observe_cleanup_timeout BEFORE DELETE ON decent_jobs FOR EACH ROW EXECUTE FUNCTION observe_cleanup_timeout();
      SQL
      _(clean(older_than: 60)).must_equal 2
      _(connection.exec("SHOW lock_timeout").getvalue(0, 0)).must_equal "5ms"
      _(connection.exec("SELECT value FROM observed_cleanup_timeout").getvalue(0, 0)).must_equal "5ms"
      _(@store.find(job.provider_job_id)).must_be_nil
    end
    database.transaction do |connection|
      connection.exec("SET LOCAL lock_timeout = '2s'")
      finished_job
      clean(older_than: 60)
      _(connection.exec("SHOW lock_timeout").getvalue(0, 0)).must_equal "2s"
    end
  end

  it "deletes finished reopened batches despite their historical pending snapshots" do
    job = RecordingJob.new("member")
    batch = DecentJob::Batch.enqueue([job], database: database)
    batch.enqueue([])
    @performer.perform_one(job_id: job.provider_job_id)
    _(batch.reload.finished?).must_equal true
    age_batch(batch)
    database.with_connection do |connection|
      _(connection.exec_params("SELECT count(*) FROM decent_job_batch_pending_jobs WHERE batch_id = $1", [batch.id]).getvalue(0, 0)).must_equal "1"
    end
    _(clean(older_than: 60)).must_equal 1
    database.with_connection do |connection|
      _(connection.exec("SELECT count(*) FROM decent_job_batch_pending_jobs").getvalue(0, 0)).must_equal "0"
    end
  end

  it "honors the current attempt's stopping callback without globally disabling explicit cleanup" do
    finished_job
    DecentJob::Current.with(stopping: -> { true }) do
      _(clean(older_than: 60)).must_equal 0
    end
    DecentJob.shutdown
    _(clean(older_than: 60)).must_equal 2
  end

  it "removes orphan batch updates without counting them and skips locked orphan rows" do
    orphan = SecureRandom.uuid
    database.with_connection do |connection|
      connection.exec_params("INSERT INTO decent_job_batch_updates (batch_id, generation) VALUES ($1, 0), ($1, 1)", [orphan])
    end
    other = database.connect
    begin
      other.exec("BEGIN")
      other.exec("SELECT id FROM decent_job_batch_updates ORDER BY id LIMIT 1 FOR UPDATE")
      _(Timeout.timeout(2) { clean(in_batches_of: 1) }).must_equal 0
      database.with_connection do |connection|
        _(connection.exec("SELECT count(*) FROM decent_job_batch_updates").getvalue(0, 0)).must_equal "1"
      end
      other.exec("COMMIT")
      _(clean(in_batches_of: 1)).must_equal 0
      database.with_connection do |connection|
        _(connection.exec("SELECT count(*) FROM decent_job_batch_updates").getvalue(0, 0)).must_equal "0"
      end
    ensure
      other.finish
    end
  end

  def after_candidate_page(table, &callback)
    database.with_connection do |connection|
      original = connection.method(:exec_params)
      fired = false
      connection.define_singleton_method(:exec_params) do |sql, *arguments|
        result = original.call(sql, *arguments)
        if !fired && sql.include?("SELECT id, finished_at FROM #{table}")
          fired = true
          callback.call
        end
        result
      end
    end
  end

  it "rechecks a discarded job retried after its cleanup candidate was read" do
    job = finished_job(DiscardingJob)
    after_candidate_page("decent_jobs") do
      DecentJob::Job.find(job.provider_job_id, database: database).retry_job
    end
    _(clean(older_than: 60)).must_equal 0
    _(@store.find(job.provider_job_id).fetch("state")).must_equal "ready"
    _(@store.executions(job.provider_job_id).size).must_equal 1
  end

  it "rechecks members retried individually after their batch candidate was read" do
    job = DiscardingJob.new
    batch = DecentJob::Batch.enqueue([job], database: database)
    @performer.perform_one(job_id: job.provider_job_id)
    age_batch(batch)
    after_candidate_page("decent_job_batches") do
      DecentJob::Job.find(job.provider_job_id, database: database).retry_job
    end
    _(clean(older_than: 60)).must_equal 0
    _(batch.reload.finished?).must_equal true
    _(DecentJob::Job.find(job.provider_job_id, database: database).batch_id).must_equal batch.id
  end

  it "rechecks batch eligibility after the candidate's generation was reopened" do
    batch = DecentJob::Batch.enqueue([], database: database)
    age_batch(batch)
    after_candidate_page("decent_job_batches") { batch.enqueue([RecordingJob.new("reopened")]) }
    _(clean(older_than: 60)).must_equal 0
    _(batch.reload.finished?).must_equal false
    _(batch.job_instances.size).must_equal 1
  end

  it "retains force-discarded members and their batch while the owner still holds its session lock" do
    job = DiscardingJob.new
    batch = DecentJob::Batch.enqueue([job], database: database)
    @performer.perform_one(job_id: job.provider_job_id)
    age_batch(batch)
    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET finished_at = now() - interval '1 hour', error = jsonb_build_object('force_discarded', true) WHERE id = $1", [job.provider_job_id])
      key = connection.exec_params("SELECT #{DecentJob::ConcurrencyClaims::JOB_KEY_SQL} FROM decent_jobs WHERE id = $1", [job.provider_job_id]).getvalue(0, 0)
      connection.exec_params("SELECT pg_advisory_lock($1::bigint)", [key])
      begin
        _(clean(older_than: 60)).must_equal 0
        _(batch.reload.finished?).must_equal true
      ensure
        connection.exec_params("SELECT pg_advisory_unlock($1::bigint)", [key])
      end
    end
    _(clean(older_than: 60)).must_equal 3
  end

  it "rolls back cleanup when a foreign key side effect meets a busy event row" do
    job = finished_job
    batch = DecentJob::Batch.enqueue([], database: database)
    database.with_connection do |connection|
      connection.exec_params("INSERT INTO decent_job_batch_events (batch_id, generation, event, job_class, job_id, dispatched_at) VALUES ($1, 0, 'finish', 'RecordingJob', $2, now())", [batch.id, job.provider_job_id])
    end
    other = database.connect
    begin
      other.exec("BEGIN")
      other.exec("SELECT * FROM decent_job_batch_events FOR UPDATE")
      _(Timeout.timeout(2) { clean(older_than: 60) }).must_equal 0
      _(@store.find(job.provider_job_id)).wont_be_nil
      _(@store.executions(job.provider_job_id).size).must_equal 1
      other.exec("COMMIT")
      _(clean(older_than: 60)).must_equal 2
    ensure
      other.finish
    end
  end

  it "bounds busy waiter promotion and rolls the cleanup back before trying later" do
    owner = finished_job
    waiter = RecordingJob.perform_later("waiter")
    execution = @store.executions(owner.provider_job_id).first.fetch("id")
    database.with_connection do |connection|
      connection.exec_params("INSERT INTO decent_job_concurrency_claims (scope, job_id, execution_id, state) VALUES ('promotion', $1, $3, 'granted'), ('promotion', $2, $3, 'waiting')", [owner.provider_job_id, waiter.provider_job_id, execution])
    end
    other = database.connect
    begin
      other.exec("BEGIN")
      other.exec_params("SELECT id FROM decent_jobs WHERE id = $1 FOR UPDATE", [waiter.provider_job_id])
      _(Timeout.timeout(2) { clean(older_than: 60) }).must_equal 0
      _(@store.find(owner.provider_job_id)).wont_be_nil
      other.exec("COMMIT")
      _(clean(older_than: 60)).must_equal 2
      _(@store.find(waiter.provider_job_id).fetch("state")).must_equal "ready"
    ensure
      other.finish
    end
  end
  it "reports committed counts when a later cleanup candidate fails" do
    first = finished_job(at: Time.now - 7200)
    second = finished_job
    database.with_connection do |connection|
      connection.exec(<<~SQL)
        CREATE FUNCTION reject_second_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$
          BEGIN
            IF OLD.id = '#{second.provider_job_id}'::uuid THEN RAISE EXCEPTION 'later cleanup failed'; END IF;
            RETURN OLD;
          END;
        $$;
        CREATE TRIGGER reject_second_cleanup BEFORE DELETE ON decent_jobs FOR EACH ROW EXECUTE FUNCTION reject_second_cleanup();
      SQL
    end
    payloads = []
    subscriber = ActiveSupport::Notifications.subscribe("cleanup_preserved_jobs.decent_job") { |event| payloads << event.payload }
    begin
      _ { clean(older_than: 60) }.must_raise PG::RaiseException
      _(payloads.first.slice(:destroyed_jobs_count, :destroyed_executions_count, :destroyed_records_count)).must_equal({destroyed_jobs_count: 1, destroyed_executions_count: 1, destroyed_records_count: 2})
      _(@store.find(first.provider_job_id)).must_be_nil
      _(@store.find(second.provider_job_id)).wont_be_nil
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber)
    end
  end
  it "excludes retained discarded records before paging through cleanup candidates" do
    discarded = finished_job(DiscardingJob, at: Time.now - 7200)
    successful = finished_job
    checks = 0
    stopping = lambda do
      checks += 1
      checks > 2
    end
    _(clean(older_than: 60, include_discarded: false, in_batches_of: 1, stopping: stopping)).must_equal 2
    _(@store.find(discarded.provider_job_id)).wont_be_nil
    _(@store.find(successful.provider_job_id)).must_be_nil
  end
end
