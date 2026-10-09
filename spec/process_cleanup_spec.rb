# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

describe "bounded process cleanup" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    RecordingJob.queue_adapter = DecentJob::Adapter.new(database: database)
  end

  def owner(id = SecureRandom.uuid)
    database.with_connection do |connection|
      connection.exec_params("INSERT INTO decent_job_processes (id, heartbeat_mode, updated_at) VALUES ($1, 'heartbeat', clock_timestamp() - interval '1 hour')", [id])
    end
    id
  end

  def job(owner_id = nil)
    queued = RecordingJob.perform_later("cleanup")
    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET locked_by_id = $2 WHERE id = $1", [queued.provider_job_id, owner_id])
    end
    queued.provider_job_id
  end

  it "bounds waiter row locks, rolls back blocked release, and restores transaction timeouts" do
    process_id = owner
    abandoned = job(process_id)
    waiter = job
    database.with_connection do |connection|
      [abandoned, waiter].each do |id|
        execution = SecureRandom.uuid
        connection.exec_params("INSERT INTO decent_job_executions (id, job_id, process_id) VALUES ($1, $2, 1)", [execution, id])
        connection.exec_params("UPDATE decent_jobs SET execution_id = $2, scheduled_at = clock_timestamp() + interval '1 hour' WHERE id = $1", [id, execution])
        connection.exec_params("INSERT INTO decent_job_concurrency_claims (scope, job_id, execution_id, state) VALUES ('shared', $1, $2, $3)", [id, execution, (id == abandoned) ? "granted" : "waiting"])
      end
    end
    separate = database.connect
    separate.exec("BEGIN")
    separate.exec_params("SELECT id FROM decent_jobs WHERE id = $1 FOR UPDATE", [waiter])
    database.transaction do |connection|
      connection.exec("SET LOCAL statement_timeout = '500ms'")
      connection.exec("SET LOCAL lock_timeout = '0'")
      _(DecentJob::ProcessRecord.cleanup(database: database)).must_equal 0
      _(connection.exec("SHOW lock_timeout").getvalue(0, 0)).must_equal "0"
      _(connection.exec("SELECT state FROM decent_job_concurrency_claims ORDER BY state").column_values(0)).must_equal %w[granted waiting]
      _(DecentJob::Job.find(abandoned, database: database).locked_by_id).must_equal process_id
      connection.exec("SET LOCAL lock_timeout = '17ms'")
      _(DecentJob::ProcessRecord.cleanup(database: database)).must_equal 0
      _(connection.exec("SHOW lock_timeout").getvalue(0, 0)).must_equal "17ms"
    end
    separate.exec("ROLLBACK")
    _(DecentJob::ProcessRecord.cleanup(database: database)).must_equal 1
    database.with_connection do |connection|
      _(connection.exec("SELECT state FROM decent_job_concurrency_claims").column_values(0)).must_equal ["promoted"]
      _(connection.exec("SHOW lock_timeout").getvalue(0, 0)).must_equal "0"
    end
    _(DecentJob::Job.find(waiter, database: database).scheduled_at).must_be :<=, Time.now
  ensure
    separate&.finish unless separate&.finished?
  end

  it "advances past contended process and job pages" do
    busy = owner("00000000-0000-0000-0000-000000000001")
    free = owner("00000000-0000-0000-0000-000000000002")
    owned = 3.times.map { job(free) }.sort
    separate = database.connect
    separate.exec("BEGIN")
    separate.exec_params("SELECT id FROM decent_job_processes WHERE id = $1 FOR UPDATE", [busy])
    separate.exec_params("SELECT id FROM decent_jobs WHERE id = $1 FOR UPDATE", [owned.first])
    _(Timeout.timeout(3) { DecentJob::ProcessRecord.cleanup(database: database, in_batches_of: 1) }).must_equal 0
    _(DecentJob::Job.find(owned.first, database: database).locked_by_id).must_equal free
    owned.drop(1).each { |id| _(DecentJob::Job.find(id, database: database).locked_by_id).must_be_nil }
    separate.exec("ROLLBACK")
    _(DecentJob::ProcessRecord.cleanup(database: database, in_batches_of: 1)).must_equal 2
  ensure
    separate&.finish unless separate&.finished?
  end

  it "honors cancellation between owned jobs without deleting their process" do
    process_id = owner
    owned = 3.times.map { job(process_id) }.sort
    stopping = -> { DecentJob::Job.find(owned.first, database: database).locked_by_id.nil? }
    _(DecentJob::ProcessRecord.cleanup(database: database, in_batches_of: 1, stopping: stopping)).must_equal 0
    _(DecentJob::Job.find(owned.first, database: database).locked_by_id).must_be_nil
    owned.drop(1).each { |id| _(DecentJob::Job.find(id, database: database).locked_by_id).must_equal process_id }
    _(DecentJob::ProcessRecord.find(process_id, database: database).id).must_equal process_id
  end
end
