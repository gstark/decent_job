# frozen_string_literal: true

require "spec_helper"
require "timeout"

class ClaimParityJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::Concurrency

  class_attribute :started, :release

  def perform(mode)
    if mode == "holder"
      started << true
      release.pop
    end
  end
end

describe "durable concurrency claim parity" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    ClaimParityJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    ClaimParityJob.decent_job_control_concurrency_with(key: "claim-parity", perform_limit: 1)
    ClaimParityJob.started = Queue.new
    ClaimParityJob.release = Queue.new
    @performer = DecentJob::Performer.new(database: database)
    @store = DecentJob::Store.new(database)
  end

  after do
    ClaimParityJob.release << true
    Timeout.timeout(5) { @worker&.value }
    @listener&.finish
    ClaimParityJob.decent_job_concurrency_config = {}
  end

  def claim_row(job)
    database.with_connection do |connection|
      rows = connection.exec_params("SELECT * FROM decent_job_concurrency_claims WHERE job_id = $1", [job.provider_job_id]).to_a
      _(rows.size).must_equal 1
      rows.first
    end
  end

  # GoodJob ad4be5c2 concurrency_claim_spec.rb:33, reuses its own grant and
  # refreshes locked_by_id. DecentJob stores authoritative execution identity
  # instead, with the process UUID retained by the job and attempt records.
  it "reuses a grant and refreshes ownership when an unfinished job is claimed again" do
    job = ClaimParityJob.perform_later("ordinary")
    controls = DecentJob::PerformControls.new(database)
    claimer = DecentJob::Claim.new(database)
    registry = DecentJob::ConcurrencyClaims.new(database)
    first_context = nil
    original = nil
    claimer.with_next(job_id: job.provider_job_id) do |row|
      first_context = {id: row.fetch("id"), execution_id: row.fetch("execution_id")}
      controls.check(job, first_context)
      original = claim_row(job)
      controls.check(job, first_context)
      reused = claim_row(job)
      _(reused.values_at("state", "execution_id", "created_at")).must_equal original.values_at("state", "execution_id", "created_at")
    end

    # Claim again directly before idle recovery removes the abandoned grant.
    # Both attempts and their execution ownership come from the normal claimer.
    claimer.with_next(job_id: job.provider_job_id) do |row|
      context = {id: row.fetch("id"), execution_id: row.fetch("execution_id")}
      _(context.fetch(:execution_id)).wont_equal first_context.fetch(:execution_id)
      controls.check(job, context)
      refreshed = claim_row(job)
      _(refreshed.fetch("execution_id")).must_equal context.fetch(:execution_id)
      _(refreshed.fetch("created_at")).must_equal original.fetch("created_at")
      _(Time.parse(refreshed.fetch("updated_at"))).must_be :>, Time.parse(original.fetch("updated_at"))
      _(refreshed.fetch("state")).must_equal "granted"
      _(DecentJob::Job.find(job.provider_job_id, database: database).execution_id).must_equal context.fetch(:execution_id)
      registry.synchronize(job.provider_job_id) { |connection| registry.release(connection, first_context) }
      _(claim_row(job).fetch("execution_id")).must_equal context.fetch(:execution_id)
      registry.synchronize(job.provider_job_id) { |connection| registry.release(connection, context) }
    end
  end

  # GoodJob ad4be5c2 concurrency_claim_spec.rb:50,64,83: modification time,
  # oldest-waiter promotion, and skipping terminal waiters. Missing jobs cannot
  # retain claims in DecentJob because the claim's job FK deletes them.
  it "promotes the oldest valid waiter and updates modification time without resetting its age" do
    holder = ClaimParityJob.perform_later("holder")
    waiters = %w[finished oldest newest].map { |name| ClaimParityJob.perform_later(name) }
    @worker = Thread.new { @performer.perform_one(job_id: holder.provider_job_id) }
    Timeout.timeout(5) { ClaimParityJob.started.pop }
    waiters.each do |job|
      _(@performer.perform_one(job_id: job.provider_job_id).state).must_equal "retried"
      _(claim_row(job).fetch("state")).must_equal "waiting"
    end
    database.with_connection do |connection|
      waiters.each_with_index do |job, index|
        timestamp = Time.utc(2020, 1, 1) + index
        connection.exec_params("UPDATE decent_job_concurrency_claims SET created_at = $2, updated_at = $2 WHERE job_id = $1", [job.provider_job_id, timestamp.iso8601(6)])
        connection.exec_params("UPDATE decent_jobs SET scheduled_at = clock_timestamp() + interval '1 hour' WHERE id = $1", [job.provider_job_id])
      end
      # Preserve a stale claim like the reference fixture's direct model update.
      # Public discard eagerly cleans claims and would not exercise this case.
      connection.exec_params("UPDATE decent_jobs SET state = 'succeeded', finished_at = clock_timestamp() WHERE id = $1", [waiters.first.provider_job_id])
    end
    oldest = waiters[1]
    newest = waiters[2]
    before_claim = claim_row(oldest)
    before_job = @store.find(oldest.provider_job_id)
    @listener = database.connect
    @listener.exec("LISTEN decent_job")

    ClaimParityJob.release << true
    _(Timeout.timeout(5) { @worker.value }.state).must_equal "succeeded"

    promoted = claim_row(oldest)
    after_job = @store.find(oldest.provider_job_id)
    _(promoted.fetch("state")).must_equal "promoted"
    _(promoted.fetch("created_at")).must_equal before_claim.fetch("created_at")
    _(Time.parse(promoted.fetch("updated_at"))).must_be :>, Time.parse(before_claim.fetch("updated_at"))
    _(after_job.fetch("created_at")).must_equal before_job.fetch("created_at")
    _(after_job.fetch("updated_at")).must_be :>, before_job.fetch("updated_at")
    _(after_job.fetch("scheduled_at")).must_be :<=, Time.now
    _(claim_row(newest).fetch("state")).must_equal "waiting"
    _(@store.find(newest.provider_job_id).fetch("scheduled_at")).must_be :>, Time.now
    removed = [holder.provider_job_id, waiters.first.provider_job_id]
    database.with_connection do |connection|
      _(connection.exec_params("SELECT count(*) FROM decent_job_concurrency_claims WHERE job_id = ANY($1::uuid[])", [PG::TextEncoder::Array.new.encode(removed)]).getvalue(0, 0)).must_equal "0"
    end
    _(@listener.wait_for_notify(5)).must_equal "decent_job"
  end
end
