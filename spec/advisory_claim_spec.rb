# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

describe "advisory claim ownership" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    RecordingJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    RecordingJob.events = Queue.new
    @observer = database.connect
  end

  after do
    @thread&.kill
    @thread&.join(2)
    @observer&.finish unless @observer&.finished?
  end

  def lock_key(job)
    @observer.exec_params("SELECT hashtextextended(current_schema() || '.decent_jobs.' || $1, 0)", [job.provider_job_id]).getvalue(0, 0)
  end

  def lock_available?(key)
    acquired = @observer.exec_params("SELECT pg_try_advisory_lock($1::bigint)", [key]).getvalue(0, 0) == "t"
    @observer.exec_params("SELECT pg_advisory_unlock($1::bigint)", [key]) if acquired
    acquired
  end

  it "skips a caller-owned candidate without changing its session lock depth" do
    first = RecordingJob.perform_later("held")
    second = RecordingJob.perform_later("available")
    key = lock_key(first)
    claimed = []

    database.with_connection do |connection|
      2.times { connection.exec_params("SELECT pg_advisory_lock($1::bigint)", [key]) }

      result = DecentJob::Claim.new(database).with_next do |row|
        claimed << row.fetch("id")
        :claimed
      end

      _(result).must_equal :claimed
      _(claimed).must_equal [second.provider_job_id]
      _(DecentJob::Store.new(database).executions(first.provider_job_id)).must_be_empty
      _(lock_available?(key)).must_equal false
      connection.exec_params("SELECT pg_advisory_unlock($1::bigint)", [key])
      _(lock_available?(key)).must_equal false
      connection.exec_params("SELECT pg_advisory_unlock($1::bigint)", [key])
      _(lock_available?(key)).must_equal true
    end
  end

  it "lets a nested claim select another job while preserving the active attempt" do
    first = RecordingJob.perform_later("outer")
    second = RecordingJob.perform_later("inner")
    claim = DecentJob::Claim.new(database)
    outer_id = nil
    nested = nil
    held = nil

    claim.with_next(job_id: first.provider_job_id) do |outer|
      outer_id = outer.fetch("execution_id")
      nested = claim.with_next { |inner| inner.fetch("id") }
      held = !lock_available?(lock_key(first))
    end

    _(nested).must_equal second.provider_job_id
    _(held).must_equal true
    executions = DecentJob::Store.new(database).executions(first.provider_job_id)
    _(executions.map { |row| [row.fetch("id"), row.fetch("state")] }).must_equal [[outer_id, "running"]]
    _(lock_available?(lock_key(first))).must_equal true
  end

  it "rechecks completion after candidate selection before creating an attempt" do
    job = RecordingJob.perform_later("finished")
    selected = false

    stopping = lambda do
      unless selected
        @observer.exec_params("UPDATE decent_jobs SET finished_at = clock_timestamp() WHERE id = $1", [job.provider_job_id])
        selected = true
      end

      false
    end

    yielded = []
    result = DecentJob::Claim.new(database).with_next(stopping: stopping) { |row| yielded << row }
    _(result).must_be_nil
    _(yielded).must_be_empty
    _(DecentJob::Store.new(database).executions(job.provider_job_id)).must_be_empty
    _(lock_available?(lock_key(job))).must_equal true
  end

  it "retires an uncertain session when an exception interrupts acquisition's return" do
    job = RecordingJob.perform_later("recoverable")
    acquired = Queue.new
    error = Class.new(StandardError).new("interrupted acquisition")
    original_pid = nil

    database.with_connection do |connection|
      original_pid = connection.backend_pid
      connection.singleton_class.alias_method :original_exec_params, :exec_params

      connection.define_singleton_method(:exec_params) do |sql, *arguments|
        result = original_exec_params(sql, *arguments)

        if sql.include?("pg_try_advisory_lock")
          acquired << true
          Queue.new.pop
        end

        result
      end
    end

    pool = database
    caught = nil

    @thread = Thread.new do
      DecentJob::Claim.new(pool).with_next { :unexpected }
    rescue => exception
      caught = exception
    end

    Timeout.timeout(5) { acquired.pop }
    @thread.raise(error)
    _(@thread.join(2)).must_equal @thread
    _(caught).must_equal error
    _(lock_available?(lock_key(job))).must_equal true
    pool.with_connection { |connection| _(connection.backend_pid).wont_equal original_pid }
    _(DecentJob::Claim.new(pool).with_next(job_id: job.provider_job_id) { :recovered }).must_equal :recovered
  end

  it "keeps caller-owned candidates within the configured selection bound" do
    first = RecordingJob.perform_later("held")
    second = RecordingJob.perform_later("outside limit")
    key = lock_key(first)
    configuration = DecentJob.configuration
    previous = configuration.queue_select_limit
    configuration.queue_select_limit = 1

    database.with_connection do |connection|
      connection.exec_params("SELECT pg_advisory_lock($1::bigint)", [key])
      yielded = []
      result = DecentJob::Claim.new(database).with_next { |row| yielded << row }
      _(result).must_be_nil
      _(yielded).must_be_empty
      configuration.queue_select_limit = 0
      _(DecentJob::Claim.new(database).with_next(job_id: first.provider_job_id) { :unexpected }).must_be_nil
      _(DecentJob::Claim.new(database).with_next(job_id: second.provider_job_id) { :targeted }).must_equal :targeted
      connection.exec_params("SELECT pg_advisory_unlock($1::bigint)", [key])
      _(lock_available?(key)).must_equal true
    end
  ensure
    configuration.queue_select_limit = previous if configuration
  end

  it "releases only its acquired lock after a nonlocal exit" do
    job = RecordingJob.perform_later("exit")
    unrelated = lock_key(RecordingJob.perform_later("caller lock"))

    database.with_connection do |connection|
      connection.exec_params("SELECT pg_advisory_lock($1::bigint)", [unrelated])

      value = catch(:leave) do
        DecentJob::Claim.new(database).with_next(job_id: job.provider_job_id) { throw :leave, :escaped }
      end

      _(value).must_equal :escaped
      _(lock_available?(lock_key(job))).must_equal true
      _(lock_available?(unrelated)).must_equal false
      connection.exec_params("SELECT pg_advisory_unlock($1::bigint)", [unrelated])
      _(lock_available?(unrelated)).must_equal true
    end
  end

  it "releases its lock if process registration fails before starting" do
    job = RecordingJob.perform_later("registration failure")
    error = Class.new(StandardError).new("registration failed")
    tracker = Object.new
    tracker.define_singleton_method(:register) { |**| raise error }
    caught = _ { DecentJob::Claim.new(database, process_tracker: tracker).with_next { :unexpected } }.must_raise error.class
    _(caught).must_equal error
    _(lock_available?(lock_key(job))).must_equal true
    _(DecentJob::Store.new(database).executions(job.provider_job_id)).must_be_empty
  end

  it "rolls back a failed start and releases its lock" do
    job = RecordingJob.perform_later("start failure")

    database.with_connection do |connection|
      connection.exec("ALTER TABLE decent_job_executions ADD CONSTRAINT reject_attempt CHECK (false)")
    end

    _ { DecentJob::Claim.new(database).with_next { :unexpected } }.must_raise PG::CheckViolation
    _(lock_available?(lock_key(job))).must_equal true
    _(DecentJob::Store.new(database).find(job.provider_job_id).fetch("state")).must_equal "ready"
    _(DecentJob::Store.new(database).executions(job.provider_job_id)).must_be_empty
  end

  it "skips a deleted candidate and can claim the next selected job" do
    first = RecordingJob.perform_later("deleted")
    second = RecordingJob.perform_later("available")
    selected = false

    stopping = lambda do
      unless selected
        @observer.exec_params("DELETE FROM decent_jobs WHERE id = $1", [first.provider_job_id])
        selected = true
      end

      false
    end

    ids = []
    DecentJob::Claim.new(database).with_next(stopping: stopping) { |row| ids << row.fetch("id") }
    _(ids).must_equal [second.provider_job_id]
    _(lock_available?(lock_key(first))).must_equal true
    _(lock_available?(lock_key(second))).must_equal true
  end

  it "preserves the original exception when connection loss makes unlocking fail" do
    job = RecordingJob.perform_later("connection lost")
    original_pid = nil
    error = Class.new(StandardError).new("original failure")

    caught = _ do
      DecentJob::Claim.new(database).with_next do
        database.with_connection do |connection|
          original_pid = connection.backend_pid
          @observer.exec_params("SELECT pg_terminate_backend($1)", [original_pid])
        end

        raise error
      end
    end.must_raise error.class

    _(caught).must_equal error
    _(lock_available?(lock_key(job))).must_equal true
    database.with_connection { |connection| _(connection.backend_pid).wont_equal original_pid }
    executions = DecentJob::Store.new(database).executions(job.provider_job_id)
    _(executions.map { |row| row.fetch("state") }).must_equal ["running"]
    _(executions.first.fetch("finished_at")).must_be_nil
    _(DecentJob::Performer.new(database: database).perform_one(job_id: job.provider_job_id).state).must_equal "succeeded"
    _(DecentJob::Store.new(database).executions(job.provider_job_id).map { |row| row.fetch("state") }).must_equal ["interrupted", "succeeded"]
  end

  it "propagates unlock failure when a successful claim runs inside an outer rescue" do
    job = RecordingJob.perform_later("connection lost after success")
    original_pid = nil

    begin
      raise "previously handled failure"
    rescue RuntimeError
      _ do
        DecentJob::Claim.new(database).with_next(job_id: job.provider_job_id) do
          database.with_connection do |connection|
            original_pid = connection.backend_pid
            @observer.exec_params("SELECT pg_terminate_backend($1)", [original_pid])
          end

          :completed
        end
      end.must_raise PG::ConnectionBad
    end

    _(lock_available?(lock_key(job))).must_equal true
    database.with_connection { |connection| _(connection.backend_pid).wont_equal original_pid }
  end
end
