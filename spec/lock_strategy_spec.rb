# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

describe "lock strategies" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    RecordingJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    RecordingJob.events = Queue.new
    @observer = database.connect
  end

  after do
    @threads&.each(&:kill)
    @threads&.each { |thread| thread.join(3) }
    @observer&.finish unless @observer&.finished?
    DecentJob.reset!
  end

  def advisory_locked?(id)
    @observer.exec_params("SELECT #{DecentJob::ConcurrencyClaims::JOB_LOCKED_SQL} FROM decent_jobs WHERE id = $1", [id]).getvalue(0, 0) == "t"
  end

  %i[advisory skiplocked hybrid].each do |strategy|
    it "persists #{strategy} ownership and releases it after a nonlocal exit without finishing the attempt" do
      DecentJob.configuration.lock_strategy = strategy
      job = RecordingJob.perform_later("owned")

      value = catch(:leave) do
        DecentJob::Claim.new(database).with_next do |row|
          _(row["lock_type"]).must_equal strategy.to_s
          _(row["locked_by_id"]).wont_be_nil
          _(DecentJob::Job.find(row.fetch("id"), database: database).running?).must_equal true
          _(advisory_locked?(row.fetch("id"))).must_equal(strategy != :skiplocked)
          _(database.stats.fetch(:active)).must_equal((strategy == :skiplocked) ? 0 : 1)

          %i[advisory skiplocked hybrid].each do |other|
            DecentJob.configuration.lock_strategy = other
            _(DecentJob::Claim.new(database).with_next(job_id: job.provider_job_id) { :stolen }).must_be_nil
          end

          throw :leave, :escaped
        end
      end

      _(value).must_equal :escaped
      row = DecentJob::Store.new(database).find(job.provider_job_id)
      _(row["locked_by_id"]).must_be_nil
      _(row["lock_type"]).must_be_nil
      _(row["state"]).must_equal "running"
      _(DecentJob::Performer.new(database: database).perform_one.state).must_equal "succeeded"
      _(DecentJob::Store.new(database).executions(job.provider_job_id).map { |execution| execution.fetch("state") }).must_equal %w[interrupted succeeded]
    end

    unless strategy == :advisory
      it "skips foreign row locks in #{strategy} without waiting" do
        DecentJob.configuration.lock_strategy = strategy
        held = RecordingJob.perform_later("held")
        available = RecordingJob.perform_later("available")
        database.with_connection { |connection| connection.exec("SET lock_timeout = '100ms'") }
        @observer.exec("BEGIN")
        @observer.exec_params("SELECT id FROM decent_jobs WHERE id = $1 FOR UPDATE", [held.provider_job_id])
        result = Timeout.timeout(2) { DecentJob::Performer.new(database: database).perform_one }
        _(result.job_id).must_equal available.provider_job_id
      ensure
        @observer.exec("ROLLBACK")
      end
    end
  end

  %i[skiplocked hybrid].each do |strategy|
    it "ignores candidate limits in #{strategy} and skips foreign advisory locks" do
      DecentJob.configuration.lock_strategy = strategy
      DecentJob.configuration.queue_select_limit = 0
      held = RecordingJob.perform_later("held")
      available = RecordingJob.perform_later("available")
      @observer.exec_params("SELECT pg_advisory_lock(hashtextextended(current_schema() || '.decent_jobs.' || $1, 0))", [held.provider_job_id])
      _(DecentJob::Performer.new(database: database).perform_one.job_id).must_equal available.provider_job_id
      _(RecordingJob.events.pop(true)).must_equal "available"
    end
  end
end

describe "skiplocked execution lifecycle" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    DecentJob.configuration.lock_strategy = :skiplocked
    BlockingJob.queue_adapter = RecordingJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    BlockingJob.started = Queue.new
    BlockingJob.release = Queue.new
  end

  after do
    @threads&.each(&:kill)
    @threads&.each { |thread| thread.join(3) }
    DecentJob.reset!
  end

  it "rejects management and cleanup while a force-discarded body still owns the job" do
    job = BlockingJob.perform_later
    pool = database
    @threads = [Thread.new { DecentJob::Performer.new(database: pool).perform_one }]
    Timeout.timeout(5) { BlockingJob.started.pop }
    record = DecentJob::Job.find(job.provider_job_id, database: database)
    _ { record.reschedule_job }.must_raise DecentJob::Job::LockedError
    _ { record.discard_job("ordinary") }.must_raise DecentJob::Job::LockedError
    record.force_discard_job("forced")
    _(record.running?).must_equal true
    _ { record.retry_job }.must_raise DecentJob::Job::LockedError
    _ { record.destroy_job }.must_raise DecentJob::Job::LockedError
    _(DecentJob::Cleaner.new(database: database).cleanup(older_than: 0)).must_equal 0
    BlockingJob.release << true
    _(@threads.first.join(3)).must_equal @threads.first
    _(record.reload.state).must_equal "discarded"
    _(record.locked_by_id).must_be_nil
    _(record.lock_type).must_be_nil
    _(DecentJob::Cleaner.new(database: database).cleanup(older_than: 0)).must_equal 2
  end

  it "reports persisted owners regardless of stored mode or terminal state" do
    job = RecordingJob.perform_later("reporting")
    record = DecentJob::Job.find(job.provider_job_id, database: database)
    _(record.running?).must_equal false

    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET locked_by_id = $2 WHERE id = $1", [record.id, SecureRandom.uuid])
    end

    _(record.running?).must_equal true

    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET finished_at = clock_timestamp(), state = 'discarded' WHERE id = $1", [record.id])
    end

    _(record.reload.running?).must_equal true
    _(record.status).must_equal :discarded
    database.with_connection { |connection| connection.exec_params("DELETE FROM decent_jobs WHERE id = $1", [record.id]) }
    _(record.running?).must_equal false
  end

  %i[advisory skiplocked hybrid].each do |strategy|
    it "excludes a live force-discarded #{strategy} batch member from retry and cleanup" do
      DecentJob.configuration.lock_strategy = strategy
      batch = DecentJob::Batch.enqueue([BlockingJob.new], database: database)
      job = batch.job_instances.first
      pool = database
      @threads = [Thread.new { DecentJob::Performer.new(database: pool).perform_one }]
      Timeout.timeout(5) { BlockingJob.started.pop }
      record = DecentJob::Job.find(job.provider_job_id, database: database)
      record.force_discard_job("forced batch")
      batch.retry
      _(record.reload.state).must_equal "discarded"
      _(record.running?).must_equal true
      _(DecentJob::Cleaner.new(database: database).cleanup(older_than: 0)).must_equal 0
      BlockingJob.release << true
      _(@threads.first.join(3)).must_equal @threads.first
      batch.retry
      _(record.reload.state).must_equal "ready"
      _(record.running?).must_equal false
    end
  end

  it "releases a killed execution while another registration keeps its process alive" do
    first = BlockingJob.perform_later
    second = BlockingJob.perform_later
    pool = database
    tracker = DecentJob::ProcessTracker.new(database: pool)

    @threads = [first, second].map do |job|
      Thread.new { DecentJob::Performer.new(database: pool, process_tracker: tracker).perform_one(job_id: job.provider_job_id) }
    end

    2.times { Timeout.timeout(5) { BlockingJob.started.pop } }
    owner = DecentJob::Job.find(first.provider_job_id, database: database).locked_by_id
    @threads.first.kill
    _(@threads.first.join(3)).must_equal @threads.first
    _(DecentJob::ProcessRecord.active(database: database).map(&:id)).must_include owner
    record = DecentJob::Job.find(first.provider_job_id, database: database)
    _(record.locked_by_id).must_be_nil
    _(record.lock_type).must_be_nil
    _(record.executions.first.fetch("state")).must_equal "running"
    claimed = DecentJob::Claim.new(database).with_next(job_id: first.provider_job_id) { |row| row }
    _(claimed.fetch("execution_interrupted")).must_be_kind_of Time
    BlockingJob.release << true
    _(@threads.last.join(3)).must_equal @threads.last
  end
end

class LockStrategyFatalError < Exception; end # standard:disable Lint/InheritException

class LockStrategyFatalJob < ActiveJob::Base
  def perform
    raise LockStrategyFatalError, "excluded"
  end
end

describe "claim interruption boundaries" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    RecordingJob.queue_adapter = LockStrategyFatalJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
  end

  after do
    @thread&.kill
    @thread&.join(3)
    @observer&.finish unless @observer&.finished?
    DecentJob.reset!
  end

  it "preserves an excluded body error and releases ownership when the pool is saturated" do
    DecentJob.configuration.lock_strategy = :skiplocked
    database.close
    @database = DecentJob::Database.new(url: @connection_options, pool_size: 1, checkout_timeout: 0.05)
    LockStrategyFatalJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    job = LockStrategyFatalJob.perform_later
    @observer = database.connect
    tracker = DecentJob::ProcessTracker.new(database: database)
    registered = Queue.new
    keep_registered = Queue.new

    keeper = Thread.new {
      tracker.register {
        registered << true
        keep_registered.pop
      }
    }

    Timeout.timeout(5) { registered.pop }
    entered = Queue.new
    proceed = Queue.new
    caught = Queue.new
    pool = database

    @thread = Thread.new do
      performer = DecentJob::Performer.new(database: pool, process_tracker: tracker,
        on_execution_start: -> {
          entered << true
          proceed.pop
        })

      performer.perform_one
    rescue Exception => error # standard:disable Lint/RescueException
      caught << error
    end

    Timeout.timeout(5) { entered.pop }
    borrowed = Queue.new
    release_holder = Queue.new

    holder = Thread.new {
      pool.with_connection {
        borrowed << true
        release_holder.pop
      }
    }

    Timeout.timeout(5) { borrowed.pop }
    proceed << true
    error = Timeout.timeout(5) { caught.pop }
    _(error).must_be_kind_of LockStrategyFatalError
    _(error.message).must_equal "excluded"
    _(@thread.join(3)).must_equal @thread
    row = @observer.exec_params("SELECT locked_by_id, lock_type FROM decent_jobs WHERE id = $1", [job.provider_job_id]).first
    _(row).must_equal({"locked_by_id" => nil, "lock_type" => nil})
    active = @observer.exec_params("SELECT #{DecentJob::ProcessRecord::ACTIVE_SQL} FROM decent_job_processes WHERE id = $1", [tracker.process_id]).getvalue(0, 0)
    _(active).must_equal "t"
    release_holder << true
    _(holder.join(3)).must_equal holder
    recovered = DecentJob::Claim.new(database).with_next(job_id: job.provider_job_id) { |claimed| claimed }
    _(recovered.fetch("execution_interrupted")).must_be_kind_of Time
  ensure
    proceed&.push(true)
    release_holder&.push(true)
    keep_registered&.push(true)
    holder&.join(3)
    keeper&.join(3)
  end

  it "releases skiplocked ownership through an independent connection after the pool closes" do
    DecentJob.configuration.lock_strategy = :skiplocked
    job = RecordingJob.perform_later("closed pool")
    @observer = database.connect
    error = LockStrategyFatalError.new("original failure")

    caught = _ do
      DecentJob::Claim.new(database).with_next do
        database.close
        raise error
      end
    end.must_raise LockStrategyFatalError

    _(caught).must_equal error
    row = @observer.exec_params("SELECT locked_by_id, lock_type FROM decent_jobs WHERE id = $1", [job.provider_job_id]).first
    _(row).must_equal({"locked_by_id" => nil, "lock_type" => nil})
  end

  it "completes ownership release if the independent fallback is killed" do
    DecentJob.configuration.lock_strategy = :skiplocked
    job = RecordingJob.perform_later("fallback cancellation")
    @observer = database.connect
    releasing = Queue.new
    pool = database

    @thread = Thread.new do
      DecentJob::Claim.new(pool).with_next do
        pool.close
        connections = 0

        pool.define_singleton_method(:connect) do
          connection = super()
          connections += 1

          if connections == 1
            connection.singleton_class.alias_method :original_exec_params, :exec_params

            connection.define_singleton_method(:exec_params) do |sql, *arguments|
              releasing << true
              Queue.new.pop
              original_exec_params(sql, *arguments)
            end
          end

          connection
        end
      end
    end

    Timeout.timeout(5) { releasing.pop }
    @thread.kill
    _(@thread.join(3)).must_equal @thread
    row = @observer.exec_params("SELECT locked_by_id, lock_type FROM decent_jobs WHERE id = $1", [job.provider_job_id]).first
    _(row).must_equal({"locked_by_id" => nil, "lock_type" => nil})
    _(pool.stats.fetch(:active)).must_equal 0
  end

  it "reports a failed independent cleanup when the body itself succeeded" do
    DecentJob.configuration.lock_strategy = :skiplocked
    RecordingJob.perform_later("failed fallback")
    error = PG::ConnectionBad.new("cleanup connection unavailable")

    caught = _ do
      DecentJob::Claim.new(database).with_next do
        database.close
        database.define_singleton_method(:connect) { raise error }
        :completed
      end
    end.must_raise PG::ConnectionBad

    _(caught).must_equal error
  end

  it "retires a hybrid session when an exception interrupts acquisition's return" do
    DecentJob.configuration.lock_strategy = :hybrid
    job = RecordingJob.perform_later("hybrid acquisition")
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
    _(@thread.join(3)).must_equal @thread
    _(caught).must_equal error
    database.with_connection { |connection| _(connection.backend_pid).wont_equal original_pid }
    _(DecentJob::Job.find(job.provider_job_id, database: database).executions).must_be_empty
    _(DecentJob::Claim.new(database).with_next { :recovered }).must_equal :recovered
  end

  it "preserves the original hybrid body exception when connection loss makes unlocking fail" do
    DecentJob.configuration.lock_strategy = :hybrid
    job = RecordingJob.perform_later("hybrid lost session")
    @observer = database.connect
    original_pid = nil
    error = Class.new(StandardError).new("original body failure")

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
    database.with_connection { |connection| _(connection.backend_pid).wont_equal original_pid }
    _(DecentJob::Performer.new(database: database).perform_one.state).must_equal "succeeded"
    _(DecentJob::Job.find(job.provider_job_id, database: database).executions.map { |row| row.fetch("state") }).must_equal %w[interrupted succeeded]
  end

  it "propagates hybrid unlock failure after a successful body inside an outer rescue" do
    DecentJob.configuration.lock_strategy = :hybrid
    job = RecordingJob.perform_later("hybrid successful body")
    @observer = database.connect

    begin
      raise "previously handled failure"
    rescue RuntimeError
      _ do
        DecentJob::Claim.new(database).with_next(job_id: job.provider_job_id) do
          database.with_connection do |connection|
            @observer.exec_params("SELECT pg_terminate_backend($1)", [connection.backend_pid])
          end

          :completed
        end
      end.must_raise PG::ConnectionBad
    end

    _(DecentJob::Performer.new(database: database).perform_one.state).must_equal "succeeded"
  end

  it "releases ownership when kill interrupts the normal cleanup checkout" do
    DecentJob.configuration.lock_strategy = :skiplocked
    job = RecordingJob.perform_later("cleanup boundary")
    releasing = Queue.new

    database.with_connection do |connection|
      connection.singleton_class.alias_method :original_exec_params, :exec_params

      connection.define_singleton_method(:exec_params) do |sql, *arguments|
        if sql.include?("UPDATE decent_jobs SET locked_by_id = NULL")
          releasing << true
          Queue.new.pop
        end

        original_exec_params(sql, *arguments)
      end
    end

    pool = database
    @thread = Thread.new { DecentJob::Claim.new(pool).with_next { :unfinished } }
    Timeout.timeout(5) { releasing.pop }
    @thread.kill
    _(@thread.join(3)).must_equal @thread
    record = DecentJob::Job.find(job.provider_job_id, database: database)
    _(record.locked_by_id).must_be_nil
    _(record.lock_type).must_be_nil
    _(record.executions.first.fetch("state")).must_equal "running"
    _(DecentJob::Performer.new(database: database).perform_one.state).must_equal "succeeded"
  end

  it "releases a killed skiplocked claim after its caller-owned checkout unwinds" do
    DecentJob.configuration.lock_strategy = :skiplocked
    job = RecordingJob.perform_later("outer checkout")
    entered = Queue.new
    unwound = Queue.new
    backend = nil
    pool = database
    @observer = database.connect

    @thread = Thread.new do
      pool.with_connection do |connection|
        backend = connection.backend_pid

        begin
          DecentJob::Claim.new(pool).with_next do |row|
            pool.transaction do
              connection.exec_params("UPDATE decent_jobs SET updated_at = clock_timestamp() WHERE id = $1", [row.fetch("id")])
              entered << true
              Queue.new.pop
            ensure
              unwound << :application
            end
          end
        ensure
          unwound << :caller
        end
      end
    end

    Timeout.timeout(5) { entered.pop }
    @thread.kill
    _(@thread.join(3)).must_equal @thread
    _(2.times.map { unwound.pop(true) }).must_equal [:application, :caller]
    record = DecentJob::Job.find(job.provider_job_id, database: database)
    _(record.locked_by_id).must_be_nil
    _(record.lock_type).must_be_nil
    _(record.executions.first.fetch("state")).must_equal "running"
    _(DecentJob::Performer.new(database: database).perform_one.state).must_equal "succeeded"
  ensure
    @observer.exec_params("SELECT pg_terminate_backend($1)", [backend]) if @thread&.alive? && backend
    @thread&.join(3)
  end

  %i[advisory skiplocked hybrid].each do |strategy|
    it "preserves excluded exceptions in #{strategy} and leaves the attempt recoverable" do
      DecentJob.configuration.lock_strategy = strategy
      job = LockStrategyFatalJob.perform_later
      _ { DecentJob::Performer.new(database: database).perform_one }.must_raise LockStrategyFatalError
      record = DecentJob::Job.find(job.provider_job_id, database: database)
      _(record.locked_by_id).must_be_nil
      _(record.lock_type).must_be_nil
      _(record.state).must_equal "running"
      _(record.executions.first.fetch("finished_at")).must_be_nil
      _(DecentJob::Claim.new(database).with_next { |row| row.fetch("execution_interrupted") }).must_be_kind_of Time
    end

    it "releases #{strategy} ownership when killed after commit before its query returns" do
      DecentJob.configuration.lock_strategy = strategy
      job = RecordingJob.perform_later("commit boundary")
      committed = Queue.new

      database.with_connection do |connection|
        connection.singleton_class.alias_method :original_exec, :exec

        connection.define_singleton_method(:exec) do |sql, *arguments|
          result = original_exec(sql, *arguments)

          if sql == "COMMIT"
            committed << true
            Queue.new.pop
          end

          result
        end
      end

      pool = database
      @thread = Thread.new { DecentJob::Claim.new(pool).with_next { :unexpected } }
      Timeout.timeout(5) { committed.pop }
      @thread.kill
      _(@thread.join(3)).must_equal @thread
      record = DecentJob::Job.find(job.provider_job_id, database: database)
      _(record.locked_by_id).must_be_nil
      _(record.lock_type).must_be_nil
      _(record.executions.size).must_equal 1
      _(record.executions.first.fetch("state")).must_equal "running"
      _(DecentJob::Performer.new(database: database).perform_one.state).must_equal "succeeded"
    end

    it "can cancel #{strategy} acquisition waiting on a prior attempt row" do
      DecentJob.configuration.lock_strategy = strategy
      job = RecordingJob.perform_later("contended attempt")
      DecentJob::Claim.new(database).with_next { :unfinished }
      @observer = database.connect
      @observer.exec("BEGIN")
      @observer.exec_params("SELECT id FROM decent_job_executions WHERE job_id = $1 FOR UPDATE", [job.provider_job_id])
      pool = database
      @thread = Thread.new { DecentJob::Claim.new(pool).with_next { :unexpected } }

      Timeout.timeout(5) do
        loop do
          @observer.exec("SELECT pg_stat_clear_snapshot()")
          blocked = @observer.exec("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%SELECT started_at FROM decent_job_executions%')").getvalue(0, 0) == "t"
          break if blocked

          sleep 0.01
        end
      end

      @thread.kill
      _(@thread.join(3)).must_equal @thread
      @observer.exec("ROLLBACK")
      _(DecentJob::Job.find(job.provider_job_id, database: database).executions.size).must_equal 1
      _(DecentJob::Performer.new(database: database).perform_one.state).must_equal "succeeded"
    end
  end
end
