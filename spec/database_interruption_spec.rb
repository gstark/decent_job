# frozen_string_literal: true

require "spec_helper"
require "timeout"

describe "database asynchronous interruption" do
  include DatabaseSpec

  after do
    @thread&.kill
    @thread&.join(2)
  end

  def pop(queue)
    Timeout.timeout(5) { queue.pop }
  end

  it "retires a connection interrupted during a PostgreSQL query" do
    ready = Queue.new
    pool = database

    @thread = Thread.new do
      pool.with_connection do |connection|
        ready << connection.backend_pid
        connection.exec("SELECT pg_sleep(60)")
      end
    end

    pid = pop(ready)

    Timeout.timeout(5) do
      sleep 0.001 until @admin.exec_params("SELECT wait_event = 'PgSleep' FROM pg_stat_activity WHERE pid = $1", [pid]).getvalue(0, 0) == "t"
    end

    @thread.kill
    _(@thread.join(1)).must_equal @thread
    _(pool.stats.fetch(:active)).must_equal 0
    _(pool.stats.fetch(:connections)).must_equal 0

    pool.with_connection do |connection|
      _(connection.backend_pid).wont_equal pid
      _(connection.exec("SELECT 1").getvalue(0, 0)).must_equal "1"
    end

    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 1

    while @admin.exec_params("SELECT 1 FROM pg_stat_activity WHERE pid = $1", [pid]).ntuples.positive? && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      sleep 0.001
    end

    _(@admin.exec_params("SELECT 1 FROM pg_stat_activity WHERE pid = $1", [pid]).ntuples).must_equal 0
  ensure
    @admin.exec_params("SELECT pg_terminate_backend($1) WHERE EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)", [pid]) if pid
  end

  it "returns checkout ownership when interrupted before the caller receives it" do
    acquired = Queue.new

    database.define_singleton_method(:checkout) do |**options|
      result = super(**options)
      acquired << true
      Queue.new.pop
      result
    end

    pool = database
    @thread = Thread.new { pool.with_connection { flunk "checkout must not finish" } }
    pop(acquired)
    @thread.kill
    _(@thread.join(1)).must_equal @thread
    _(pool.stats.fetch(:active)).must_equal 0
    _(pool.stats.fetch(:connections)).must_equal 0
  end

  it "cleans up a transaction interrupted while BEGIN returns" do
    began = Queue.new
    pool = database

    pool.with_connection do |connection|
      connection.singleton_class.alias_method :original_exec, :exec

      connection.define_singleton_method(:exec) do |sql|
        result = original_exec(sql)

        if sql == "BEGIN"
          began << true
          Queue.new.pop
        end

        result
      end
    end

    @thread = Thread.new { pool.transaction { flunk "BEGIN must not return" } }
    pop(began)
    @thread.kill
    _(@thread.join(1)).must_equal @thread
    _(pool.stats.fetch(:active)).must_equal 0
    _(pool.stats.fetch(:connections)).must_equal 0
    pool.with_connection { |connection| _(connection.transaction_status).must_equal PG::PQTRANS_IDLE }
  end

  it "remains cancellable while waiting for an exhausted pool" do
    pool = DecentJob::Database.new(url: @connection_options, pool_size: 1, checkout_timeout: 60)
    ready = Queue.new

    pool.with_connection do
      @thread = Thread.new do
        ready << true
        pool.with_connection { flunk "pool is exhausted" }
      end

      pop(ready)
      @thread.kill
      _(@thread.join(1)).must_equal @thread
      _(pool.stats.fetch(:active)).must_equal 1
    end

    _(pool.stats.fetch(:active)).must_equal 0
  ensure
    pool&.close
  end

  it "releases a session lock when interrupted as advisory acquisition returns" do
    DecentJob::Schema.new(database).migrate
    job = ActiveJob::Base.new
    adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    adapter.enqueue(job)
    acquired = Queue.new
    pool = database

    pool.with_connection do |connection|
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

    @thread = Thread.new { DecentJob::Claim.new(pool).with_next { flunk "must not execute" } }
    pop(acquired)
    @thread.kill
    _(@thread.join(1)).must_equal @thread
    _(pool.stats.fetch(:active)).must_equal 0
    _(pool.stats.fetch(:connections)).must_equal 0
    result = DecentJob::Claim.new(pool).with_next(job_id: job.provider_job_id) { :claimed }
    _(result).must_equal :claimed
  ensure
    DecentJob.reset!
  end

  it "does not commit outer or nested transactions after user handlers swallow a killed cleanup error" do
    entered = Queue.new
    committed = Queue.new
    pool = database
    pool.with_connection { |connection| connection.exec("CREATE TABLE examples (value integer)") }

    @thread = Thread.new do
      pool.transaction do |connection|
        connection.exec("INSERT INTO examples VALUES (1)")
        pool.after_commit { committed << :outer }

        pool.transaction do |nested|
          nested.exec("INSERT INTO examples VALUES (2)")
          pool.after_commit { committed << :inner }

          begin
            begin
              entered << true
              Queue.new.pop
            ensure
              raise "cleanup error"
            end
          rescue
            :rescued
          end
        end
      end
    end

    pop(entered)
    @thread.kill
    _(@thread.join(1)).must_equal @thread
    _(committed.size).must_equal 0

    pool.with_connection do |connection|
      _(connection.exec("SELECT count(*) FROM examples").getvalue(0, 0)).must_equal "0"
    end
  end
end
