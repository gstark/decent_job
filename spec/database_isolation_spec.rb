# frozen_string_literal: true

require "spec_helper"

describe "database execution context isolation" do
  include DatabaseSpec

  before do
    @isolation_level = ActiveSupport::IsolatedExecutionState.isolation_level
  end

  after do
    ActiveSupport::IsolatedExecutionState.isolation_level = @isolation_level
  end

  [:thread, :fiber].each do |isolation|
    it "uses #{isolation} isolation for borrowed sessions while reusing nested checkouts" do
      ActiveSupport::IsolatedExecutionState.isolation_level = isolation
      pids = []
      database.with_connection do |outer|
        database.with_connection { |nested| _(nested).must_be_same_as outer }
        pids << outer.backend_pid
        Fiber.new do
          database.with_connection do |other|
            pids << other.backend_pid
            database.with_connection { |nested| _(nested).must_be_same_as other }
          end
        end.resume
      end
      _(pids.uniq.size).must_equal((isolation == :fiber) ? 2 : 1)
      _(database.stats[:active]).must_equal 0
    end
  end

  it "keeps commits and rollbacks independent when fiber transactions finish out of order" do
    ActiveSupport::IsolatedExecutionState.isolation_level = :fiber
    events = []
    pool = database
    pool.with_connection { |connection| connection.exec("CREATE TABLE examples (value integer)") }
    committing = Fiber.new do
      pool.transaction do |connection|
        connection.exec("INSERT INTO examples VALUES (1)")
        pool.after_commit { events << :first_commit }
        pool.after_rollback { events << :first_rollback }
        pool.transaction do
          pool.after_commit { events << :nested_commit }
          pool.after_rollback { events << :nested_rollback }
        end
        Fiber.yield
      end
    end
    rolling_back = Fiber.new do
      pool.transaction do |connection|
        connection.exec("INSERT INTO examples VALUES (2)")
        pool.after_commit { events << :second_commit }
        pool.after_rollback { events << :second_rollback }
        Fiber.yield
        raise "second rollback"
      end
    rescue => error
      error.message
    end
    committing.resume
    rolling_back.resume
    outside = Fiber.new do
      pool.after_commit { events << :outside_commit }
      pool.after_rollback { events << :outside_rollback }
    end
    outside.resume
    committing.resume
    first_events = events.dup
    result = rolling_back.resume
    _(first_events).must_equal [:outside_commit, :first_commit, :nested_commit]
    _(events).must_equal [:outside_commit, :first_commit, :nested_commit, :second_rollback]
    _(result).must_equal "second rollback"
    pool.with_connection do |connection|
      _(connection.exec("SELECT value FROM examples ORDER BY value").column_values(0)).must_equal ["1"]
    end
    _(pool.stats[:active]).must_equal 0
  ensure
    committing.resume if committing&.alive?
    rolling_back.resume if rolling_back&.alive?
  end

  it "enforces pool limits across isolated fibers and returns the checkout after unwind" do
    ActiveSupport::IsolatedExecutionState.isolation_level = :fiber
    @database = DecentJob::Database.new(url: @connection_options, pool_size: 1, checkout_timeout: 0.02)
    borrowing = Fiber.new { database.with_connection { Fiber.yield } }
    borrowing.resume
    result = Fiber.new do
      database.with_connection { :incorrectly_shared }
    rescue DecentJob::ConnectionTimeout
      :timed_out
    end.resume
    _(result).must_equal :timed_out
    borrowing.resume
    _(database.with_connection { |connection| connection.exec("SELECT 1").getvalue(0, 0) }).must_equal "1"
    _(database.stats[:active]).must_equal 0
  ensure
    borrowing.resume if borrowing&.alive?
  end
end
