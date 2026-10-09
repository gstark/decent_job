# frozen_string_literal: true

require "spec_helper"

describe "database connections" do
  include DatabaseSpec

  it "provides the same connection inside nested operations" do
    database.with_connection do |outer|
      database.with_connection { |inner| _(inner).must_equal outer }
    end
  end

  it "rolls back failed transactions" do
    database.with_connection { |connection| connection.exec("CREATE TABLE examples (value integer)") }
    _ {
      database.transaction do |connection|
        connection.exec("INSERT INTO examples VALUES (1)")
        raise "abort"
      end
    }.must_raise RuntimeError
    database.with_connection { |connection| _(connection.exec("SELECT * FROM examples").ntuples).must_equal 0 }
  end

  it "isolates nested rollbacks with savepoints" do
    database.with_connection { |connection| connection.exec("CREATE TABLE examples (value integer)") }
    database.transaction do |connection|
      connection.exec("INSERT INTO examples VALUES (1)")
      _ {
        database.transaction do |nested|
          nested.exec("INSERT INTO examples VALUES (2)")
          raise "abort nested"
        end
      }.must_raise RuntimeError
      connection.exec("INSERT INTO examples VALUES (3)")
    end
    database.with_connection do |connection|
      _(connection.exec("SELECT value FROM examples ORDER BY value").column_values(0)).must_equal ["1", "3"]
    end
  end

  it "never lends one connection to two threads" do
    ready = Queue.new
    release = Queue.new
    database
    workers = 3.times.map do
      Thread.new do
        database.with_connection do |connection|
          ready << connection.backend_pid
          release.pop
        end
      end
    end
    pids = 3.times.map { ready.pop }
    _(pids.uniq.size).must_equal 3
  ensure
    3.times { release << true }
    workers&.each(&:join)
  end

  it "times out when its connection budget is exhausted" do
    @database = DecentJob::Database.new(url: @connection_options, pool_size: 1, checkout_timeout: 0.02)
    database.with_connection do
      error = Thread.new do
        database.with_connection { flunk "must not borrow the busy connection" }
      rescue DecentJob::ConnectionTimeout => exception
        exception
      end.value
      _(error).must_be_kind_of DecentJob::ConnectionTimeout
    end
  end

  it "rejects new work after closing" do
    database.close
    _ { database.with_connection {} }.must_raise DecentJob::ClosedDatabase
  end

  it "replaces a disconnected checkout" do
    old_pid = database.with_connection do |connection|
      pid = connection.backend_pid
      connection.finish
      pid
    end
    database.with_connection { |connection| _(connection.backend_pid).wont_equal old_pid }
  end

  it "rolls back a transaction when its caller returns" do
    database.with_connection { |connection| connection.exec("CREATE TABLE examples (value integer)") }
    operation = lambda do
      database.transaction do |connection|
        connection.exec("INSERT INTO examples VALUES (1)")
        return nil
      end
    end
    operation.call
    database.with_connection do |connection|
      _(connection.transaction_status).must_equal PG::PQTRANS_IDLE
      _(connection.exec("SELECT * FROM examples").ntuples).must_equal 0
    end
  end

  it "rolls back a transaction when its block exits with break" do
    database.with_connection { |connection| connection.exec("CREATE TABLE examples (value integer)") }
    database.transaction do |connection|
      connection.exec("INSERT INTO examples VALUES (1)")
      break
    end
    database.with_connection do |connection|
      _(connection.transaction_status).must_equal PG::PQTRANS_IDLE
      _(connection.exec("SELECT * FROM examples").ntuples).must_equal 0
    end
  end

  it "rolls back throw while leaving an outer transaction usable" do
    database.with_connection { |connection| connection.exec("CREATE TABLE examples (value integer)") }
    database.transaction do |connection|
      connection.exec("INSERT INTO examples VALUES (1)")
      catch(:abort) do
        database.transaction do |nested|
          nested.exec("INSERT INTO examples VALUES (2)")
          throw :abort
        end
      end
    end
    database.with_connection do |connection|
      _(connection.exec("SELECT value FROM examples").column_values(0)).must_equal ["1"]
    end
  end

  it "runs every connection-release cleanup before propagating a cleanup failure" do
    events = []
    error = RuntimeError.new("first cleanup failed")
    caught = _ do
      database.with_connection do
        database.after_connection_release { raise error }
        database.after_connection_release { events << database.stats.fetch(:active) }
      end
    end.must_raise RuntimeError
    _(caught).must_equal error
    _(events).must_equal [0]
  end

  it "runs after-commit callbacks only after the outer transaction commits" do
    events = []
    database.transaction do
      database.after_commit { events << :outer }
      database.transaction { database.after_commit { events << :nested } }
      _(events).must_be_empty
    end
    _(events).must_equal [:outer, :nested]
  end

  it "drops callbacks belonging to rolled back savepoints" do
    events = []
    database.transaction do
      _ {
        database.transaction do
          database.after_commit { events << :rolled_back }
          raise "abort"
        end
      }.must_raise RuntimeError
      database.after_commit { events << :committed }
    end
    _(events).must_equal [:committed]
  end

  it "does not report an aborted PostgreSQL transaction as committed" do
    events = []
    _ {
      database.transaction do |connection|
        database.after_commit { events << :committed }
        begin
          connection.exec("SELECT 1 / 0")
        rescue PG::DivisionByZero
          # Applications can rescue SQL exceptions inside transaction blocks.
        end
      end
    }.must_raise DecentJob::TransactionAborted
    _(events).must_be_empty
  end
  it "runs rollback callbacks from committed savepoints when their outer transaction rolls back" do
    events = []
    _ {
      database.transaction do
        database.after_rollback { events << :outer }
        database.transaction { database.after_rollback { events << :nested } }
        database.after_rollback { events << :last }
        _(events).must_equal []
        raise "abort outer"
      end
    }.must_raise RuntimeError
    _(events).must_equal [:outer, :nested, :last]
  end

  it "runs only a failed savepoint's rollback callbacks while leaving the outer transaction usable" do
    events = []
    database.transaction do |connection|
      database.after_rollback { events << :outer_rollback }
      _ {
        database.transaction do
          database.after_rollback { events << :nested_rollback }
          database.after_commit { events << :nested_commit }
          raise "abort nested"
        end
      }.must_raise RuntimeError
      _(events).must_equal [:nested_rollback]
      _(connection.exec("SELECT 1").getvalue(0, 0)).must_equal "1"
      database.after_commit { events << :outer_commit }
    end
    _(events).must_equal [:nested_rollback, :outer_commit]
  end

  it "drops rollback callbacks on commit and does not invoke them outside a transaction" do
    events = []
    database.after_rollback { events << :outside }
    database.transaction do
      database.after_rollback { events << :outer }
      database.transaction { database.after_rollback { events << :nested } }
    end
    _(events).must_equal []
  end

  it "runs rollback callbacks when a transaction exits without raising" do
    events = []
    database.transaction do
      database.after_rollback { events << :rolled_back }
      break
    end
    _(events).must_equal [:rolled_back]
  end

  it "does not run rollback callbacks when an after-commit callback raises" do
    events = []
    _ {
      database.transaction do
        database.after_rollback { events << :rolled_back }
        database.after_commit { raise "already committed" }
      end
    }.must_raise RuntimeError
    _(events).must_equal []
  end
end
