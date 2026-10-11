# frozen_string_literal: true

require "spec_helper"

describe "database connection lending" do
  include DatabaseSpec

  before do
    @host = PG.connect(@connection_options)
    @deferred = []

    # Stands in for a host transaction: register only while one is open.
    @defer = lambda do |commit, rollback|
      next false if @host.transaction_status == PG::PQTRANS_IDLE

      @deferred << [commit, rollback]
      true
    end
  end

  after { @host.finish }

  it "runs pool work on the lent connection and hands commit callbacks to the host transaction" do
    events = []
    @host.exec("BEGIN")

    database.lend(@host, defer: @defer) do
      database.transaction do |connection|
        events << connection.equal?(@host)
        database.after_commit { events << :commit }
        database.after_rollback { events << :rollback }
      end

      database.after_commit { events << :outside }
    end

    _(events).must_equal [true]
    _(@deferred.map { |commit, rollback| [commit.size, rollback.size] }).must_equal [[1, 1], [1, 0]]
    @host.exec("ROLLBACK")
    _(@host.finished?).must_equal false
    _(database.stats.fetch(:connections)).must_equal 0
  end

  it "routes rollback callbacks to the host transaction outside a pool transaction" do
    @host.exec("BEGIN")
    database.lend(@host, defer: @defer) { database.after_rollback { :rolled_back } }
    @host.exec("ROLLBACK")

    _(@deferred.size).must_equal 1
    _(@deferred.first.last.map(&:call)).must_equal [:rolled_back]
  end

  it "runs commit callbacks after the lend when no host transaction is open" do
    events = []

    database.lend(@host, defer: @defer) do
      database.transaction do |connection|
        connection.exec("CREATE TABLE lent (id integer)")
        database.after_commit { events << database.with_connection { |pool| pool.equal?(@host) } }
      end

      events << :returned
    end

    _(events).must_equal [:returned, false]
    _(@deferred).must_be_empty
    _(database.with_connection { |connection| connection.exec("SELECT to_regclass('lent')").getvalue(0, 0) }).must_equal "lent"
  end

  it "joins an open pool transaction instead of lending" do
    database.transaction do |outer|
      database.lend(@host, defer: @defer) do
        database.with_connection { |connection| _(connection).must_be_same_as outer }
      end
    end
  end

  it "restores the previous checkout after the lend" do
    database.with_connection do |checkout|
      database.lend(@host, defer: @defer) do
        database.with_connection { |connection| _(connection).must_be_same_as @host }
      end

      database.with_connection { |connection| _(connection).must_be_same_as checkout }
    end
  end

  it "runs pending callbacks without hiding an error from the block" do
    events = []

    error = _ do
      database.lend(@host, defer: @defer) do
        database.after_commit { raise "callback failed" }
        database.after_commit { events << :second }
        raise "block failed"
      end
    end.must_raise RuntimeError

    _(error.message).must_equal "block failed"
    _(events).must_equal [:second]
  end
end
