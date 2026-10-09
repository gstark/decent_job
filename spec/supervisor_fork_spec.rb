# frozen_string_literal: true

require "spec_helper"

describe "supervisor fork safety" do
  include DatabaseSpec

  after {
    DecentJob.clear_lifecycle_hooks
    DecentJob.reset!
  }

  it "disconnects cached pools before fork and reuses their identity in the child" do
    _(DecentJob.respond_to?(:with_prepared_fork)).must_equal true
    pool = database
    adapter = DecentJob::Adapter.new(database: pool)
    parent_backend = pool.with_connection { |connection| connection.backend_pid }
    reader, writer = IO.pipe
    pid = nil
    DecentJob.with_prepared_fork do
      _(pool.stats[:connections]).must_equal 0
      _ { pool.with_connection {} }.must_raise DecentJob::ForkInProgress
      pid = fork do
        reader.close
        DecentJob.after_supervisor_fork(DecentJob.configuration.single_process_config)
        writer.write(Marshal.dump([adapter.database.equal?(pool), pool.with_connection { |connection| connection.backend_pid }]))
        writer.close
        exit! 0
      end
    end
    writer.close
    same, child_backend = Marshal.load(reader.read)
    Process.waitpid(pid)
    _(same).must_equal true
    _(child_backend).wont_equal parent_backend
    _(pool.with_connection { |connection| connection.exec("SELECT 1").getvalue(0, 0) }).must_equal "1"
  ensure
    reader&.close unless reader&.closed?
    writer&.close unless writer&.closed?
  end

  it "refuses a borrowed pool and releases all preparation gates" do
    _(DecentJob.respond_to?(:with_prepared_fork)).must_equal true
    database.with_connection do
      _ { DecentJob.with_prepared_fork { flunk "must not fork" } }.must_raise DecentJob::ForkInProgress
    end
    _(database.with_connection { |connection| connection.exec("SELECT 2").getvalue(0, 0) }).must_equal "2"
  end

  it "disconnects pooled connections opened by user hooks after draining workers" do
    _(DecentJob.respond_to?(:with_prepared_fork)).must_equal true
    DecentJob.before_supervisor_fork { database.with_connection { |connection| connection.exec("SELECT 1") } }
    DecentJob.with_prepared_fork { _(database.stats[:connections]).must_equal 0 }
  end
  it "rejects manual live workers until application hooks drain them" do
    DecentJob::Schema.new(database).migrate
    worker = DecentJob::Worker.new(database: database, max_threads: 1)
    worker.start
    require "timeout"
    Timeout.timeout(2) { sleep 0.005 until worker.connected? }
    _ { DecentJob.with_prepared_fork { flunk "must not fork live library threads" } }.must_raise DecentJob::ForkInProgress
    DecentJob.before_supervisor_fork { worker.shutdown(timeout: 2) }
    DecentJob.with_prepared_fork { _(database.stats[:connections]).must_equal 0 }
  ensure
    worker&.shutdown(timeout: 2)
  end

  it "prevents hooks from starting library worker threads during preparation" do
    DecentJob::Schema.new(database).migrate
    worker = DecentJob::Worker.new(database: database, max_threads: 1)
    errors = []
    DecentJob.configuration.on_thread_error = ->(error) { errors << error }
    DecentJob.before_supervisor_fork { worker.start }
    DecentJob.with_prepared_fork { _(worker.started?).must_equal false }
    _(errors.map(&:class)).must_equal [DecentJob::ForkInProgress]
  ensure
    worker&.shutdown(timeout: 2)
  end

  it "refuses an in-flight connection and leaves the pool usable after it finishes" do
    require "minitest/mock"
    entered = Queue.new
    release = Queue.new
    original = PG.method(:connect)
    connector = nil
    PG.stub(:connect, ->(*arguments) {
      entered << true
      release.pop
      original.call(*arguments)
    }) do
      connector = Thread.new { database.with_connection { |connection| connection.exec("SELECT 1") } }
      entered.pop
      _ { DecentJob.with_prepared_fork { flunk "must not inherit a connecting session" } }.must_raise DecentJob::ForkInProgress
      release << true
      connector.join
    end
    DecentJob.with_prepared_fork { _(database.stats[:connections]).must_equal 0 }
  ensure
    release << true if release
    connector&.join
  end

  it "refuses preparation in an arbitrary fork without closing the parent's connection" do
    pool = database
    backend = pool.with_connection { |connection| connection.backend_pid }
    reader, writer = IO.pipe
    pid = fork do
      reader.close
      begin
        pool.prepare_fork
        writer.write("unsafe")
      rescue DecentJob::ForkedDatabase
        writer.write("guarded")
      end
      writer.close
      exit! 0
    end
    writer.close
    _(reader.read).must_equal "guarded"
    Process.waitpid(pid)
    _(pool.with_connection { |connection| connection.backend_pid }).must_equal backend
  ensure
    reader&.close unless reader&.closed?
    writer&.close unless writer&.closed?
  end
  it "refuses directly owned process trackers until their registration drains" do
    DecentJob::Schema.new(database).migrate
    tracker = DecentJob::ProcessTracker.new(database: database)
    tracker.register do
      _ { DecentJob.with_prepared_fork { flunk "must not inherit a tracker session" } }.must_raise DecentJob::ForkInProgress
    end
    DecentJob.with_prepared_fork { _(database.stats[:connections]).must_equal 0 }
  end

  it "gates direct process tracker registration during fork preparation" do
    DecentJob::Schema.new(database).migrate
    tracker = DecentJob::ProcessTracker.new(database: database)
    errors = []
    DecentJob.configuration.on_thread_error = ->(error) { errors << error }
    DecentJob.before_supervisor_fork { tracker.register { flunk "must not register while forking" } }
    DecentJob.with_prepared_fork { _(tracker.connected?).must_equal false }
    _(errors).must_be_empty
  end
  it "drains a registered worker whose first tracker registration has not started" do
    require "minitest/mock"
    entered = Queue.new
    release = Queue.new
    tracker = DecentJob::ProcessTracker.new(database: database)
    original_register = tracker.method(:register)
    tracker.define_singleton_method(:register) do |**options, &block|
      entered << true
      release.pop
      original_register.call(**options, &block)
    end
    worker = DecentJob::Worker.new(database: database, max_threads: 1, enable_listen_notify: false)
    original_shutdown = worker.method(:begin_shutdown)
    worker.define_singleton_method(:begin_shutdown) do
      release << true
      original_shutdown.call
    end
    DecentJob::ProcessTracker.stub(:new, tracker) do
      DecentJob.start_worker(worker)
      entered.pop
      DecentJob.with_prepared_fork { _(worker.shutdown?).must_equal true }
    end
  ensure
    release << true if release
    worker&.shutdown(timeout: 2)
  end
end
