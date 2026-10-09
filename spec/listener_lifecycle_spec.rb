# frozen_string_literal: true

require "active_support/executor"
require "spec_helper"
require "support/jobs"
require "timeout"

describe "notification listener lifecycle" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @errors = Queue.new
    @connections = Queue.new
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    RecordingJob.queue_adapter = @adapter
    RecordingJob.events = Queue.new
  end

  after do
    @release&.push(true)
    @holder&.join(5)
    @worker&.shutdown(timeout: 5)
    @adapter.shutdown(timeout: 5)
  end

  def await(seconds: 5)
    Timeout.timeout(seconds) { sleep 0.01 until yield }
  end

  def observe_connections(&decorate)
    original = database.method(:connect)
    connections = @connections
    database.define_singleton_method(:connect) do
      original.call.tap do |connection|
        connection.set_notice_processor {}
        connections << connection.backend_pid
        decorate&.call(connection)
      end
    end
  end

  def start_worker(**options)
    @worker = DecentJob::Worker.new(database: database, max_threads: 1, poll_interval: -1,
      process_heartbeat_interval: 0.05, on_thread_error: ->(error) { @errors << error }, **options).start
  end

  def listener_pid
    pids = []
    result = nil
    await do
      pids << @connections.pop(true) until @connections.empty?
      result = @admin.exec_params("SELECT pid FROM pg_stat_activity WHERE pid = ANY($1::integer[]) AND query = 'LISTEN decent_job'",
        [PG::TextEncoder::Array.new.encode(pids)]).first&.fetch("pid")
    end
    result.to_i
  end

  it "keeps its dedicated connection alive and replaces it cleanly after shutdown" do
    observe_connections
    start_worker
    await { @worker.connected? }
    first_id = @worker.process_id
    first_pid = listener_pid
    await(seconds: 13) do
      @admin.exec_params("SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND query = 'SELECT 1'", [first_pid]).ntuples == 1
    end
    _(@worker.connected?).must_equal true
    _(@errors).must_be_empty
    _(@worker.shutdown(timeout: 2)).must_equal true
    _(@admin.exec_params("SELECT 1 FROM pg_stat_activity WHERE pid = $1", [first_pid]).ntuples).must_equal 0
    _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
    @worker.start
    await { @worker.connected? }
    _(listener_pid).wont_equal first_pid
    _(@worker.process_id).wont_equal first_id
    RecordingJob.perform_later("after listener restart")
    _(Timeout.timeout(3) { RecordingJob.events.pop }).must_equal "after listener restart"
  end

  it "retains registration through repeated listener failures and a failing error reporter" do
    unavailable = true
    failed_connections = Queue.new
    observe_connections do |connection|
      original = connection.method(:exec)
      connection.define_singleton_method(:exec) do |sql, *arguments|
        if sql == "LISTEN decent_job" && unavailable
          failed_connections << connection
          raise PG::ConnectionBad, "listener unavailable"
        end
        original.call(sql, *arguments)
      end
    end
    start_worker(on_thread_error: ->(error) {
      @errors << error
      raise "reporter unavailable"
    })
    await { @errors.size >= 3 }
    first_id = @worker.process_id
    _(DecentJob::ProcessRecord.active(database: database).map(&:id)).must_include first_id
    _(@worker.started?).must_equal true
    _(@worker.connected?).must_equal false
    unavailable = false
    await { @worker.connected? }
    _(@worker.process_id).must_equal first_id
    failed = []
    failed << failed_connections.pop(true) until failed_connections.empty?
    _(failed.size).must_be :>=, 3
    _(failed.all?(&:finished?)).must_equal true
    RecordingJob.perform_later("after repeated errors")
    _(Timeout.timeout(3) { RecordingJob.events.pop }).must_equal "after repeated errors"
  end

  it "dispatches notifications inside the supplied host executor and restores its context" do
    previous_rails = Object.const_get(:Rails) if Object.const_defined?(:Rails)
    Object.send(:remove_const, :Rails) if previous_rails
    executor = Class.new(ActiveSupport::Executor)
    application = Struct.new(:executor).new(executor)
    rails = Module.new
    rails.define_singleton_method(:application) { application }
    Object.const_set(:Rails, rails)
    dispatched = Queue.new
    start_worker
    original_wake = @worker.method(:wake)
    @worker.define_singleton_method(:wake) do |state = nil|
      dispatched << executor.active? if state.is_a?(Hash) && state["queue_name"] == "executor check"
      original_wake.call(state)
    end
    await { @worker.connected? }
    database.with_connection { |connection| DecentJob::Notifier.notify(connection, queue_name: "executor check") }
    _(Timeout.timeout(3) { dispatched.pop }).must_equal true
    _(executor.active?).must_equal false
    _(@errors).must_be_empty
  ensure
    @worker&.shutdown(timeout: 5)
    Object.send(:remove_const, :Rails) if Object.const_defined?(:Rails)
    Object.const_set(:Rails, previous_rails) if previous_rails
  end

  it "retains process ownership through execution pool timeouts and drains without that pool" do
    database.close
    @database = DecentJob::Database.new(url: @connection_options, pool_size: 1, checkout_timeout: 0.03)
    @release = Queue.new
    acquired = Queue.new
    @holder = Thread.new do
      database.with_connection do
        acquired << true
        @release.pop
      end
    end
    Timeout.timeout(3) { acquired.pop }
    start_worker(advisory_lock_heartbeat: true)
    await { @errors.size >= 2 && @worker.connected? }
    probe = PG.connect(@connection_options)
    row = probe.exec_params("SELECT id, #{DecentJob::ProcessRecord::ACTIVE_SQL} AS active FROM decent_job_processes WHERE id = $1", [@worker.process_id]).first
    _(row.fetch("active")).must_equal "t"
    _(@errors.pop).must_be_kind_of DecentJob::ConnectionTimeout
    _(@worker.shutdown(timeout: 2)).must_equal true
    _(probe.exec("SELECT 1 FROM decent_job_processes").ntuples).must_equal 0
    _(@holder.alive?).must_equal true
    @release << true
    @holder.join(2)
    _(@holder.alive?).must_equal false
  ensure
    probe&.finish
  end
end
