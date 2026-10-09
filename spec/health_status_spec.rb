# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

describe "worker health status" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @workers = []
    @errors = Queue.new
  end

  after do
    @workers.each do |worker|
      worker.shutdown(timeout: 5)
    rescue Exception # standard:disable Lint/RescueException
      nil
    end
    DecentJob.reset!
  end

  def eventually
    Timeout.timeout(5) { sleep 0.005 until yield }
  end

  def worker(**options)
    instance = DecentJob::Worker.new(database: database, max_threads: 1, process_heartbeat_interval: 0.03,
      poll_interval: 0.2, on_thread_error: ->(error) { @errors << error }, **options)
    @workers << instance
    instance
  end

  it "reads empty global status without opening the database and requires every registered worker" do
    _(DecentJob.started?).must_equal false
    _(DecentJob.connected?).must_equal false
    _(DecentJob.instance_variable_get(:@database)).must_be_nil
    first = worker(enable_listen_notify: false)
    second = worker(enable_listen_notify: false)
    DecentJob.start_worker(first)
    DecentJob.start_worker(second)
    eventually { DecentJob.connected? }
    _(DecentJob.started?).must_equal true
    second.shutdown(timeout: 0)
    _(first.started?).must_equal true
    _(DecentJob.started?).must_equal false
    _(DecentJob.connected?).must_equal false
    DecentJob.shutdown(timeout: 0)
    _(first.started?).must_equal false
  end

  it "requires all execution pools and turns unready immediately during drain" do
    instance = worker(queues: "urgent:1;ordinary:1", enable_listen_notify: false)
    _(instance.started?).must_equal false
    _(instance.connected?).must_equal false
    instance.start
    eventually { instance.connected? }
    _(instance.started?).must_equal true
    instance.shutdown(timeout: 0)
    _(instance.started?).must_equal false
    _(instance.connected?).must_equal false
  end

  it "clears listener readiness on connection loss and restores it after LISTEN" do
    connections = Queue.new
    database.define_singleton_method(:connect) do
      super().tap { |connection|
        connection.set_notice_processor {}
        connections << connection.backend_pid
      }
    end
    instance = worker
    instance.start
    eventually { instance.connected? }
    pids = []
    pids << connections.pop(true) until connections.empty?
    listener = @admin.exec_params("SELECT pid FROM pg_stat_activity WHERE pid = ANY($1::integer[]) AND query = 'LISTEN decent_job'", [PG::TextEncoder::Array.new.encode(pids)]).first.fetch("pid")
    @admin.exec_params("SELECT pg_terminate_backend($1)", [listener])
    eventually { !@errors.empty? }
    _(instance.started?).must_equal true
    _(instance.connected?).must_equal false
    eventually { instance.connected? }
  end

  it "clears heartbeat readiness without issuing SQL from readers and recovers" do
    release = Queue.new
    errors = Queue.new
    tracker = DecentJob::ProcessTracker.new(database: database, process_heartbeat_interval: 0.03,
      on_thread_error: ->(error) {
        errors << error
        release.pop
      })
    connection_pid = Queue.new
    database.define_singleton_method(:connect) do
      super().tap { |connection|
        connection.set_notice_processor {}
        connection_pid << connection.backend_pid
      }
    end
    _(tracker.connected?).must_equal false
    tracker.register do
      _(tracker.connected?).must_equal true
      @admin.exec_params("SELECT pg_terminate_backend($1)", [connection_pid.pop])
      eventually { !errors.empty? }
      100.times { _(tracker.connected?).must_equal false }
      release << true
      eventually { tracker.connected? }
      tracker.stop
      _(tracker.connected?).must_equal false
    end
    _(tracker.connected?).must_equal false
  ensure
    release << true
    tracker&.stop
    tracker&.join(5)
  end
end
