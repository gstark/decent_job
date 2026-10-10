# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

describe "listener events" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @subscriptions = []
    @events = Queue.new
    @errors = Queue.new
    @connections = Queue.new
    @worker = DecentJob::Worker.new(database: database, max_threads: 1, poll_interval: -1,
      on_thread_error: ->(error) { @errors << error })
    adapter = DecentJob::Adapter.new(database: database)
    RecordingJob.queue_adapter = adapter
    RecordingJob.events = Queue.new
  end

  after do
    @worker&.shutdown(timeout: 5)
    @subscriptions.each { |subscription| ActiveSupport::Notifications.unsubscribe(subscription) }
    DecentJob.reset!
  end

  def subscribe(name, observer = nil, &block)
    @subscriptions << ActiveSupport::Notifications.subscribe("#{name}.decent_job", observer, &block)
  end

  def pop(queue = @events)
    Timeout.timeout(5) { queue.pop }
  end

  def await
    Timeout.timeout(5) { sleep 0.005 until yield }
  end

  def decorate_connections(&decorate)
    original = database.method(:connect)
    database.define_singleton_method(:connect) { original.call.tap(&decorate) }
  end

  def notify(payload)
    @admin.exec_params("SELECT pg_notify('decent_job', $1)", [payload])
  end

  it "wraps actual LISTEN and healthy UNLISTEN operations before closing the dedicated session" do
    operations = Queue.new
    listener = Queue.new

    decorate_connections do |connection|
      original = connection.method(:exec)

      connection.define_singleton_method(:exec) do |sql, *args|
        if ["LISTEN decent_job", "UNLISTEN decent_job"].include?(sql)
          operations << sql
          listener << connection if sql == "LISTEN decent_job"
        end

        original.call(sql, *args)
      end
    end

    %w[notifier_listen notifier_unlisten].each do |name|
      observer = Object.new
      observer.define_singleton_method(:start) { |*| operations << "#{name}:start" }
      observer.define_singleton_method(:finish) { |*| operations << "#{name}:finish" }
      subscribe(name, observer)
    end

    @worker.start
    await { @worker.connected? }
    connection = pop(listener)
    @worker.shutdown(timeout: 5)
    _(Array.new(6) { pop(operations) }).must_equal ["notifier_listen:start", "LISTEN decent_job", "notifier_listen:finish", "notifier_unlisten:start", "UNLISTEN decent_job", "notifier_unlisten:finish"]
    _(connection.finished?).must_equal true
  end

  it "emits raw invalid and nil payloads before validation without reconnecting or changing routing" do
    listens = Queue.new
    subscribe("notifier_listen") { listens << true }
    subscribe("notifier_notify_error") { |event| @events << [:failure, event.payload] }

    subscribe("notifier_notified") do |event|
      @events << [:raw, event.payload[:payload]&.dup]
      event.payload[:payload]&.replace('{"queue_name":"other"}')
    end

    @worker.start
    await { @worker.connected? }
    pop(listens)
    notify("{")
    _(pop).must_equal [:raw, "{"]
    _(pop(@errors)).must_be_kind_of JSON::ParserError
    notify("")
    _(pop).must_equal [:raw, nil]
    job = RecordingJob.set(queue: "default").perform_later("original route")
    _(pop(RecordingJob.events)).must_equal "original route"
    _(pop.first).must_equal :raw
    @worker.shutdown(timeout: 5)
    _(listens).must_be_empty
    _(@events).must_be_empty
    _(job.provider_job_id).wont_be_nil
  end

  it "reports the original listener failure once and reconnects after LISTEN failure" do
    attempts = 0
    failed = Queue.new

    decorate_connections do |connection|
      original = connection.method(:exec)

      connection.define_singleton_method(:exec) do |sql, *args|
        if sql == "LISTEN decent_job"
          attempts += 1

          if attempts == 1
            failed << connection
            raise IOError, "listen failed"
          end
        end

        original.call(sql, *args)
      end
    end

    subscribe("notifier_notify_error") { |event| @events << event.payload.dup }
    @worker.start
    payload = pop
    _(payload[:error].message).must_equal "listen failed"
    _(pop(@errors)).must_be_same_as payload[:error]
    await { @worker.connected? }
    _(pop(failed).finished?).must_equal true
    _(attempts).must_equal 2
  end

  it "closes the dedicated session when UNLISTEN fails and reports that failure during shutdown" do
    connection_used = Queue.new

    decorate_connections do |connection|
      original = connection.method(:exec)

      connection.define_singleton_method(:exec) do |sql, *args|
        connection_used << connection if sql == "LISTEN decent_job"
        raise IOError, "unlisten failed" if sql == "UNLISTEN decent_job"

        original.call(sql, *args)
      end
    end

    subscribe("notifier_notify_error") { |event| @events << event.payload.dup }
    @worker.start
    await { @worker.connected? }
    connection = pop(connection_used)
    _(@worker.shutdown(timeout: 5)).must_equal true
    _(connection.finished?).must_equal true
    _(pop[:error].message).must_equal "unlisten failed"
    _(pop(@errors).message).must_equal "unlisten failed"
  end

  it "closes after killed UNLISTEN observers and permits callback shutdown without deadlocking" do
    connection_used = Queue.new

    decorate_connections do |connection|
      original = connection.method(:exec)

      connection.define_singleton_method(:exec) do |sql, *args|
        connection_used << connection if sql == "LISTEN decent_job"
        original.call(sql, *args)
      end
    end

    subscribe("notifier_unlisten") do |event|
      @worker.stats
      @worker.shutdown(timeout: 0)
      @events << Thread.current
      Queue.new.pop
    end

    @worker.start
    await { @worker.connected? }
    connection = pop(connection_used)
    @worker.shutdown(timeout: 0)
    thread = pop
    thread.kill.join
    _(connection.finished?).must_equal true
    _(@worker.shutdown(timeout: 5)).must_equal true
    _(@errors).must_be_empty
  end

  it "retains the original wait error when UNLISTEN cleanup also fails" do
    original_error = IOError.new("wait failed")
    failures = Queue.new
    subscribe("notifier_notify_error") { |event| failures << event.payload[:error] }

    decorate_connections do |connection|
      original_exec = connection.method(:exec)

      connection.define_singleton_method(:exec) do |sql, *args|
        raise IOError, "cleanup failed" if sql == "UNLISTEN decent_job"

        original_exec.call(sql, *args)
      end

      connection.define_singleton_method(:wait_for_notify) { |*| raise original_error }
    end

    @worker.start
    _(pop(failures)).must_be_same_as original_error
    _(pop(@errors)).must_be_same_as original_error
    @worker.shutdown(timeout: 5)
  end

  it "closes a session interrupted inside LISTEN start without inventing UNLISTEN or listener failure" do
    connection_used = Queue.new
    sql = Queue.new

    decorate_connections do |connection|
      connection_used << connection
      original = connection.method(:exec)

      connection.define_singleton_method(:exec) do |query, *args|
        sql << query if query.include?("LISTEN")
        original.call(query, *args)
      end
    end

    observer = Object.new
    entered = Queue.new

    observer.define_singleton_method(:start) { |*|
      entered << Thread.current
      Queue.new.pop
    }

    observer.define_singleton_method(:finish) { |*| }
    subscribe("notifier_listen", observer)
    subscribe("notifier_notify_error") { |event| @events << event }
    @worker.start
    thread = pop(entered)
    thread.kill.join
    @worker.shutdown(timeout: 5)
    connections = []
    connections << connection_used.pop until connection_used.empty?
    _(connections.all?(&:finished?)).must_equal true
    _(sql).must_be_empty
    _(@events).must_be_empty
    _(@errors).must_be_empty
  end

  it "observes invalid shape messages before rejecting them" do
    subscribe("notifier_notified") { |event| @events << event.payload[:payload] }
    failures = Queue.new
    subscribe("notifier_notify_error") { |event| failures << event }
    @worker.start
    await { @worker.connected? }

    ["[]", '{"count":0}'].each do |raw|
      notify(raw)
      _(pop).must_equal raw
      _(pop(@errors)).must_be_kind_of ArgumentError
    end

    @worker.shutdown(timeout: 5)
    _(failures).must_be_empty
  end

  it "contains listener observer failures through the worker reporter" do
    subscribe("notifier_listen") { raise "listen observer failed" }
    subscribe("notifier_unlisten") { raise "unlisten observer failed" }
    @worker.start
    await { @worker.connected? }
    @worker.shutdown(timeout: 5)
    _(Array.new(2) { pop(@errors).message }).must_equal ["listen observer failed", "unlisten observer failed"]
  end
end
