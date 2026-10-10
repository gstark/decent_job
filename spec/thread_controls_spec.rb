# frozen_string_literal: true

require "spec_helper"
require "decent_job/cli"
require "minitest/mock"
require "support/jobs"
require "timeout"

class ThreadControlsJob < ActiveJob::Base
  class_attribute :events, :commands

  def perform
    loop do
      events << [DecentJob.current_thread_running?, DecentJob.current_thread_shutting_down?, Thread.current.priority]

      case commands.pop
      when :finish then break
      when :nested
        DecentJob::Current.with(nil) do
          ThreadControlsInlineJob.perform_now
        end
      when :priority then Thread.current.priority = 2
      end
    end
  end
end

class ThreadControlsInlineJob < ActiveJob::Base
  def perform
    ThreadControlsJob.events << [DecentJob.current_thread_running?, DecentJob.current_thread_shutting_down?, Thread.current.priority]
  end
end

describe "worker thread controls" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    ThreadControlsJob.queue_adapter = DecentJob::Adapter.new(database: database)
    ThreadControlsJob.events = Queue.new
    ThreadControlsJob.commands = Queue.new
    @workers = []
  end

  after do
    10.times { ThreadControlsJob.commands << :finish }
    @workers.each { |worker| worker.shutdown(timeout: 5) }
    DecentJob.reset!
  end

  def worker(**options)
    DecentJob::Worker.new(database: database, max_threads: 1, poll_interval: -1, enable_listen_notify: false, **options).tap { |value| @workers << value }
  end

  def event
    Timeout.timeout(5) { ThreadControlsJob.events.pop }
  end

  it "reports shutdown from nested inline work without leaking onto the application thread" do
    _(DecentJob.current_thread_running?).must_equal true
    _(DecentJob.current_thread_shutting_down?).must_be_nil
    ThreadControlsJob.perform_later
    instance = worker.start
    _(event.take(2)).must_equal [true, false]
    instance.shutdown(timeout: 0)
    ThreadControlsJob.commands << :nested
    _(event.take(2)).must_equal [false, true]
    _(event.take(2)).must_equal [false, true]
    _(DecentJob.current_thread_running?).must_equal true
    _(DecentJob.current_thread_shutting_down?).must_be_nil
  end

  it "leaves priority alone by default and restores the original when disabled between attempts" do
    original = Thread.current.priority
    ThreadControlsJob.perform_later
    instance = worker.start
    _(event.last).must_equal original
    instance.lower_thread_priority = true
    ThreadControlsJob.perform_later
    ThreadControlsJob.commands << :finish
    _(event.last).must_equal(-3)
    ThreadControlsJob.commands << :priority
    _(event.last).must_equal 2
    ThreadControlsJob.perform_later
    ThreadControlsJob.commands << :finish
    _(event.last).must_equal(-3)
    instance.lower_thread_priority = false
    ThreadControlsJob.perform_later
    ThreadControlsJob.commands << :finish
    _(event.last).must_equal original
    _(Thread.current.priority).must_equal original
  end

  it "does not adjust application-selected execution priorities while the option remains disabled" do
    ThreadControlsJob.perform_later
    worker.start
    event
    ThreadControlsJob.commands << :priority
    _(event.last).must_equal 2
    ThreadControlsJob.perform_later
    ThreadControlsJob.commands << :finish
    _(event.last).must_equal 2
  end

  it "changes registered worker priorities through the runtime writer" do
    ThreadControlsJob.perform_later
    instance = worker
    DecentJob.start_worker(instance)
    event
    DecentJob.lower_thread_priority = true
    _(DecentJob.configuration.lower_thread_priority).must_equal true
    ThreadControlsJob.perform_later
    ThreadControlsJob.commands << :finish
    _(event.last).must_equal(-3)
    _ { DecentJob.lower_thread_priority = "true" }.must_raise ArgumentError
  end

  it "isolates shutdown status between concurrent workers" do
    ThreadControlsJob.set(queue: "first").perform_later
    first = worker(queues: "first").start
    event
    first.shutdown(timeout: 0)
    ThreadControlsJob.set(queue: "second").perform_later
    worker(queues: "second").start
    _(event.take(2)).must_equal [true, false]
  end
end

describe "thread control configuration" do
  after { DecentJob.reset! }

  it "defaults to normal priorities and accepts only application booleans" do
    configuration = DecentJob::Configuration.new
    _(configuration.lower_thread_priority).must_equal false
    configuration.lower_thread_priority = true
    _(configuration.worker_options[:lower_thread_priority]).must_equal true

    [nil, "true", "false", 1].each do |value|
      _ { configuration.lower_thread_priority = value }.must_raise ArgumentError
      _ { DecentJob::Worker.new(database: Object.new, lower_thread_priority: value) }.must_raise ArgumentError
    end
  end

  it "reads true and false environment values" do
    previous = ENV["DECENT_JOB_LOWER_THREAD_PRIORITY"]

    %w[true false].each do |value|
      ENV["DECENT_JOB_LOWER_THREAD_PRIORITY"] = value
      _(DecentJob::Configuration.new.lower_thread_priority).must_equal(value == "true")
    end
  ensure
    ENV["DECENT_JOB_LOWER_THREAD_PRIORITY"] = previous
  end

  it "lets CLI flags override application priority settings" do
    %w[--lower-thread-priority --no-lower-thread-priority].each do |flag|
      cli = DecentJob::CLI.new(stdout: StringIO.new, stderr: StringIO.new)
      cli.define_singleton_method(:start) { 0 }
      _(cli.run(["start", flag])).must_equal 0
      _(DecentJob.configuration.lower_thread_priority).must_equal(flag == "--lower-thread-priority")
    end
  end
end

describe "isolated worker status scope" do
  it "restores outer scope after nested failures and respects thread or fiber isolation" do
    original = ActiveSupport::IsolatedExecutionState.isolation_level
    running = Struct.new(:running?).new(true)
    stopping = Struct.new(:running?).new(false)

    %i[thread fiber].each do |level|
      ActiveSupport::IsolatedExecutionState.isolation_level = level

      DecentJob::ThreadStatus.with_worker(running) do
        _ {
          DecentJob::ThreadStatus.with_worker(stopping) do
            _(DecentJob.current_thread_running?).must_equal false
            raise "nested failure"
          end
        }.must_raise RuntimeError

        _(DecentJob.current_thread_running?).must_equal true
        _(DecentJob.current_thread_shutting_down?).must_equal false
        other_thread = Thread.new { DecentJob.current_thread_shutting_down? }.value
        _(other_thread).must_be_nil
        other_fiber = Fiber.new { DecentJob.current_thread_shutting_down? }.resume

        if level == :thread
          _(other_fiber).must_equal false
        else
          _(other_fiber).must_be_nil
        end
      end

      _(DecentJob.current_thread_shutting_down?).must_be_nil
    end
  ensure
    ActiveSupport::IsolatedExecutionState.isolation_level = original
  end
end

describe "runtime thread priority update ordering" do
  after { DecentJob.reset! }

  it "allows priority updates in a child forked during an update" do
    skip "fork is unavailable" unless Process.respond_to?(:fork)
    reader, writer = IO.pipe
    pid = nil
    worker = DecentJob::Worker.new(database: Object.new)
    worker.define_singleton_method(:start) { |**| self }
    setter = worker.method(:lower_thread_priority=)

    worker.define_singleton_method(:lower_thread_priority=) do |value|
      DecentJob.with_prepared_fork do
        pid = fork do
          reader.close

          begin
            DecentJob.after_supervisor_fork(DecentJob.configuration.dup)
            DecentJob.lower_thread_priority = false
            writer.write(Marshal.dump([DecentJob.configuration.lower_thread_priority, DecentJob.current_thread_shutting_down?]))
          rescue => error
            writer.write(Marshal.dump([error.class.name, error.message]))
          ensure
            writer.close
          end

          exit! 0
        end
      end

      setter.call(value)
    end

    DecentJob.start_worker(worker)
    DecentJob::ThreadStatus.with_worker(worker) { DecentJob.lower_thread_priority = true }
    writer.close
    result = Timeout.timeout(2) { Marshal.load(reader.read) }
    _(result).must_equal [false, nil]
    Process.waitpid(pid)
  ensure
    reader&.close unless reader&.closed?
    writer&.close unless writer&.closed?

    if pid
      begin
        Process.kill("KILL", pid)
        Process.waitpid(pid)
      rescue Errno::ESRCH, Errno::ECHILD
        nil
      end
    end
  end

  it "applies a concurrent update to an adapter worker being constructed" do
    constructed = Queue.new
    release = Queue.new
    update_started = Queue.new
    starter = updater = nil
    adapter = DecentJob::Adapter.new(database: Object.new, execution_mode: :async_all)
    constructor = DecentJob::Worker.method(:new)

    blocking_constructor = lambda do |**options|
      constructor.call(**options).tap do |worker|
        worker.define_singleton_method(:start) { |**| self }
        constructed << worker
        release.pop
      end
    end

    DecentJob::Worker.stub(:new, blocking_constructor) do
      starter = Thread.new { adapter.start_async }
      worker = Timeout.timeout(2) { constructed.pop }

      updater = Thread.new do
        update_started << true
        DecentJob.lower_thread_priority = true
      end

      Timeout.timeout(2) do
        update_started.pop
        sleep 0.001 while updater.status == "run"
        release << true
        starter.value
        updater.value
      end

      _(DecentJob.configuration.lower_thread_priority).must_equal true
      _(worker.lower_thread_priority).must_equal true
    end
  ensure
    release << true
    starter&.join
    updater&.join
  end

  it "keeps workers consistent with configuration when global updates overlap" do
    entered = Queue.new
    release = Queue.new
    second_started = Queue.new
    worker = DecentJob::Worker.new(database: Object.new)
    worker.define_singleton_method(:start) { |**| self }
    setter = worker.method(:lower_thread_priority=)

    worker.define_singleton_method(:lower_thread_priority=) do |value|
      DecentJob.stopping?

      if value
        entered << true
        release.pop
      end

      setter.call(value)
    end

    DecentJob.start_worker(worker)
    first = Thread.new { DecentJob.lower_thread_priority = true }
    Timeout.timeout(2) { entered.pop }

    second = Thread.new do
      second_started << true
      DecentJob.lower_thread_priority = false
    end

    Timeout.timeout(2) do
      second_started.pop
      sleep 0.001 while second.status == "run"
      release << true
      first.value
      second.value
    end

    _(DecentJob.configuration.lower_thread_priority).must_equal false
    _(worker.lower_thread_priority).must_equal false
  ensure
    release << true
    first&.join
    second&.join
  end
end
