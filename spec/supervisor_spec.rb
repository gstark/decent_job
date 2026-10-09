# frozen_string_literal: true

require "minitest/mock"
require "spec_helper"

describe "supervisor" do
  before do
    @config = DecentJob::Configuration.new
    @config.subprocesses = 2
    @config.shutdown_timeout = 0.15
    @config.on_thread_error = ->(_) {}
  end
  after { DecentJob.clear_lifecycle_hooks }

  it "reaps only its children, uses a shared deadline, and restores signal handlers" do
    _(defined?(DecentJob::Supervisor)).wont_be_nil
    DecentJob.before_subprocess_boot {
      Signal.trap("TERM", "IGNORE")
      sleep 10
    }
    unrelated = fork { exit! 23 }
    previous = Signal.trap("TERM", "IGNORE")
    supervisor = DecentJob::Supervisor.new(@config)
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    supervisor.start(on_start: -> { Process.kill("TERM", Process.pid) })
    duration = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
    _(duration).must_be :<, 1
    _(supervisor.started?).must_equal false
    _(supervisor.subprocesses).must_be_empty
    _(Signal.trap("TERM", "IGNORE")).must_equal "IGNORE"
    _(Process.wait2(unrelated).last.exitstatus).must_equal 23
  ensure
    Signal.trap("TERM", previous) if previous
  end

  it "restarts stable slots without blocking other slots or shutdown during backoff" do
    _(defined?(DecentJob::Supervisor)).wont_be_nil
    DecentJob.before_subprocess_boot { exit! 1 }
    spawns = []
    subscriber = ActiveSupport::Notifications.subscribe("cluster_spawn.decent_job") { |event| spawns << event.payload }
    supervisor = DecentJob::Supervisor.new(@config)
    timer = nil
    supervisor.start(on_start: -> {
      timer = Thread.new {
        sleep 1.3
        Process.kill("TERM", Process.pid)
      }
    })
    timer.join
    _(spawns.group_by { |event| event[:index] }.transform_values(&:size)).must_equal({0 => 3, 1 => 3})
    _(supervisor.subprocesses).must_be_empty
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
    timer&.join
  end

  it "kills and reaps already spawned children on startup failure" do
    _(defined?(DecentJob::Supervisor)).wont_be_nil
    DecentJob.before_subprocess_boot { sleep 10 }
    pids = []
    subscriber = ActiveSupport::Notifications.subscribe("cluster_spawn.decent_job") do |event|
      pids << event.payload[:pid]
      raise "startup failed" if pids.size == 2
    end
    supervisor = DecentJob::Supervisor.new(@config)
    _ { supervisor.start }.must_raise RuntimeError
    _(supervisor.subprocesses).must_be_empty
    pids.each { |pid| _ { Process.waitpid(pid, Process::WNOHANG) }.must_raise Errno::ECHILD }
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
  end
  it "caps rapid restart backoff and resets it after healthy uptime" do
    @config.subprocesses = 1
    DecentJob.before_subprocess_boot { exit! 1 }
    offset = 0
    real_clock = DecentJob::Supervisor.method(:monotonic)
    delays = []
    spawns = 0
    spawn_subscription = ActiveSupport::Notifications.subscribe("cluster_spawn.decent_job") do
      spawns += 1
      offset += 11 if spawns == 9
    end
    backoff_subscription = ActiveSupport::Notifications.subscribe("cluster_backoff.decent_job") do |event|
      delays << event.payload[:delay]
      offset += event.payload[:delay]
      Process.kill("TERM", Process.pid) if delays.size == 9
    end
    DecentJob::Supervisor.stub(:monotonic, -> { real_clock.call + offset }) do
      DecentJob::Supervisor.new(@config).start
    end
    _(delays).must_equal [0, 1, 2, 4, 8, 16, 30, 30, 0]
  ensure
    [spawn_subscription, backoff_subscription].compact.each { |subscription| ActiveSupport::Notifications.unsubscribe(subscription) }
  end

  it "handles SIGQUIT immediately even with a long configured drain timeout" do
    @config.shutdown_timeout = 20
    DecentJob.before_subprocess_boot {
      Signal.trap("TERM", "IGNORE")
      sleep 10
    }
    supervisor = DecentJob::Supervisor.new(@config)
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    supervisor.start(on_start: -> { Process.kill("QUIT", Process.pid) })
    _(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).must_be :<, 1
    _(supervisor.shutdown_timed_out).must_equal false
    _(supervisor.subprocesses).must_be_empty
  end
  it "treats default TERM during a child boot hook as expected shutdown" do
    @config.subprocesses = 1
    reader, writer = IO.pipe
    error_reader, error_writer = IO.pipe
    @config.on_thread_error = ->(error) { error_writer.write(error.class.name) }
    DecentJob.before_subprocess_boot {
      writer.write("booting")
      sleep 10
    }
    supervisor = DecentJob::Supervisor.new(@config)
    supervisor.start(on_start: -> {
      reader.read(7)
      Process.kill("TERM", Process.pid)
    })
    writer.close
    error_writer.close
    _(supervisor.shutdown_timed_out).must_equal false
    _(error_reader.read).must_equal ""
  ensure
    [reader, writer, error_reader, error_writer].compact.each { |io| io.close unless io.closed? }
  end
  it "honors a stop received before the child resets inherited supervisor traps" do
    @config.subprocesses = 1
    reader, writer = IO.pipe
    booted = nil
    DecentJob.before_subprocess_boot {
      writer.write("booted")
      exit! 0
    }
    supervisor = DecentJob::Supervisor.new(@config)
    original_fork = supervisor.method(:fork)
    supervisor.define_singleton_method(:fork) do |&child|
      original_fork.call do
        Process.kill("TERM", Process.pid)
        child.call
      end
    end
    supervisor.start(on_start: -> {
      writer.close
      booted = reader.read
      Process.kill("TERM", Process.pid)
    })
    _(booted).must_equal ""
    _(supervisor.shutdown_timed_out).must_equal false
  ensure
    [reader, writer].compact.each { |io| io.close unless io.closed? }
  end
end
