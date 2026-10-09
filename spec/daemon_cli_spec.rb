# frozen_string_literal: true

require "spec_helper"
require "decent_job/cli"
require "minitest/mock"
require "timeout"

describe "daemon and idle CLI lifecycle" do
  # Async specs elsewhere can leave stopped workers registered. CLI
  # configuration drains registered workers, which these specs count.
  before { DecentJob.reset! }
  after { DecentJob.reset! }

  def cli
    @output = StringIO.new
    DecentJob::CLI.new(stdout: @output, stderr: @output)
  end

  def fake_daemon(events)
    Object.new.tap do |instance|
      instance.define_singleton_method(:daemonize) { events << :detach }
      instance.define_singleton_method(:cleanup) { events << :pid_cleanup }
    end
  end

  it "detaches before services start and removes the PID after shutdown on errors" do
    events = []
    instance = fake_daemon(events)
    command = cli
    DecentJob::Daemon.stub(:new, instance) do
      DecentJob.stub(:start, -> {
        events << :start
        raise "startup failed"
      }) do
        DecentJob.stub(:shutdown, ->(**) {
          events << :drain
          true
        }) do
          _(command.run(%w[--daemonize])).must_equal 1
        end
      end
    end
    _(events).must_equal [:detach, :start, :drain, :pid_cleanup]
    _(@output.string).must_include "startup failed"
  end

  it "preserves the startup failure when PID cleanup also fails" do
    events = []
    instance = fake_daemon(events)
    instance.define_singleton_method(:cleanup) { raise "secondary PID cleanup failure" }
    command = cli
    DecentJob::Daemon.stub(:new, instance) do
      DecentJob.stub(:start, -> { raise "original startup failure" }) do
        _(command.run(%w[--daemonize])).must_equal 1
      end
    end
    _(@output.string).must_include "original startup failure"
    _(@output.string).wont_include "secondary PID cleanup failure"
  end

  it "cleans the PID explicitly before returning a timed out shutdown" do
    events = []
    instance = fake_daemon(events)
    worker = Object.new
    worker.define_singleton_method(:shutdown?) { true }
    command = cli
    DecentJob::Daemon.stub(:new, instance) do
      DecentJob.stub(:start, worker) do
        DecentJob.stub(:shutdown, ->(**) {
          events << :drain
          false
        }) do
          Timeout.timeout(2) { _(command.run(%w[--daemonize])).must_equal 1 }
        end
      end
    end
    _(command.shutdown_timed_out).must_equal true
    _(events).must_equal [:detach, :drain, :pid_cleanup]
  end

  it "checks the worker idle duration without creating a daemon in foreground mode" do
    durations = []
    worker = Object.new
    worker.define_singleton_method(:shutdown?) { false }
    worker.define_singleton_method(:idle?) { |duration|
      durations << duration
      true
    }
    command = cli
    DecentJob::Daemon.stub(:new, ->(**) { flunk "foreground must not claim a PID file" }) do
      DecentJob.stub(:start, worker) do
        Timeout.timeout(2) { _(command.run(%w[--idle-timeout 0.25])).must_equal 0 }
      end
    end
    _(durations).must_equal [0.25]
  end

  it "reports ignored cluster idle settings through the configured logger" do
    logs = StringIO.new
    DecentJob.configure { |config| config.logger = Logger.new(logs) }
    command = cli
    DecentJob::Supervisor.stub(:new, -> { raise "cluster chosen" }) do
      _(command.run(%w[--subprocesses 2 --idle-timeout 1])).must_equal 1
    end
    _(logs.string).must_match(/idle_timeout.*ignor/i)
  end

  it "honors idle timeout when fork fallback selects one process" do
    worker = Object.new
    worker.define_singleton_method(:shutdown?) { false }
    worker.define_singleton_method(:idle?) { |duration| duration == 0 }
    responds = Process.method(:respond_to?)
    command = cli
    Process.stub(:respond_to?, ->(name, *args) { (name == :fork) ? false : responds.call(name, *args) }) do
      DecentJob.stub(:start, worker) do
        Timeout.timeout(2) { _(command.run(%w[--subprocesses 2 --idle-timeout 0])).must_equal 0 }
      end
    end
  end
end
