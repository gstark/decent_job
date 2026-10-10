# frozen_string_literal: true

require "minitest/mock"
require "spec_helper"

describe "supervisor cleanup safety" do
  after { DecentJob.clear_lifecycle_hooks }

  it "does not let shutdown observers prevent child reaping" do
    config = DecentJob::Configuration.new
    config.subprocesses = 2
    config.shutdown_timeout = 0.05
    config.on_thread_error = ->(_) { raise Interrupt, "reporter failed" }
    DecentJob.before_subprocess_boot { sleep 10 }
    pids = []
    subscriber = ActiveSupport::Notifications.subscribe("cluster_spawn.decent_job") { |event| pids << event.payload[:pid] }
    reaper = ActiveSupport::Notifications.subscribe("cluster_reap.decent_job") { raise Interrupt, "reap observer failed" }
    supervisor = DecentJob::Supervisor.new(config)
    supervisor.start(on_start: -> { Process.kill("TERM", Process.pid) }, on_shutdown: -> { raise Interrupt, "shutdown observer failed" })
    _(supervisor.subprocesses).must_be_empty
    pids.each { |pid| _ { Process.waitpid(pid, Process::WNOHANG) }.must_raise Errno::ECHILD }
  ensure
    [subscriber, reaper].compact.each { |subscription| ActiveSupport::Notifications.unsubscribe(subscription) }

    pids&.each do |pid|
      Process.kill("KILL", pid)
      Process.waitpid(pid)
    rescue Errno::ESRCH, Errno::ECHILD
      nil
    end
  end

  it "expires readiness heartbeats" do
    handle = DecentJob::Supervisor::SubprocessHandle.new(pid: 1, recipe: nil, index: 0)
    _(!!handle.connected?(10)).must_equal false
    handle.mark_ready(10)
    _(handle.connected?(25)).must_equal true
    _(handle.connected?(25.01)).must_equal false
  end

  it "refuses resetting an unprepared pool and validates resizing" do
    database = DecentJob::Database.new(url: "postgres:///postgres")
    _ { database.reset_after_fork }.must_raise DecentJob::ForkInProgress
    _ { database.resize(0) }.must_raise ArgumentError
  ensure
    database&.close
  end

  it "turns inherited async adapters into external producers and preserves inline adapters" do
    async_adapter = DecentJob::Adapter.new(execution_mode: :async)
    inline_adapter = DecentJob::Adapter.new(execution_mode: :inline)
    config = DecentJob::Configuration.new
    config.execution_mode = :async
    reader, writer = IO.pipe
    pid = nil

    DecentJob.with_prepared_fork do
      pid = fork do
        reader.close
        DecentJob.after_supervisor_fork(config)
        writer.write(Marshal.dump([async_adapter.execution_mode, inline_adapter.execution_mode, DecentJob.configuration.execution_mode]))
        writer.close
        exit! 0
      end
    end

    writer.close
    _(Marshal.load(reader.read)).must_equal [:external, :inline, :external]
    Process.waitpid(pid)
  ensure
    reader&.close unless reader&.closed?
    writer&.close unless writer&.closed?
  end

  it "retires inherited registered adapters when the child runtime resets" do
    adapter = DecentJob::Adapter.new
    uninitialized_adapter = DecentJob::Adapter.allocate
    reader, writer = IO.pipe
    pid = nil

    DecentJob.with_prepared_fork do
      pid = fork do
        reader.close

        begin
          DecentJob.after_supervisor_fork(DecentJob.configuration.dup)
          DecentJob.reset!
          writer.write(Marshal.dump([adapter.stopping?, uninitialized_adapter.stopping?]))
        rescue => error
          writer.write(Marshal.dump([error.class.name, error.message]))
        ensure
          writer.close
        end

        exit! 0
      end
    end

    writer.close
    _(Marshal.load(reader.read)).must_equal [:stopping, false]
    Process.waitpid(pid)
  ensure
    reader&.close unless reader&.closed?
    writer&.close unless writer&.closed?
  end

  it "ignores allocated resources that have not initialized or started threads" do
    allocated = [DecentJob::Worker, DecentJob::CronManager, DecentJob::ProcessTracker].map(&:allocate)
    DecentJob.with_prepared_fork { _(allocated.size).must_equal 3 }
  end
end
