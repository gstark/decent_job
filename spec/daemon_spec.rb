# frozen_string_literal: true

require "spec_helper"
require "fileutils"
require "minitest/mock"
require "tmpdir"

describe "daemon PID ownership" do
  before do
    @directory = Dir.mktmpdir("decent-job-daemon")
    @path = File.join(@directory, "worker.pid")
    @daemons = []
  end

  after do
    @daemons.each(&:cleanup)
    FileUtils.remove_entry(@directory)
    DecentJob.clear_lifecycle_hooks
  end

  def daemon(path = @path)
    _(DecentJob.const_defined?(:Daemon)).must_equal true
    @daemons << DecentJob::Daemon.new(pidfile: path)
    @daemons.last
  end

  def detach(instance)
    Process.stub(:daemon, ->(*arguments) { _(arguments).must_equal [true] }) do
      DecentJob.stub(:after_supervisor_fork, ->(*) { DecentJob::Daemon.after_fork! }) { instance.daemonize }
    end
  end

  it "uses the PID path from an explicitly supplied configuration" do
    config = DecentJob::Configuration.new
    config.pidfile = @path
    instance = DecentJob::Daemon.new(configuration: config)
    _(instance.pidfile).must_equal @path
  end

  it "checks the directory before detaching" do
    instance = daemon(File.join(@directory, "missing", "worker.pid"))

    Process.stub(:daemon, ->(*) { flunk "must not detach" }) do
      _ { instance.daemonize }.must_raise ArgumentError
    end
  end

  it "preserves cwd, prepares resources, and writes and removes its owned PID" do
    instance = daemon
    cwd = Dir.pwd
    hooks = []
    DecentJob.before_supervisor_fork { hooks << :prepared }
    detach(instance)
    _(hooks).must_equal [:prepared]
    _(Dir.pwd).must_equal cwd
    _(File.read(@path)).must_equal Process.pid.to_s
    instance.cleanup
    _(File.exist?(@path)).must_equal false
  end

  it "refuses a live PID without truncating the file" do
    instance = daemon
    File.write(@path, Process.pid.to_s)

    Process.stub(:daemon, ->(*) { flunk "must not detach" }) do
      _ { instance.daemonize }.must_raise DecentJob::Daemon::AlreadyRunning
    end

    _(File.read(@path)).must_equal Process.pid.to_s
  end

  it "treats EPERM as a live owner" do
    instance = daemon
    File.write(@path, "123")

    Process.stub(:kill, ->(signal, pid) {
      _(signal).must_equal 0
      _(pid).must_equal 123
      raise Errno::EPERM
    }) do
      _ { instance.daemonize }.must_raise DecentJob::Daemon::AlreadyRunning
    end

    _(File.read(@path)).must_equal "123"
  end

  it "reclaims stale and invalid PID contents without signaling a process group" do
    ["123", "-1", "0", "junk", "123junk", ""].each do |contents|
      instance = daemon
      File.write(@path, contents)
      Process.stub(:kill, ->(signal, pid) {
        _(signal).must_equal 0
        _(pid).must_equal 123
        raise Errno::ESRCH
      }) { detach(instance) }
      _(File.read(@path)).must_equal Process.pid.to_s
      instance.cleanup
    end
  end

  it "excludes concurrent launches while the descriptor is owned" do
    first, second = daemon, daemon
    detach(first)
    File.write(@path, "") # A competing launch must respect the lock even without a live PID.

    Process.stub(:daemon, ->(*) { flunk "must not detach" }) do
      _ { second.daemonize }.must_raise DecentJob::Daemon::AlreadyRunning
    end
  end

  it "retains files replaced by a different inode or owner" do
    first = daemon
    detach(first)
    File.unlink(@path)
    File.write(@path, Process.pid.to_s)
    first.cleanup
    _(File.exist?(@path)).must_equal true
    File.unlink(@path)
    second = daemon
    detach(second)
    File.write(@path, "123")
    second.cleanup
    _(File.read(@path)).must_equal "123"
  end

  it "refuses symbolic links and multiply linked files without changing targets" do
    target = File.join(@directory, "target")
    File.write(target, "contents")
    File.symlink(target, @path)
    _ { daemon.daemonize }.must_raise ArgumentError
    File.unlink(@path)
    File.link(target, @path)
    _ { daemon.daemonize }.must_raise ArgumentError
    _(File.read(target)).must_equal "contents"
  end

  it "closes inherited descriptors without unlocking or removing the parent's file" do
    instance = daemon
    detach(instance)
    reader, writer = IO.pipe

    pid = Process.fork do
      reader.close
      DecentJob::Daemon.after_fork!
      instance.cleanup
      contender = File.open(@path, File::RDWR)
      writer.write(contender.flock(File::LOCK_EX | File::LOCK_NB) ? "unlocked" : "locked")
      contender.close
      writer.close
      exit! 0
    end

    writer.close
    _(reader.read).must_equal "locked"
    Process.wait(pid)
    _(File.read(@path)).must_equal Process.pid.to_s
  ensure
    reader&.close
    writer&.close unless writer&.closed?
  end

  it "releases the descriptor when detaching fails" do
    instance = daemon

    Process.stub(:daemon, ->(*) { raise "detach failed" }) do
      error = _ { instance.daemonize }.must_raise RuntimeError
      _(error.message).must_equal "detach failed"
    end

    detach(daemon)
  end

  it "rejects manually owned workers before detaching" do
    worker = DecentJob::Worker.new(database: nil)
    instance = daemon

    worker.stub(:shutdown?, false) do
      Process.stub(:daemon, ->(*) { flunk "must not detach" }) do
        _ { instance.daemonize }.must_raise DecentJob::ForkInProgress
      end
    end

    contender = File.open(@path, File::RDWR)
    _(contender.flock(File::LOCK_EX | File::LOCK_NB)).must_equal 0
  ensure
    contender&.close
  end

  it "does not release its lock when daemonize is called again" do
    instance = daemon
    detach(instance)
    Process.stub(:daemon, ->(*) { flunk "must not detach twice" }) { _(instance.daemonize).must_equal instance }
    contender = File.open(@path, File::RDWR)
    _(contender.flock(File::LOCK_EX | File::LOCK_NB)).must_equal false
    _(File.read(@path)).must_equal Process.pid.to_s
  ensure
    contender&.close
  end

  it "does not delete a foreign PID file when reusing a cleaned instance" do
    instance = daemon
    detach(instance)
    instance.cleanup
    File.write(@path, Process.pid.to_s)
    _ { instance.daemonize }.must_raise DecentJob::Daemon::AlreadyRunning
    _(File.read(@path)).must_equal Process.pid.to_s
  end

  it "reports unsupported native daemon implementations as ordinary startup failures" do
    instance = daemon

    Process.stub(:daemon, ->(*) { raise NotImplementedError }) do
      error = _ { instance.daemonize }.must_raise RuntimeError
      _(error.message).must_match(/unavailable/)
    end
  end

  it "fails clearly when native daemonization is unavailable" do
    instance = daemon
    responds = Process.method(:respond_to?)

    Process.stub(:respond_to?, ->(name, *args) { (name == :daemon) ? false : responds.call(name, *args) }) do
      error = _ { instance.daemonize }.must_raise RuntimeError
      _(error.message).must_match(/unavailable/)
    end

    _(File.exist?(@path)).must_equal false
  end
end
