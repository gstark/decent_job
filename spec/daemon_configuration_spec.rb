# frozen_string_literal: true

require "spec_helper"
require "decent_job/cli"
require "minitest/mock"
require "pathname"

describe "daemon and idle configuration" do
  it "defaults to foreground with no idle deadline and a cwd PID path" do
    config = DecentJob::Configuration.new
    _(config.respond_to?(:daemonize)).must_equal true
    _(config.daemonize).must_equal false
    _(config.idle_timeout).must_be_nil
    _(config.pidfile).must_equal File.expand_path("tmp/pids/decent_job.pid")
  end

  it "validates booleans, nonempty paths, and finite nonnegative idle durations" do
    config = DecentJob::Configuration.new
    _(config.respond_to?(:idle_timeout=)).must_equal true

    [-1, Float::INFINITY, Float::NAN, "2", true, Complex(1, 1)].each do |value|
      _ { config.idle_timeout = value }.must_raise ArgumentError
    end

    [nil, 0, 0.5, 2].each { |value| config.idle_timeout = value }
    [nil, "yes", 1].each { |value| _ { config.daemonize = value }.must_raise ArgumentError }
    [nil, "", "  ", 1, "bad\0path"].each { |value| _ { config.pidfile = value }.must_raise ArgumentError }
    config.pidfile = Pathname.new("tmp/worker.pid")
    _(config.pidfile).must_equal File.expand_path("tmp/worker.pid")
  end

  it "applies CLI over application over environment values" do
    names = %w[DECENT_JOB_DAEMONIZE DECENT_JOB_PIDFILE DECENT_JOB_IDLE_TIMEOUT]
    previous = names.to_h { |name| [name, ENV[name]] }
    ENV.update("DECENT_JOB_DAEMONIZE" => "true", "DECENT_JOB_PIDFILE" => "env.pid", "DECENT_JOB_IDLE_TIMEOUT" => "2.5")
    DecentJob.reset!
    _(DecentJob.configuration.respond_to?(:daemonize)).must_equal true
    _(DecentJob.configuration.daemonize).must_equal true
    _(DecentJob.configuration.pidfile).must_equal File.expand_path("env.pid")
    _(DecentJob.configuration.idle_timeout).must_equal 2.5

    DecentJob.configure do |config|
      config.daemonize = true
      config.pidfile = "app.pid"
      config.idle_timeout = 3
    end

    output = StringIO.new
    cli = DecentJob::CLI.new(stdout: output, stderr: output)
    cli.stub(:start, 0) { _(cli.run(%w[--no-daemonize --pidfile cli.pid --idle-timeout 4])).must_equal 0 }
    _(DecentJob.configuration.daemonize).must_equal false
    _(DecentJob.configuration.pidfile).must_equal File.expand_path("cli.pid")
    _(DecentJob.configuration.idle_timeout).must_equal 4
    _(cli.run(%w[--idle-timeout -1])).must_equal 2
  ensure
    previous&.each { |name, value| ENV[name] = value }
    DecentJob.reset!
  end
end
