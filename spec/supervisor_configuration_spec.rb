# frozen_string_literal: true

require "spec_helper"

describe "supervisor configuration" do
  it "defaults to a single process and validates subprocess counts" do
    config = DecentJob::Configuration.new
    _(config.respond_to?(:subprocesses)).must_equal true
    _(config.subprocesses).must_equal 0
    [-1, 1.5, "2", nil].each { |value| _ { config.subprocesses = value }.must_raise ArgumentError }
    config.subprocesses = 2
    _(config.subprocess_configs.map(&:queues)).must_equal ["*", "*"]
  end

  it "uses pipe groups as stable recipes and preserves semicolon pools" do
    config = DecentJob::Configuration.new
    config.queues = "critical:2;mail:1 | default:3"
    config.subprocesses = 9
    messages = StringIO.new
    config.logger = Logger.new(messages)
    _(config.subprocesses).must_equal 2
    recipes = config.subprocess_configs
    _(recipes.map(&:queues)).must_equal ["critical:2;mail:1", "default:3"]
    _(recipes.map(&:subprocesses)).must_equal [0, 0]
    _(messages.string).must_match(/subprocesses/)
    _(config.single_process_config.queues).must_equal "critical:2;mail:1;default:3"
    config.validate!

    ["a|", "|b", "a||b", "a| ;b", " "].each do |queues|
      config.queues = queues
      _ { config.validate! }.must_raise ArgumentError
    end
  end
end

describe "supervisor lifecycle hooks" do
  after { DecentJob.clear_lifecycle_hooks if DecentJob.respond_to?(:clear_lifecycle_hooks) }

  it "runs hooks in registration order and isolates failures" do
    _(DecentJob.respond_to?(:before_supervisor_fork)).must_equal true
    calls = []
    original = DecentJob.configuration.on_thread_error
    DecentJob.configuration.on_thread_error = ->(error) { calls << error.message }
    DecentJob.before_supervisor_fork { calls << 1 }
    DecentJob.before_supervisor_fork { raise "hook failed" }
    DecentJob.before_supervisor_fork { calls << 3 }
    DecentJob.run_lifecycle_hooks(:before_supervisor_fork)
    _(calls).must_equal [1, "hook failed", 3]
    DecentJob.clear_lifecycle_hooks
    DecentJob.run_lifecycle_hooks(:before_supervisor_fork)
    _(calls.size).must_equal 3
  ensure
    DecentJob.configuration.on_thread_error = original if original
  end
end

describe "cluster CLI options" do
  it "accepts subprocess counts and rejects invalid values before startup" do
    require "decent_job/cli"
    require "minitest/mock"
    output = StringIO.new
    cli = DecentJob::CLI.new(stdout: output, stderr: output)

    DecentJob::Supervisor.stub(:new, ->(*) { raise "cluster chosen" }) do
      _(cli.run(["--subprocesses", "2"])).must_equal 1
    end

    _(output.string).must_include "cluster chosen"
    _(cli.run(["--subprocesses", "-1"])).must_equal 2
  ensure
    DecentJob.reset!
  end

  it "applies CLI values over application values over environment defaults" do
    require "decent_job/cli"
    require "minitest/mock"
    previous = ENV["DECENT_JOB_SUBPROCESSES"]
    ENV["DECENT_JOB_SUBPROCESSES"] = "3"
    DecentJob.reset!
    _(DecentJob.configuration.subprocesses).must_equal 3
    DecentJob.configure { |configuration| configuration.subprocesses = 2 }
    _(DecentJob.configuration.subprocesses).must_equal 2
    output = StringIO.new
    cli = DecentJob::CLI.new(stdout: output, stderr: output)

    DecentJob::Supervisor.stub(:new, ->(*) { raise "chosen #{DecentJob.configuration.subprocesses}" }) do
      _(cli.run(["--subprocesses", "4"])).must_equal 1
    end

    _(output.string).must_include "chosen 4"
  ensure
    ENV["DECENT_JOB_SUBPROCESSES"] = previous
    DecentJob.reset!
  end

  it "flattens pipe groups into one worker when fork is unavailable" do
    require "decent_job/cli"
    require "minitest/mock"
    output = StringIO.new
    cli = DecentJob::CLI.new(stdout: output, stderr: output)
    responds = Process.method(:respond_to?)

    Process.stub(:respond_to?, ->(name, *args) { (name == :fork) ? false : responds.call(name, *args) }) do
      DecentJob.stub(:start, -> { Process.kill("TERM", Process.pid) }) do
        _(cli.run(["--queues", "fast:2;mail:1|slow:3", "--subprocesses", "7"])).must_equal 0
      end
    end

    _(DecentJob.configuration.queues).must_equal "fast:2;mail:1;slow:3"
    _(DecentJob.configuration.subprocesses).must_equal 0
    _(output.string).must_include "fork is unavailable"
  ensure
    DecentJob.reset!
  end
end
