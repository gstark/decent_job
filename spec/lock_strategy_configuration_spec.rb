# frozen_string_literal: true

require "decent_job/cli"
require "minitest/mock"
require "spec_helper"
require "stringio"

describe "lock strategy configuration" do
  before do
    @environment = ENV.delete("DECENT_JOB_LOCK_STRATEGY")
    DecentJob.reset!
  end

  after do
    ENV.delete("DECENT_JOB_LOCK_STRATEGY")
    ENV["DECENT_JOB_LOCK_STRATEGY"] = @environment if @environment
    DecentJob.reset!
  end

  it "defaults to advisory and accepts exact strings and symbols" do
    configuration = DecentJob::Configuration.new
    _(configuration.lock_strategy).must_equal :advisory
    %i[advisory skiplocked hybrid].each do |strategy|
      [strategy, strategy.to_s].each do |value|
        configuration.lock_strategy = value
        _(configuration.lock_strategy).must_equal strategy
      end
    end
    [false, true, 0, "", "skip_locked", "Hybrid", " hybrid"].each do |value|
      _ { configuration.lock_strategy = value }.must_raise ArgumentError
      _(configuration.lock_strategy).must_equal :hybrid
    end
  end

  it "resolves explicit then Rails then environment and clears overrides" do
    ENV["DECENT_JOB_LOCK_STRATEGY"] = "skiplocked"
    configuration = DecentJob::Configuration.new
    _(configuration.lock_strategy).must_equal :skiplocked
    configuration.apply_rails_config({lock_strategy: "hybrid"}, environment: "production", logger: nil)
    _(configuration.lock_strategy).must_equal :hybrid
    configuration.lock_strategy = :advisory
    configuration.apply_rails_config({lock_strategy: "skiplocked"}, environment: "production", logger: nil)
    _(configuration.lock_strategy).must_equal :advisory
    configuration.lock_strategy = nil
    _(configuration.lock_strategy).must_equal :skiplocked
    configuration.apply_rails_config({}, environment: "production", logger: nil)
    _(configuration.lock_strategy).must_equal :skiplocked
    ENV["DECENT_JOB_LOCK_STRATEGY"] = "invalid"
    _ { DecentJob::Configuration.new }.must_raise ArgumentError
  end

  it "accepts the CLI option and rejects invalid choices before execution" do
    output = StringIO.new
    errors = StringIO.new
    cli = DecentJob::CLI.new(stdout: output, stderr: errors)
    DecentJob.stub(:install, nil) do
      _(cli.run(["migrate", "--lock-strategy", "hybrid"])).must_equal 0
    end
    _(DecentJob.configuration.lock_strategy).must_equal :hybrid
    _(cli.run(["help"])).must_equal 0
    _(output.string).must_include "--lock-strategy"
    _(cli.run(["--lock-strategy", "invalid"])).must_equal 2
  end
end
