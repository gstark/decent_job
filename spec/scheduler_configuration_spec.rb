# frozen_string_literal: true

require "spec_helper"

describe "scheduler configuration" do
  it "defaults to ten seconds and ten thousand cached deadlines per pool" do
    config = DecentJob::Configuration.new
    _(config.poll_interval).must_equal 10
    _(config.max_cache).must_equal 10_000
    _(config.worker_options.fetch(:max_cache)).must_equal 10_000
  end

  it "accepts finite real disabled and fractional polling intervals" do
    config = DecentJob::Configuration.new

    [0, -1, -0.5, 0.025, Rational(1, 10)].each do |interval|
      config.poll_interval = interval
      _(config.validate!.worker_options.fetch(:poll_interval)).must_equal interval
    end

    [nil, false, true, "1", Float::NAN, Float::INFINITY, Complex(1, 0)].each do |interval|
      _ { config.poll_interval = interval }.must_raise ArgumentError
    end

    [-1, 1.5, nil, false, "1"].each do |value|
      _ { config.max_cache = value }.must_raise ArgumentError
    end

    config.max_cache = 0
    _(config.worker_options.fetch(:max_cache)).must_equal 0
  end

  it "resolves Rails development polling dynamically with explicit values taking precedence" do
    config = DecentJob::Configuration.new
    config.apply_rails_config({}, environment: "development", logger: nil)
    _(config.poll_interval).must_equal(-1)
    config.execution_mode = :external
    _(config.poll_interval).must_equal 10
    config.execution_mode = :async_all
    _(config.poll_interval).must_equal(-1)
    config.apply_rails_config({poll_interval: 2.5}, environment: "development", logger: nil)
    _(config.poll_interval).must_equal 2.5
    config.poll_interval = 0.1
    config.apply_rails_config({poll_interval: 3}, environment: "development", logger: nil)
    _(config.poll_interval).must_equal 0.1
  end

  it "loads environment cache and polling settings below application overrides" do
    previous = ENV.to_h.slice("DECENT_JOB_MAX_CACHE", "DECENT_JOB_POLL_INTERVAL")
    ENV["DECENT_JOB_MAX_CACHE"] = "7"
    ENV["DECENT_JOB_POLL_INTERVAL"] = "-2"
    config = DecentJob::Configuration.new
    _(config.max_cache).must_equal 7
    _(config.poll_interval).must_equal(-2)
    config.apply_rails_config({max_cache: 3, poll_interval: 0.5}, environment: "development", logger: nil)
    _(config.max_cache).must_equal 3
    _(config.poll_interval).must_equal 0.5
  ensure
    %w[DECENT_JOB_MAX_CACHE DECENT_JOB_POLL_INTERVAL].each { |key| previous.key?(key) ? ENV[key] = previous[key] : ENV.delete(key) }
  end
end
