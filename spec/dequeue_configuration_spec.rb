# frozen_string_literal: true

require "decent_job/cli"
require "minitest/mock"
require "spec_helper"
require "stringio"

describe "dequeue configuration" do
  before do
    @environment = ENV.to_h.slice("DECENT_JOB_DEQUEUE_QUERY_SORT", "DECENT_JOB_QUEUE_SELECT_LIMIT")
    ENV.delete("DECENT_JOB_DEQUEUE_QUERY_SORT")
    ENV.delete("DECENT_JOB_QUEUE_SELECT_LIMIT")
    DecentJob.reset!
  end

  after do
    %w[DECENT_JOB_DEQUEUE_QUERY_SORT DECENT_JOB_QUEUE_SELECT_LIMIT].each { |key| ENV.delete(key) }
    ENV.update(@environment)
    DecentJob.reset!
  end

  it "defaults to creation order and a thousand candidates" do
    config = DecentJob::Configuration.new
    _(config.dequeue_query_sort).must_equal :created_at
    _(config.queue_select_limit).must_equal 1000
  end

  it "normalizes exact sort names and decimal nonnegative limits" do
    config = DecentJob::Configuration.new
    [:created_at, :scheduled_at, "created_at", "scheduled_at"].each do |value|
      config.dequeue_query_sort = value
      _(config.dequeue_query_sort).must_equal value.to_sym
    end
    [0, 1, 100_000, "0", "001", "100000"].each do |value|
      config.queue_select_limit = value
      _(config.queue_select_limit).must_equal value.to_i
    end
  end

  it "rejects invalid values without replacing the current policy" do
    config = DecentJob::Configuration.new
    [false, true, 1, "Created_at", " scheduled_at", "id", "created_at; DROP TABLE decent_jobs"].each do |value|
      _ { config.dequeue_query_sort = value }.must_raise ArgumentError
      _(config.dequeue_query_sort).must_equal :created_at
    end
    [false, true, -1, 1.0, Float::INFINITY, "", " ", "-1", "+1", " 1", "1.0", "1e3", "0x10", "1_000", "1; SELECT 1"].each do |value|
      _ { config.queue_select_limit = value }.must_raise ArgumentError
      _(config.queue_select_limit).must_equal 1000
    end
  end

  it "resolves explicit choices over Rails over environment and clears removed host values" do
    ENV["DECENT_JOB_DEQUEUE_QUERY_SORT"] = "scheduled_at"
    ENV["DECENT_JOB_QUEUE_SELECT_LIMIT"] = "8"
    config = DecentJob::Configuration.new
    _(config.dequeue_query_sort).must_equal :scheduled_at
    _(config.queue_select_limit).must_equal 8
    host = {dequeue_query_sort: "created_at", queue_select_limit: "7"}
    config.apply_rails_config(host, environment: "production", logger: nil)
    _(config.dequeue_query_sort).must_equal :created_at
    _(config.queue_select_limit).must_equal 7
    config.dequeue_query_sort = :scheduled_at
    config.queue_select_limit = 0
    2.times { config.apply_rails_config(host, environment: "production", logger: nil) }
    _(config.dequeue_query_sort).must_equal :scheduled_at
    _(config.queue_select_limit).must_equal 0
    config.dequeue_query_sort = nil
    config.queue_select_limit = nil
    _(config.dequeue_query_sort).must_equal :created_at
    _(config.queue_select_limit).must_equal 7
    config.apply_rails_config({}, environment: "production", logger: nil)
    _(config.dequeue_query_sort).must_equal :scheduled_at
    _(config.queue_select_limit).must_equal 8
  end

  it "clears overrides to defaults and accepts changed Rails values without replacing explicit choices" do
    config = DecentJob::Configuration.new
    config.dequeue_query_sort = :created_at
    config.queue_select_limit = 1
    config.apply_rails_config({dequeue_query_sort: "scheduled_at", queue_select_limit: "9"}, environment: "production", logger: nil)
    _(config.dequeue_query_sort).must_equal :created_at
    _(config.queue_select_limit).must_equal 1
    config.dequeue_query_sort = nil
    config.queue_select_limit = nil
    _(config.dequeue_query_sort).must_equal :scheduled_at
    _(config.queue_select_limit).must_equal 9
    config.apply_rails_config({dequeue_query_sort: nil, queue_select_limit: nil}, environment: "production", logger: nil)
    _(config.dequeue_query_sort).must_equal :created_at
    _(config.queue_select_limit).must_equal 1000
  end

  it "validates environment and host inputs" do
    {"DECENT_JOB_DEQUEUE_QUERY_SORT" => "unknown", "DECENT_JOB_QUEUE_SELECT_LIMIT" => "1.5"}.each do |key, value|
      ENV[key] = value
      _ { DecentJob::Configuration.new }.must_raise ArgumentError
      ENV.delete(key)
    end
    [{dequeue_query_sort: false}, {queue_select_limit: false}, {queue_select_limit: 1.5}].each do |options|
      _ { DecentJob::Configuration.new.apply_rails_config(options, environment: "test", logger: nil) }.must_raise ArgumentError
    end
  end

  it "copies the policy and its precedence into independent child configurations" do
    config = DecentJob::Configuration.new
    config.queues = "first|second"
    config.apply_rails_config({dequeue_query_sort: :scheduled_at, queue_select_limit: 2}, environment: "production", logger: nil)
    config.queue_select_limit = 1
    first, second = config.subprocess_configs
    first.queue_select_limit = nil
    first.dequeue_query_sort = :created_at
    _(first.queue_select_limit).must_equal 2
    _(second.queue_select_limit).must_equal 1
    _(second.dequeue_query_sort).must_equal :scheduled_at
    _(config.queue_select_limit).must_equal 1
    _(config.single_process_config.dequeue_query_sort).must_equal :scheduled_at
  end

  it "accepts CLI policy flags as explicit choices and documents them" do
    output = StringIO.new
    error = StringIO.new
    cli = DecentJob::CLI.new(stdout: output, stderr: error)
    DecentJob.configuration.apply_rails_config({dequeue_query_sort: :created_at, queue_select_limit: 8}, environment: "production", logger: nil)
    DecentJob.stub(:install, nil) do
      _(cli.run(["migrate", "--dequeue-query-sort", "scheduled_at", "--queue-select-limit", "0"])).must_equal 0
    end
    config = DecentJob.configuration
    config.apply_rails_config({dequeue_query_sort: :created_at, queue_select_limit: 3}, environment: "production", logger: nil)
    _(config.dequeue_query_sort).must_equal :scheduled_at
    _(config.queue_select_limit).must_equal 0
    _(cli.run(["help"])).must_equal 0
    _(output.string).must_include "--dequeue-query-sort"
    _(output.string).must_include "--queue-select-limit"
    _(error.string).must_equal ""
  end

  it "rejects invalid CLI policies before starting workers" do
    [["--dequeue-query-sort", "id"], ["--queue-select-limit", "-1"], ["--queue-select-limit", "1.1"]].each do |arguments|
      error = StringIO.new
      _(DecentJob::CLI.new(stderr: error).run(arguments)).must_equal 2
      _(error.string).must_include "Error:"
    end
  end
end
