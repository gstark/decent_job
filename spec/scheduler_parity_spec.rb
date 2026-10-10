# frozen_string_literal: true

require "active_support/json"
require "spec_helper"

describe "scheduler hint input parity" do
  it "accepts the reference Time, JSON string, and numeric hints without database access" do
    errors = []
    worker = DecentJob::Worker.new(database: Object.new, on_thread_error: ->(error) { errors << error })
    future = Time.now + 86_400

    [future, future.to_json, future.to_i, future.iso8601].each do |scheduled_at|
      _(worker.wake(scheduled_at: scheduled_at)).must_be_same_as worker
    end

    _(errors).must_equal []
  end

  it "reports malformed, nested, and non-time JSON hints without database access" do
    errors = []
    worker = DecentJob::Worker.new(database: Object.new, on_thread_error: ->(error) { errors << error })
    future = Time.now + 86_400
    values = ["\"unterminated", future.to_json.to_json, [future.iso8601].to_json,
      {at: future.iso8601}.to_json, "not a timestamp".to_json, "123".to_json, "null", "true"]

    values.each do |scheduled_at|
      _(worker.wake(scheduled_at: scheduled_at)).must_be_same_as worker
    end

    _(errors.size).must_equal values.size
    _(errors.all? { |error| error.is_a?(ArgumentError) }).must_equal true
    _(worker.stats.fetch(:schedulers).first.fetch(:active_cache)).must_equal 0
  end
end

describe "scheduler future cache parity" do
  include DatabaseSpec

  before do
    @database = DecentJob::Database.new(url: @connection_options, pool_size: 1)
    DecentJob::Schema.new(database).migrate
  end

  after { @worker&.shutdown(timeout: 5) }

  it "caches future Time, JSON string, and numeric hints through the public worker API" do
    errors = []
    @worker = DecentJob::Worker.new(database: database, max_threads: 1, poll_interval: -1,
      max_cache: 3, enable_listen_notify: false, on_thread_error: ->(error) { errors << error })
    future = Time.now + 86_400

    # Hold the execution checkout so startup discovery cannot replace the hints
    # before the cache assertions. The heartbeat has its own session.
    database.with_connection do
      @worker.start

      [future, (future + 1).to_json, (future + 2).to_i].each_with_index do |scheduled_at, index|
        @worker.wake(scheduled_at: scheduled_at)
        _(@worker.stats.fetch(:schedulers).first.fetch(:active_cache)).must_equal index + 1
      end

      _(errors).must_equal []
    end
  end
end
