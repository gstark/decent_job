# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

describe "host adapter lifecycle" do
  include DatabaseSpec

  before do
    DecentJob.reset!
    DecentJob::Schema.new(database).migrate
    RecordingJob.events = Queue.new
  end

  after do
    DecentJob.reset!
  end

  it "starts boot-time backlog only after framework readiness" do
    DecentJob.prepare_rails!
    adapter = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    RecordingJob.queue_adapter = adapter
    RecordingJob.perform_later("boot backlog")
    _(adapter.async_started?).must_equal false
    _(RecordingJob.events).must_be_empty
    DecentJob.finish_rails_initialization!
    _(Timeout.timeout(5) { RecordingJob.events.pop }).must_equal "boot backlog"
    _(adapter.async_started?).must_equal true
  end

  it "starts standalone backlog through the public method" do
    producer = DecentJob::Adapter.new(database: database, execution_mode: :external)
    RecordingJob.queue_adapter = producer
    RecordingJob.perform_later("backlog")
    consumer = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    consumer.start_async
    _(Timeout.timeout(5) { RecordingJob.events.pop }).must_equal "backlog"
  end

  it "keeps shutdown adapters stopped when framework readiness is signaled again" do
    DecentJob.prepare_rails!
    adapter = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    adapter.shutdown
    DecentJob.finish_rails_initialization!
    _(adapter.async_started?).must_equal false
    _(adapter.start_async).must_be_nil
  end

  it "retires retained boot-time adapters on reset" do
    DecentJob.prepare_rails!
    adapter = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    DecentJob.reset!
    DecentJob.finish_rails_initialization!
    _(adapter.async_started?).must_equal false
    _(adapter.start_async).must_be_nil
  end

  it "keeps CLI application loading from starting host adapters" do
    DecentJob.with_cli_application do
      DecentJob.prepare_rails!
      adapter = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
      DecentJob.finish_rails_initialization!
      _(adapter.async_started?).must_equal false
    end
  end
end
