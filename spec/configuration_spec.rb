# frozen_string_literal: true

require "spec_helper"

describe "configuration" do
  include DatabaseSpec

  after { DecentJob.reset! }

  it "loads as an Active Job adapter by name" do
    DecentJob.configure { |config| config.database_url = @connection_options }
    job_class = Class.new(ActiveJob::Base)
    job_class.queue_adapter = :decent_job
    _(job_class.queue_adapter).must_be_kind_of DecentJob::Adapter
    _(DecentJob.database).must_equal job_class.queue_adapter.database
  end

  it "rejects invalid execution modes and nonpositive thread counts" do
    config = DecentJob::Configuration.new
    config.execution_mode = :bogus
    _ { config.validate! }.must_raise ArgumentError
    config.execution_mode = :external
    config.max_threads = 0
    _ { config.validate! }.must_raise ArgumentError
  end

  it "does not permit reconfiguration after creating the database" do
    DecentJob.configure { |config| config.database_url = @connection_options }
    DecentJob.database
    _ { DecentJob.configure { |config| config.max_threads = 2 } }.must_raise ArgumentError
  end

  it "allows application configuration after selecting the adapter" do
    job_class = Class.new(ActiveJob::Base)
    job_class.queue_adapter = :decent_job
    DecentJob.configure { |config| config.database_url = @connection_options }
    _(job_class.queue_adapter.database).must_equal DecentJob.database
  end

  it "installs the schema and drains due jobs with the public API" do
    DecentJob.configure { |config| config.database_url = @connection_options }
    DecentJob.install
    job_class = Class.new(ActiveJob::Base)
    # This check only needs an empty queue, so no anonymous class is persisted.
    job_class.queue_adapter = :decent_job
    _(DecentJob.perform_inline).must_equal 0
  end
end
