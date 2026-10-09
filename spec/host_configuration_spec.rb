# frozen_string_literal: true

require "decent_job/cli"
require "spec_helper"
require "stringio"
require "timeout"

describe "host configuration" do
  before { DecentJob.reset! }
  after { DecentJob.reset! }

  it "uses Rails environment defaults for adapters created before host configuration" do
    adapter = DecentJob::Adapter.new
    config = DecentJob.configuration
    config.apply_rails_config({}, environment: "development", logger: nil)
    _(adapter.execution_mode).must_equal :async
    config.apply_rails_config({}, environment: "test", logger: nil)
    _(adapter.execution_mode).must_equal :inline
    config.apply_rails_config({}, environment: "production", logger: nil)
    _(adapter.execution_mode).must_equal :external
  end

  it "prefers explicit configuration over host options over environment defaults" do
    config = DecentJob::Configuration.new
    config.apply_rails_config({execution_mode: :async_all}, environment: "test", logger: nil)
    _(config.execution_mode).must_equal :async_all
    config.execution_mode = :external
    config.apply_rails_config({execution_mode: :inline}, environment: "development", logger: nil)
    _(config.execution_mode).must_equal :external
  end

  it "uses the Rails logger unless an application selected a logger" do
    config = DecentJob::Configuration.new
    host_logger = Logger.new(File::NULL)
    config.apply_rails_config({}, environment: "test", logger: host_logger)
    _(config.logger).must_equal host_logger
    custom_logger = Logger.new(File::NULL)
    config.logger = custom_logger
    config.apply_rails_config({}, environment: "test", logger: host_logger)
    _(config.logger).must_equal custom_logger
  end

  it "allows host options to supply their own logger" do
    config = DecentJob::Configuration.new
    custom_logger = Logger.new(File::NULL)
    config.apply_rails_config({logger: custom_logger}, environment: "test", logger: Logger.new(File::NULL))
    _(config.logger).must_equal custom_logger
  end

  it "applies host options once without overwriting later application configuration" do
    config = DecentJob::Configuration.new
    options = {max_threads: 2, database_url: "postgres:///example"}
    config.apply_rails_config(options, environment: "test", logger: nil)
    _(config.max_threads).must_equal 2
    _(config.database_url).must_equal "postgres:///example"
    config.max_threads = 3
    config.apply_rails_config(options, environment: "test", logger: nil)
    _(config.max_threads).must_equal 3
  end

  it "reports default thread errors to the logger of a copied configuration" do
    config = DecentJob.configuration.dup
    output = StringIO.new
    config.apply_rails_config({}, environment: "test", logger: Logger.new(output))
    config.on_thread_error.call(RuntimeError.new("host failure"))
    _(output.string).must_include "host failure"
  end

  it "suppresses adapter startup throughout a CLI command and restores the caller context" do
    observed = nil
    output = StringIO.new
    DecentJob.stub(:install, -> { observed = DecentJob.async_ready? }) do
      _(DecentJob::CLI.new(stdout: output).run(["migrate"])).must_equal 0
    end
    _(observed).must_equal false
    _(DecentJob.async_ready?).must_equal true
  end

  it "allows validating cron classes to load an Active Job adapter during Rails configuration" do
    namespace = Module.new
    namespace.define_singleton_method(:const_missing) do |name|
      const_set(name, Class.new(ActiveJob::Base) { self.queue_adapter = :decent_job })
    end
    Object.const_set(:HostConfigurationCron, namespace)
    host = Module.new
    host.define_singleton_method(:env) { "test" }
    host.define_singleton_method(:logger) { nil }
    Object.const_set(:Rails, host)
    DecentJob.prepare_rails!
    options = {cron: {hourly: {cron: "0 * * * *", class: "HostConfigurationCron::HourlyJob"}}}
    application = Struct.new(:config).new(Struct.new(:decent_job).new(options))
    DecentJob.configure_rails(application)
    _(HostConfigurationCron::HourlyJob.queue_adapter).must_be_kind_of DecentJob::Adapter
  ensure
    Object.send(:remove_const, :Rails)
    Object.send(:remove_const, :HostConfigurationCron)
  end

  it "permits selecting adapters inside a configuration block" do
    job_class = Class.new(ActiveJob::Base)
    DecentJob.configure do |config|
      config.execution_mode = :external
      job_class.queue_adapter = :decent_job
    end
    _(job_class.queue_adapter.execute_external?).must_equal true
  end

  it "does not open a database or start adapters while the host is booting" do
    DecentJob.prepare_rails!
    adapter = DecentJob::Adapter.new(execution_mode: :async_all)
    _(adapter.start_async).must_be_nil
    _(adapter.async_started?).must_equal false
    DecentJob.configure { |config| config.max_threads = 2 }
  ensure
    DecentJob.finish_rails_initialization!
  end

  it "does not start unconfigured adapters after the host is ready" do
    DecentJob.prepare_rails!
    adapter = DecentJob::Adapter.new(execution_mode: :async_all)
    DecentJob.configuration.database_url = nil
    DecentJob.finish_rails_initialization!
    _(adapter.async_started?).must_equal false
  end

  it "excludes reset and startup while Rails configuration is resolving application options" do
    entered = Queue.new
    release = Queue.new
    host = Module.new
    host.define_singleton_method(:env) { "test" }
    host.define_singleton_method(:logger) { nil }
    Object.const_set(:Rails, host)
    options = {max_threads: 2}
    options.define_singleton_method(:each) do |&block|
      DecentJob.stats
      entered << true
      release.pop
      super(&block)
    end
    application = Struct.new(:config).new(Struct.new(:decent_job).new(options))
    configuring = Thread.new { DecentJob.configure_rails(application) }
    Timeout.timeout(3) { entered.pop }
    _ { DecentJob.reset! }.must_raise DecentJob::LifecycleInProgress
    owner = DecentJob::CronManager.new
    _ { owner.start }.must_raise DecentJob::LifecycleInProgress
    release << true
    configuring.value
    _(DecentJob.configuration.max_threads).must_equal 2
  ensure
    release << true if release
    configuring&.join(3)
    owner&.shutdown
    Object.send(:remove_const, :Rails) if Object.const_defined?(:Rails)
  end
end
