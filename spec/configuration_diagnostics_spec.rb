# frozen_string_literal: true

require "open3"
require "rbconfig"
require "spec_helper"

describe "configuration diagnostics" do
  it "starts with empty errors and validates a default configuration" do
    config = DecentJob::Configuration.new
    assert_respond_to config, :errors
    assert_empty config.errors
    assert_equal [], config.errors[:cron]
    assert_equal({}, config.errors.to_h)
    assert_equal [], config.errors.full_messages
    assert config.valid?
    assert_same config, config.validate!
  end

  it "returns copies of error messages" do
    config = DecentJob::Configuration.new
    config.enable_cron = nil
    refute config.valid?
    config.errors[:enable_cron] << "changed"
    config.errors[:missing] << "added"
    assert_equal ["must be true or false"], config.errors[:enable_cron]
    assert_empty config.errors[:missing]
  end

  it "collects independent deferred errors in one pass" do
    config = DecentJob::Configuration.new
    config.handled_exceptions = [Object]
    config.retry_on_unhandled_error = nil
    config.execution_mode = :unknown
    config.queues = "bad:0"
    config.max_threads = 0
    config.shutdown_timeout = -1
    config.on_thread_error = false
    config.enable_cron = nil
    config.enable_listen_notify = "yes"
    config.enable_pauses = 1
    config.cron_graceful_restart_period = 0
    config.preserve_job_records = :unknown
    config.cron = []
    assert_respond_to config, :valid?
    refute config.valid?
    fields = %i[handled_exceptions retry_on_unhandled_error execution_mode queues max_threads shutdown_timeout on_thread_error enable_cron enable_listen_notify enable_pauses cron_graceful_restart_period preserve_job_records cron]
    assert_equal fields.sort, config.errors.to_h.keys.sort
    fields.each { |field| assert config.errors[field].all? { |message| message.is_a?(String) } }
    error = assert_raises(ArgumentError) { config.validate! }
    fields.each { |field| assert_includes error.message, field.to_s }
  end

  it "validates host polling values with the same diagnostics" do
    config = DecentJob::Configuration.new
    config.apply_rails_config({poll_interval: Complex(1, 2)}, environment: "test", logger: nil)
    assert_respond_to config, :valid?
    refute config.valid?
    assert_includes config.errors.to_h.keys, :poll_interval
  end

  it "refreshes diagnostics after correction and repeated validation" do
    config = DecentJob::Configuration.new
    config.shutdown_timeout = -1
    assert_respond_to config, :valid?
    refute config.valid?
    first = config.errors.to_h
    refute config.valid?
    assert_equal first, config.errors.to_h
    config.shutdown_timeout = 0
    assert config.valid?
    assert_empty config.errors
    assert_same config, config.validate!
  end

  it "does not create errors when reading an unknown field" do
    config = DecentJob::Configuration.new
    assert_respond_to config, :errors
    config.errors[:missing] << "local mutation"
    assert_empty config.errors
    assert_equal({}, config.errors.to_h)
  end

  it "duplicates existing diagnostics without sharing arrays or messages" do
    config = DecentJob::Configuration.new
    config.shutdown_timeout = -1
    assert_respond_to config, :valid?
    refute config.valid?
    original = config.errors.to_h
    copy = config.dup
    refute_same config.errors, copy.errors
    copy.errors[:shutdown_timeout].first.replace("changed")
    copy.errors[:shutdown_timeout] << "additional"
    assert_equal original, config.errors.to_h
    copy.shutdown_timeout = 0
    assert copy.valid?
    assert_empty copy.errors
    refute config.valid?
    assert_equal original, config.errors.to_h
  end

  [Complex(1, 2), Float::INFINITY, Float::NAN, "1"].each do |value|
    it "reports nonreal or nonfinite deferred numeric values #{value.inspect}" do
      config = DecentJob::Configuration.new
      config.shutdown_timeout = value
      config.cron_graceful_restart_period = value
      assert_respond_to config, :valid?
      refute config.valid?
      assert_equal %i[cron_graceful_restart_period shutdown_timeout], config.errors.to_h.keys.sort
      assert_raises(ArgumentError) { config.validate! }
    end
  end

  it "collects all cron entries and all invalid fields with entry and class names" do
    config = DecentJob::Configuration.new
    config.cron = {
      :malformed => [],
      :invalid => {cron: "nonsense", class: "MissingDiagnosticsJob", args: 1, kwargs: [], set: []},
      "string_key" => {cron: "@daily", class: "AnotherMissingDiagnosticsJob"}
    }
    assert_respond_to config, :valid?
    refute config.valid?
    messages = config.errors[:cron]
    assert_equal 8, messages.size

    %w[malformed invalid string_key MissingDiagnosticsJob AnotherMissingDiagnosticsJob args kwargs set].each do |part|
      assert_includes messages.join("; "), part
    end

    assert_raises(ArgumentError) { config.cron_entries }
  end

  it "reports malformed cron containers without hiding other options" do
    config = DecentJob::Configuration.new
    config.cron = nil
    config.shutdown_timeout = -1
    assert_respond_to config, :valid?
    refute config.valid?
    assert_equal %i[cron shutdown_timeout], config.errors.to_h.keys.sort
    assert_raises(ArgumentError) { config.cron_entries }
  end

  it "continues after a cron entry construction raises StandardError" do
    attributes = Class.new(Hash) do
      def merge(*)
        raise "cannot build this entry"
      end
    end.new

    config = DecentJob::Configuration.new
    config.cron = {broken: attributes, later: {cron: "wrong"}}
    assert_respond_to config, :valid?
    refute config.valid?
    assert_equal 2, config.errors[:cron].size
    assert_includes config.errors[:cron].join, "broken"
    assert_includes config.errors[:cron].join, "cannot build this entry"
    assert_includes config.errors[:cron].join, "later"
  end

  it "continues after a cron entry validation raises StandardError" do
    value = Object.new
    def value.blank?
      raise "cannot validate this class"
    end
    config = DecentJob::Configuration.new
    config.cron = {broken: {cron: "@daily", class: value}, later: {cron: "wrong"}}
    assert_respond_to config, :valid?
    refute config.valid?
    assert_equal 2, config.errors[:cron].size
    assert_includes config.errors[:cron].join, "cannot validate this class"
    assert_includes config.errors[:cron].join, "later"
  end

  it "propagates fatal exceptions from cron entry validation" do
    value = Object.new
    failure = Class.new(ScriptError).new("fatal")
    value.define_singleton_method(:blank?) { raise failure }
    config = DecentJob::Configuration.new
    config.cron = {fatal: {cron: "@daily", class: value}}
    assert_respond_to config, :valid?
    assert_same failure, assert_raises(failure.class) { config.valid? }
  end

  it "preserves nonlocal exits from cron entry validation" do
    value = Object.new
    value.define_singleton_method(:blank?) { throw :validation_exit, :escaped }
    config = DecentJob::Configuration.new
    config.cron = {escape: {cron: "@daily", class: value}}
    assert_respond_to config, :valid?
    assert_equal :escaped, catch(:validation_exit) { config.valid? }
  end

  it "does not invoke callbacks or open the database while validating" do
    config = DecentJob::Configuration.new
    config.database_url = "postgres://invalid.example/no_database"
    callback = ->(*) { flunk "validation executed an application callback" }
    config.preserve_job_records = callback
    config.on_thread_error = callback
    config.cron = {dynamic: {cron: callback, class: callback, args: callback, kwargs: callback, set: callback, enabled_by_default: callback}}
    assert_respond_to config, :valid?
    assert config.valid?
    assert_same config, config.validate!
  end

  it "allows explicitly disabling error reporting while keeping the default reporter" do
    config = DecentJob::Configuration.new
    assert_respond_to config.on_thread_error, :call
    config.on_thread_error = nil
    assert_respond_to config, :valid?
    assert config.valid?
    assert_nil config.on_thread_error
    assert_same config, config.validate!
    assert_nil config.worker_options[:on_thread_error]
    assert_nil config.cron_manager_options[:on_thread_error]
  end

  it "accepts named and direct cron classes without resolving handled exception names" do
    config = DecentJob::Configuration.new
    config.handled_exceptions = ["DeferredMissingException"]
    config.cron = {named: {cron: "@daily", class: "ActiveJob::Base"}, direct: {cron: "@hourly", class: ActiveJob::Base}}
    assert config.valid?
    assert_empty config.errors
    assert_same config, config.validate!
    assert_equal [:named, :direct], config.cron_entries.map(&:key)
    assert_raises(ArgumentError) { config.handled_exception_classes }
  end

  it "keeps diagnostics and cron settings on rejected configuration candidates isolated" do
    DecentJob.configure { |config| config.cron = {existing: {cron: "@daily"}} }
    active = DecentJob.configuration
    assert active.valid?
    rejected = nil

    error = assert_raises(ArgumentError) do
      DecentJob.configure do |config|
        rejected = config
        config.cron[:existing][:cron] = "not a schedule"
        config.cron[:new] = []
      end
    end

    assert_includes error.message, "existing"
    assert_includes error.message, "new"
    refute_empty rejected.errors
    assert_same active, DecentJob.configuration
    assert_empty active.errors
    assert_equal({existing: {cron: "@daily"}}, active.cron)
  ensure
    DecentJob.reset!
  end

  it "loads and validates without Rails or Active Record" do
    code = <<~RUBY
      require "decent_job"
      abort "validation failed" unless DecentJob::Configuration.new.valid?
      abort "Rails loaded" if defined?(Rails)
      abort "Active Record loaded" if defined?(ActiveRecord)
    RUBY
    output, status = Open3.capture2e(RbConfig.ruby, "-Ilib", "-e", code)
    assert status.success?, output
  end
end
