# frozen_string_literal: true

require "spec_helper"

describe "configuration defaults" do
  before do
    @saved_environment = ENV.to_h
    ENV.keys.grep(/\A(?:DECENT_JOB_|RAILS_MAX_THREADS\z|DATABASE_URL\z)/).each { |name| ENV.delete(name) }
    DecentJob.reset!
  end

  after do
    ENV.replace(@saved_environment)
    DecentJob.reset!
  end

  def apply_host(config, environment: "development", **options)
    config.apply_rails_config(options, environment: environment, logger: nil)
  end

  def definitions(name = :hourly)
    {name => {cron: "0 * * * *", class: "ActiveJob::Base"}}
  end

  it "defaults process advisory locking to Rails development and updates worker options" do
    config = DecentJob::Configuration.new
    assert_equal false, config.advisory_lock_heartbeat
    %w[development test production development].each do |environment|
      apply_host(config, environment: environment)
      expected = environment == "development"
      assert_equal expected, config.advisory_lock_heartbeat
      assert_equal expected, config.worker_options.fetch(:advisory_lock_heartbeat)
    end
    config.process_heartbeat_interval = 2
    assert_equal 2, config.worker_options.fetch(:process_heartbeat_interval)
    assert_raises(ArgumentError) { config.process_heartbeat_interval = nil }
    assert_raises(ArgumentError) { config.advisory_lock_heartbeat = nil }
  end

  it "preserves every captured environment false value above the development default" do
    ["", "0", "f", "F", "false", "FALSE", "off", "OFF"].each do |value|
      ENV["DECENT_JOB_ADVISORY_LOCK_HEARTBEAT"] = value
      config = DecentJob::Configuration.new
      ENV.delete("DECENT_JOB_ADVISORY_LOCK_HEARTBEAT")
      apply_host(config)
      assert_equal false, config.advisory_lock_heartbeat, value.inspect
      apply_host(config, advisory_lock_heartbeat: true)
      assert_equal true, config.advisory_lock_heartbeat
      apply_host(config)
      assert_equal false, config.advisory_lock_heartbeat
    end
  end

  it "restores the applicable heartbeat default after host removal without rereading ENV" do
    config = DecentJob::Configuration.new
    ENV["DECENT_JOB_ADVISORY_LOCK_HEARTBEAT"] = "false"
    apply_host(config, advisory_lock_heartbeat: false)
    assert_equal false, config.advisory_lock_heartbeat
    apply_host(config)
    assert_equal true, config.advisory_lock_heartbeat
    apply_host(config, advisory_lock_heartbeat: true)
    apply_host(config, environment: "production")
    assert_equal false, config.advisory_lock_heartbeat
  end

  it "retains captured true and explicit false across Rails environments and copies" do
    ENV["DECENT_JOB_ADVISORY_LOCK_HEARTBEAT"] = "true"
    config = DecentJob::Configuration.new
    ENV["DECENT_JOB_ADVISORY_LOCK_HEARTBEAT"] = "false"
    apply_host(config, advisory_lock_heartbeat: false)
    copy = config.dup
    copy.advisory_lock_heartbeat = false
    apply_host(config, environment: "production")
    apply_host(copy)
    assert_equal true, config.advisory_lock_heartbeat
    assert_equal false, copy.advisory_lock_heartbeat
    apply_host(copy, advisory_lock_heartbeat: true)
    assert_equal false, copy.worker_options.fetch(:advisory_lock_heartbeat)
  end

  it "lets CLI heartbeat false override development and later host options" do
    apply_host(DecentJob.configuration)
    DecentJob.apply_cli_configuration(advisory_lock_heartbeat: false)
    apply_host(DecentJob.configuration, advisory_lock_heartbeat: true)
    assert_equal false, DecentJob.configuration.worker_options.fetch(:advisory_lock_heartbeat)
  end

  it "treats blank cron ENV as empty while retaining strict JSON parsing" do
    [nil, "", " \t\n", "\u00a0"].each do |value|
      ENV["DECENT_JOB_CRON"] = value
      assert_equal({}, DecentJob::Configuration.new.cron)
    end
    ENV["DECENT_JOB_CRON"] = "not json"
    assert_raises(JSON::ParserError) { DecentJob::Configuration.new }
  end

  it "falls through blank Rails cron options to captured ENV definitions" do
    ENV["DECENT_JOB_CRON"] = JSON.generate(definitions)
    config = DecentJob::Configuration.new
    ENV["DECENT_JOB_CRON"] = JSON.generate(definitions(:changed))
    [{}, nil, false, "", " \t\n", "\u00a0", []].each do |blank|
      apply_host(config, cron: definitions(:host))
      apply_host(config, "cron" => blank)
      assert_equal definitions, config.cron
    end
    apply_host(config, cron: definitions(:replacement))
    assert_equal definitions(:replacement), config.cron
    apply_host(config)
    assert_equal definitions, config.cron
  end

  it "falls through blank Rails cron to the empty default but keeps explicit empty and nil" do
    config = DecentJob::Configuration.new
    apply_host(config, cron: false)
    assert_equal({}, config.cron)
    config.cron = {}
    apply_host(config, cron: definitions)
    assert_equal({}, config.cron)
    config.cron = nil
    apply_host(config, cron: definitions)
    assert_nil config.cron
    refute config.valid?
    assert_includes config.errors[:cron], "must be a Hash"
  end

  it "isolates captured cron definitions across copies and preserves edits for unchanged host sources" do
    ENV["DECENT_JOB_CRON"] = JSON.generate(definitions)
    config = DecentJob::Configuration.new
    copy = config.dup
    apply_host(copy, cron: definitions(:host))
    copy.cron[:host][:cron].replace("@daily")
    apply_host(copy, cron: definitions(:host))
    assert_equal "@daily", copy.cron[:host][:cron]
    apply_host(copy, cron: {})
    copy.cron[:hourly][:cron].replace("@daily")
    assert_equal definitions, config.cron
    apply_host(copy, cron: definitions(:replacement))
    apply_host(copy, cron: nil)
    assert_equal definitions, copy.cron
  end

  it "enables cron only while definitions exist and keeps the predicate and pool size consistent" do
    config = DecentJob::Configuration.new
    base = config.pool_size
    config.enable_cron = true
    assert_equal false, config.enable_cron
    assert_equal false, config.enable_cron?
    assert_equal base, config.pool_size
    config.cron.merge!(definitions)
    assert_equal true, config.enable_cron
    assert_equal true, config.enable_cron?
    assert_equal base + 3, config.pool_size
    config.cron.clear
    assert_equal false, config.enable_cron
    assert_equal base, config.pool_size
    config.cron = definitions(:replacement)
    assert_equal true, config.enable_cron?
    config.cron.delete(:replacement)
    assert_equal false, config.enable_cron?
  end

  it "preserves invalid flag and cron diagnostics even when cron is effectively disabled" do
    config = DecentJob::Configuration.new
    [nil, "true", 1, {}, []].each do |flag|
      config.enable_cron = flag
      refute config.valid?
      assert_includes config.errors[:enable_cron], "must be true or false"
    end
    config.enable_cron = true
    [nil, false, "invalid", [:invalid], 1].each do |invalid|
      config.cron = invalid
      assert_equal false, config.enable_cron
      refute config.valid?
      assert_includes config.errors[:cron], "must be a Hash"
    end
    ["invalid", [:invalid], true, 1].each do |invalid|
      host_config = DecentJob::Configuration.new
      apply_host(host_config, cron: invalid)
      refute host_config.valid?
      assert_includes host_config.errors[:cron], "must be a Hash"
    end
  end
end
