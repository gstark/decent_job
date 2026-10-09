# frozen_string_literal: true

require "spec_helper"

# These tests exercise configuration ownership without opening a database.
describe "configuration sources" do
  before do
    @saved_environment = ENV.to_h
    ENV.keys.grep(/\A(?:DECENT_JOB_|RAILS_MAX_THREADS\z|DATABASE_URL\z)/).each { |name| ENV.delete(name) }
    DecentJob.reset!
  end

  after do
    ENV.replace(@saved_environment)
    DecentJob.reset!
  end

  def host(config, options = {})
    config.apply_rails_config(options, environment: "production", logger: nil)
  end

  def assert_setting(expected, actual, name)
    expected.nil? ? assert_nil(actual, name.to_s) : assert_equal(expected, actual, name.to_s)
  end

  def source_cases
    {
      database_url: ["postgres:///explicit", "postgres:///host", "postgres:///changed"],
      queues: ["explicit", "host", "changed"],
      max_threads: [2, 3, 4],
      handled_exceptions: [[ArgumentError], [RuntimeError], [StandardError]],
      retry_on_unhandled_error: [false, true, true],
      enable_listen_notify: [false, true, true],
      shutdown_timeout: [0, 1, 2],
      enable_pauses: [false, true, true],
      preserve_job_records: [false, true, :on_unhandled_error],
      cron: [{explicit: {cron: "* * * * *"}}, {host: {cron: "0 * * * *"}}, {changed: {cron: "@daily"}}],
      enable_cron: [false, true, true],
      cron_graceful_restart_period: [nil, 60, 120],
      max_cache: [0, 10, 20],
      advisory_lock_heartbeat: [false, true, true],
      process_heartbeat_interval: [1, 2, 3],
      cleanup_discarded_jobs: [false, true, true],
      cleanup_preserved_jobs_before_seconds_ago: [0, 1, 2],
      cleanup_interval_jobs: [false, 10, 20],
      cleanup_interval_seconds: [false, 10, 20],
      probe_port: [0, 1234, 2345],
      probe_handler: [:simple, :webrick, :simple],
      probe_app: [nil, ->(*) {}, ->(*) {}],
      daemonize: [false, true, true],
      pidfile: [File.expand_path("explicit.pid"), File.expand_path("host.pid"), File.expand_path("changed.pid")],
      idle_timeout: [nil, 1, 2],
      lower_thread_priority: [false, true, true],
      subprocesses: [0, 2, 3],
      on_thread_error: [->(*) {}, ->(*) {}, ->(*) {}]
    }
  end

  it "retains every explicit generic setting assigned before the first host application" do
    source_cases.each do |name, (explicit, rails, changed)|
      config = DecentJob::Configuration.new
      config.public_send("#{name}=", explicit)
      host(config, name => rails)
      assert_setting explicit, config.public_send(name), name
      host(config, name.to_s => changed)
      assert_setting explicit, config.public_send(name), name
      host(config)
      assert_setting explicit, config.public_send(name), name
    end
  end

  it "retains explicit settings assigned after host application when the host changes or removes them" do
    source_cases.each do |name, (explicit, rails, changed)|
      config = DecentJob::Configuration.new
      host(config, name => rails)
      config.public_send("#{name}=", explicit)
      host(config, name.to_s => changed)
      assert_setting explicit, config.public_send(name), name
      host(config)
      assert_setting explicit, config.public_send(name), name
    end
  end

  it "updates unowned host settings and restores the captured baseline when keys disappear" do
    source_cases.except(:on_thread_error).each do |name, (_, rails, changed)|
      config = DecentJob::Configuration.new
      config.cron = {task: {cron: "@hourly", class: "ActiveJob::Base"}} if name == :enable_cron
      baseline = config.public_send(name)
      host(config, name.to_s => rails)
      assert_setting rails, config.public_send(name), name
      host(config, name => changed)
      assert_setting changed, config.public_send(name), name
      host(config)
      assert_setting baseline, config.public_send(name), name
    end
  end

  it "restores the dynamic default thread error callback when its host key disappears" do
    config = DecentJob::Configuration.new
    callback = ->(*) {}
    host(config, on_thread_error: callback)
    assert_same callback, config.on_thread_error
    host(config)
    refute_same callback, config.on_thread_error
    assert_respond_to config.on_thread_error, :call
  end

  it "does not grant explicit ownership to invalid assignments rejected by existing setters" do
    {max_cache: -1, subprocesses: -1, probe_port: -1, probe_app: false,
     idle_timeout: -1, lower_thread_priority: nil, daemonize: "off",
     advisory_lock_heartbeat: "off", cleanup_discarded_jobs: "off",
     process_heartbeat_interval: 0, cleanup_interval_jobs: "bogus",
     cleanup_interval_seconds: "bogus", cleanup_preserved_jobs_before_seconds_ago: -1,
     pidfile: "", probe_handler: 1}.each do |name, invalid|
      config = DecentJob::Configuration.new
      _, rails, changed = source_cases.fetch(name)
      host(config, name => rails)
      assert_raises(ArgumentError, name.to_s) { config.public_send("#{name}=", invalid) }
      host(config, name => changed)
      assert_setting changed, config.public_send(name), name
    end
  end

  it "keeps explicit database nil while rejecting nil thread counts at validation" do
    config = DecentJob::Configuration.new
    config.database_url = nil
    host(config, database_url: "postgres:///host")
    assert_nil config.database_url
    config.max_threads = nil
    assert_raises(ArgumentError) { config.validate! }
  end

  it "rejects unknown or non-setting host keys with ArgumentError before invoking them" do
    [:validate!, :apply_rails_config, :instance_variable_set, :unknown, 1].each do |key|
      config = DecentJob::Configuration.new
      assert_raises(ArgumentError) { host(config, key => true) }
      assert_raises(ArgumentError) { host(config, {:max_threads => 3, key => true}) }
    end
  end

  it "normalizes string keys for existing specialized source settings" do
    config = DecentJob::Configuration.new
    custom_logger = Logger.new(File::NULL)
    host(config, "execution_mode" => "inline", "logger" => custom_logger,
      "poll_interval" => 2, "lock_strategy" => "hybrid",
      "dequeue_query_sort" => "scheduled_at", "queue_select_limit" => 0)
    assert_equal :inline, config.execution_mode
    assert_same custom_logger, config.logger
    assert_equal 2, config.poll_interval
    assert_equal :hybrid, config.lock_strategy
    assert_equal :scheduled_at, config.dequeue_query_sort
    assert_equal 0, config.queue_select_limit
  end

  it "keeps nested edits through getters when identical host values are reapplied" do
    config = DecentJob::Configuration.new
    options = {cron: {task: {args: [+"original"]}}, database_url: {host: +"original"}}
    host(config, options)
    config.cron[:task][:args].first.replace("edited")
    config.database_url[:host].replace("edited")
    host(config, {"cron" => {task: {args: [+"original"]}}, "database_url" => {host: +"original"}})
    assert_equal ["edited"], config.cron[:task][:args]
    assert_equal "edited", config.database_url[:host]
    assert_equal "original", options[:database_url][:host]
    assert_equal ["original"], options[:cron][:task][:args]
    host(config, cron: {replacement: {}}, database_url: {host: "replacement"})
    assert_equal({replacement: {}}, config.cron)
    assert_equal({host: "replacement"}, config.database_url)
  end

  it "owns recursively copied containers while retaining application object identities" do
    config = DecentJob::Configuration.new
    callback = ->(*) {}
    application = Object.new
    application.define_singleton_method(:call) {}
    custom_logger = Logger.new(File::NULL)
    config.logger = custom_logger
    config.probe_app = application
    config.on_thread_error = callback
    config.handled_exceptions = [StandardError, +"RuntimeError"]
    host(config, cron: {task: {args: [+"original"], class: ActiveJob::Base, callable: callback}}, database_url: {host: +"original"})
    copy = config.dup
    copy.cron[:task][:args].first.replace("copy")
    copy.database_url[:host].replace("copy")
    copy.handled_exceptions.last.replace("ArgumentError")
    assert_equal ["original"], config.cron[:task][:args]
    assert_equal "original", config.database_url[:host]
    assert_equal "RuntimeError", config.handled_exceptions.last
    assert_same StandardError, copy.handled_exceptions.first
    assert_same ActiveJob::Base, copy.cron[:task][:class]
    assert_same callback, copy.cron[:task][:callable]
    assert_same callback, copy.on_thread_error
    assert_same application, copy.probe_app
    assert_same custom_logger, copy.logger
    copy.max_threads = 9
    host(config, max_threads: 3)
    assert_equal 3, config.max_threads
    host(copy, max_threads: 4)
    assert_equal 9, copy.max_threads
  end

  it "copies shared and cyclic containers, including mutable hash defaults" do
    config = DecentJob::Configuration.new
    shared = [+"original"]
    container = Hash.new(shared)
    container[:first] = shared
    container[:second] = shared
    container[:cycle] = container
    config.database_url = container
    copy = config.dup
    assert_same copy.database_url, copy.database_url[:cycle]
    assert_same copy.database_url[:first], copy.database_url[:second]
    assert_same copy.database_url[:first], copy.database_url.default
    copy.database_url[:missing].first.replace("copy")
    assert_equal ["original"], config.database_url.default
  end

  it "preserves accepted generic nil setter meanings across host applications" do
    {database_url: [nil, "postgres:///host"], idle_timeout: [nil, 1],
     probe_port: [nil, 1234], probe_handler: [nil, :simple], probe_app: [nil, ->(*) {}],
     cron_graceful_restart_period: [nil, 60], cleanup_interval_jobs: [1000, 10],
     cleanup_interval_seconds: [600, 10]}.each do |name, (expected, rails)|
      config = DecentJob::Configuration.new
      config.public_send("#{name}=", nil)
      host(config, name => rails)
      assert_setting expected, config.public_send(name), name
      host(config)
      assert_setting expected, config.public_send(name), name
    end
  end

  it "isolates rejected configure edits from the active configuration" do
    DecentJob.configure { |config| config.database_url = {host: +"original"} }
    original = DecentJob.configuration
    assert_raises(ArgumentError) do
      DecentJob.configure do |config|
        config.database_url[:host].replace("rejected")
        config.cron[:invalid] = {}
        config.handled_exceptions << "RejectedError"
        config.max_threads = 0
      end
    end
    assert_same original, DecentJob.configuration
    assert_equal({host: +"original"}, original.database_url)
    assert_empty original.cron
    assert_equal [StandardError, NotImplementedError], original.handled_exceptions
    host(original, max_threads: 3)
    assert_equal 3, original.max_threads
  end

  it "isolates rejected CLI and host candidates without leaking source ownership" do
    host(DecentJob.configuration, max_threads: 3, database_url: {host: +"original"})
    original = DecentJob.configuration
    assert_raises(ArgumentError) { DecentJob.apply_cli_configuration(max_threads: 7, idle_timeout: -1) }
    assert_same original, DecentJob.configuration
    assert_equal 3, original.max_threads
    host(original, max_threads: 4, database_url: {host: +"original"})
    assert_equal 4, original.max_threads

    rails = Module.new
    rails.define_singleton_method(:env) { "production" }
    rails.define_singleton_method(:logger) { nil }
    Object.const_set(:Rails, rails)
    options = {database_url: {host: "rejected"}, cron: {invalid: {}}, max_threads: 0}
    application = Struct.new(:config).new(Struct.new(:decent_job).new(options))
    assert_raises(ArgumentError) { DecentJob.configure_rails(application) }
    assert_same original, DecentJob.configuration
    assert_equal({host: "original"}, original.database_url)
    assert_empty original.cron
    host(original, max_threads: 5)
    assert_equal 5, original.max_threads
  ensure
    Object.send(:remove_const, :Rails) if Object.const_defined?(:Rails)
  end

  it "retains specialized override precedence and existing nil-clearing behavior" do
    ENV["DECENT_JOB_EXECUTION_MODE"] = "external"
    ENV["DECENT_JOB_LOCK_STRATEGY"] = "skiplocked"
    ENV["DECENT_JOB_DEQUEUE_QUERY_SORT"] = "scheduled_at"
    ENV["DECENT_JOB_QUEUE_SELECT_LIMIT"] = "23"
    ENV["DECENT_JOB_POLL_INTERVAL"] = "4"
    config = DecentJob::Configuration.new
    explicit_logger = Logger.new(File::NULL)
    host_logger = Logger.new(File::NULL)
    explicit = {execution_mode: :async_all, lock_strategy: :hybrid, dequeue_query_sort: :created_at,
                queue_select_limit: 0, poll_interval: 0, logger: explicit_logger}
    explicit.each { |name, value| config.public_send("#{name}=", value) }
    host(config, execution_mode: :inline, lock_strategy: :advisory, dequeue_query_sort: :scheduled_at,
      queue_select_limit: 2, poll_interval: 2, logger: host_logger)
    explicit.each { |name, value| assert_equal value, config.public_send(name), name.to_s }
    explicit.except(:poll_interval).each_key { |name| config.public_send("#{name}=", nil) }
    assert_equal :inline, config.execution_mode
    assert_equal :advisory, config.lock_strategy
    assert_equal :scheduled_at, config.dequeue_query_sort
    assert_equal 2, config.queue_select_limit
    assert_same host_logger, config.logger
    assert_raises(ArgumentError) { config.poll_interval = nil }
    host(config)
    assert_equal :external, config.execution_mode
    assert_equal :skiplocked, config.lock_strategy
    assert_equal :scheduled_at, config.dequeue_query_sort
    assert_equal 23, config.queue_select_limit
    assert_equal 0, config.poll_interval
    refute_same host_logger, config.logger
  end

  it "restores container baselines independently across copies and repeated host changes" do
    ENV["DECENT_JOB_CRON"] = '{"task":{"args":["environment"]}}'
    config = DecentJob::Configuration.new
    config.cron[:task][:args] << "edited baseline"
    copy = config.dup
    host(copy, cron: {replacement: {}})
    host(copy)
    copy.cron[:task][:args].first.replace("copy")
    host(config, cron: {replacement: {}})
    host(config)
    assert_equal ["environment"], config.cron[:task][:args]
    host(copy, cron: {replacement: {}})
    host(copy)
    assert_equal ["environment"], copy.cron[:task][:args]
  end

  it "gives supervisor recipes independent containers and source ownership" do
    config = DecentJob::Configuration.new
    config.subprocesses = 2
    host(config, database_url: {host: +"original"}, cron: {task: {args: [+"original"]}}, max_threads: 3)
    first, second = config.subprocess_configs
    first.database_url[:host].replace("first")
    first.cron[:task][:args] << "first"
    first.max_threads = 7
    host(second, max_threads: 4)
    assert_equal "original", config.database_url[:host]
    assert_equal ["original"], config.cron[:task][:args]
    assert_equal 3, config.max_threads
    assert_equal 4, second.max_threads
    assert_equal 7, first.max_threads
  end

  it "captures environment baselines and does not reread them after host changes" do
    ENV["DECENT_JOB_MAX_THREADS"] = "7"
    ENV["DECENT_JOB_QUEUES"] = "environment"
    config = DecentJob::Configuration.new
    ENV["DECENT_JOB_MAX_THREADS"] = "9"
    ENV["DECENT_JOB_QUEUES"] = "changed"
    host(config, max_threads: 3, queues: "host")
    host(config)
    assert_equal 7, config.max_threads
    assert_equal "environment", config.queues
  end
end

describe "configuration environment parsing" do
  before { @saved_environment = ENV.to_h }
  after { ENV.replace(@saved_environment) }

  {
    enable_cron: false, enable_pauses: false, enable_listen_notify: true,
    lower_thread_priority: false, advisory_lock_heartbeat: false,
    daemonize: false, cleanup_discarded_jobs: true
  }.each do |name, default|
    it "casts #{name} environment booleans without changing the absent default" do
      variable = "DECENT_JOB_#{name.upcase}"
      ENV["DECENT_JOB_CRON"] = '{"task":{"cron":"@hourly","class":"ActiveJob::Base"}}' if name == :enable_cron
      {nil => default, "" => false, "0" => false, "f" => false, "F" => false,
       "false" => false, "FALSE" => false, "off" => false, "OFF" => false,
       "true" => true, "1" => true, "yes" => true, "False" => true, " " => true}.each do |value, expected|
        ENV[variable] = value
        config = DecentJob::Configuration.new
        assert_equal expected, config.public_send(name), "#{variable}=#{value.inspect}"
        ENV[variable] = expected ? "false" : "true"
        assert_equal expected, config.public_send(name), "#{variable} snapshot"
      end
    end
  end

  it "uses RAILS_MAX_THREADS below DECENT_JOB_MAX_THREADS with strict numeric parsing" do
    ENV.delete("DECENT_JOB_MAX_THREADS")
    ENV.delete("RAILS_MAX_THREADS")
    assert_equal 5, DecentJob::Configuration.new.max_threads
    ENV["RAILS_MAX_THREADS"] = "8"
    config = DecentJob::Configuration.new
    assert_equal 8, config.max_threads
    ENV["RAILS_MAX_THREADS"] = "9"
    assert_equal 8, config.max_threads
    ENV["DECENT_JOB_MAX_THREADS"] = "3"
    assert_equal 3, DecentJob::Configuration.new.max_threads
    ENV.delete("DECENT_JOB_MAX_THREADS")
    ["", "3workers", "2.5"].each do |value|
      ENV["RAILS_MAX_THREADS"] = value
      assert_raises(ArgumentError) { DecentJob::Configuration.new }
    end
  end
end
