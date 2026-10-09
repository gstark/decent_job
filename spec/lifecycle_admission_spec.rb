# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

describe "automatic pool admission" do
  before { DecentJob.reset! }
  after { DecentJob.reset! }

  it "defers implicit-pool adapter startup while a configuration block is running" do
    DecentJob.configuration.database_url = "postgres://unused"
    DecentJob.prepare_rails!
    DecentJob.finish_rails_initialization!
    adapter = nil
    DecentJob.configure do
      adapter = DecentJob::Adapter.new(execution_mode: :async_all)
      _(adapter.async_started?).must_equal false
      _(DecentJob.instance_variable_get(:@database)).must_be_nil
    end
    _(adapter.async_started?).must_equal false
  end

  it "defers pool publication when a transition begins during automatic validation" do
    DecentJob.configuration.database_url = "postgres://unused"
    adapter = DecentJob::Adapter.new(execution_mode: :async_all)
    entered = Queue.new
    release = Queue.new
    configuration = DecentJob.configuration
    original = configuration.method(:validate!)
    configuration.define_singleton_method(:validate!) do
      entered << true
      release.pop
      original.call
    end
    starter = Thread.new { adapter.start_async }
    Timeout.timeout(3) { entered.pop }
    DecentJob.configure do
      release << true
      _(starter.value).must_be_nil
      _(DecentJob.instance_variable_get(:@database)).must_be_nil
    end
  ensure
    release << true if release
    starter&.join(3)
    configuration.singleton_class.remove_method(:validate!) if configuration&.singleton_methods&.include?(:validate!)
  end
end

describe "shared cron restart admission" do
  after { DecentJob.reset! }

  it "preserves a locally stopped shared manager across global restart" do
    pool = Object.new
    first = DecentJob::Adapter.new(database: pool, execution_mode: :external)
    second = DecentJob::Adapter.new(database: pool, execution_mode: :external)
    manager = DecentJob.start_cron(database: pool)
    _(first.shutdown(timeout: 0)).must_equal true
    _(DecentJob.restart(timeout: 0)).must_be_same_as DecentJob
    _(manager.running?).must_equal false
    _(first.stopping?).must_equal :stopping
    _(second.restart(timeout: 0)).must_be_same_as second
    _(manager.running?).must_equal true
    _(first.stopping?).must_equal :stopping
  end

  it "keeps a database-specific cron stop closed until explicit cron startup" do
    pool = Object.new
    manager = DecentJob.start_cron(database: pool)
    DecentJob.stop_cron(database: pool, timeout: 0)
    DecentJob.restart(timeout: 0)
    _(manager.running?).must_equal false
    _(DecentJob.start_cron(database: pool, automatic: true)).must_be_nil
    _(DecentJob.start_cron(database: pool)).must_be_same_as manager
    _(manager.running?).must_equal true
  end
end

describe "performer admission before process registration" do
  include DatabaseSpec

  before do
    DecentJob.reset!
    DecentJob.configure { |config| config.database_url = @connection_options }
    DecentJob.install
  end

  after { DecentJob.reset! }

  [:reset, :configuration, :fork].each do |boundary|
    it "protects standalone maintenance from #{boundary} between database checkouts" do
      entered = Queue.new
      release = Queue.new
      pool = DecentJob.database
      configuration = DecentJob.configuration
      pool.define_singleton_method(:with_connection) do |&block|
        if Thread.current[:pause_maintenance]
          Thread.current[:pause_maintenance] = false
          entered << true
          release.pop
        end
        super(&block)
      end
      performer = DecentJob::Performer.new(database: pool)
      execution = Thread.new do
        Thread.current[:pause_maintenance] = true
        performer.perform_one
      rescue DecentJob::ClosedDatabase => error
        error
      end
      Timeout.timeout(3) { entered.pop }
      _(pool.stats[:active]).must_equal 0
      _(pool.with_connection { |connection| connection.exec("SELECT count(*) FROM decent_job_processes").getvalue(0, 0) }).must_equal "0"
      case boundary
      when :reset
        _(DecentJob.reset!).must_equal false
      when :configuration
        _ { DecentJob.apply_cli_configuration(max_threads: 1) }.must_raise ArgumentError
      when :fork
        _ { DecentJob.with_prepared_fork { flunk "must not fork during maintenance" } }.must_raise DecentJob::ForkInProgress
      end
      _(DecentJob.configuration).must_be_same_as configuration
      _(DecentJob.database).must_be_same_as pool
      release << true
      _(execution.value).must_be_nil
      _(DecentJob.reset!).must_equal true
    ensure
      release << true if release
      execution&.join(3)
    end
  end
end

describe "performer admission release" do
  include DatabaseSpec

  before do
    DecentJob.reset!
    DecentJob.configure { |config| config.database_url = @connection_options }
    DecentJob.install
  end

  after { DecentJob.reset! }

  it "releases admission after errors and nonlocal exits and allows another attempt" do
    pool = DecentJob.database
    pool.define_singleton_method(:with_connection) do |&block|
      operation = Thread.current[:maintenance_exit]
      Thread.current[:maintenance_exit] = nil
      raise IOError, "maintenance failed" if operation == :raise
      throw :maintenance_exit, :returned if operation == :throw
      super(&block)
    end
    performer = DecentJob::Performer.new(database: pool)
    Thread.current[:maintenance_exit] = :raise
    _ { performer.perform_one }.must_raise IOError
    _(performer.active?).must_equal false
    Thread.current[:maintenance_exit] = :throw
    _(catch(:maintenance_exit) { performer.perform_one }).must_equal :returned
    _(performer.active?).must_equal false
    _(performer.perform_one).must_be_nil
    _(performer.active?).must_equal false
    _(DecentJob.reset!).must_equal true
  ensure
    Thread.current[:maintenance_exit] = nil
  end

  it "keeps admission until interrupted database cleanup has unwound" do
    entered = Queue.new
    unwinding = Queue.new
    release = Queue.new
    pool = DecentJob.database
    pool.define_singleton_method(:with_connection) do |&block|
      if Thread.current[:pause_maintenance]
        Thread.current[:pause_maintenance] = false
        begin
          entered << true
          Queue.new.pop
        ensure
          unwinding << true
          release.pop
        end
      end
      super(&block)
    end
    performer = DecentJob::Performer.new(database: pool)
    execution = Thread.new do
      Thread.current[:pause_maintenance] = true
      performer.perform_one
    end
    Timeout.timeout(3) { entered.pop }
    execution.kill
    Timeout.timeout(3) { unwinding.pop }
    _(performer.active?).must_equal true
    _(DecentJob.reset!).must_equal false
    release << true
    _(execution.join(3)).must_equal execution
    _(performer.active?).must_equal false
    _(DecentJob.reset!).must_equal true
  ensure
    release << true if release
    execution&.kill if execution&.alive?
    execution&.join(3)
  end

  it "retains outer admission when a nested call finishes" do
    pool = DecentJob.database
    performer = DecentJob::Performer.new(database: pool)
    active_after_nested = nil
    pool.define_singleton_method(:with_connection) do |&block|
      if Thread.current[:nested_maintenance]
        Thread.current[:nested_maintenance] = false
        performer.perform_one
        active_after_nested = performer.active?
      end
      super(&block)
    end
    Thread.current[:nested_maintenance] = true
    _(performer.perform_one).must_be_nil
    _(active_after_nested).must_equal true
    _(performer.active?).must_equal false
    _(DecentJob.reset!).must_equal true
  ensure
    Thread.current[:nested_maintenance] = nil
  end
end

describe "shared cron callback lifecycle" do
  include DatabaseSpec

  before do
    DecentJob.reset!
    DecentJob::Schema.new(database).migrate
    DecentJob.configuration.enable_cron = true
    DecentJob.configuration.enable_listen_notify = false
    DecentJob.configuration.max_threads = 1
    @deliveries = Queue.new
    DecentJob.configuration.cron = {callback: {
      cron: ->(previous) { previous ? previous + 3600 : Time.now },
      class: -> {
        @deliveries << :delivered
        nil
      }
    }}
  end

  after { DecentJob.reset! }

  [:same_adapter, :shared_adapter].each do |owner|
    it "keeps real callbacks dormant across global restart until #{owner} explicitly resumes them" do
      first = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
      first.start_async
      manager = DecentJob.cron_managers.fetch(0)
      _(Timeout.timeout(3) { @deliveries.pop }).must_equal :delivered
      second = DecentJob::Adapter.new(database: database, execution_mode: :async_all) if owner == :shared_adapter
      second&.start_async
      _(first.shutdown(timeout: 3)).must_equal true
      _(DecentJob.restart(timeout: 3)).must_be_same_as DecentJob
      _(manager.shutdown?).must_equal true
      _(@deliveries).must_be_empty
      _(first.async_started?).must_equal false
      resumer = second || first
      _(resumer.restart(timeout: 3)).must_be_same_as resumer
      _(Timeout.timeout(3) { @deliveries.pop }).must_equal :delivered
      _(manager.running?).must_equal true
      _(first.async_started?).must_equal(owner == :same_adapter)
    end
  end

  it "preserves shared callback dormancy through CLI worker replacement" do
    first = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    second = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    first.start_async
    second.start_async
    previous = DecentJob.cron_managers.fetch(0)
    _(Timeout.timeout(3) { @deliveries.pop }).must_equal :delivered
    _(first.shutdown(timeout: 3)).must_equal true
    DecentJob.apply_cli_configuration(max_threads: 2)
    second.start_async
    _(first.async_started?).must_equal false
    _(second.async_started?).must_equal true
    _(previous.shutdown?).must_equal true
    _(DecentJob.cron_managers).must_be_empty
    _(@deliveries).must_be_empty
    _(first.restart(timeout: 3)).must_be_same_as first
    replacement = DecentJob.cron_managers.fetch(0)
    _(replacement).wont_be_same_as previous
    _(Timeout.timeout(3) { @deliveries.pop }).must_equal :delivered
    _(replacement.running?).must_equal true
  end
end

describe "performer initialization inspection" do
  it "allows concurrent activity inspection at every initialization boundary" do
    snapshots = []
    observer = TracePoint.new(:line) do |event|
      next unless event.defined_class == DecentJob::Performer && event.method_id == :initialize

      instance = event.self
      snapshots << Thread.new do
        instance.active?
      rescue => error
        error
      end.value
    end
    observer.enable { DecentJob::Performer.new(database: Object.new) }
    _(snapshots).wont_be_empty
    _(snapshots.grep(Exception)).must_be_empty
    _(snapshots.compact.uniq).must_equal [false]
  ensure
    observer&.disable
  end
end

describe "explicit global cron resumption" do
  include DatabaseSpec

  before do
    DecentJob.reset!
    @deliveries = Queue.new
    DecentJob.configure do |config|
      config.database_url = @connection_options
      config.max_threads = 1
      config.enable_listen_notify = false
      config.enable_cron = true
      config.cron = {callback: {
        cron: ->(previous) { previous ? previous + 3600 : Time.now },
        class: -> {
          @deliveries << :delivered
          nil
        }
      }}
    end
    DecentJob.install
  end

  after { DecentJob.reset! }

  [:global, :database].each do |scope|
    it "explicitly resumes configured cron after a #{scope} shutdown" do
      DecentJob.start
      _(Timeout.timeout(3) { @deliveries.pop }).must_equal :delivered
      manager = DecentJob.cron_managers.fetch(0)
      stopped = (scope == :global) ? DecentJob.shutdown(timeout: 3) : DecentJob.stop_cron(database: DecentJob.database, timeout: 3)
      _(stopped).must_equal true
      _(DecentJob.start).must_be_kind_of DecentJob::Worker
      _(manager.running?).must_equal true
      _(Timeout.timeout(3) { @deliveries.pop }).must_equal :delivered
    end
  end

  [:before_resolution, :during_resolution].each do |phase|
    it "lets newer shutdown win #{phase} after publishing the global worker" do
      DecentJob.start
      Timeout.timeout(3) { @deliveries.pop }
      manager = DecentJob.cron_managers.fetch(0)
      DecentJob.shutdown(timeout: 3)
      entered = Queue.new
      release = Queue.new
      configuration = DecentJob.configuration
      resolve = configuration.method(:cron_entries)
      configuration.define_singleton_method(:cron_entries) do |**options|
        if Thread.current[:pause_cron_resolution]
          Thread.current[:pause_cron_resolution] = false
          entered << true
          release.pop
        end
        resolve.call(**options)
      end
      start_cron = DecentJob.method(:start_cron)
      DecentJob.define_singleton_method(:start_cron) do |**options|
        if phase == :before_resolution
          entered << true
          release.pop
        else
          Thread.current[:pause_cron_resolution] = true
        end
        start_cron.call(**options)
      end
      starter = Thread.new { DecentJob.start }
      Timeout.timeout(3) { entered.pop }
      _(DecentJob.shutdown(timeout: 3)).must_equal true
      release << true
      worker = starter.value
      _(worker.shutdown?).must_equal true
      _(manager.shutdown?).must_equal true
      _(DecentJob.stopping?).must_equal true
      _(@deliveries).must_be_empty
    ensure
      release << true if release
      starter&.join(3)
      DecentJob.define_singleton_method(:start_cron, start_cron) if start_cron
      configuration.singleton_class.remove_method(:cron_entries) if configuration&.singleton_methods&.include?(:cron_entries)
    end
  end
end

describe "CLI cron stop preservation" do
  after { DecentJob.reset! }

  it "retains database stop gates while replacing managers and adapter workers" do
    pool = Object.new
    adapter = DecentJob::Adapter.new(database: pool, execution_mode: :external)
    previous = DecentJob.start_cron(database: pool)
    _(adapter.shutdown(timeout: 0)).must_equal true
    DecentJob.apply_cli_configuration(max_threads: 2)
    _(adapter.stopping?).must_equal :stopping
    _(DecentJob.start_cron(database: pool, automatic: true)).must_be_nil
    _(DecentJob.cron_managers).must_be_empty
    replacement = DecentJob.start_cron(database: pool)
    _(replacement).wont_be_same_as previous
    _(replacement.running?).must_equal true
  end
end
