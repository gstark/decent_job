# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

describe "concurrent lifecycle ownership" do
  after { DecentJob.reset! }

  def pop(queue)
    Timeout.timeout(3) { queue.pop }
  end

  it "drains a captured global worker run without stopping an explicitly started replacement" do
    worker = DecentJob::Worker.new(database: Object.new, max_threads: 1, enable_listen_notify: false)
    entered = Queue.new
    release = Queue.new
    draining = Queue.new
    resume = Queue.new

    worker.define_singleton_method(:run) { |*_arguments|
      entered << Thread.current
      release.pop
    }

    DecentJob.start_worker(worker)
    previous = pop(entered)

    worker.define_singleton_method(:begin_shutdown) do
      super().tap do |handle|
        original = handle.method(:wait)

        handle.define_singleton_method(:wait) do |**options|
          draining << true
          resume.pop
          original.call(**options)
        end
      end
    end

    stopper = Thread.new { DecentJob.shutdown(timeout: 1) }
    pop(draining)
    release << true
    previous.join
    worker.start
    replacement = pop(entered)
    resume << true
    _(stopper.value).must_equal true
    _(replacement.alive?).must_equal true
    _(worker.running?).must_equal true
  ensure
    worker.singleton_class.remove_method(:begin_shutdown) if worker&.singleton_methods&.include?(:begin_shutdown)
    2.times { release << true } if release
    resume << true if resume
    stopper&.join(1)
    worker&.shutdown(timeout: 1)
  end

  [DecentJob::Worker, DecentJob::CronManager].each do |klass|
    it "lets a newer shutdown supersede #{klass} restart" do
      owner = (klass == DecentJob::Worker) ? klass.new(database: Object.new, max_threads: 1, enable_listen_notify: false) : klass.new
      draining = Queue.new
      resume = Queue.new
      original_begin = owner.method(:begin_shutdown) if owner.respond_to?(:begin_shutdown)
      _(original_begin).wont_be_nil

      owner.define_singleton_method(:begin_shutdown) do
        original_begin.call.tap do |handle|
          original = handle.method(:wait)

          handle.define_singleton_method(:wait) do |**options|
            draining << true
            resume.pop
            original.call(**options)
          end
        end
      end

      restarter = Thread.new { owner.restart(timeout: 1) }
      pop(draining)
      owner.singleton_class.remove_method(:begin_shutdown)
      _(owner.shutdown(timeout: 0)).must_equal true
      resume << true
      _(restarter.value).must_equal false
      _(owner.shutdown?).must_equal true
    ensure
      owner.singleton_class.remove_method(:begin_shutdown) if owner&.singleton_methods&.include?(:begin_shutdown)
      resume << true if resume
      restarter&.join(1)
      owner&.shutdown(timeout: 1)
    end
  end
end

describe "adapter and global restart" do
  include DatabaseSpec

  before do
    DecentJob.reset!
    DecentJob::Schema.new(database).migrate
    DecentJob.configuration.max_threads = 1
    DecentJob.configuration.enable_listen_notify = false
    DecentJob.configuration.poll_interval = -1
    RecordingJob.events = Queue.new
    BlockingJob.started = Queue.new
    BlockingJob.release = Queue.new
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    [RecordingJob, BlockingJob].each { |job| job.queue_adapter = @adapter }
  end

  after do
    BlockingJob.release << true
    DecentJob.reset!
  end

  def pop(queue)
    Timeout.timeout(3) { queue.pop }
  end

  [:adapter, :global].each do |scope|
    it "drains #{scope} jobs before restart and leaves stopped enqueue dormant" do
      target = (scope == :adapter) ? @adapter : DecentJob
      BlockingJob.perform_later
      pop(BlockingJob.started)
      worker = @adapter.instance_variable_get(:@worker)
      process_id = worker.process_id
      _(target.respond_to?(:restart)).must_equal true
      _(target.restart(timeout: 0)).must_equal false
      RecordingJob.perform_later("after restart")
      _(RecordingJob.events).must_be_empty
      BlockingJob.release << true
      _(target.shutdown(timeout: 3)).must_equal true
      _(RecordingJob.events).must_be_empty
      _(target.restart(timeout: 3)).must_be :!=, false
      _(pop(RecordingJob.events)).must_equal "after restart"
      _(worker.process_id).wont_equal process_id
      _(DecentJob.stats[:schedulers].size).must_equal 1
    end
  end

  it "does not let global restart reopen an explicitly shut down adapter" do
    @adapter.start_async
    _(@adapter.shutdown(timeout: 3)).must_equal true
    _(DecentJob.respond_to?(:restart)).must_equal true
    DecentJob.restart(timeout: 3)
    RecordingJob.perform_later("dormant")
    _(@adapter.async_started?).must_equal false
    _(RecordingJob.events).must_be_empty
    @adapter.restart(timeout: 3)
    _(pop(RecordingJob.events)).must_equal "dormant"
  end
end

describe "reset lifecycle boundaries" do
  include DatabaseSpec

  before do
    DecentJob.reset!

    DecentJob.configure do |config|
      config.database_url = @connection_options
      config.enable_listen_notify = false
      config.max_threads = 1
    end

    DecentJob.install
  end

  after { DecentJob.reset! }

  it "returns false from a cron-owned reset without closing the database during unwind" do
    result = Queue.new
    release = Queue.new
    pool = DecentJob.database

    DecentJob.configuration.cron = {self_reset: {cron: ->(previous) { previous ? previous + 60 : Time.now }, class: -> {
      result << DecentJob.reset!
      release.pop
      pool.with_connection { |connection| result << connection.exec("SELECT 1").getvalue(0, 0) }
    }}}

    manager = DecentJob.start_cron
    _(Timeout.timeout(3) { result.pop }).must_equal false
    release << true
    _(Timeout.timeout(3) { result.pop }).must_equal "1"
    _(manager.shutdown(timeout: 3)).must_equal true
    DecentJob.reset!
    _ { pool.with_connection {} }.must_raise DecentJob::ClosedDatabase
  ensure
    release << true if release
  end

  it "prevents direct startup during reset drain without holding the registry mutex" do
    entered = Queue.new
    release = Queue.new

    DecentJob.configuration.cron = {blocked: {cron: ->(previous) { previous ? previous + 60 : Time.now }, class: -> {
      entered << true
      release.pop
      DecentJob.database
    }}}

    manager = DecentJob.start_cron
    Timeout.timeout(3) { entered.pop }
    reset = Thread.new { DecentJob.reset! }
    Timeout.timeout(3) { sleep 0.001 while manager.running? }
    worker = DecentJob::Worker.new(database: database, max_threads: 1, enable_listen_notify: false)
    _ { worker.start }.must_raise DecentJob::LifecycleInProgress
    release << true
    _(reset.join(3)).must_equal reset
    _(worker.shutdown?).must_equal true
  ensure
    release << true if release
    reset&.join(3)
    worker&.shutdown(timeout: 3)
  end

  it "runs configuration blocks without the runtime mutex while excluding startup" do
    entered = Queue.new
    release = Queue.new
    DecentJob.reset!

    configuring = Thread.new do
      DecentJob.configure do |config|
        _(DecentJob.stopping?).must_equal false
        entered << true
        release.pop
        config.max_threads = 2
      end
    end

    Timeout.timeout(3) { entered.pop }
    owner = DecentJob::CronManager.new
    _ { owner.start }.must_raise DecentJob::LifecycleInProgress
    release << true
    configuring.value
    _(DecentJob.configuration.max_threads).must_equal 2
  ensure
    release << true if release
    configuring&.join(1)
    owner&.shutdown
  end
end

describe "automatic lifecycle admission" do
  include DatabaseSpec

  before do
    DecentJob.reset!
    DecentJob::Schema.new(database).migrate
    DecentJob.configuration.max_threads = 1
    DecentJob.configuration.enable_listen_notify = false
    RecordingJob.events = Queue.new
  end

  after { DecentJob.reset! }

  it "keeps directly stopped registered workers dormant on adapter enqueue and readiness" do
    adapter = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    RecordingJob.queue_adapter = adapter
    worker = adapter.start_async
    _(worker.shutdown(timeout: 3)).must_equal true
    RecordingJob.perform_later("held")
    DecentJob.finish_rails_initialization!
    _(worker.shutdown?).must_equal true
    _(RecordingJob.events).must_be_empty
    adapter.restart(timeout: 3)
    _(Timeout.timeout(3) { RecordingJob.events.pop }).must_equal "held"
  end

  it "keeps reset-retired adapters retired even after explicit restart" do
    adapter = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    adapter.start_async
    DecentJob.reset!
    _(adapter.restart(timeout: 3)).must_equal false
    _(adapter.start_async).must_be_nil
  end

  it "rebuilds adapter worker recipes only after CLI configuration has drained them" do
    adapter = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    worker = adapter.start_async
    DecentJob.apply_cli_configuration(max_threads: 2)
    replacement = adapter.start_async
    _(replacement).wont_be_same_as worker
    _(replacement.stats[:schedulers].first[:max_threads]).must_equal 2
    _(worker.shutdown?).must_equal true
  end

  it "excludes explicit startup while CLI configuration waits for a callback" do
    entered = Queue.new
    release = Queue.new

    DecentJob.configuration.cron = {blocked: {cron: ->(previous) { previous ? previous + 60 : Time.now }, class: -> {
      entered << true
      release.pop
    }}}

    manager = DecentJob.start_cron(database: database)
    Timeout.timeout(3) { entered.pop }
    applying = Thread.new { DecentJob.apply_cli_configuration(max_threads: 2) }
    Timeout.timeout(3) { sleep 0.001 while manager.running? }
    owner = DecentJob::CronManager.new
    _ { owner.start }.must_raise DecentJob::LifecycleInProgress
    release << true
    _(applying.join(3)).must_equal applying
  ensure
    release << true if release
    applying&.join(3)
    owner&.shutdown
  end
end

describe "runtime startup transitions" do
  include DatabaseSpec

  before do
    DecentJob.reset!
    DecentJob::Schema.new(database).migrate
    DecentJob.configuration.max_threads = 1
    DecentJob.configuration.enable_listen_notify = false
    BlockingJob.started = Queue.new
    BlockingJob.release = Queue.new
  end

  after do
    BlockingJob.release << true
    DecentJob.reset!
  end

  it "leaves global start stopped while any previous registered run drains" do
    DecentJob.configuration.database_url = @connection_options
    adapter = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    BlockingJob.queue_adapter = adapter
    BlockingJob.perform_later
    Timeout.timeout(3) { BlockingJob.started.pop }
    _(DecentJob.shutdown(timeout: 0)).must_equal false
    _(DecentJob.start).must_equal false
    _(DecentJob.stopping?).must_equal true
    _(DecentJob.instance_variable_get(:@database)).must_be_nil
    _(DecentJob.stats[:schedulers].size).must_equal 1
  end

  it "validates database configuration outside the runtime mutex" do
    DecentJob.configuration.database_url = @connection_options

    DecentJob.configuration.define_singleton_method(:validate!) do
      DecentJob.stats
      super()
    end

    _(DecentJob.database).must_be_kind_of DecentJob::Database
  end

  it "allows adapter registration in a configuration block while deferring startup" do
    DecentJob.prepare_rails!
    DecentJob.finish_rails_initialization!
    adapter = nil

    DecentJob.configure do |config|
      config.max_threads = 1
      adapter = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
      _(adapter.async_started?).must_equal false
    end

    _(adapter.start_async).must_be_kind_of DecentJob::Worker
  end

  it "requires every registered owner for global readiness, including locally stopped adapters" do
    first = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    second = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    first.start_async
    second.start_async
    Timeout.timeout(3) { sleep 0.001 until DecentJob.connected? }
    first.shutdown(timeout: 3)
    _(DecentJob.started?).must_equal false
    _(DecentJob.connected?).must_equal false
    DecentJob.restart(timeout: 3)
    _(second.async_started?).must_equal true
    _(first.async_started?).must_equal false
    _(DecentJob.started?).must_equal false
  end
end

describe "parallel lifecycle calls" do
  after { DecentJob.reset! }

  def concurrently(count, &operation)
    ready = Queue.new
    release = Queue.new

    threads = Array.new(count) do
      Thread.new do
        ready << true
        release.pop
        operation.call
      end
    end

    count.times { Timeout.timeout(3) { ready.pop } }
    count.times { release << true }
    threads.map(&:value)
  ensure
    threads&.each { |thread| thread.join(1) }
  end

  it "creates only one worker run for simultaneous explicit starts" do
    entered = Queue.new
    release = Queue.new
    worker = DecentJob::Worker.new(database: Object.new, max_threads: 1, enable_listen_notify: false)

    worker.define_singleton_method(:run) { |*_arguments|
      entered << Thread.current
      release.pop
    }

    concurrently(8) { worker.start }
    Timeout.timeout(3) { entered.pop }
    _(entered).must_be_empty
    _(worker.stats[:schedulers].first[:max_threads]).must_equal 1
  ensure
    release << true if release
    worker&.shutdown(timeout: 3)
  end

  it "creates only one cron run for simultaneous explicit starts" do
    entered = Queue.new
    entry = Object.new

    entry.define_singleton_method(:next_at) { |**|
      entered << true
      Time.now + 3600
    }

    manager = DecentJob::CronManager.new([entry])
    concurrently(8) { manager.start }
    Timeout.timeout(3) { entered.pop }
    _(entered).must_be_empty
    _(manager.running?).must_equal true
  ensure
    manager&.shutdown(timeout: 3)
  end

  it "coalesces concurrent worker restarts after every stop has captured the old run" do
    worker = DecentJob::Worker.new(database: Object.new, max_threads: 1, enable_listen_notify: false)
    entered = Queue.new
    release = Queue.new
    waiting = Queue.new
    resume = Queue.new

    worker.define_singleton_method(:run) { |*_arguments|
      entered << true
      release.pop
    }

    worker.define_singleton_method(:begin_shutdown) do
      super().tap do |handle|
        original = handle.method(:wait)

        handle.define_singleton_method(:wait) do |**options|
          waiting << true
          resume.pop
          original.call(**options)
        end
      end
    end

    threads = Array.new(8) { Thread.new { worker.restart(timeout: 3) } }
    8.times { Timeout.timeout(3) { waiting.pop } }
    8.times { resume << true }
    results = threads.map(&:value)
    _(results.count(false)).must_equal 7
    _(results.count(worker)).must_equal 1
    Timeout.timeout(3) { entered.pop }
    _(entered).must_be_empty
  ensure
    worker.singleton_class.remove_method(:begin_shutdown) if worker&.singleton_methods&.include?(:begin_shutdown)
    8.times { resume << true } if resume
    release << true if release
    threads&.each { |thread| thread.join(1) }
    worker&.shutdown(timeout: 3)
  end
end

describe "startup generation boundaries" do
  include DatabaseSpec

  before { DecentJob.reset! }
  after { DecentJob.reset! }

  it "does not publish a cron run whose configuration resolution raced a later shutdown" do
    entered = Queue.new
    release = Queue.new
    configuration = DecentJob.configuration
    original = configuration.method(:cron_entries)

    configuration.define_singleton_method(:cron_entries) do |**options|
      entered << true
      release.pop
      original.call(**options)
    end

    starter = Thread.new { DecentJob.start_cron(database: database) }
    Timeout.timeout(3) { entered.pop }
    _(DecentJob.shutdown(timeout: 0)).must_equal true
    release << true
    _(starter.value).must_equal false
    _(DecentJob.cron_managers).must_be_empty
    _(DecentJob.stopping?).must_equal true
  ensure
    release << true if release
    starter&.join(1)
    configuration.singleton_class.remove_method(:cron_entries) if configuration&.singleton_methods&.include?(:cron_entries)
  end

  it "does not close a pool still used by a manually owned worker during reset" do
    DecentJob.configure { |config| config.database_url = @connection_options }
    DecentJob.install
    pool = DecentJob.database
    DecentJob.configuration.enable_listen_notify = false
    BlockingJob.started = Queue.new
    BlockingJob.release = Queue.new
    BlockingJob.queue_adapter = DecentJob::Adapter.new(database: pool, execution_mode: :external)
    BlockingJob.perform_later
    worker = DecentJob::Worker.new(database: pool, max_threads: 1, enable_listen_notify: false).start
    Timeout.timeout(3) { BlockingJob.started.pop }
    _(DecentJob.reset!).must_equal false
    _(worker.running?).must_equal true
    _(pool.with_connection { |connection| connection.exec("SELECT 1").getvalue(0, 0) }).must_equal "1"
    BlockingJob.release << true
    _(worker.shutdown(timeout: 3)).must_equal true
    _(DecentJob.reset!).must_equal true
    _ { pool.with_connection {} }.must_raise DecentJob::ClosedDatabase
  ensure
    BlockingJob.release << true
    worker&.shutdown(timeout: 3)
  end
end

describe "interrupted runtime transitions" do
  it "releases the startup gate when interrupted during transition acquisition" do
    entered = Queue.new
    release = Queue.new
    mutex = DecentJob.runtime_mutex

    mutex.define_singleton_method(:synchronize) do |&block|
      super(&block).tap do
        if Thread.current[:pause_transition] && DecentJob.instance_variable_get(:@runtime_transition)
          Thread.current[:pause_transition] = false
          entered << true
          release.pop
        end
      end
    end

    configuring = Thread.new do
      Thread.current[:pause_transition] = true
      DecentJob.configure { |config| config.max_threads = 1 }
    end

    Timeout.timeout(3) { entered.pop }
    configuring.kill
    release << true
    configuring.join(3)
    mutex.singleton_class.remove_method(:synchronize)
    _(DecentJob.instance_variable_get(:@runtime_transition)).must_be_nil
    manager = DecentJob::CronManager.new.start
    _(manager.running?).must_equal true
  ensure
    release << true if release
    configuring&.join(1)
    mutex.singleton_class.remove_method(:synchronize) if mutex&.singleton_methods&.include?(:synchronize)
    DecentJob.instance_variable_set(:@runtime_transition, nil)
    manager&.shutdown
    DecentJob.reset!
  end
end

describe "restarting an adapter before its first run" do
  include DatabaseSpec

  after { DecentJob.reset! }

  it "reopens its database cron gate when the adapter is explicitly restarted" do
    DecentJob::Schema.new(database).migrate
    DecentJob.configuration.enable_cron = true
    DecentJob.configuration.cron = {future: {cron: ->(previous) { (previous || Time.now) + 3600 }, class: RecordingJob, args: ["future"]}}
    DecentJob.configuration.enable_listen_notify = false
    DecentJob.configuration.max_threads = 1
    adapter = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    RecordingJob.queue_adapter = adapter
    _(adapter.shutdown(timeout: 3)).must_equal true
    _(adapter.restart(timeout: 3)).must_be_same_as adapter
    _(DecentJob.cron_running?).must_equal true
  end
end

describe "standalone execution reset ownership" do
  include DatabaseSpec

  before { DecentJob.reset! }
  after { DecentJob.reset! }

  it "keeps the global pool open until a standalone performer and its tracker drain" do
    DecentJob.configure { |config| config.database_url = @connection_options }
    DecentJob.install
    pool = DecentJob.database
    BlockingJob.started = Queue.new
    BlockingJob.release = Queue.new
    BlockingJob.queue_adapter = DecentJob::Adapter.new(database: pool, execution_mode: :external)
    BlockingJob.perform_later
    performer = Thread.new { DecentJob::Performer.new(database: pool).perform_one }
    Timeout.timeout(3) { BlockingJob.started.pop }
    _(DecentJob.reset!).must_equal false
    _(performer.alive?).must_equal true
    _(pool.with_connection { |connection| connection.exec("SELECT 1").getvalue(0, 0) }).must_equal "1"
    BlockingJob.release << true
    _(performer.value.state).must_equal "succeeded"
    _(DecentJob.reset!).must_equal true
    _ { pool.with_connection {} }.must_raise DecentJob::ClosedDatabase
  ensure
    BlockingJob.release << true
    performer&.join(3)
  end
end

describe "pending cron startup" do
  after { DecentJob.reset! }

  it "lets a newer cron shutdown supersede pending explicit cron startup" do
    entered = Queue.new
    release = Queue.new
    configuration = DecentJob.configuration
    original = configuration.method(:cron_entries)

    configuration.define_singleton_method(:cron_entries) do |**options|
      entered << true
      release.pop
      original.call(**options)
    end

    starter = Thread.new { DecentJob.start_cron(database: Object.new) }
    Timeout.timeout(3) { entered.pop }
    _(DecentJob.stop_cron(timeout: 0)).must_equal true
    release << true
    _(starter.value).must_equal false
    _(DecentJob.cron_managers).must_be_empty
  ensure
    release << true if release
    starter&.join(1)
    configuration.singleton_class.remove_method(:cron_entries) if configuration&.singleton_methods&.include?(:cron_entries)
  end
end
