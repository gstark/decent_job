# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

describe "scheduler lifecycle events" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @subscriptions = []
    @events = Queue.new
    @errors = Queue.new
    @workers = []
    BlockingJob.started = Queue.new
    BlockingJob.release = Queue.new
    BlockingJob.queue_adapter = DecentJob::Adapter.new(database: database)
  end

  after do
    @subscriptions.each { |subscription| ActiveSupport::Notifications.unsubscribe(subscription) }
    5.times { BlockingJob.release << true }
    @workers.each { |worker| worker.shutdown(timeout: 5) }
    DecentJob.reset!
  end

  def worker(**options)
    DecentJob::Worker.new(database: database, max_threads: 1, poll_interval: -1,
      enable_listen_notify: false, on_thread_error: ->(error) { @errors << error }, **options).tap { |worker| @workers << worker }
  end

  def subscribe(name, &block)
    @subscriptions << ActiveSupport::Notifications.subscribe("#{name}.decent_job", &block)
  end

  def pop(queue = @events)
    Timeout.timeout(5) { queue.pop }
  end

  it "reports each allocated pool exactly once with stable captured run identity" do
    subscribe("scheduler_create_pool") { |event| @events << event.payload.dup }
    instance = worker(queues: "fast:1;slow:2")
    _(instance.start).must_be_same_as instance
    instance.start
    _(@events.size).must_equal 2
    payloads = 2.times.map { pop }
    _(payloads.map { |payload| payload[:queues] }).must_equal %w[fast slow]
    _(payloads.map { |payload| payload[:max_threads] }).must_equal [1, 2]
    _(payloads.map { |payload| payload[:pool_index] }).must_equal [0, 1]

    payloads.each do |payload|
      _(payload[:scheduler]).must_be_same_as instance
      _(payload[:performer_name]).must_equal payload[:queues]
      _(payload[:process_id]).must_equal Process.pid
      _(payload[:thread_name]).must_be_kind_of String
      _(payload[:run_id]).wont_be_nil
      _(payload[:operation_duration]).must_be :>=, 0
    end

    _(payloads.map { |payload| payload[:run_id] }.uniq.size).must_equal 1
  end

  it "permits reentrant adapter inspection, stop, and restart without recursive observations" do
    adapter = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    DecentJob.configuration.max_threads = 1
    DecentJob.configuration.enable_listen_notify = false

    subscribe("scheduler_create_pool") do |event|
      @events << [adapter.async_started?, DecentJob.stats, event.payload[:scheduler].stats]
      adapter.shutdown(timeout: 5)
      adapter.restart(timeout: 5)
    end

    Timeout.timeout(5) { adapter.start_async }
    _(@events.size).must_equal 1
    _(pop.first).must_equal true
    _(adapter.async_started?).must_equal true
  end

  it "reports signal-only draining and later completion with the caller timeout" do
    subscribe("scheduler_shutdown_start") { |event| @events << [:start, event.payload.dup] }
    subscribe("scheduler_shutdown") { |event| @events << [:finish, event.payload.dup] }
    BlockingJob.perform_later
    instance = worker.start
    pop(BlockingJob.started)
    _(instance.shutdown(timeout: 0)).must_equal false
    _(@events.size).must_equal 2
    start, finish = pop, pop
    _(start.first).must_equal :start
    _(start.last[:timeout]).must_equal 0
    _(finish.first).must_equal :finish
    _(finish.last.values_at(:stopped, :draining, :force)).must_equal [false, true, false]
    _(finish.last[:run_id]).must_equal start.last[:run_id]
    BlockingJob.release << true
    _(instance.shutdown(timeout: nil)).must_equal true
    pop
    _(pop.last.values_at(:stopped, :draining, :timeout)).must_equal [true, false, nil]
    instance.shutdown
    _(@events).must_be_empty
  end

  it "allows root start observers to reset the runtime without holding registry locks" do
    DecentJob.configuration.database_url = @connection_options
    DecentJob.configuration.max_threads = 1
    DecentJob.configuration.enable_listen_notify = false

    subscribe("scheduler_create_pool") do |event|
      @events << [DecentJob.stats, DecentJob.reset!, event.payload.dup]
    end

    returned = Timeout.timeout(5) { DecentJob.start }
    _(@events.size).must_equal 1
    observed = pop
    _(observed[1]).must_equal true
    _(observed[2][:scheduler]).must_be_same_as returned
    _(returned.shutdown?).must_equal true
    _(DecentJob.instance_variable_get(:@database)).must_be_nil
  end

  it "contains failed create observers and failing reporters after successful allocation" do
    reported = []
    subscribe("scheduler_create_pool") { raise "create observer failed" }

    instance = worker(on_thread_error: ->(error) {
      reported << error
      instance.stats
      raise "reporter failed"
    })

    _(instance.start).must_be_same_as instance
    _(instance.running?).must_equal true
    _(reported.map(&:message)).must_equal ["create observer failed"]
  end

  it "signals every registered sibling before shutdown observers run" do
    first = worker
    second = worker
    DecentJob.start_worker(first)
    DecentJob.start_worker(second)

    subscribe("scheduler_shutdown_start") do |event|
      @events << [first.running?, second.running?, DecentJob.stats, event.payload]
      DecentJob.shutdown(timeout: 0)
    end

    _(Timeout.timeout(5) { DecentJob.shutdown(timeout: 5) }).must_equal true
    received = []
    received << pop until @events.empty?
    _(received).wont_be_empty
    _(received.all? { |entry| entry[0..1] == [false, false] }).must_equal true
  end

  it "reports an incomplete restart without claiming a new run" do
    subscribe("scheduler_restart_pools") { |event| @events << event.payload.dup }
    BlockingJob.perform_later
    instance = worker.start
    pop(BlockingJob.started)
    _(instance.restart(timeout: 0)).must_equal false
    _(@events.size).must_equal 1
    payload = pop
    _(payload.values_at(:restarted, :outcome, :new_run_id)).must_equal [false, :incomplete, nil]
    _(payload[:old_run_id]).wont_be_nil
  end

  [:adapter, :root].each do |owner|
    it "reports incomplete #{owner} restarts and retains the old run identity" do
      DecentJob.configuration.max_threads = 1
      DecentJob.configuration.enable_listen_notify = false
      adapter = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
      BlockingJob.queue_adapter = adapter
      subscribe("scheduler_restart_pools") { |event| @events << event.payload.dup }
      BlockingJob.perform_later
      pop(BlockingJob.started)
      target = (owner == :adapter) ? adapter : DecentJob
      _(target.restart(timeout: 0)).must_equal false
      _(@events.size).must_equal 1
      _(pop.values_at(:restarted, :outcome, :new_run_id)).must_equal [false, :incomplete, nil]
    end
  end

  it "reports an old shutdown after a replacement starts using the captured run" do
    subscribe("scheduler_create_pool") { |event| @events << [:create, event.payload.dup] }
    subscribe("scheduler_shutdown") { |event| @events << [:shutdown, event.payload.dup] }
    instance = worker.start
    old_run = pop.last[:run_id]
    handle = instance.begin_shutdown
    Timeout.timeout(5) { sleep 0.001 until instance.shutdown? }
    instance.start
    replacement = pop.last[:run_id]
    _(handle.wait(deadline: DecentJob::ResourceShutdown.deadline(5), timeout: 5)).must_equal true
    payload = pop.last
    _(payload[:run_id]).must_equal old_run
    _(payload[:run_id]).wont_equal replacement
    _(payload[:stopped]).must_equal true
    _(instance.running?).must_equal true
  end

  it "refuses a captured restart after a newer stop and observes the refusal" do
    subscribe("scheduler_restart_pools") { |event| @events << event.payload.dup }
    instance = worker.start
    handle = instance.begin_shutdown
    handle.wait(deadline: DecentJob::ResourceShutdown.deadline(5))
    instance.shutdown
    _(handle.restart).must_equal false
    _(@events.size).must_equal 1
    _(pop.values_at(:restarted, :outcome, :new_run_id)).must_equal [false, :refused, nil]
  end

  [:adapter, :root].each do |owner|
    it "observes refusal when a newer #{owner} stop races its drained restart" do
      DecentJob.configuration.max_threads = 1
      DecentJob.configuration.enable_listen_notify = false
      adapter = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
      adapter.start_async
      target = (owner == :adapter) ? adapter : DecentJob
      subscribe("scheduler_shutdown") { Thread.new { target.shutdown(timeout: 0) }.value }
      subscribe("scheduler_restart_pools") { |event| @events << event.payload.dup }
      _(Timeout.timeout(5) { target.restart(timeout: 5) }).must_equal false
      _(@events.size).must_equal 1
      _(pop.values_at(:restarted, :outcome, :new_run_id)).must_equal [false, :refused, nil]
      _(adapter.async_started?).must_equal false
    end
  end

  it "keeps the replacement identity when a create observer starts a still newer run" do
    instance = worker.start

    subscribe("scheduler_create_pool") do |event|
      @events << [:create, event.payload.dup]

      if event.payload[:run_id] == 2
        Thread.new do
          instance.shutdown(timeout: 5)
          instance.start
        end.value
      end
    end

    subscribe("scheduler_restart_pools") { |event| @events << [:restart, event.payload.dup] }
    _(Timeout.timeout(5) { instance.restart(timeout: 5) }).must_be_same_as instance
    observations = 3.times.map { pop }
    _(observations.map(&:first)).must_equal [:create, :create, :restart]
    _(observations.map { |event| event.last[:run_id] }).must_equal [2, 3, 1]
    _(observations.last.last[:new_run_id]).must_equal 2
  end

  it "includes shutdown observer latency in the grace deadline" do
    BlockingJob.perform_later
    instance = worker.start
    pop(BlockingJob.started)
    subscribe("scheduler_shutdown_start") { sleep 0.1 }
    began = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    _(instance.shutdown(timeout: 0.08)).must_equal false
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - began
    _(elapsed).must_be :<, 0.16
  end

  it "enriches force interruption events with the captured pool and run" do
    subscribe("scheduler_create_pool") { |event| @events << event.payload.dup }
    subscribe("scheduler_shutdown_kill") { |event| @events << event.payload.dup }
    BlockingJob.perform_later
    instance = worker.start
    created = pop
    pop(BlockingJob.started)
    instance.shutdown(timeout: 0, force: true)
    _(@events.size).must_equal 1
    killed = pop
    _(killed.values_at(:scheduler, :run_id, :pool_index, :queues, :process_id)).must_equal created.values_at(:scheduler, :run_id, :pool_index, :queues, :process_id)
    _(killed[:active_job_ids].size).must_equal 1
    _(instance.shutdown(timeout: 5)).must_equal true
  end

  it "reports restart after pool creation with distinct old and new run identities" do
    subscribe("scheduler_create_pool") { |event| @events << [:create, event.payload.dup] }
    subscribe("scheduler_restart_pools") { |event| @events << [:restart, event.payload.dup] }
    instance = worker.start
    _(@events.size).must_equal 1
    original = pop.last
    _(instance.restart(timeout: 5)).must_be_same_as instance
    created, restarted = pop, pop
    _(created.first).must_equal :create
    _(restarted.first).must_equal :restart
    _(restarted.last[:old_run_id]).must_equal original[:run_id]
    _(restarted.last[:new_run_id]).must_equal created.last[:run_id]
    _(restarted.last[:new_run_id]).wont_equal original[:run_id]
    _(restarted.last.values_at(:restarted, :outcome)).must_equal [true, :restarted]
  end
end

describe "standalone cron lifecycle events" do
  before do
    @events = []
    @subscription = ActiveSupport::Notifications.subscribe("cron_manager_start.decent_job") { |event| @events << event.payload.dup }
  end

  after do
    ActiveSupport::Notifications.unsubscribe(@subscription)
    @manager&.shutdown
    DecentJob.reset!
  end

  it "observes a manager spanning databases and isolates its entries array from reentrant observers" do
    entries = [Object.new, Object.new].map do |pool|
      Object.new.tap do |entry|
        entry.define_singleton_method(:database) { pool }
        entry.define_singleton_method(:next_at) { |**| nil }
      end
    end

    callback = ActiveSupport::Notifications.subscribe("cron_manager_start.decent_job") do |event|
      manager = event.payload[:cron_manager]
      event.payload[:cron_entries].clear
      manager.shutdown(timeout: 5)
      manager.restart(timeout: 5)
    end

    @manager = DecentJob::CronManager.new(entries)
    Timeout.timeout(5) { @manager.start }
    _(@manager.cron_entries).must_equal entries
    _(@manager.running?).must_equal true
    _(@events.size).must_equal 1
    _(DecentJob.instance_variable_get(:@database)).must_be_nil
  ensure
    ActiveSupport::Notifications.unsubscribe(callback) if callback
  end

  it "observes empty start-on-initialize without opening a default database" do
    @manager = DecentJob::CronManager.new([], start_on_initialize: true)
    @manager.start
    _(@events.size).must_equal 1
    payload = @events.first
    _(payload[:cron_manager]).must_be_same_as @manager
    _(payload[:cron_entries]).must_equal []
    _(payload[:cron_entries]).wont_be_same_as @manager.cron_entries
    _(DecentJob.instance_variable_get(:@database)).must_be_nil
    @manager.restart
    _(@events.size).must_equal 2
    _(@events.last[:run_id]).wont_equal payload[:run_id]
  end
end
