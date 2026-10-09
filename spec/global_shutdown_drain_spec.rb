# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

describe "global shutdown drain inspection" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    DecentJob.configure do |config|
      config.database_url = @connection_options
      config.max_threads = 1
      config.poll_interval = 0.01
      config.enable_listen_notify = false
    end
    BlockingJob.started = Queue.new
    BlockingJob.release = Queue.new
    @release = Queue.new
  end

  after do
    BlockingJob.release << true
    @release << true
    DecentJob.reset!
  end

  def pop(queue)
    Timeout.timeout(5) { queue.pop }
  end

  def await
    Timeout.timeout(5) { sleep 0.001 until yield }
  end

  it "excludes a directly owned worker while its job is running" do
    BlockingJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    BlockingJob.perform_later
    worker = DecentJob::Worker.new(database: database, max_threads: 1, enable_listen_notify: false).start
    pop(BlockingJob.started)
    _(worker.shutdown?).must_equal false
    _(DecentJob.shutdown?).must_equal true
  ensure
    BlockingJob.release << true
    worker&.shutdown(timeout: 5)
  end

  it "remains false until both a running job and a cron callback finish draining" do
    BlockingJob.queue_adapter = DecentJob::Adapter.new(database: DecentJob.database, execution_mode: :external)
    BlockingJob.perform_later
    entered = Queue.new
    DecentJob.configuration.cron = {holding: {class: -> {
      entered << true
      @release.pop
    }, cron: ->(previous) { previous ? previous + 60 : Time.now }}}
    worker = DecentJob.start
    manager = DecentJob.start_cron
    pop(BlockingJob.started)
    pop(entered)
    _(DecentJob.shutdown?).must_equal false
    _(DecentJob.shutdown(timeout: 0)).must_equal false
    _(DecentJob.stopping?).must_equal true
    _(DecentJob.shutdown?).must_equal false
    BlockingJob.release << true
    _(worker.shutdown(timeout: 5)).must_equal true
    _(manager.shutdown?).must_equal false
    _(DecentJob.shutdown?).must_equal false
    @release << true
    _(DecentJob.shutdown(timeout: 5)).must_equal true
    _(DecentJob.shutdown?).must_equal true
  end

  it "tracks a locally stopped adapter worker when it is restarted" do
    adapter = DecentJob::Adapter.new(database: DecentJob.database, execution_mode: :async_all)
    BlockingJob.queue_adapter = adapter
    BlockingJob.perform_later
    pop(BlockingJob.started)
    _(DecentJob.shutdown?).must_equal false
    _(adapter.shutdown(timeout: 0)).must_equal false
    _(DecentJob.shutdown?).must_equal false
    BlockingJob.release << true
    _(adapter.shutdown(timeout: 5)).must_equal true
    _(DecentJob.shutdown?).must_equal true
    _(adapter.restart(timeout: 5)).must_be_same_as adapter
    _(DecentJob.shutdown?).must_equal false
    _(adapter.shutdown(timeout: 5)).must_equal true
    _(DecentJob.shutdown?).must_equal true
  end

  %i[listener heartbeat].each do |role|
    it "remains false while the #{role} connection is finishing cleanup" do
      DecentJob.configuration.enable_listen_notify = true
      entered = Queue.new
      release = @release
      original = DecentJob.database.method(:connect)
      DecentJob.database.define_singleton_method(:connect) do
        original.call.tap do |connection|
          connection_role = nil
          connection.define_singleton_method(:exec) do |sql, *arguments, &block|
            connection_role = :listener if sql == "LISTEN decent_job"
            super(sql, *arguments, &block)
          end
          connection.define_singleton_method(:exec_params) do |sql, *arguments, &block|
            connection_role = :heartbeat if sql.include?("INSERT INTO decent_job_processes")
            super(sql, *arguments, &block)
          end
          connection.define_singleton_method(:finish) do
            if connection_role == role
              entered << true
              release.pop
            end
            super()
          end
        end
      end
      worker = DecentJob.start
      await { worker.connected? }
      _(DecentJob.shutdown(timeout: 0)).must_equal false
      pop(entered)
      _(worker.started?).must_equal false
      _(DecentJob.shutdown?).must_equal false
      @release << true
      _(DecentJob.shutdown(timeout: 5)).must_equal true
      _(DecentJob.shutdown?).must_equal true
    end
  end
end
