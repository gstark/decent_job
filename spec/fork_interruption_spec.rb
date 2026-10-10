# frozen_string_literal: true

require "spec_helper"
require "timeout"

describe "fork preparation interruption" do
  include DatabaseSpec

  after do
    @release << true if @release
    @controller&.kill
    @controller&.join(2)
    DecentJob.clear_lifecycle_hooks
    DecentJob.instance_variable_set(:@forking, false)
    DecentJob.reset!
  end

  def await(queue)
    Timeout.timeout(2) { queue.pop }
  end

  it "releases the runtime gate when killed while its ownership returns from the mutex" do
    entered = Queue.new
    @release = release = Queue.new
    mutex = DecentJob.runtime_mutex
    controller = nil
    interrupted = false

    mutex.define_singleton_method(:synchronize) do |&block|
      result = super(&block)

      if Thread.current.equal?(controller) && DecentJob.instance_variable_get(:@forking) && !interrupted
        interrupted = true
        entered << true
        release.pop
      end

      result
    end

    @controller = Thread.new do
      controller = Thread.current
      DecentJob.with_prepared_fork { flunk "must not reach fork" }
    end

    await(entered)
    @controller.kill
    release << true
    _(@controller.join(2)).must_equal @controller

    result = begin
      DecentJob.synchronize_resource_start { :available }
    rescue DecentJob::ForkInProgress
      :stranded
    end

    _(result).must_equal :available
  ensure
    mutex&.singleton_class&.remove_method(:synchronize)
  end

  it "resumes every prepared pool when killed as a preparation returns" do
    entered = Queue.new
    @release = release = Queue.new
    pool = database
    pool.with_connection { |connection| connection.exec("SELECT 1") }

    pool.define_singleton_method(:prepare_fork) do
      super()
      entered << true
      release.pop
    end

    @controller = Thread.new { DecentJob::Database.with_prepared_fork { flunk "must not reach fork" } }
    await(entered)
    @controller.kill
    release << true
    _(@controller.join(2)).must_equal @controller

    result = begin
      pool.with_connection { |connection| connection.exec("SELECT 1").getvalue(0, 0) }
    rescue DecentJob::ForkInProgress
      :stranded
    end

    _(result).must_equal "1"
  ensure
    pool&.singleton_class&.remove_method(:prepare_fork)
    pool&.resume_after_fork
  end

  it "keeps application fork hooks interruptible and releases the runtime gate" do
    entered = Queue.new

    DecentJob.before_supervisor_fork do
      entered << true
      Queue.new.pop
    end

    @controller = Thread.new { DecentJob.with_prepared_fork { flunk "must not reach fork" } }
    await(entered)
    @controller.kill
    _(@controller.join(2)).must_equal @controller
    _(DecentJob.synchronize_resource_start { :available }).must_equal :available
  end

  it "keeps the fork block interruptible and resumes prepared pools" do
    entered = Queue.new
    pool = database

    @controller = Thread.new do
      DecentJob.with_prepared_fork do
        entered << true
        Queue.new.pop
      end
    end

    await(entered)
    @controller.kill
    _(@controller.join(2)).must_equal @controller
    _(pool.with_connection { |connection| connection.exec("SELECT 1").getvalue(0, 0) }).must_equal "1"
    _(DecentJob.synchronize_resource_start { :available }).must_equal :available
  end

  it "rejects new pools in the fork block without holding the registry mutex" do
    _ {
      Timeout.timeout(1) do
        DecentJob::Database.with_prepared_fork do
          DecentJob::Database.new(url: @connection_options)
        end
      end
    }.must_raise DecentJob::ForkInProgress

    pool = DecentJob::Database.new(url: @connection_options)
    _(pool.with_connection { |connection| connection.exec("SELECT 1").getvalue(0, 0) }).must_equal "1"
  ensure
    pool&.close
  end
end
