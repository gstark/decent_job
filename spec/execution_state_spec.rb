# frozen_string_literal: true

require "minitest/mock"
require "spec_helper"
require "timeout"

describe "runtime execution state" do
  before do
    @isolation_level = ActiveSupport::IsolatedExecutionState.isolation_level
  end

  after do
    ActiveSupport::IsolatedExecutionState.isolation_level = @isolation_level
  end

  scopes = {
    execution: [->(value, &block) { DecentJob::Current.with(value, &block) }, -> { DecentJob::Current.execution }],
    bulk: [->(value, &block) { DecentJob::Bulk.with_buffer(value, &block) }, -> { DecentJob::Bulk.current_buffer }],
    batch: [->(value, &block) { DecentJob::Batch.with_context(value, &block) }, -> { DecentJob::Batch.current_context }],
    cron: [->(value, &block) { DecentJob::CronEntry.with_context(value, &block) }, -> { DecentJob::CronEntry.current_context }],
    worker: [->(value, &block) { DecentJob::ThreadStatus.with_worker(value, &block) }, -> { DecentJob.current_thread_shutting_down? }]
  }

  scopes.each do |name, (within, read)|
    [:thread, :fiber].each do |isolation|
      it "uses #{isolation} isolation for #{name}" do
        ActiveSupport::IsolatedExecutionState.isolation_level = isolation
        outer = (name == :worker) ? Struct.new(:running?).new(false) : :outer
        inner = (name == :worker) ? Struct.new(:running?).new(true) : :inner
        outer_value = (name == :worker) ? true : outer
        inner_value = (name == :worker) ? false : inner
        within.call(outer) do
          _(read.call).must_equal outer_value
          _(Thread.new { read.call }.value).must_be_nil
          fiber = Fiber.new do
            initial = read.call
            within.call(inner) do
              Fiber.yield [initial, read.call]
              _(read.call).must_equal inner_value
            end
            read.call
          end
          initial, current = fiber.resume
          (isolation == :thread) ? _(initial).must_equal(outer_value) : _(initial).must_be_nil
          _(current).must_equal inner_value
          _(read.call).must_equal((isolation == :thread) ? inner_value : outer_value)
          restored = fiber.resume
          (isolation == :thread) ? _(restored).must_equal(outer_value) : _(restored).must_be_nil
          _(read.call).must_equal outer_value
        end
        _(read.call).must_be_nil
      end

      it "restores #{name} after nested returns, exceptions, and throws in #{isolation} mode" do
        ActiveSupport::IsolatedExecutionState.isolation_level = isolation
        value = (name == :worker) ? Struct.new(:running?).new(false) : :outer
        expected = (name == :worker) ? true : value
        result = within.call(value) do
          _(within.call(nil) { :returned }).must_equal :returned
          _(read.call).must_equal expected
          _ { within.call(nil) { raise "unwind" } }.must_raise RuntimeError
          _(read.call).must_equal expected
          _(catch(:unwind) { within.call(nil) { throw :unwind, :thrown } }).must_equal :thrown
          _(read.call).must_equal expected
          :complete
        end
        _(result).must_equal :complete
        _(read.call).must_be_nil
      end
    end
  end

  [:thread, :fiber].each do |isolation|
    it "isolates scope assignments after the host shares #{isolation} execution state" do
      ActiveSupport::IsolatedExecutionState.isolation_level = isolation
      source = ActiveSupport::IsolatedExecutionState.context
      observed = Queue.new
      release = Queue.new
      child = nil
      operation = lambda do
        ActiveSupport::IsolatedExecutionState.share_with(source) do
          initial = DecentJob::Current.execution
          DecentJob::Current.with(:child) do
            observed << [initial, DecentJob::Current.execution]
            (isolation == :thread) ? release.pop : Fiber.yield
          end
          observed << DecentJob::Current.execution
        end
      end
      DecentJob::Current.with(:parent) do
        child = (isolation == :thread) ? Thread.new(&operation) : Fiber.new(&operation)
        child.resume if isolation == :fiber
        _(Timeout.timeout(2) { observed.pop }).must_equal [:parent, :child]
        parent_while_child = DecentJob::Current.execution
        DecentJob::Current.with(:parent_changed) do
          (isolation == :thread) ? release.push(true) : child.resume
          child.value if isolation == :thread
          _(Timeout.timeout(2) { observed.pop }).must_equal :parent
          _(parent_while_child).must_equal :parent
          _(DecentJob::Current.execution).must_equal :parent_changed
        end
      end
      _(DecentJob::Current.execution).must_be_nil
    ensure
      release << true if release
      if child&.alive?
        (isolation == :thread) ? child.join(2) : child.resume
      end
    end

    it "retains the copied snapshot before a shared #{isolation} context first reads it" do
      ActiveSupport::IsolatedExecutionState.isolation_level = isolation
      source = ActiveSupport::IsolatedExecutionState.context
      shared = Queue.new
      release = Queue.new
      observed = Queue.new
      child = nil
      operation = lambda do
        ActiveSupport::IsolatedExecutionState.share_with(source) do
          shared << true
          (isolation == :thread) ? release.pop : Fiber.yield
          observed << DecentJob::Current.execution
        end
      end
      DecentJob::Current.with(:snapshot) do
        child = (isolation == :thread) ? Thread.new(&operation) : Fiber.new(&operation)
        child.resume if isolation == :fiber
        Timeout.timeout(2) { shared.pop }
        DecentJob::Current.with(:changed_after_sharing) do
          (isolation == :thread) ? release.push(true) : child.resume
          child.value if isolation == :thread
          _(Timeout.timeout(2) { observed.pop }).must_equal :snapshot
          _(DecentJob::Current.execution).must_equal :changed_after_sharing
        end
      end
      _(DecentJob::Current.execution).must_be_nil
    ensure
      release << true if release
      if child&.alive?
        (isolation == :thread) ? child.join(2) : child.resume
      end
    end

    it "uses #{isolation} isolation for inline drains" do
      ActiveSupport::IsolatedExecutionState.isolation_level = isolation
      events = []
      pool = Object.new
      performer = Object.new
      performer.define_singleton_method(:perform_one) do |job_id:, **|
        events << [job_id, :start]
        Fiber.new { DecentJob::InlineBuffer.perform(pool, [:inner]) }.resume if job_id == :outer
        events << [job_id, :finish]
        nil
      end
      DecentJob::Performer.stub(:new, performer) { DecentJob::InlineBuffer.perform(pool, [:outer]) }
      expected = if isolation == :thread
        [[:outer, :start], [:outer, :finish], [:inner, :start], [:inner, :finish]]
      else
        [[:outer, :start], [:inner, :start], [:inner, :finish], [:outer, :finish]]
      end
      _(events).must_equal expected
    end

    it "preserves scoped state across executor and reloader boundaries in #{isolation} mode" do
      require "active_support/reloader"
      ActiveSupport::IsolatedExecutionState.isolation_level = isolation
      executor = Class.new(ActiveSupport::Executor)
      reloader = Class.new(ActiveSupport::Reloader)
      reloader.executor = executor
      reloader.check = -> { true }
      current = Class.new(ActiveSupport::CurrentAttributes) { attribute :value }
      current.define_singleton_method(:name) { "RuntimeStateCurrent" }
      executor.to_run { current.reset }
      executor.to_complete { current.reset }
      DecentJob::Current.with(:execution) do
        current.value = :request
        reloader.wrap { _(DecentJob::Current.execution).must_equal :execution }
        _(current.value).must_be_nil
        _(DecentJob::Current.execution).must_equal :execution
      end
      _(DecentJob::Current.execution).must_be_nil
    end

    it "discards inherited state without restoring it when #{isolation} scopes unwind after fork" do
      skip "fork is unavailable" unless Process.respond_to?(:fork)
      ActiveSupport::IsolatedExecutionState.isolation_level = isolation
      reader, writer = IO.pipe
      pid = nil
      read_all = -> { scopes.values.map { |_within, read| read.call } }
      nest = lambda do |remaining, &block|
        if remaining.empty?
          block.call
        else
          name, (within, _read) = remaining.first
          value = (name == :worker) ? Struct.new(:running?).new(false) : :inherited
          within.call(value) { nest.call(remaining.drop(1), &block) }
        end
      end
      suspended = Fiber.new do
        nest.call(scopes.to_a) do
          nest.call(scopes.to_a) do
            Fiber.yield
            read_all.call
          end
          read_all.call
        end
      end
      suspended.resume
      nest.call(scopes.to_a) do
        DecentJob.with_prepared_fork do
          pid = fork do
            reader.close
            begin
              ActiveSupport::IsolatedExecutionState[:application_state] = :preserved
              DecentJob.after_supervisor_fork(DecentJob.configuration.dup)
              values = [read_all.call, suspended.resume, read_all.call]
              values << ActiveSupport::IsolatedExecutionState[:application_state]
              writer.write(Marshal.dump(values))
            rescue => error
              writer.write(Marshal.dump([error.class.name, error.message]))
            ensure
              writer.close
            end
            exit! 0
          end
        end
      end
      suspended.resume
      writer.close
      _(Timeout.timeout(5) { Marshal.load(reader.read) }).must_equal [Array.new(scopes.size), Array.new(scopes.size), Array.new(scopes.size), :preserved]
      Process.waitpid(pid)
    ensure
      reader&.close unless reader&.closed?
      writer&.close unless writer&.closed?
      if pid
        begin
          Process.kill("KILL", pid)
          Process.waitpid(pid)
        rescue Errno::ESRCH, Errno::ECHILD
          nil
        end
      end
    end
  end
end
