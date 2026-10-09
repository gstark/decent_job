# frozen_string_literal: true

require "minitest/mock"
require "spec_helper"
require "timeout"

describe "lifecycle allocation failures" do
  [:worker, :cron].each do |type|
    it "rolls back #{type} children when their creator is killed during thread handoff" do
      entered = Queue.new
      release = Queue.new
      resource = if type == :worker
        DecentJob::Worker.new(database: Object.new, max_threads: 1, enable_listen_notify: false).tap do |worker|
          worker.define_singleton_method(:run) { |*|
            entered << true
            release.pop
          }
        end
      else
        entry = Object.new
        entry.define_singleton_method(:next_at) { |**|
          entered << true
          release.pop
          nil
        }
        DecentJob::CronManager.new([entry])
      end
      begin_creation = Queue.new
      created = Queue.new
      handoff = Queue.new
      factory = Thread.method(:new)
      creator = Thread.new {
        begin_creation.pop
        resource.start
      }
      children = []
      replacement = lambda do |*args, &block|
        child = factory.call(*args, &block)
        if Thread.current == creator
          children << child
          if children.size == 1
            created << child
            handoff.pop
          end
        end
        child
      end
      Thread.stub(:new, replacement) do
        begin_creation << true
        Timeout.timeout(2) { created.pop }
        creator.kill
        handoff << true
        _(creator.join(2)).must_be_same_as creator
      end
      children.each { |child| _(child.join(0.2)).must_be_same_as child }
      _(resource.shutdown?).must_equal true
      _(entered).must_be_empty
    ensure
      begin_creation << true if begin_creation
      handoff << true if handoff
      release << true if release
      creator&.kill&.join(2)
      children&.each { |child| child.kill.join }
      resource&.shutdown(timeout: 1)
    end

    it "owns and stops partially allocated #{type} threads without reporting a successful start" do
      subscriptions = []
      events = []
      %w[scheduler_create_pool cron_manager_start].each do |name|
        subscriptions << ActiveSupport::Notifications.subscribe("#{name}.decent_job") { |event| events << event }
      end
      entered = Queue.new
      release = Queue.new
      resource = if type == :worker
        DecentJob::Worker.new(database: Object.new, max_threads: 2, enable_listen_notify: false).tap do |worker|
          worker.define_singleton_method(:run) { |*|
            entered << true
            release.pop
          }
        end
      else
        entry = Object.new
        entry.define_singleton_method(:next_at) { |**|
          entered << true
          release.pop
          nil
        }
        DecentJob::CronManager.new([entry])
      end
      created = []
      factory = Thread.method(:new)
      failure = IOError.new("thread allocation failed")
      replacement = lambda do |*args, &block|
        raise failure unless created.empty?
        factory.call(*args, &block).tap { |thread| created << thread }
      end
      raised = assert_raises(IOError) { Thread.stub(:new, replacement) { resource.start } }
      _(raised).must_be_same_as failure
      _(events).must_be_empty
      _(created.first.join(0.2)).must_be_same_as created.first
      _(resource.shutdown?).must_equal true
      _(entered).must_be_empty
    ensure
      subscriptions&.each { |subscription| ActiveSupport::Notifications.unsubscribe(subscription) }
      release << true if release
      created&.each { |thread| thread.kill.join }
      resource&.shutdown(timeout: 1)
    end
  end
end
