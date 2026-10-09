# frozen_string_literal: true

require "spec_helper"
require "timeout"

describe "database pool resizing" do
  include DatabaseSpec

  it "closes excess idle connections when shrinking" do
    ready = Queue.new
    release = Queue.new
    threads = 2.times.map do
      Thread.new {
        database.with_connection {
          ready << true
          release.pop
        }
      }
    end
    2.times { ready.pop }
    2.times { release << true }
    threads.each(&:join)
    _(database.stats[:connections]).must_equal 2
    database.resize(1)
    _(database.stats).must_equal({size: 1, connections: 1, active: 0, idle: 1})
  ensure
    2.times { release << true } if release
    threads&.each(&:join)
  end

  it "retires surplus borrowed connections as their owners return them" do
    ready = Queue.new
    release = Queue.new
    threads = 2.times.map do
      Thread.new {
        database.with_connection {
          ready << true
          release.pop
        }
      }
    end
    2.times { ready.pop }
    database.resize(1)
    _(database.stats[:active]).must_equal 2
    2.times { release << true }
    threads.each(&:join)
    _(database.stats).must_equal({size: 1, connections: 1, active: 0, idle: 1})
  ensure
    2.times { release << true } if release
    threads&.each(&:join)
  end

  it "wakes waiting checkouts when capacity grows" do
    database.resize(1)
    entered = Queue.new
    acquired = Queue.new
    waiter = nil
    database.with_connection do
      waiter = Thread.new do
        entered << true
        database.with_connection { acquired << true }
      end
      entered.pop
      Timeout.timeout(2) { Thread.pass until waiter.status == "sleep" }
      database.resize(2)
      _(Timeout.timeout(2) { acquired.pop }).must_equal true
    end
    waiter.join
  ensure
    waiter&.join
  end
end
