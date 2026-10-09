# frozen_string_literal: true

require "spec_helper"
require "timeout"

describe "process tracking with inherited databases" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
  end

  def in_child
    reader, writer = IO.pipe
    pid = fork do
      reader.close
      Thread.report_on_exception = false
      begin
        writer.write(JSON.generate(yield))
      rescue Exception => error # standard:disable Lint/RescueException
        writer.write(JSON.generate(error: error.class.name))
      ensure
        writer.close
        exit! 0
      end
    end
    writer.close
    JSON.parse(Timeout.timeout(5) { reader.read })
  ensure
    reader&.close
    writer&.close unless writer&.closed?
    Process.wait(pid) if pid
  end

  it "rejects a new tracker around an inherited database without altering parent presence" do
    inherited = database
    parent_tracker = DecentJob::ProcessTracker.new(database: inherited)
    parent_tracker.register do |parent_id|
      result = in_child do
        tracker = DecentJob::ProcessTracker.new(database: inherited, on_thread_error: ->(_error) {})
        begin
          Timeout.timeout(1) { tracker.register { raise "must not register" } }
        rescue Exception => error # standard:disable Lint/RescueException
          {error: error.class.name}
        ensure
          tracker.stop
          begin
            tracker.join(1)
          rescue DecentJob::ForkedDatabase
            nil
          end
        end
      end
      _(result.fetch("error")).must_equal "DecentJob::ForkedDatabase"
      _(DecentJob::ProcessRecord.active(database: database).map(&:id)).must_equal [parent_id]
    end
  end

  it "terminates a new worker around an inherited database and drains its listener" do
    inherited = database
    result = in_child do
      worker = DecentJob::Worker.new(database: inherited, max_threads: 2, on_thread_error: ->(_error) {})
      worker.start
      begin
        Timeout.timeout(1) { sleep 0.005 while worker.running? }
        worker.shutdown(timeout: 1)
        {error: "missing failure"}
      rescue Exception => error # standard:disable Lint/RescueException
        {error: error.class.name, live_threads: worker.stats.fetch(:schedulers).sum { |pool| pool.fetch(:live_threads) }}
      ensure
        begin
          worker.shutdown(timeout: 1)
        rescue DecentJob::ForkedDatabase
          nil
        end
      end
    end
    _(result.fetch("error")).must_equal "DecentJob::ForkedDatabase"
    _(result.fetch("live_threads")).must_equal 0
    _(DecentJob::ProcessRecord.all(database: database)).must_be_empty
  end
end
