# frozen_string_literal: true

require "spec_helper"
require "timeout"

describe "forced shutdown generation ownership" do
  it "does not interrupt or report a restarted run while the previous shutdown escalates" do
    worker = DecentJob::Worker.new(database: Object.new, max_threads: 1, enable_listen_notify: false)
    entered = Queue.new
    release = Queue.new
    at_force = Queue.new
    resume_force = Queue.new
    worker.define_singleton_method(:run) do |_pool, _index|
      @mutex.synchronize { @executing_job_ids[Thread.current] = "job-#{Thread.current.object_id}" }
      entered << Thread.current
      release.pop
    end
    worker.define_singleton_method(:interrupt_executions) do |**snapshot|
      at_force << true
      resume_force.pop
      super(**snapshot)
    end
    worker.start
    old_thread = Timeout.timeout(2) { entered.pop }
    shutdown = Thread.new { worker.shutdown(timeout: 0, force: true) }
    Timeout.timeout(2) { at_force.pop }
    release << true
    old_thread.join
    worker.start
    new_thread = Timeout.timeout(2) { entered.pop }
    resume_force << true
    _(shutdown.value).must_equal true
    _(new_thread.alive?).must_equal true
    _(worker.interrupted_job_ids).must_be_empty
  ensure
    2.times { release << true } if release
    resume_force << true if resume_force
    shutdown&.join(1)
    worker&.shutdown(timeout: 1)
  end
end
