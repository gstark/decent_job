# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "timeout"

describe "disabled error reporting" do
  before { DecentJob.configure { |config| config.on_thread_error = nil } }

  after do
    DecentJob.clear_lifecycle_hooks
    DecentJob.reset!
  end

  it "continues lifecycle hooks after a callback fails" do
    events = []
    DecentJob.before_supervisor_fork { raise "hook failed" }
    DecentJob.before_supervisor_fork { events << :continued }
    DecentJob.run_lifecycle_hooks(:before_supervisor_fork)
    assert_equal [:continued], events
  end

  it "preserves instrumentation outcomes after an observer fails" do
    subscription = ActiveSupport::Notifications.subscribe("optional_reporter.decent_job") { raise "observer failed" }
    assert_equal false, DecentJob::Instrumentation.instrument("optional_reporter.decent_job", {}, database: nil) { false }
    application_error = RuntimeError.new("application failed")

    actual = assert_raises(RuntimeError) do
      DecentJob::Instrumentation.instrument("optional_reporter.decent_job", {}, database: nil) { raise application_error }
    end

    assert_same application_error, actual
  ensure
    ActiveSupport::Notifications.unsubscribe(subscription) if subscription
  end

  it "reaps supervisor children when shutdown callbacks fail" do
    config = DecentJob.configuration.dup
    config.subprocesses = 1
    config.shutdown_timeout = 0.05
    DecentJob.before_subprocess_boot { sleep 10 }
    pids = []
    subscription = ActiveSupport::Notifications.subscribe("cluster_spawn.decent_job") { |event| pids << event.payload[:pid] }
    supervisor = DecentJob::Supervisor.new(config)
    supervisor.start(on_start: -> { Process.kill("TERM", Process.pid) }, on_shutdown: -> { raise "shutdown failed" })
    assert_empty supervisor.subprocesses
    pids.each { |pid| assert_raises(Errno::ECHILD) { Process.waitpid(pid, Process::WNOHANG) } }
  ensure
    ActiveSupport::Notifications.unsubscribe(subscription) if subscription

    pids&.each do |pid|
      next if Process.waitpid(pid, Process::WNOHANG)
      Process.kill("KILL", pid)
      Process.waitpid(pid)
    rescue Errno::ESRCH, Errno::ECHILD
      nil
    end
  end
end

describe "disabled error reporting during job execution" do
  include DatabaseSpec

  before do
    DecentJob.configure { |config| config.on_thread_error = nil }
    DecentJob::Schema.new(database).migrate
    adapter = DecentJob::Adapter.new(database: database)
    [RecordingJob, FailingJob].each { |klass| klass.queue_adapter = adapter }
    RecordingJob.events = Queue.new
  end

  after do
    @worker&.shutdown(timeout: 5)
    DecentJob.reset!
  end

  it "retains records when a preservation callback aborts its savepoint" do
    DecentJob.configuration.preserve_job_records = ->(*) {
      database.with_connection { |connection| connection.exec("SELECT missing_optional_reporter_function()") }
    }

    job = RecordingJob.perform_later("preserved")
    result = DecentJob::Performer.new(database: database).perform_one
    assert_equal "succeeded", result.state
    store = DecentJob::Store.new(database)
    assert_equal "succeeded", store.find(job.provider_job_id).fetch("state")
    assert_equal ["succeeded"], store.executions(job.provider_job_id).map { |row| row.fetch("state") }
  end

  it "recovers pending batches after an earlier reconciliation fails" do
    batches = DecentJob::BatchStore.new(database)
    broken = batches.save(id: nil, attributes: {on_finish: RecordingJob}).fetch("id")
    healthy = batches.save(id: nil, attributes: {}).fetch("id")

    [broken, healthy].each do |id|
      batches.open(id)
      batches.mark_enqueued(id)
    end

    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_job_batches SET on_finish = $1 WHERE id = $2", ["MissingOptionalReporterJob", broken])
      batches.request(connection, [broken, healthy])
    end

    assert_equal 2, batches.reconcile_pending
    assert_nil batches.find(broken).fetch("finished_at")
    refute_nil batches.find(healthy).fetch("finished_at")
    batches.save(id: broken, attributes: {on_finish: nil})
    assert_equal 1, batches.reconcile_pending
    refute_nil batches.find(broken).fetch("finished_at")
    assert_equal 0, batches.reconcile_pending
  end

  it "keeps workers available after an unhandled job error" do
    failed = FailingJob.perform_later
    RecordingJob.perform_later("continued")
    @worker = DecentJob::Worker.new(database: database, max_threads: 1, poll_interval: 0.02, enable_listen_notify: false)
    @worker.start
    assert_equal "continued", Timeout.timeout(5) { RecordingJob.events.pop }
    assert @worker.shutdown(timeout: 5)
    assert_equal "discarded", DecentJob::Store.new(database).find(failed.provider_job_id).fetch("state")
  end
end
