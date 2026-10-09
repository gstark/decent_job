# frozen_string_literal: true

require "open3"
require "spec_helper"
require "support/management_cli_app"
require "timeout"
require "tmpdir"

describe "execution timing across processes" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    ManagementProcessJob.queue_adapter = DecentJob::Adapter.new(database: database)
    @directory = Dir.mktmpdir("decent_job_metrics")
    @started = File.join(@directory, "started")
    @release = File.join(@directory, "release")
    @result = File.join(@directory, "result")
    @error = File.join(@directory, "error")
  end

  after do
    if @process&.alive?
      Process.kill("KILL", @process.pid)
      @process.join
    end
    [@input, @output, @stderr].compact.each { |io| io.close unless io.closed? }
    FileUtils.remove_entry(@directory)
    DecentJob.reset!
  end

  def start_worker
    active_job = ManagementProcessJob.set(queue: "metrics", priority: 4).perform_later("return", @started, @release, @result)
    @original = DecentJob::Store.new(database).find(active_job.provider_job_id)
    environment = {"DECENT_JOB_MANAGEMENT_SPEC_CONNECTION" => JSON.generate(@connection_options), "DECENT_JOB_MANAGEMENT_SPEC_ERROR" => @error}
    @input, @output, @stderr, @process = Open3.popen3(environment, RbConfig.ruby, "-Ilib", "exe/decent_job", "start",
      "--require", File.expand_path("support/management_cli_app.rb", __dir__), "--shutdown-timeout", "2")
    @input.close
    _(Timeout.timeout(8) { @output.gets }.to_s).must_include "DecentJob started"
    Timeout.timeout(8) { sleep 0.01 until File.exist?(@started) }
    DecentJob::Job.find(active_job.provider_job_id, database: database)
  end

  def release_and_stop
    File.write(@release, "continue")
    Timeout.timeout(8) { sleep 0.01 until File.exist?(@result) }
    Process.kill("TERM", @process.pid)
    _(@process.join(5)).wont_be_nil
    _(@process.value.exitstatus).must_equal 0
    _(@stderr.read).must_equal ""
    _(File.exist?(@error)).must_equal false
  end

  def assert_snapshot(attempt)
    _(attempt.fetch("payload")).must_equal @original.fetch("payload")
    _(attempt.fetch("scheduled_at")).must_equal @original.fetch("scheduled_at")
    _(attempt.fetch("job_class")).must_equal "ManagementProcessJob"
    _(attempt.fetch("queue_name")).must_equal "metrics"
  end

  it "publishes a live snapshot and persists completed timing before a clean worker exit" do
    job = start_worker
    attempt = job.executions.fetch(0)
    _(attempt.key?("duration")).must_equal true
    assert_snapshot(attempt)
    _(attempt.fetch("duration")).must_be_nil
    _(job.performed_at).must_equal attempt.fetch("started_at")
    _(job.queue_latency).must_equal job.performed_at - job.scheduled_at
    _(job.runtime_latency).must_be :>=, 0
    release_and_stop

    job.reload
    execution = DecentJob::Execution.find(attempt.fetch("id"), database: database)
    _(execution.status).must_equal :succeeded
    _(execution.duration).must_be :>, 0
    _(execution.duration.finite?).must_equal true
    _(execution.runtime_latency).must_equal execution.duration
    _(execution.queue_latency).must_equal execution.started_at - execution.scheduled_at
    _(execution.total_latency).must_equal execution.queue_latency + execution.duration
    _(job.runtime_latency).must_equal job.finished_at - job.performed_at
    assert_snapshot(job.executions.fetch(0))
  end

  it "keeps a killed attempt's runtime unknown while timing its replacement independently" do
    job = start_worker
    old_attempt = job.executions.fetch(0)
    _(old_attempt.key?("duration")).must_equal true
    Process.kill("KILL", @process.pid)
    _(@process.join(5)).wont_be_nil
    File.write(@release, "continue")
    _(DecentJob::Performer.new(database: database).perform_one(job_id: job.id).state).must_equal "succeeded"

    abandoned, replacement = DecentJob::Execution.for_job(job.id, database: database)
    _(abandoned.id).must_equal old_attempt.fetch("id")
    _(abandoned.status).must_equal :interrupted
    _(abandoned.duration).must_be_nil
    _(abandoned.runtime_latency).must_be_nil
    _(abandoned.total_latency).must_be_nil
    _(abandoned.interrupted_duration).must_equal abandoned.finished_at - abandoned.started_at
    _(abandoned.interrupted_duration).must_be :>, 0
    _(replacement.status).must_equal :succeeded
    _(replacement.duration).must_be :>, 0
    _(replacement.interrupted_duration).must_be_nil
    _(job.reload.performed_at).must_equal replacement.started_at
    _(job.executions.size).must_equal 2
    job.executions.each { |attempt| assert_snapshot(attempt) }
  end

  it "does not backfill a force-discarded attempt's runtime when its original worker returns" do
    job = start_worker
    _(job.executions.fetch(0).key?("duration")).must_equal true
    job.force_discard_job("operator finished the attempt")
    discarded = job.executions.fetch(0)
    _(discarded.fetch("duration")).must_be_nil
    _(discarded.fetch("state")).must_equal "discarded"
    release_and_stop

    _(job.executions.fetch(0)).must_equal discarded
    execution = DecentJob::Execution.find(discarded.fetch("id"), database: database)
    _(execution.runtime_latency).must_be_nil
    _(execution.total_latency).must_be_nil
    _(job.reload.runtime_latency).must_equal job.finished_at - job.performed_at
    assert_snapshot(job.executions.fetch(0))
  end
end
