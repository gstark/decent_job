# frozen_string_literal: true

require "open3"
require "spec_helper"
require "support/cli_startup"
require "support/recovery_cli_app"
require "timeout"
require "tmpdir"

describe "execution recovery across processes" do
  include DatabaseSpec
  include CliStartupSpec

  before do
    DecentJob::Schema.new(database).migrate
    adapter = DecentJob::Adapter.new(database: database)
    [RecoveryProcessJob, RecoveryProcessRetryJob, RecoveryProcessDiscardJob, RecoveryProcessPlainJob, RecoveryProcessErrorJob].each do |klass|
      klass.queue_adapter = adapter
    end
    @directory = Dir.mktmpdir("decent_job_recovery")
    @started = File.join(@directory, "started")
    @completed = File.join(@directory, "completed")
    @errors = File.join(@directory, "errors")
    @environment = {"DECENT_JOB_RECOVERY_SPEC_CONNECTION" => JSON.generate(@connection_options),
                    "DECENT_JOB_RECOVERY_SPEC_ERRORS" => @errors, "DECENT_JOB_RECOVERY_SPEC_BLOCK" => "true"}
    @command = [RbConfig.ruby, "-Ilib", "exe/decent_job", "start", "--require", File.expand_path("support/recovery_cli_app.rb", __dir__), "--shutdown-timeout", "2"]
    @store = DecentJob::Store.new(database)
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
    @input, @output, @stderr, @process = Open3.popen3(@environment, *@command)
    @input.close
    await_started(@output, @stderr)
  end

  def interrupted_job(klass)
    job = klass.perform_later(@started, @completed)
    start_worker
    Timeout.timeout(8) { sleep 0.01 until File.exist?(@started) }
    attempts = @store.executions(job.provider_job_id)
    _(attempts.size).must_equal 1
    _(attempts.first.fetch("state")).must_equal "running"
    Process.kill("KILL", @process.pid)
    _(@process.join(5)).wont_be_nil
    _(File.exist?(@errors)).must_equal false
    _(File.exist?(@completed)).must_equal false
    job
  end

  def assert_interrupted_attempt(attempt)
    _(attempt.fetch("state")).must_equal "interrupted"
    _(attempt.fetch("error_event")).must_equal "interrupted"
    _(attempt.fetch("error").fetch("class")).must_equal "DecentJob::InterruptedError"
    _(attempt.fetch("error").fetch("message")).must_include "Interrupted after starting perform at"
    _(attempt.fetch("started_at")).must_be_kind_of Time
    _(attempt.fetch("finished_at")).must_be :>=, attempt.fetch("started_at")
  end

  it "lets retry_on handle interruption before the body and records only the old and replacement attempts" do
    job = interrupted_job(RecoveryProcessRetryJob)
    performer = DecentJob::Performer.new(database: database)
    result = performer.perform_one(job_id: job.provider_job_id, raise_errors: true)
    _(result.state).must_equal "retried"
    _(result.error).must_be_kind_of DecentJob::InterruptError
    _(File.exist?(@completed)).must_equal false
    attempts = @store.executions(job.provider_job_id)
    _(attempts.size).must_equal 2
    assert_interrupted_attempt(attempts.first)
    _(attempts.last.fetch("error").fetch("class")).must_equal "DecentJob::InterruptError"
    _(attempts.last.fetch("error_event")).must_equal "retried"
    row = @store.find(job.provider_job_id)
    _(row.fetch("state")).must_equal "ready"
    _(row.fetch("scheduled_at")).must_be :>, Time.now + 3500
    _(row.fetch("finished_at")).must_be_nil
    _(performer.perform_one(job_id: job.provider_job_id)).must_be_nil
    DecentJob::Job.find(job.provider_job_id, database: database).reschedule_job
    _(performer.perform_one(job_id: job.provider_job_id).state).must_equal "succeeded"
    _(JSON.parse(File.read(@completed)).fetch("provider_job_id")).must_equal job.provider_job_id
    _(@store.executions(job.provider_job_id).map { |attempt| attempt.fetch("state") }).must_equal %w[interrupted retried succeeded]
    _(DecentJob::Current.execution).must_be_nil
  end

  it "lets discard_on terminate the replacement without running the body" do
    job = interrupted_job(RecoveryProcessDiscardJob)
    result = DecentJob::Performer.new(database: database).perform_one(job_id: job.provider_job_id, raise_errors: true)
    _(result.state).must_equal "discarded"
    _(File.exist?(@completed)).must_equal false
    attempts = @store.executions(job.provider_job_id)
    _(attempts.size).must_equal 2
    assert_interrupted_attempt(attempts.first)
    _(attempts.last.fetch("error_event")).must_equal "discarded"
    _(@store.find(job.provider_job_id).fetch("error").fetch("class")).must_equal "DecentJob::InterruptError"
  end

  it "persists an unhandled interruption before propagating it to inline callers" do
    job = interrupted_job(RecoveryProcessJob)
    error = _ { DecentJob::Performer.new(database: database).perform_one(job_id: job.provider_job_id, raise_errors: true) }.must_raise DecentJob::InterruptError
    _(error.message).must_include "Interrupted after starting perform at"
    _(File.exist?(@completed)).must_equal false
    attempts = @store.executions(job.provider_job_id)
    _(attempts.size).must_equal 2
    assert_interrupted_attempt(attempts.first)
    _(attempts.last.fetch("error_event")).must_equal "interrupted"
    _(@store.find(job.provider_job_id).fetch("error_event")).must_equal "interrupted"
  end

  it "executes the replacement normally when the interruption extension is absent" do
    job = interrupted_job(RecoveryProcessPlainJob)
    result = DecentJob::Performer.new(database: database).perform_one(job_id: job.provider_job_id, raise_errors: true)
    _(result.state).must_equal "succeeded"
    _(File.exist?(@completed)).must_equal true
    attempts = @store.executions(job.provider_job_id)
    _(attempts.size).must_equal 2
    assert_interrupted_attempt(attempts.first)
    _(attempts.last.fetch("error")).must_be_nil
  end

  it "reports NotImplementedError once and keeps the worker available for the next job" do
    failed = RecoveryProcessErrorJob.perform_later(@completed, true)
    successful = RecoveryProcessErrorJob.perform_later(@completed, false)
    start_worker
    Timeout.timeout(8) do
      sleep 0.01 until @store.find(failed.provider_job_id).fetch("finished_at") && @store.find(successful.provider_job_id).fetch("finished_at") && File.exist?(@errors)
    end
    Process.kill("TERM", @process.pid)
    _(@process.join(5)).wont_be_nil
    _(@process.value.exitstatus).must_equal 0
    _(@stderr.read).must_equal ""
    _(@store.find(failed.provider_job_id).fetch("state")).must_equal "discarded"
    _(@store.find(successful.provider_job_id).fetch("state")).must_equal "succeeded"
    errors = File.readlines(@errors).map { |line| JSON.parse(line) }
    _(errors).must_equal [{"class" => "NotImplementedError", "message" => "process job not implemented"}]
    _(@store.executions(failed.provider_job_id).size).must_equal 1
  end
end
