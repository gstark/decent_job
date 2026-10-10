# frozen_string_literal: true

require "open3"
require "spec_helper"
require "support/cli_startup"
require "support/management_cli_app"
require "timeout"
require "tmpdir"

describe "job management across processes" do
  include DatabaseSpec
  include CliStartupSpec

  before do
    DecentJob::Schema.new(database).migrate
    ManagementProcessJob.queue_adapter = DecentJob::Adapter.new(database: database)
    @directory = Dir.mktmpdir("decent_job_management")
    @started = File.join(@directory, "started")
    @release = File.join(@directory, "release")
    @result = File.join(@directory, "result")
    @error = File.join(@directory, "error")
  end

  after do
    if @process&.alive?
      begin
        Process.kill("KILL", @process.pid)
      rescue Errno::ESRCH
        nil
      end

      @process.join
    end

    [@input, @output, @stderr].compact.each { |io| io.close unless io.closed? }
    FileUtils.remove_entry(@directory)
  end

  def start_worker(mode)
    job = ManagementProcessJob.perform_later(mode, @started, @release, @result)
    environment = {"DECENT_JOB_MANAGEMENT_SPEC_CONNECTION" => JSON.generate(@connection_options), "DECENT_JOB_MANAGEMENT_SPEC_ERROR" => @error}
    @input, @output, @stderr, @process = Open3.popen3(environment, RbConfig.ruby, "-Ilib", "exe/decent_job", "start",
      "--require", File.expand_path("support/management_cli_app.rb", __dir__), "--shutdown-timeout", "2")
    @input.close
    await_started(@output, @stderr)
    Timeout.timeout(8) { sleep 0.01 until File.exist?(@started) }
    DecentJob::Job.find(job.provider_job_id, database: database)
  end

  def release_and_stop
    File.write(@release, "continue")
    Timeout.timeout(8) { sleep 0.01 until File.exist?(@result) }
    Process.kill("TERM", @process.pid)
    _(@process.join(5)).wont_be_nil
    _(@process.value.exitstatus).must_equal 0
    _(@stderr.read).must_equal ""
    _(File.exist?(@error)).must_equal false, (File.read(@error) if File.exist?(@error))
    JSON.parse(File.read(@result))
  end

  it "rejects ordinary actions on a live worker and preserves force-discard after the job returns" do
    job = start_worker("return")
    _ { job.retry_job }.must_raise DecentJob::Job::LockedError
    _ { job.discard_job("ordinary") }.must_raise DecentJob::Job::LockedError
    _ { job.reschedule_job }.must_raise DecentJob::Job::LockedError
    _ { job.destroy_job }.must_raise DecentJob::Job::LockedError
    job.force_discard_job("operator stopped this job")
    _(job.reload.discarded?).must_equal true
    _ { job.destroy_job }.must_raise DecentJob::Job::LockedError
    result = release_and_stop
    _(result.fetch("reason")).must_equal "force_discarded"
    _(job.reload.discarded?).must_equal true
    row = DecentJob::Store.new(database).find(job.id)
    _(row.fetch("error").fetch("message")).must_equal "operator stopped this job"
    _(row.fetch("error_event")).must_equal "discarded"
    attempts = DecentJob::Store.new(database).executions(job.id)
    _(attempts.map { |attempt| attempt.fetch("state") }).must_equal ["discarded"]
    _(attempts.first.fetch("error").fetch("message")).must_equal "operator stopped this job"
    _(attempts.first.fetch("error_event")).must_equal "discarded"
    job.destroy_job
    _(DecentJob::Store.new(database).find(job.id)).must_be_nil
    _(DecentJob::Store.new(database).executions(job.id)).must_be_empty
  end

  it "prevents a force-discarded worker from rescheduling its own job" do
    job = start_worker("retry")
    job.force_discard_job("do not retry")
    result = release_and_stop
    _(result.fetch("reason")).must_equal "force_discarded"
    _(result.fetch("successfully_enqueued")).must_equal false
    _(job.reload.discarded?).must_equal true
    _(DecentJob::Store.new(database).executions(job.id).size).must_equal 1
    _(DecentJob::Store.new(database).find(job.id).fetch("error").fetch("message")).must_equal "do not retry"
  end

  it "lets a job observe CLI shutdown through the no-argument adapter call" do
    job = start_worker("shutdown")
    Process.kill("TERM", @process.pid)
    _(@process.join(5)).wont_be_nil
    _(@process.value.exitstatus).must_equal 0
    _(@stderr.read).must_equal ""
    _(File.exist?(@error)).must_equal false
    _(JSON.parse(File.read(@result)).fetch("reason")).must_equal "stopping"
    _(job.reload.succeeded?).must_equal true
  end
end
