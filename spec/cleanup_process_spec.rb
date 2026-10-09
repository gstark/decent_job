# frozen_string_literal: true

require "open3"
require "spec_helper"
require "support/cleanup_cli_app"
require "timeout"
require "tmpdir"

describe "cleanup through the executable" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    CleanupProcessJob.queue_adapter = DecentJob::Adapter.new(database: database)
    @directory = Dir.mktmpdir("decent_job_cleanup")
    @marker = File.join(@directory, "performed")
    @event = File.join(@directory, "event")
    @environment = {"DECENT_JOB_CLEANUP_SPEC_CONNECTION" => JSON.generate(@connection_options),
                    "DECENT_JOB_CLEANUP_SPEC_EVENT" => @event,
                    "DECENT_JOB_CLEANUP_INTERVAL_JOBS" => "false",
                    "DECENT_JOB_CLEANUP_INTERVAL_SECONDS" => "false",
                    "DECENT_JOB_CLEANUP_DISCARDED_JOBS" => "true",
                    "DECENT_JOB_CLEANUP_PRESERVED_JOBS_BEFORE_SECONDS_AGO" => "1209600"}
    @command = [RbConfig.ruby, "-Ilib", "exe/decent_job", "--require", File.expand_path("support/cleanup_cli_app.rb", __dir__)]
    @store = DecentJob::Store.new(database)
  end

  after do
    if @process&.alive?
      Process.kill("KILL", @process.pid)
      @process.join
    end
    [@input, @output, @stderr].compact.each { |io| io.close unless io.closed? }
    @lock_connection&.finish unless @lock_connection&.finished?
    FileUtils.remove_entry(@directory)
  end

  def completed_job(age: 1_300_000, discarded: false)
    job = CleanupProcessJob.perform_later(nil, discarded)
    DecentJob::Performer.new(database: database).perform_one(job_id: job.provider_job_id)
    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_jobs SET finished_at = clock_timestamp() - $2::double precision * interval '1 second' WHERE id = $1", [job.provider_job_id, age])
    end
    job
  end

  def run_cleanup(*arguments)
    output, error, status = Open3.capture3(@environment, *@command, *arguments)
    _(status.exitstatus).must_equal 0, error
    _(error).must_equal ""
    _(output).must_match(/\d/)
    JSON.parse(File.read(@event))
  end

  it "cleans old jobs and batches with bounded pages and reports actual record counts" do
    success = completed_job
    failure = completed_job(discarded: true)
    recent = completed_job(age: 30)
    pending = CleanupProcessJob.set(wait: 3600).perform_later
    batch = DecentJob::Batch.enqueue([], database: database)
    database.with_connection do |connection|
      connection.exec_params("UPDATE decent_job_batches SET finished_at = clock_timestamp() - interval '15 days' WHERE id = $1", [batch.id])
    end
    event = run_cleanup("cleanup_preserved_jobs", "--in-batches-of", "1")
    _(event.fetch("destroyed_jobs_count")).must_equal 2
    _(event.fetch("destroyed_executions_count")).must_equal 2
    _(event.fetch("destroyed_batches_count")).must_equal 1
    _(event.fetch("destroyed_records_count")).must_equal 5
    [success, failure].each do |job|
      _(@store.find(job.provider_job_id)).must_be_nil
      _(@store.executions(job.provider_job_id)).must_be_empty
    end
    _(@store.find(recent.provider_job_id)).wont_be_nil
    _(@store.find(pending.provider_job_id)).wont_be_nil
    _(DecentJob::BatchStore.new(database).find(batch.id)).must_be_nil
  end

  it "applies application retention after environment settings and CLI age after the application" do
    job = completed_job(age: 5400)
    @environment["DECENT_JOB_CLEANUP_PRESERVED_JOBS_BEFORE_SECONDS_AGO"] = "0"
    @environment["DECENT_JOB_CLEANUP_SPEC_APP_AGE"] = "7200"
    _(run_cleanup("cleanup").fetch("destroyed_records_count")).must_equal 0
    _(@store.find(job.provider_job_id)).wont_be_nil
    _(run_cleanup("cleanup", "--older-than", "3600").fetch("destroyed_records_count")).must_equal 2
    _(@store.find(job.provider_job_id)).must_be_nil
  end

  it "overrides discarded retention in both directions and accepts the reference age flag" do
    failure = completed_job(discarded: true)
    _(run_cleanup("cleanup", "--no-include-discarded").fetch("destroyed_records_count")).must_equal 0
    _(@store.find(failure.provider_job_id)).wont_be_nil
    @environment["DECENT_JOB_CLEANUP_SPEC_APP_KEEP_DISCARDED"] = "true"
    _(run_cleanup("cleanup_preserved_jobs", "--before-seconds-ago", "0", "--include-discarded").fetch("destroyed_records_count")).must_equal 2
    _(@store.find(failure.provider_job_id)).must_be_nil
  end

  it "rejects invalid cleanup options before deleting records" do
    job = completed_job
    [["--in-batches-of", "0"], ["--older-than", "-1"]].each do |arguments|
      _output, error, status = Open3.capture3(@environment, *@command, "cleanup", *arguments)
      _(status.exitstatus).must_equal 2
      _(error).must_include "Error:"
      _(@store.find(job.provider_job_id)).wont_be_nil
    end
  end

  it "reports a database failure with an unsuccessful exit" do
    @environment["DECENT_JOB_CLEANUP_SPEC_CONNECTION"] = JSON.generate(@connection_options.merge(host: "/no/such/decent_job_socket"))
    _output, error, status = Open3.capture3(@environment, *@command, "cleanup")
    _(status.exitstatus).must_equal 1
    _(error).must_include "Error:"
  end

  it "automatically cleans after a completed job while skipping a locked old record" do
    locked = completed_job
    removable = completed_job
    @lock_connection = database.connect
    @lock_connection.exec_params("SELECT pg_advisory_lock(hashtextextended(current_schema() || '.decent_jobs.' || $1, 0))", [locked.provider_job_id])
    trigger = CleanupProcessJob.perform_later(@marker)
    @environment["DECENT_JOB_CLEANUP_INTERVAL_JOBS"] = "-1"
    @input, @output, @stderr, @process = Open3.popen3(@environment, *@command, "start", "--shutdown-timeout", "2")
    @input.close
    _(Timeout.timeout(8) { @output.gets }.to_s).must_include "DecentJob started"
    Timeout.timeout(8) { sleep 0.01 until File.exist?(@event) }
    event = JSON.parse(File.read(@event))
    _(event.fetch("destroyed_records_count")).must_equal 2
    _(File.exist?(@marker)).must_equal true
    _(@store.find(locked.provider_job_id)).wont_be_nil
    _(@store.find(removable.provider_job_id)).must_be_nil
    _(@store.find(trigger.provider_job_id).fetch("state")).must_equal "succeeded"
    Process.kill("TERM", @process.pid)
    _(@process.join(5)).wont_be_nil
    _(@process.value.exitstatus).must_equal 0
    _(@stderr.read).must_equal ""
  end
end
