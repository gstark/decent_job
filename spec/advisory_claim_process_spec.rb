# frozen_string_literal: true

require "open3"
require "spec_helper"
require "support/advisory_claim_process_app"
require "timeout"

describe "advisory claims across nested execution in a fresh process" do
  include DatabaseSpec

  it "skips the executing job in targeted and ordinary nested calls while executing its peer" do
    DecentJob::Schema.new(database).migrate
    adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    AdvisoryClaimProcessJob.queue_adapter = adapter
    outer = AdvisoryClaimProcessJob.set(priority: -1).perform_later("outer")
    peer = AdvisoryClaimProcessJob.perform_later("peer")
    environment = {"ADVISORY_CLAIM_PROCESS_CONNECTION" => JSON.generate(@connection_options),
                   "ADVISORY_CLAIM_PROCESS_JOB" => outer.provider_job_id,
                   "DECENT_JOB_EXECUTION_MODE" => "external", "DECENT_JOB_ENABLE_CRON" => "false",
                   "DECENT_JOB_QUEUE_SELECT_LIMIT" => "1000", "DECENT_JOB_DEQUEUE_QUERY_SORT" => "created_at"}

    Open3.popen3(environment, RbConfig.ruby, "-Ilib", "spec/support/advisory_claim_process_app.rb", pgroup: true) do |input, output, error, process|
      input.close
      stdout = Thread.new { output.read }
      stderr = Thread.new { error.read }
      status = Timeout.timeout(15) { process.value }
      _(status.exitstatus).must_equal 0, "#{stderr.value}\n#{stdout.value}"
      line = stdout.value.lines.find { |value| value.start_with?("ADVISORY_RESULT:") }
      _(line).wont_be_nil "Process did not report results: #{stderr.value}\n#{stdout.value}"
      result = JSON.parse(line.delete_prefix("ADVISORY_RESULT:"))
      _(result).must_equal({"state" => "succeeded", "visits" => %w[outer peer], "nested_results" => [nil, "succeeded"]})
    ensure
      if process&.alive?
        Process.kill("KILL", -process.pid)
        process.join
      end

      stdout&.join
      stderr&.join
    end

    store = DecentJob::Store.new(database)

    [outer, peer].each do |job|
      _(store.find(job.provider_job_id).fetch("state")).must_equal "succeeded"
      _(store.executions(job.provider_job_id).map { |attempt| attempt.fetch("state") }).must_equal ["succeeded"]
    end

    database.with_connection do |connection|
      _(connection.exec("SELECT count(*) FROM decent_job_processes").getvalue(0, 0)).must_equal "0"

      [outer, peer].each do |job|
        key = connection.exec_params("SELECT hashtextextended(current_schema() || '.decent_jobs.' || $1, 0)", [job.provider_job_id]).getvalue(0, 0)
        _(connection.exec_params("SELECT pg_try_advisory_lock($1::bigint)", [key]).getvalue(0, 0)).must_equal "t"
        connection.exec_params("SELECT pg_advisory_unlock($1::bigint)", [key])
      end
    end
  ensure
    adapter&.shutdown
  end
end
