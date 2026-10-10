# frozen_string_literal: true

require "fileutils"
require "open3"
require "spec_helper"
require "support/forced_shutdown_process"
require "timeout"
require "tmpdir"

describe "forced worker shutdown across processes" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    database.with_connection { |connection| connection.exec("CREATE TABLE forced_shutdown_probe (id bigint GENERATED ALWAYS AS IDENTITY)") }
    @directory = Dir.mktmpdir("dj-forced-shutdown-", "/tmp")
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    ForcedShutdownProcessJob.queue_adapter = @adapter
    @store = DecentJob::Store.new(database)
  end

  after do
    @adapter.shutdown(timeout: 5)

    if @original_pid
      @admin.exec_params("SELECT pg_cancel_backend($1) WHERE EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND query = 'SELECT pg_sleep(60)')", [@original_pid])
    end

    FileUtils.remove_entry(@directory)
  end

  def await
    Timeout.timeout(10) { sleep 0.01 until yield }
  end

  def snapshot(name)
    path = File.join(@directory, "#{name}.json")
    JSON.parse(File.read(path)) if File.exist?(path)
  rescue JSON::ParserError
    nil
  end

  def signal(name)
    File.write(File.join(@directory, name), "ready")
  end

  def run_child
    environment = {"FORCED_SHUTDOWN_DATABASE" => JSON.generate(@connection_options), "FORCED_SHUTDOWN_DIRECTORY" => @directory}
    command = [RbConfig.ruby, "-Ilib", "spec/support/forced_shutdown_process.rb"]

    Open3.popen3(environment, *command, pgroup: true) do |input, output, error, process|
      input.close
      stdout = Thread.new { output.read }
      stderr = Thread.new { error.read }
      yield
      status = Timeout.timeout(8) { process.value }
      _(status.exitstatus).must_equal 0, "#{stderr.value}\n#{stdout.value}"
    ensure
      if process&.alive?
        Process.kill("KILL", -process.pid)
        process.join
      end

      stdout&.join
      stderr&.join
    end
  end

  %w[ruby postgres].each do |mode|
    it "interrupts blocked #{mode} work while preserving cleanup ownership and replacement recovery" do
      job = ForcedShutdownProcessJob.perform_later(@directory, mode)

      run_child do
        await { snapshot("started") }
        original_pid = @original_pid = snapshot("started").fetch("backend_pid")

        if mode == "postgres"
          await do
            @admin.exec_params("SELECT wait_event FROM pg_stat_activity WHERE pid = $1", [original_pid]).first&.fetch("wait_event") == "PgSleep"
          end
        end

        signal("force")
        await { snapshot("forced") && File.exist?(File.join(@directory, "cleanup")) }
        forced = snapshot("forced")
        _(forced.fetch("drained")).must_equal false
        _(forced.fetch("ids")).must_equal [job.job_id]
        _(forced.fetch("events")).must_equal [[job.job_id]]
        _(DecentJob::Performer.new(database: database).perform_one(job_id: job.provider_job_id)).must_be_nil
        _(DecentJob::ProcessRecord.active(database: database)).wont_be_empty
        attempts = @store.executions(job.provider_job_id)
        _(attempts.size).must_equal 1
        attempt = attempts.first
        _(attempt.fetch("state")).must_equal "running"
        _(attempt.fetch("finished_at")).must_be_nil
        signal("release")
        await { snapshot("drained") }
        drained = snapshot("drained")
        _(drained.fetch("drained")).must_equal true
        _(drained.fetch("backend_pid")).wont_equal original_pid
        _(drained.fetch("pool").fetch("active")).must_equal 0
        _(drained.fetch("errors")).must_be_empty

        Timeout.timeout(3) do
          sleep 0.01 until @admin.exec_params("SELECT pid FROM pg_stat_activity WHERE pid = $1", [original_pid]).ntuples.zero?
        end
      end

      _(File.exist?(File.join(@directory, "completed"))).must_equal false
      _(database.with_connection { |connection| connection.exec("SELECT count(*) FROM forced_shutdown_probe").getvalue(0, 0) }).must_equal "0"
      result = DecentJob::Performer.new(database: database).perform_one(job_id: job.provider_job_id, raise_errors: true)
      _(result.state).must_equal "discarded"
      attempts = @store.executions(job.provider_job_id)
      _(attempts.map { |attempt| attempt.fetch("state") }).must_equal %w[interrupted discarded]
      _(attempts.first.fetch("error").fetch("class")).must_equal "DecentJob::InterruptedError"
      _(attempts.first.fetch("duration")).must_be_nil
      _(attempts.last.fetch("error").fetch("class")).must_equal "DecentJob::InterruptError"
      _(File.exist?(File.join(@directory, "completed"))).must_equal false
    end
  end
end
