# frozen_string_literal: true

require "fileutils"
require "open3"
require "spec_helper"
require "support/runtime_utilities_process"
require "timeout"
require "tmpdir"

describe "runtime utility ownership across processes" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @directory = Dir.mktmpdir("dj-utilities-", "/tmp")
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    RuntimeUtilitiesJob.queue_adapter = @adapter
  end

  after do
    @adapter.shutdown(timeout: 5)
    DecentJob.reset!
    FileUtils.remove_entry(@directory)
  end

  def await
    Timeout.timeout(10) { sleep 0.01 until yield }
  end

  def signal(name)
    File.write(File.join(@directory, name), "true")
  end

  def snapshot(name)
    path = File.join(@directory, "#{name}.json")
    JSON.parse(File.read(path)) if File.exist?(path)
  rescue JSON::ParserError
    nil
  end

  def run_child(isolation)
    environment = {"UTILITY_DATABASE" => JSON.generate(@connection_options),
                   "UTILITY_DIRECTORY" => @directory, "UTILITY_ISOLATION" => isolation}
    command = [RbConfig.ruby, "-Ilib", "spec/support/runtime_utilities_process.rb"]
    Open3.popen3(environment, *command, pgroup: true) do |input, output, error, process|
      input.close
      stdout = Thread.new { output.read }
      stderr = Thread.new { error.read }
      yield process
      status = Timeout.timeout(5) { process.value }
      _(status.exitstatus).must_equal 0, "#{stderr.value}\n#{stdout.value}"
    ensure
      if process&.alive?
        Process.kill("KILL", -process.pid)
        process.join
      end
      stdout&.join
      stderr&.join
      warn "Child diagnostics: #{stderr&.value}\n#{stdout&.value}" if $!
    end
  end

  %w[thread fiber].each do |isolation|
    it "keeps notifications and heartbeat active through blocked work with #{isolation} isolation" do
      active_job = RuntimeUtilitiesJob.perform_later(@directory)
      job = DecentJob::Job.find(active_job.provider_job_id, database: database)
      run_child(isolation) do |process|
        await { snapshot("ready") && snapshot("job") && snapshot("cron") }
        ready = snapshot("ready")
        job_state = snapshot("job")
        cron_state = snapshot("cron")
        _(ready.fetch("pool").fetch("active")).must_equal 2
        _(job_state.fetch("current")).must_equal active_job.job_id
        _(cron_state.fetch("current")).must_equal "hold"
        if isolation == "thread"
          _(job_state.fetch("nested")).must_equal active_job.job_id
          _(cron_state.fetch("nested")).must_equal "hold"
        else
          _(job_state.fetch("nested")).must_be_nil
          _(cron_state.fetch("nested")).must_be_nil
        end
        process_id = ready.fetch("process_id")
        record = DecentJob::ProcessRecord.find(process_id, database: database)
        await { DecentJob::ProcessRecord.find(process_id, database: database).updated_at > record.updated_at }
        database.with_connection do |connection|
          DecentJob::Notifier.notify(connection, queue_name: "utility-probe")
        end
        await { File.exist?(File.join(@directory, "notified")) }
        _(job.reload.locked_by_id).must_equal process_id
        _(DecentJob::ProcessRecord.active(database: database).map(&:id)).must_include process_id
        signal("stop")
        await { snapshot("stopping") }
        stopping = snapshot("stopping")
        _(stopping.fetch("stopped")).must_equal false
        _(stopping.fetch("started")).must_equal false
        _(stopping.fetch("pool").fetch("active")).must_equal 2
        record = DecentJob::ProcessRecord.find(process_id, database: database)
        await { DecentJob::ProcessRecord.find(process_id, database: database).updated_at > record.updated_at }
        signal("release_job")
        await { job.reload.finished? }
        _(job.succeeded?).must_equal true
        _(File.read(File.join(@directory, "job_stopping"))).must_equal "true"
        _(File.exist?(File.join(@directory, "cron_finished"))).must_equal false
        _(process.alive?).must_equal true
        signal("release_cron")
        await { snapshot("finished") }
        finished = snapshot("finished")
        _(finished.fetch("drained")).must_equal true
        _(finished.fetch("pool").fetch("active")).must_equal 0
      end
      _(DecentJob::ProcessRecord.active(database: database)).must_be_empty
      _(job.executions.size).must_equal 1
    end
  end
end
