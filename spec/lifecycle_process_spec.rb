# frozen_string_literal: true

require "fileutils"
require "open3"
require "spec_helper"
require "support/lifecycle_process"
require "timeout"
require "tmpdir"

describe "explicit lifecycle restart across processes" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @directory = Dir.mktmpdir("dj-lifecycle-", "/tmp")
    @adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    LifecycleProcessJob.queue_adapter = @adapter
    @store = DecentJob::Store.new(database)
  end

  after do
    @adapter.shutdown(timeout: 5)
    FileUtils.remove_entry(@directory)
  end

  def await
    Timeout.timeout(10) { sleep 0.01 until yield }
  end

  def signal(name)
    File.write(File.join(@directory, name), "ready")
  end

  def snapshot(name)
    path = File.join(@directory, "#{name}.json")
    JSON.parse(File.read(path)) if File.exist?(path)
  rescue JSON::ParserError
    nil
  end

  def run_child(scope)
    environment = {"LIFECYCLE_DATABASE" => JSON.generate(@connection_options), "LIFECYCLE_DIRECTORY" => @directory,
                   "LIFECYCLE_SCOPE" => scope}
    command = [RbConfig.ruby, "-Ilib", "spec/support/lifecycle_process.rb"]
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

  %w[adapter global].each do |scope|
    it "keeps #{scope} work stopped through drain and executes queued work after explicit restart" do
      first = LifecycleProcessJob.perform_later(@directory, "started")
      run_child(scope) do
        await { File.exist?(File.join(@directory, "started")) }
        original_id = @store.find(first.provider_job_id).fetch("locked_by_id")
        signal("stop")
        await { snapshot("stopping") }
        stopping = snapshot("stopping")
        _(stopping.fetch("stopped")).must_equal false
        _(stopping.fetch("restarted")).must_equal false
        _(stopping.fetch("process_ids")).must_equal [original_id]
        followup_id = stopping.fetch("followup_id")
        _(@store.find(followup_id).fetch("state")).must_equal "ready"
        _(File.exist?(File.join(@directory, "followup"))).must_equal false
        signal("release")
        await { snapshot("drained") }
        drained = snapshot("drained")
        _(drained.fetch("stopped")).must_equal true
        _(drained.fetch("active")).must_be_empty
        _(@store.find(first.provider_job_id).fetch("state")).must_equal "succeeded"
        _(@store.executions(followup_id)).must_be_empty
        signal("restart")
        await { snapshot("resumed") }
        resumed = snapshot("resumed")
        _(resumed.fetch("restarted")).must_equal true
        _(resumed.fetch("process_ids").size).must_equal 1
        _(resumed.fetch("process_ids")).wont_include original_id
        _(resumed.fetch("stats").fetch("succeeded_executions_count")).must_equal 1
        _(resumed.fetch("stats").fetch("schedulers").sum { |pool| pool.fetch("max_threads") }).must_equal 1
        _(@store.executions(first.provider_job_id).size).must_equal 1
        _(@store.executions(followup_id).size).must_equal 1
        _(File.read(File.join(@directory, "followup"))).must_equal followup_id
        signal("finish")
        await { snapshot("finished") }
        finished = snapshot("finished")
        _(finished.fetch("global")).must_equal true
        _(finished.fetch("adapter")).must_equal true
        _(finished.fetch("pool").fetch("active")).must_equal 0
      end
      _(DecentJob::ProcessRecord.active(database: database)).must_be_empty
    end
  end
end
