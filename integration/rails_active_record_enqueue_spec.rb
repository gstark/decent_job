# frozen_string_literal: true

require "fileutils"
require "open3"
require "spec_helper"
require "timeout"
require "tmpdir"

describe "Rails enqueue through the Active Record connection" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @directory = Dir.mktmpdir("dj-rails-ar-", "/tmp")
  end

  after { FileUtils.remove_entry(@directory) }

  def run_host(scenario, mode: "external", connection: nil, database_url: nil, defer: false, isolation: "thread")
    environment = {"HOST_CONNECTION" => JSON.generate(@connection_options), "HOST_ROOT" => @directory, "HOST_SCENARIO" => scenario,
                   "HOST_MODE" => mode, "HOST_CONNECTION_MODE" => connection, "HOST_DATABASE_URL" => database_url,
                   "HOST_DEFER" => defer.to_s, "HOST_ISOLATION" => isolation, "RAILS_ENV" => "production",
                   "DATABASE_URL" => nil, "DECENT_JOB_DATABASE_URL" => nil, "DECENT_JOB_EXECUTION_MODE" => nil,
                   "DECENT_JOB_ENABLE_CRON" => "false", "RAILS_MAX_THREADS" => nil}

    output, error, status = Timeout.timeout(30) do
      Open3.capture3(environment, RbConfig.ruby, "-Ilib", "integration/support/rails_active_record_app.rb")
    end

    _(status.exitstatus).must_equal 0, "Host failed: #{error}\n#{output}"
    line = output.lines.find { |value| value.start_with?("HOST_RESULT:") }
    _(line).wont_be_nil "Host did not report results: #{error}"
    JSON.parse(line.delete_prefix("HOST_RESULT:"))
  end

  it "defaults to the Active Record connection without an explicit database_url" do
    _(run_host("boot").fetch("connection")).must_equal "active_record"
    _(run_host("boot", database_url: "same").fetch("connection")).must_equal "own"
    _(run_host("boot", database_url: "same", connection: "active_record").fetch("connection")).must_equal "active_record"
    _(run_host("boot", connection: "own").fetch("connection")).must_equal "own"
  end

  it "rejects the Active Record connection with a database_url for another database" do
    result = run_host("boot", database_url: "other", connection: "active_record")
    _(result.fetch("boot_error")).must_match(/connection active_record requires database_url to name the Active Record database/)
  end

  it "warns at boot only when own-connection enqueues are not deferred" do
    _(run_host("boot", connection: "own").fetch("warned")).must_equal true
    _(run_host("boot", connection: "own", defer: true).fetch("warned")).must_equal false
    _(run_host("boot").fetch("warned")).must_equal false
    _(run_host("boot", defer: true).fetch("warned")).must_equal false
  end

  it "drops the job row and the wakeup when the application transaction rolls back" do
    result = run_host("rollback")
    _(result.fetch("inside")).must_equal 0
    _(result.fetch("after")).must_equal 0
    _(result.fetch("notified")).must_equal false
    _(result.key?("commit_callback")).must_equal false
    _(result.fetch("rollback_callback")).must_equal true
  end

  it "hides the job from other connections until the application commits" do
    result = run_host("commit")
    _(result.fetch("inside")).must_equal 0
    _(result.fetch("pool_inside")).must_equal 0
    _(result.fetch("notified_inside")).must_equal false
    _(result.fetch("after")).must_equal 1
    _(result.fetch("notified_after")).must_equal true
    _(result.fetch("commit_callback_count")).must_equal 1
  end

  it "drops only the inner enqueue when a nested transaction rolls back" do
    _(run_host("nested").fetch("values")).must_equal ["outer"]
  end

  it "makes bulk enqueue atomic with the application transaction" do
    result = run_host("bulk")
    _(result.fetch("rolled_back")).must_equal 0
    _(result.fetch("inside")).must_equal 0
    _(result.fetch("committed")).must_equal 2
  end

  it "makes batch creation and enqueue atomic with the application transaction" do
    result = run_host("batch")
    _(result.fetch("rolled_back")).must_equal [0, 0]
    _(result.fetch("reloaded")).must_equal true
    _(result.fetch("inside")).must_equal [0, 0]
    _(result.fetch("committed")).must_equal [1, 2]
    _(result.fetch("enqueue_all_rolled_back")).must_equal [1, 2]
  end

  it "holds the admission lock until the application commits" do
    result = run_host("concurrency")
    _(result.fetch("second_waiting")).must_equal true
    _(result.fetch("first")).must_equal true
    _(result.fetch("second")).must_equal false
    _(result.fetch("after")).must_equal 1
  end

  it "does not let a running worker claim the job before the application commits" do
    result = run_host("worker")
    _(result.fetch("before_commit")).must_equal []
    _(result.fetch("after_commit")).must_equal ["worker"]
  end

  it "runs inline jobs only after the outer commit" do
    result = run_host("inline", mode: "inline")
    _(result.fetch("before_commit")).must_equal []
    _(result.fetch("after_commit")).must_equal ["inner", "outer"]
    _(result.fetch("no_transaction")).must_equal ["plain"]
  end

  it "wakes async workers only after the outer commit" do
    result = run_host("async", mode: "async_all")
    _(result.fetch("before_commit")).must_equal []
    _(result.fetch("after_commit")).must_equal ["inner", "outer"]
    _(result.fetch("no_transaction")).must_equal ["plain"]
  end

  it "borrows the Active Record connection of the current fiber under fiber isolation" do
    result = run_host("rollback", isolation: "fiber")
    _(result.fetch("after")).must_equal 0
    _(run_host("nested", isolation: "fiber").fetch("values")).must_equal ["outer"]
  end
end
