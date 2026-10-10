# frozen_string_literal: true

require "spec_helper"
require "support/jobs"
require "stringio"
require "uri"

class DashboardLabelledJob < ActiveJob::Base
  include DecentJob::ActiveJobExtensions::Labels

  def perform(*)
  end
end

describe DecentJob::Dashboard do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)
    [RecordingJob, FailingJob, DiscardingJob, DashboardLabelledJob].each { |klass| klass.queue_adapter = @adapter }
    @performer = DecentJob::Performer.new(database: database)
    @app = DecentJob::Dashboard.new(database: database)
    @cookies = {}
  end

  def request(method, path, params: {}, script_name: "", headers: {})
    query = ""
    body = ""

    if method == "GET"
      query = URI.encode_www_form(params)
    else
      params = params.merge("authenticity_token" => @cookies["decent_job_csrf"]) unless params.key?("authenticity_token")
      body = URI.encode_www_form(params)
    end

    env = {"REQUEST_METHOD" => method, "PATH_INFO" => path, "QUERY_STRING" => query, "SCRIPT_NAME" => script_name,
           "CONTENT_TYPE" => "application/x-www-form-urlencoded", "rack.input" => StringIO.new(body),
           "HTTP_COOKIE" => @cookies.map { |key, value| "#{key}=#{value}" }.join("; ")}.merge(headers)
    status, response_headers, body = @app.call(env)

    response_headers.fetch("set-cookie", "").split("\n").each do |cookie|
      pair, *attributes = cookie.split("; ")
      name, value = pair.split("=", 2)

      if attributes.include?("Max-Age=0")
        @cookies.delete(name)
      else
        @cookies[name] = value
      end
    end

    [status, response_headers, body.join]
  end

  def get(path, params: {}, **options)
    request("GET", path, params: params, **options)
  end

  def post(path, params: {}, **options)
    request("POST", path, params: params, **options)
  end

  def job_for(enqueued)
    DecentJob::Job.find(enqueued.provider_job_id, database: database)
  end

  describe "overview" do
    it "renders the state ribbon with counts, queues, and workers" do
      RecordingJob.perform_later("a")
      RecordingJob.set(wait: 3600).perform_later("b")
      status, headers, body = get("/")
      _(status).must_equal 200
      _(headers["content-type"]).must_include "text/html"
      _(headers["content-security-policy"]).must_include "script-src 'self';"
      _(headers["content-security-policy"]).must_include "style-src 'self' 'unsafe-inline'"
      _(body).must_include "Right now"
      _(body).must_include 'data-count-for="queued">1<'
      _(body).must_include 'data-count-for="scheduled">1<'
      _(body).must_include "No worker is registered"
    end

    it "serves live metrics as JSON" do
      RecordingJob.perform_later("a")
      status, headers, body = get("/metrics.json")
      _(status).must_equal 200
      _(headers["content-type"]).must_include "application/json"
      data = JSON.parse(body)
      _(data.fetch("counts").fetch("queued")).must_equal 1
      _(data.fetch("queues").first.fetch("queue_name")).must_equal "default"
    end

    it "serves only allowlisted assets" do
      status, headers, body = get("/assets/dashboard.css")
      _(status).must_equal 200
      _(headers["content-type"]).must_include "text/css"
      _(body).must_include ".ribbon"
      status, _, _ = get("/assets/../dashboard.rb")
      _(status).must_equal 404
      status, _, _ = get("/assets/other.css")
      _(status).must_equal 404
    end

    it "prefixes links with the mount point" do
      _, _, body = get("/", script_name: "/admin/jobs")
      _(body).must_include 'href="/admin/jobs/jobs"'
      _(body).must_include 'href="/admin/jobs/assets/dashboard.css"'
    end
  end

  describe "jobs" do
    it "hides serializer markers in job arguments" do
      argument = ActiveSupport::HashWithIndifferentAccess.new("nested" => {symbol: :value})
      enqueued = RecordingJob.perform_later(argument)
      job = job_for(enqueued)
      _(job.payload.to_json).must_include DecentJob::Arguments::INDIFFERENT_KEY
      _, _, body = get("/jobs/#{job.id}")
      arguments_panel = body[%r{<h2>Arguments</h2>\s*<pre class="code">(.*?)</pre>}m, 1]
      _(arguments_panel).must_include "nested"
      _(arguments_panel).wont_include "_aj_"
      _, _, body = get("/jobs")
      _(body).wont_include "_aj_"
    end

    it "lists, filters, searches, and paginates jobs" do
      RecordingJob.set(queue: "mailers").perform_later("first")
      DashboardLabelledJob.set(wait: 60, decent_job_labels: ["nightly"]).perform_later("second")
      FailingJob.perform_later
      @performer.perform_one(queues: "default")
      @performer.perform_one(queues: "default")
      _, _, body = get("/jobs")
      _(body).must_include "RecordingJob"
      _(body).must_include "DashboardLabelledJob"
      _(body).must_include "FailingJob"
      _(body).must_include "3 jobs match"

      _, _, body = get("/jobs", params: {"queue" => "mailers"})
      _(body).must_include "1 job matches"
      _(body).must_include ">RecordingJob</a>"
      _(body).wont_include ">DashboardLabelledJob</a>"

      _, _, body = get("/jobs", params: {"status[]" => "scheduled"})
      _(body).must_include ">DashboardLabelledJob</a>"
      _(body).wont_include ">RecordingJob</a>"

      _, _, body = get("/jobs", params: {"label" => "nightly"})
      _(body).must_include "1 job matches"

      _, _, body = get("/jobs", params: {"q" => "unhandled error"})
      _(body).must_include "FailingJob"
      _(body).must_include "1 job matches"

      _, _, body = get("/jobs", params: {"q" => "second"})
      _(body).must_include "DashboardLabelledJob"
      _(body).must_include "1 job matches"

      _, _, body = get("/jobs", params: {"limit" => "2"})
      _(body).must_include "Older jobs"
      cursor = body[/after=([^"&]+)/, 1]
      _, _, body = get("/jobs", params: {"limit" => "2", "after" => URI.decode_www_form_component(cursor)})
      _(body).wont_include "Older jobs"
    end

    it "shows a job with attempts, errors, backtraces, and arguments" do
      enqueued = FailingJob.perform_later
      @performer.perform_one
      job = job_for(enqueued)
      status, _, body = get("/jobs/#{job.id}")
      _(status).must_equal 200
      _(body).must_include "FailingJob"
      _(body).must_include "Discarded"
      _(body).must_include "RuntimeError: unhandled error"
      _(body).must_include "Attempt 1"
      _(body).must_include 'data-tab="full"'
      _(body).must_include "Retry"

      status, _, body = get("/jobs/#{SecureRandom.uuid}")
      _(status).must_equal 404
      _(body).must_include "There is no page"
      status, _, _ = get("/jobs/not-a-uuid")
      _(status).must_equal 404
    end

    it "escapes job data in markup" do
      RecordingJob.perform_later("<script>alert(1)</script>")
      _, _, body = get("/jobs")
      _(body).wont_include "<script>alert(1)</script>"
      _(body).must_include "&lt;script&gt;"
    end

    it "retries, reschedules, discards, and deletes jobs with CSRF protection" do
      enqueued = FailingJob.perform_later
      @performer.perform_one
      job = job_for(enqueued)
      get("/")

      status, headers, _ = post("/jobs/#{job.id}/retry", params: {"authenticity_token" => "wrong"})
      _(status).must_equal 400
      _(headers).wont_include "location"
      _(job.reload.status).must_equal :discarded

      status, headers, _ = post("/jobs/#{job.id}/retry")
      _(status).must_equal 303
      _(headers["location"]).must_equal "/jobs/#{job.id}"
      _(job.reload.status).must_equal :queued
      _, _, body = get("/jobs/#{job.id}")
      _(body).must_include "Retried job"

      status, _, _ = post("/jobs/#{job.id}/reschedule", params: {"at" => (Time.now + 3600).utc.iso8601})
      _(status).must_equal 303
      _(job.reload.status).must_equal :retried

      status, _, _ = post("/jobs/#{job.id}/discard", params: {"reason" => "No longer needed"})
      _(status).must_equal 303
      _(job.reload.status).must_equal :discarded
      _(job.error.fetch("message")).must_equal "No longer needed"

      status, _, _ = post("/jobs/#{job.id}/destroy")
      _(status).must_equal 303
      _ { job.reload }.must_raise KeyError

      status, _, _ = post("/jobs/#{job.id}/explode")
      _(status).must_equal 404
    end

    it "reports an ineligible action without failing" do
      enqueued = RecordingJob.perform_later("a")
      @performer.perform_one
      job = job_for(enqueued)
      get("/")
      status, headers, _ = post("/jobs/#{job.id}/retry", headers: {"HTTP_REFERER" => "http://localhost/jobs/#{job.id}"})
      _(status).must_equal 303
      _(headers["location"]).must_equal "/jobs/#{job.id}"
      _, _, body = get("/jobs/#{job.id}")
      _(body).must_include "Could not complete that action"
    end

    it "applies bulk actions to selected jobs and to every matching job" do
      jobs = 3.times.map { job_for(FailingJob.perform_later) }
      3.times { @performer.perform_one }
      get("/")
      status, _, _ = post("/jobs/bulk", params: {"action" => "retry", "ids[]" => jobs.first.id})
      _(status).must_equal 303
      _(jobs.first.reload.status).must_equal :queued
      _(jobs.last.reload.status).must_equal :discarded

      status, _, _ = post("/jobs/bulk", params: {"action" => "retry", "all" => "1", "status[]" => "discarded"})
      _(status).must_equal 303
      _(jobs.map { |job| job.reload.status }.uniq).must_equal [:queued]

      status, _, body = post("/jobs/bulk", params: {"action" => "explode", "all" => "1"})
      _(status).must_equal 400
      _(body).must_include "Unknown bulk action"
    end

    it "redirects only inside the mount point" do
      enqueued = FailingJob.perform_later
      @performer.perform_one
      job = job_for(enqueued)
      get("/", script_name: "/admin")
      _, headers, _ = post("/jobs/#{job.id}/retry", script_name: "/admin", headers: {"HTTP_REFERER" => "http://evil.example/elsewhere"})
      _(headers["location"]).must_equal "/admin/jobs/#{job.id}"
      # A second retry is ineligible; the redirect must ignore the foreign referer and stay inside the mount.
      _, headers, _ = post("/jobs/#{job.id}/retry", script_name: "/admin", headers: {"HTTP_REFERER" => "http://evil.example/elsewhere"})
      _(headers["location"]).must_equal "/admin/"
    end
  end

  describe "batches" do
    it "lists batches, shows members, and retries discarded members" do
      batch = DecentJob::Batch.enqueue([FailingJob.new, RecordingJob.new("ok")], database: database, description: "Nightly export")
      2.times { @performer.perform_one }
      _, _, body = get("/batches")
      _(body).must_include "Nightly export"
      _, _, body = get("/batches/#{batch.id}")
      _(body).must_include "Nightly export"
      _(body).must_include "FailingJob"
      _(body).must_include "Retry discarded jobs"
      get("/")
      status, _, _ = post("/batches/#{batch.id}/retry")
      _(status).must_equal 303
      _, _, body = get("/batches/#{batch.id}")
      _(body).must_include "Retrying discarded jobs"
      _(body).wont_include "Retry discarded jobs"
      status, _, _ = get("/batches/#{SecureRandom.uuid}")
      _(status).must_equal 404
    end
  end

  describe "schedules" do
    before do
      configuration = DecentJob::Configuration.new
      configuration.cron = {nightly: {class: "RecordingJob", cron: "0 2 * * *", args: ["nightly"], description: "Nightly report"}}
      @app = DecentJob::Dashboard.new(database: database, configuration: configuration)
    end

    it "lists schedules and lets operators run, disable, and enable them" do
      _, _, body = get("/schedules")
      _(body).must_include "nightly"
      _(body).must_include "Nightly report"
      _(body).must_include "0 2 * * *"
      get("/")
      status, _, _ = post("/schedules/nightly/run")
      _(status).must_equal 303
      _, _, body = get("/schedules/nightly")
      _(body).must_include "Ran schedule nightly"
      _(body).must_include "RecordingJob"
      post("/schedules/nightly/disable")
      _, _, body = get("/schedules")
      _(body).must_include "<td>No</td>"
      post("/schedules/nightly/enable")
      _, _, body = get("/schedules")
      _(body).must_include "<td>Yes</td>"
      status, _, _ = get("/schedules/missing")
      _(status).must_equal 404
    end
  end

  describe "pauses, processes, and cleaner" do
    it "pauses and resumes queues" do
      get("/")
      status, _, _ = post("/pauses", params: {"kind" => "queue", "key" => "mailers", "action" => "pause"})
      _(status).must_equal 303
      _(DecentJob::Pauses.new(database).paused?(queue: "mailers")).must_equal true
      _, _, body = get("/pauses")
      _(body).must_include "mailers"
      _(body).must_include "Resume"
      post("/pauses", params: {"kind" => "queue", "key" => "mailers", "action" => "unpause"})
      _(DecentJob::Pauses.new(database).paused?(queue: "mailers")).must_equal false
      status, _, _ = post("/pauses", params: {"kind" => "nope", "key" => ""})
      _(status).must_equal 400
    end

    it "shows registered worker processes" do
      _, _, body = get("/processes")
      _(body).must_include "No worker has registered"

      database.with_connection do |connection|
        connection.exec_params("INSERT INTO decent_job_processes (id, state, heartbeat_mode) VALUES ($1, $2, 'heartbeat')",
          [SecureRandom.uuid, JSON.generate(hostname: "worker-1", pid: 4242, proctitle: "decent_job", schedulers: [{queues: "*", max_threads: 5, active_threads: 2}])])
      end

      _, _, body = get("/processes")
      _(body).must_include "worker-1"
      _(body).must_include "pid 4242"
      _(body).must_include "2 of 5 busy"
    end

    it "groups discarded jobs by class and error and deletes a group" do
      2.times { FailingJob.perform_later }
      2.times { @performer.perform_one }
      _, _, body = get("/cleaner")
      _(body).must_include "FailingJob"
      _(body).must_include "RuntimeError"
      _(body).must_include "Delete 2"
      get("/")
      status, _, _ = post("/cleaner", params: {"job_class" => "FailingJob", "error_class" => "RuntimeError"})
      _(status).must_equal 303
      _, _, body = get("/cleaner")
      _(body).must_include "Deleted 2 discarded jobs"
      _(body).must_include "No discarded job is waiting"
    end
  end

  describe "performance" do
    it "summarizes latency and rejects malformed ranges" do
      RecordingJob.perform_later("a")
      @performer.perform_one
      _, _, body = get("/performance")
      _(body).must_include "RecordingJob"
      _(body).must_include "1 attempts across 1 jobs"
      _, _, body = get("/performance", params: {"metric" => "queue", "range" => "7d", "job_class" => "RecordingJob"})
      _(body).must_include "Queue wait distribution"
      status, _, body = get("/performance", params: {"from" => "yesterday"})
      _(status).must_equal 400
      _(body).must_include "ISO 8601"
    end
  end

  describe "theme" do
    it "stores the theme choice in a cookie" do
      get("/")
      _, headers, _ = post("/theme", params: {"theme" => "dark"})
      _(headers["set-cookie"]).must_include "decent_job_theme=dark"
      _, _, body = get("/")
      _(body).must_include 'data-theme="dark"'
    end
  end
end
