# frozen_string_literal: true

require "json"
require "securerandom"
require "time"
require "decent_job/dashboard/request"
require "decent_job/dashboard/queries"
require "decent_job/dashboard/view"

module DecentJob
  # A Rack-compatible operations dashboard with no runtime dependencies.
  #
  #   run DecentJob::Dashboard.new                      # config.ru
  #   mount DecentJob::Dashboard.new => "/decent_job"   # Rails routes
  #
  # The host application owns authentication. Wrap the app in your own
  # constraint or middleware; the dashboard performs no login of its own.
  #
  # Collaborators: {Request} parses the Rack env, {Queries} runs every read,
  # and {View} renders ERB templates. Writes go through the public management
  # APIs ({Job}, {Batch}, {CronEntry}, {Pauses}), never through raw SQL here.
  # {Server} hosts the app for the standalone CLI command.
  #
  # Ownership: one instance serves many threads. Each request builds its own
  # Request, View, and Queries. Each query checks out a connection for that
  # query only, so the numbers on one page do not come from one snapshot.
  #
  # Routes (paths are relative to SCRIPT_NAME, the mount path):
  # - GET /assets/dashboard.css, /assets/dashboard.js: static files from an
  #   allowlist. Any other asset name returns 404.
  # - GET /, /metrics.json, /jobs, /jobs/:id, /batches, /batches/:id,
  #   /schedules, /schedules/:key, /processes, /pauses, /cleaner,
  #   /performance: read only. HEAD is handled as GET with an empty body.
  # - POST /theme: sets the theme cookie. No database write.
  # - POST /jobs/:id/(retry|reschedule|discard|force_discard|destroy):
  #   writes one job.
  # - POST /jobs/bulk: runs one job action on the selected ids, or on up to
  #   10000 jobs that match the filter when all=1.
  # - POST /batches/:id/retry: retries the discarded jobs of a batch.
  # - POST /schedules/:key/(enable|disable|run): writes the enabled override
  #   for the cron entry, or enqueues the cron entry now.
  # - POST /pauses: pauses or resumes a queue, job class, or label.
  # - POST /cleaner: deletes up to 10000 discarded jobs with one job class and
  #   error class.
  # Other methods return 405. Unknown paths return 404. A successful POST
  # redirects with 303. Every POST except /theme sets a flash notice.
  #
  # Security:
  # - CSRF: a double-submit token. A random 64-hex-character value lives in
  #   the HttpOnly, SameSite=Lax decent_job_csrf cookie, and each form repeats
  #   it as authenticity_token. Every POST compares the two in constant time
  #   before routing. A mismatch returns 400.
  # - Escaping: {View#h} HTML-escapes data in templates. Responses set a
  #   content security policy that blocks inline scripts.
  # - Redirects: only paths under the mount path are used. A foreign Referer
  #   falls back to the dashboard root.
  #
  # See docs/dashboard.md.
  class Dashboard
    ASSETS = File.expand_path("dashboard/assets", __dir__)
    ASSET_TYPES = {"dashboard.css" => "text/css", "dashboard.js" => "text/javascript"}.freeze
    CSRF_COOKIE = "decent_job_csrf"
    FLASH_COOKIE = "decent_job_flash"
    THEME_COOKIE = "decent_job_theme"
    JOB_ACTIONS = %w[retry reschedule discard force_discard destroy].freeze
    UUID = /\A\h{8}-\h{4}-\h{4}-\h{4}-\h{12}\z/

    # Base class for errors that {#call} turns into an error page. The
    # subclass picks the HTTP status and the message.
    class HttpError < StandardError
      # @return [Integer] the HTTP status of the error page
      def status
        500
      end

      # @param request [Request] the request that failed
      # @return [String] the text for the error page
      def page_message(request)
        message
      end
    end

    # Raised by routes and pages. {#call} turns it into a 404 page.
    class NotFound < HttpError
      def status
        404
      end

      def page_message(request)
        "There is no page at #{request.path}."
      end
    end

    # Raised for invalid input or a bad CSRF token. {#call} turns it into a
    # 400 page that shows the message.
    class BadRequest < HttpError
      def status
        400
      end
    end

    # Raised when a form body is over {Request::MAX_BODY_BYTES}. {#call}
    # turns it into a 413 page.
    class PayloadTooLarge < BadRequest
      def status
        413
      end
    end

    # @param database [Database, nil] the pool to read and write. nil uses
    #   {DecentJob.database} on each request.
    # @param configuration [Configuration, nil] the source of cron entries.
    #   nil uses {DecentJob.configuration} on each request.
    # @param title [String] page title
    def initialize(database: nil, configuration: nil, title: "DecentJob")
      @explicit_database = database
      @configuration = configuration
      @title = title
    end

    # @return [Database] the explicit pool, or the global pool
    def database
      @explicit_database || DecentJob.database
    end

    # @return [Configuration] the explicit configuration, or the global one
    def configuration
      @configuration || DecentJob.configuration
    end

    # Handles one Rack request.
    #
    # @param env [Hash{String => Object}] the Rack environment
    # @return [Array(Integer, Hash{String => String}, Array<String>)] the Rack
    #   response. Multiple cookies are joined with "\n" in "set-cookie".
    # @note POST requests write to the database through the management APIs.
    #   Routed responses set the CSRF cookie again. Asset, 404, and 400
    #   responses do not. A HEAD request gets the GET headers and an empty
    #   body, as Rack requires.
    def call(env)
      status, headers, body = respond(env)
      (env["REQUEST_METHOD"].to_s.upcase == "HEAD") ? [status, headers, []] : [status, headers, body]
    end

    private

    def respond(env)
      request = Request.new(env)
      return asset(request) if request.path.start_with?("/assets/")

      csrf = request.cookies[CSRF_COOKIE]
      csrf = SecureRandom.hex(32) unless csrf.is_a?(String) && csrf.match?(/\A\h{64}\z/)
      flash = request.cookies[FLASH_COOKIE]
      theme = request.cookies[THEME_COOKIE]
      theme = nil unless %w[light dark].include?(theme)
      view = View.new(request: request, root: request.script_name, csrf_token: csrf, flash: flash, theme: theme)
      queries = Queries.new(database)
      status, headers, body = route(request, view, queries)
      headers = default_headers(request).merge(headers)
      cookies = [cookie(CSRF_COOKIE, csrf, request)]
      cookies << cookie(FLASH_COOKIE, "", request, expire: true) if flash
      cookies << headers.delete("set-cookie-extra") if headers["set-cookie-extra"]
      headers["set-cookie"] = cookies.compact.join("\n")
      [status, headers, [body]]
    rescue HttpError => error
      message = error.page_message(request)
      [error.status, default_headers(request).merge("content-type" => "text/html; charset=utf-8"), [view ? view.layout("error", status: error.status, message: message) : message]]
    end

    # ----- routing ----------------------------------------------------------

    def route(request, view, queries)
      segments = request.path.split("/").reject(&:empty?)

      if request.get?
        get(segments, request, view, queries)
      elsif request.post?
        verify_csrf!(request, view)
        post(segments, request, view, queries)
      else
        [405, {"content-type" => "text/plain", "allow" => "GET, HEAD, POST"}, "Method not allowed"]
      end
    end

    def get(segments, request, view, queries)
      case segments
      in [] then html(view.layout("overview", overview(queries)))
      in ["metrics.json"] then json(metrics(queries))
      in ["jobs"] then html(view.layout("jobs", jobs_page(request, queries)))
      in ["jobs", id] then html(view.layout("job", job_page(id, queries)))
      in ["batches"] then html(view.layout("batches", batches_page(request, queries)))
      in ["batches", id] then html(view.layout("batch", batch_page(id, queries)))
      in ["schedules"] then html(view.layout("schedules", schedules: queries.schedules(configuration)))
      in ["schedules", key] then html(view.layout("schedule", schedule_page(key, queries)))
      in ["processes"] then html(view.layout("processes", processes: queries.processes, running: queries.running_by_process))
      in ["pauses"] then html(view.layout("pauses", paused: Pauses.new(database).paused, options: queries.filter_options))
      in ["cleaner"] then html(view.layout("cleaner", groups: queries.discard_groups, counts: queries.cleaner_counts))
      in ["performance"] then html(view.layout("performance", performance_page(request, queries)))
      else raise NotFound
      end
    end

    # Management errors (stale state, lock held, unknown record) become a
    # redirect with a notice, not an error page.
    def post(segments, request, view, queries)
      form = request.form

      case segments
      in ["theme"]
        theme = %w[light dark].include?(form["theme"]) ? form["theme"] : ""
        redirect(view, back(request), extra_cookie: cookie(THEME_COOKIE, theme, request, expire: theme.empty?, http_only: false))
      in ["jobs", "bulk"] then bulk_jobs(request, view, queries)
      in ["jobs", id, action] if JOB_ACTIONS.include?(action)
        job = load_job(id)
        perform_job_action(job, action, form)
        redirect(view, view.url("/jobs/#{id}"), notice: "#{verb(action)} job #{view.short_id(id)}")
      in ["batches", id, "retry"]
        batch = Batch.find(id, database: database)
        batch.retry
        redirect(view, view.url("/batches/#{id}"), notice: "Retrying discarded jobs in batch #{view.short_id(id)}")
      in ["schedules", key, action] if %w[enable disable run].include?(action)
        entry = CronEntry.find(key, configuration: configuration)
        entry = CronEntry.new(entry.params, database: database)

        case action
        when "enable" then entry.enable
        when "disable" then entry.disable
        when "run" then entry.enqueue
        end

        redirect(view, view.url("/schedules/#{key}"), notice: "#{verb(action)} schedule #{key}")
      in ["pauses"]
        kind = {"queue" => :queue, "job_class" => :job_class, "label" => :label}[form["kind"]]
        key = form["key"].to_s.strip
        raise BadRequest, "Choose a queue, job class, or label, and give its name." if kind.nil? || key.empty?

        pauses = Pauses.new(database)
        (form["action"] == "unpause") ? pauses.unpause(kind => key) : pauses.pause(kind => key)
        redirect(view, view.url("/pauses"), notice: "#{(form["action"] == "unpause") ? "Resumed" : "Paused"} #{form["kind"].tr("_", " ")} #{key}")
      in ["cleaner"]
        deleted = destroy_discarded(form["job_class"].to_s, form["error_class"].to_s)
        redirect(view, view.url("/cleaner"), notice: "Deleted #{deleted} discarded jobs")
      else raise NotFound
      end
    rescue Job::StateMismatchError, Job::LockedError, Job::DeserializationError, Job::AdapterMismatchError, KeyError, ArgumentError => error
      redirect(view, back(request), notice: "Could not complete that action: #{error.message}")
    end

    # ----- pages ------------------------------------------------------------

    def overview(queries)
      {counts: queries.status_counts, queues: queries.queue_counts, upcoming: queries.upcoming_by_queue,
       discards: queries.recent_discards, processes: queries.processes, paused: Pauses.new(database).paused,
       throughput: queries.throughput, schedules_count: configuration.cron_entries(database: database).size}
    end

    def metrics(queries)
      {"generated_at" => Time.now.utc.iso8601(3), "counts" => queries.status_counts, "queues" => queries.queue_counts.map { |row| row.merge("next_at" => row["next_at"]&.utc&.iso8601) },
       "upcoming" => queries.upcoming_by_queue, "processes" => queries.processes.count { |row| row["active"] }}
    end

    def jobs_page(request, queries)
      filter = request.query.slice("status", "queue", "job_class", "label", "cron_key", "batch_id", "q", "finished_after", "finished_before")
      filter["status"] = Array(filter["status"]) if filter["status"]
      page = queries.jobs(filter, cursor: cursor(request.query["after"]), limit: Integer(request.query["limit"] || Queries::PAGE_SIZE, exception: false) || Queries::PAGE_SIZE)
      {filter: filter, page: page, options: queries.filter_options, counts: queries.status_counts, total: queries.job_count(filter)}
    end

    def job_page(id, queries)
      raise NotFound unless id.match?(UUID)

      job = queries.job(id) || raise(NotFound)
      {job: job, executions: queries.executions(id), claims: queries.claims(id),
       batch: job["batch_id"] && queries.batch(job["batch_id"])}
    end

    def batches_page(request, queries)
      {page: queries.batches(cursor: cursor(request.query["after"]))}
    end

    def batch_page(id, queries)
      raise NotFound unless id.match?(UUID)

      batch = queries.batch(id) || raise(NotFound)
      {batch: batch, jobs: queries.batch_jobs(id), callbacks: queries.batch_jobs(id, callbacks: true)}
    end

    def schedule_page(key, queries)
      item = queries.schedule(key, configuration) || raise(NotFound)
      item.merge(jobs: queries.schedule_jobs(key))
    end

    def performance_page(request, queries)
      to = parse_time(request.query["to"]) || Time.now.utc
      from = parse_time(request.query["from"]) || (to - range_seconds(request.query["range"]))
      raise BadRequest, "The start of the range must come before its end." unless from < to

      job_class = request.query["job_class"].to_s.strip
      job_class = nil if job_class.empty?
      metric = %w[execution queue total].include?(request.query["metric"]) ? request.query["metric"] : "execution"
      data = queries.performance(from: from, to: to, job_class: job_class)
      data.merge(from: from, to: to, job_class: job_class, metric: metric, range: request.query["range"] || "24h",
        histogram: queries.latency_histogram(from: from, to: to, metric: metric, job_class: job_class))
    end

    # ----- actions ----------------------------------------------------------

    # Counts per-job failures as skipped and keeps going, so one locked or
    # changed job does not stop the batch.
    def bulk_jobs(request, view, queries)
      form = request.form
      action = form["action"].to_s
      raise BadRequest, "Unknown bulk action #{action.inspect}." unless JOB_ACTIONS.include?(action)

      ids = if form["all"] == "1"
        filter = form.slice("status", "queue", "job_class", "label", "cron_key", "batch_id", "q", "finished_after", "finished_before")
        filter["status"] = Array(filter["status"]) if filter["status"]
        queries.job_ids(filter)
      else
        Array(form["ids"]).select { |id| id.to_s.match?(UUID) }
      end

      done = 0
      failed = 0

      ids.each do |id|
        perform_job_action(load_job(id), action, form)
        done += 1
      rescue Job::StateMismatchError, Job::LockedError, Job::DeserializationError, Job::AdapterMismatchError, KeyError
        failed += 1
      end

      summary = "#{verb(action)} #{done} jobs"
      summary += ", #{failed} skipped" if failed.positive?
      redirect(view, back(request, view.url("/jobs")), notice: summary)
    end

    def perform_job_action(job, action, form)
      case action
      when "retry" then job.retry_job
      when "reschedule" then job.reschedule_job(parse_time(form["at"]) || Time.now)
      when "discard" then job.discard_job(form["reason"].to_s.strip.then { |reason| reason.empty? ? "Discarded from the dashboard" : reason })
      when "force_discard" then job.force_discard_job(form["reason"].to_s.strip.then { |reason| reason.empty? ? "Force discarded from the dashboard" : reason })
      when "destroy" then job.destroy_job
      end
    end

    # Selects ids first, then deletes each job through Job#destroy_job, so the
    # normal locking and history cleanup apply. Returns the number deleted.
    def destroy_discarded(job_class, error_class)
      ids = database.with_connection do |connection|
        connection.exec_params(<<~SQL, [job_class, error_class]).map { |row| row.fetch("id") }
          SELECT id FROM decent_jobs WHERE state = 'discarded' AND job_class = $1 AND COALESCE(error->>'class', '') = $2 ORDER BY id LIMIT 10000
        SQL
      end

      ids.count do |id|
        load_job(id).destroy_job
        true
      rescue Job::StateMismatchError, Job::LockedError, KeyError
        false
      end
    end

    def load_job(id)
      raise NotFound unless id.to_s.match?(UUID)

      Job.find(id, database: database)
    rescue KeyError
      raise NotFound
    end

    def verb(action)
      {"retry" => "Retried", "reschedule" => "Rescheduled", "discard" => "Discarded", "force_discard" => "Force discarded",
       "destroy" => "Deleted", "enable" => "Enabled", "disable" => "Disabled", "run" => "Ran"}.fetch(action)
    end

    # ----- responses --------------------------------------------------------

    def html(body, status: 200)
      [status, {"content-type" => "text/html; charset=utf-8"}, body]
    end

    def json(value)
      [200, {"content-type" => "application/json; charset=utf-8", "cache-control" => "no-store"}, JSON.generate(value)]
    end

    # "set-cookie-extra" is an internal header. {#call} moves it into
    # "set-cookie" next to the CSRF cookie.
    def redirect(view, location, notice: nil, extra_cookie: nil)
      headers = {"location" => safe_location(location, view)}
      headers["set-cookie-extra"] = cookie(FLASH_COOKIE, notice, view.request) if notice
      headers["set-cookie-extra"] = [headers["set-cookie-extra"], extra_cookie].compact.join("\n") if extra_cookie
      [303, headers, ""]
    end

    # Only paths under this mount are trusted redirect targets.
    def safe_location(location, view)
      root = view.root.empty? ? "/" : view.root
      (location.is_a?(String) && location.start_with?(root) && !location.start_with?("//")) ? location : view.url("/")
    end

    def back(request, fallback = nil)
      referer = request.env["HTTP_REFERER"].to_s

      path = begin
        uri = URI.parse(referer)
        [uri.path, uri.query].compact.join("?") if uri.path
      rescue URI::InvalidURIError
        nil
      end

      mount = request.script_name.empty? ? "/" : request.script_name
      return path if path&.start_with?(mount)

      fallback || "#{request.script_name}/"
    end

    def asset(request)
      name = request.path.delete_prefix("/assets/")
      type = ASSET_TYPES[name] || raise(NotFound)
      body = File.binread(File.join(ASSETS, name))
      [200, {"content-type" => "#{type}; charset=utf-8", "cache-control" => "public, max-age=3600"}, [body]]
    end

    def default_headers(request)
      {"x-content-type-options" => "nosniff", "x-frame-options" => "SAMEORIGIN", "referrer-policy" => "same-origin",
       "content-security-policy" => "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'self'"}
    end

    def cookie(name, value, request, expire: false, http_only: true)
      path = request.script_name.empty? ? "/" : request.script_name
      parts = ["#{name}=#{URI.encode_www_form_component(value.to_s)}", "Path=#{path}", "SameSite=Lax"]
      parts << "HttpOnly" if http_only
      parts << "Secure" if request.env["rack.url_scheme"] == "https" || request.env["HTTP_X_FORWARDED_PROTO"] == "https"
      parts << "Max-Age=0" if expire
      parts.join("; ")
    end

    # Compares the form token with the cookie token. The cookie is the source
    # of truth, so a missing cookie always fails.
    def verify_csrf!(request, view)
      token = request.form["authenticity_token"].to_s
      expected = request.cookies[CSRF_COOKIE].to_s
      return if !expected.empty? && token.bytesize == expected.bytesize && secure_compare(token, expected)

      raise BadRequest, "The form token did not match. Reload the page and try again."
    end

    # Constant-time comparison. The caller checks that the lengths match.
    def secure_compare(left, right)
      result = 0
      left.bytes.zip(right.bytes) { |a, b| result |= a ^ b }
      result.zero?
    end

    # Parses a "created_at|id" keyset cursor. Returns nil for any invalid
    # value, which shows the first page.
    def cursor(value)
      return unless value.is_a?(String)

      created_at, id = value.split("|", 2)
      return unless id&.match?(UUID)

      Time.iso8601(created_at)
      [created_at, id]
    rescue ArgumentError
      nil
    end

    # Accepts ISO 8601, or the "YYYY-MM-DDTHH:MM" value of a datetime-local
    # input. Returns nil for a blank value.
    def parse_time(value)
      return unless value.is_a?(String) && !value.strip.empty?

      Time.iso8601(value.strip)
    rescue ArgumentError
      begin
        Time.strptime(value.strip, "%Y-%m-%dT%H:%M")
      rescue ArgumentError
        raise BadRequest, "Enter a time as an ISO 8601 value, such as 2026-10-09T14:30:00Z."
      end
    end

    def range_seconds(range)
      {"1h" => 3600, "6h" => 6 * 3600, "24h" => 86_400, "7d" => 7 * 86_400, "30d" => 30 * 86_400}.fetch(range.to_s, 86_400)
    end
  end
end
