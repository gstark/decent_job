# frozen_string_literal: true

require "json"
require "time"

module DecentJob
  class Dashboard
    # Read-only SQL for dashboard pages. Every list is bounded and uses keyset
    # pagination on (created_at, id) so ties never skip or repeat rows.
    #
    # {Dashboard} builds one per request. Each method checks out a connection
    # from the {Database} pool for one statement and returns it. There is no
    # shared transaction, so two methods can see different data. Writes do not
    # happen here; {Dashboard} uses the management APIs for them.
    #
    # The dashboard "status" is derived by {STATUS_SQL} and differs from the
    # stored decent_jobs.state column ('ready', 'running', 'succeeded',
    # 'discarded'). Unfinished rows map to queued, scheduled, retried, or
    # running. Finished rows keep their stored state.
    #
    # Job rows are decoded with {Store#decode}, so time columns are Time
    # objects and JSON columns are parsed.
    #
    # See docs/dashboard.md.
    class Queries
      # Every derived status, in display order.
      STATUSES = %w[queued scheduled retried running succeeded discarded].freeze
      # Default rows per list page.
      PAGE_SIZE = 50
      # Largest page size a request can ask for.
      MAX_PAGE_SIZE = 200

      # An unfinished job is running when a process recorded itself in
      # locked_by_id, or when a session holds the job's advisory lock.
      RUNNING_SQL = "(decent_jobs.finished_at IS NULL AND (decent_jobs.locked_by_id IS NOT NULL OR (#{ConcurrencyClaims::JOB_LOCKED_SQL})))"
      STATUS_SQL = <<~SQL
        CASE
          WHEN decent_jobs.finished_at IS NOT NULL THEN decent_jobs.state
          WHEN #{RUNNING_SQL} THEN 'running'
          WHEN decent_jobs.scheduled_at <= clock_timestamp() THEN 'queued'
          WHEN decent_jobs.error IS NOT NULL THEN 'retried'
          ELSE 'scheduled'
        END
      SQL

      # One page of a keyset list. next_cursor is [created_at ISO 8601, id],
      # or nil on the last page.
      Page = Struct.new(:rows, :next_cursor)

      # @param database [Database] the pool to read from
      def initialize(database)
        @database = database
        @store = Store.new(database)
      end

      # ----- overview -------------------------------------------------------

      # @return [Hash{String => Integer}] job count per derived status. Every
      #   status in {STATUSES} is present, with 0 when there are none.
      def status_counts
        counts = STATUSES.to_h { |status| [status, 0] }

        query("SELECT (#{STATUS_SQL}) AS status, count(*) AS count FROM decent_jobs GROUP BY 1").each do |row|
          counts[row.fetch("status")] = row.fetch("count").to_i
        end

        counts
      end

      # @return [Array<Hash{String => Object}>] one row per queue with queued,
      #   scheduled (includes retried), running, and discarded counts, and
      #   next_at, the earliest unfinished scheduled_at (nil when none)
      def queue_counts
        query(<<~SQL).map { |row| decode_counts(row) }
          SELECT queue_name,
            count(*) FILTER (WHERE (#{STATUS_SQL}) = 'queued') AS queued,
            count(*) FILTER (WHERE (#{STATUS_SQL}) IN ('scheduled', 'retried')) AS scheduled,
            count(*) FILTER (WHERE (#{STATUS_SQL}) = 'running') AS running,
            count(*) FILTER (WHERE decent_jobs.state = 'discarded') AS discarded,
            min(scheduled_at) FILTER (WHERE finished_at IS NULL) AS next_at
          FROM decent_jobs GROUP BY queue_name ORDER BY queue_name
        SQL
      end

      # Jobs due within the next hour, bucketed by minute and queue, for the ribbon's timeline.
      #
      # @param minutes [Integer] size of the window
      # @return [Array<Hash{String => Object}>] queue_name, minute (0 for jobs
      #   already due), and count
      def upcoming_by_queue(minutes: 60)
        query(<<~SQL, [minutes]).map { |row| {"queue_name" => row.fetch("queue_name"), "minute" => row.fetch("minute").to_i, "count" => row.fetch("count").to_i} }
          SELECT queue_name,
            GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (scheduled_at - clock_timestamp())) / 60))::int AS minute,
            count(*) AS count
          FROM decent_jobs
          WHERE finished_at IS NULL AND scheduled_at < clock_timestamp() + make_interval(mins => $1)
          GROUP BY queue_name, minute ORDER BY queue_name, minute
        SQL
      end

      # @param limit [Integer] number of rows
      # @return [Array<Hash{String => Object}>] the most recently finished
      #   discarded jobs
      def recent_discards(limit: 8)
        query("SELECT * FROM decent_jobs WHERE state = 'discarded' ORDER BY finished_at DESC, id DESC LIMIT $1", [limit]).map { |row| @store.decode(row) }
      end

      # Finished attempts per hour, from decent_job_executions.
      #
      # @param hours [Integer] number of hours, including the current hour
      # @return [Array<Hash{String => Object}>] hour (Time), succeeded, and
      #   errored (discarded or retried attempts). Hours with no attempts are
      #   missing.
      def throughput(hours: 24)
        query(<<~SQL, [hours]).map { |row| {"hour" => Time.parse(row.fetch("hour")), "succeeded" => row.fetch("succeeded").to_i, "errored" => row.fetch("errored").to_i} }
          SELECT date_trunc('hour', finished_at) AS hour,
            count(*) FILTER (WHERE state = 'succeeded') AS succeeded,
            count(*) FILTER (WHERE state IN ('discarded', 'retried')) AS errored
          FROM decent_job_executions
          WHERE finished_at >= date_trunc('hour', clock_timestamp()) - make_interval(hours => $1 - 1)
          GROUP BY 1 ORDER BY 1
        SQL
      end

      # ----- jobs -----------------------------------------------------------

      # @return [Hash{String => Array<String>}] distinct job_classes, queues,
      #   labels, and cron_keys for the filter menus
      # @note Scans decent_jobs four times. Cost grows with table size.
      def filter_options
        query(<<~SQL).first.to_h { |key, value| [key, value ? PG::TextDecoder::Array.new.decode(value) : []] }
          SELECT
            (SELECT array_agg(DISTINCT job_class ORDER BY job_class) FROM decent_jobs) AS job_classes,
            (SELECT array_agg(DISTINCT queue_name ORDER BY queue_name) FROM decent_jobs) AS queues,
            (SELECT array_agg(DISTINCT label ORDER BY label) FROM decent_jobs, unnest(labels) AS label) AS labels,
            (SELECT array_agg(DISTINCT cron_key ORDER BY cron_key) FROM decent_jobs WHERE cron_key IS NOT NULL) AS cron_keys
        SQL
      end

      # One page of jobs, newest first.
      #
      # @param filter [Hash{String => Object}] keys status (Array), queue,
      #   job_class, label, cron_key, batch_id, q (search text or a UUID),
      #   finished_after, finished_before. Invalid values are ignored.
      # @param cursor [Array(String, String), nil] [created_at, id] of the last
      #   row on the previous page. nil starts at the newest job.
      # @param limit [Integer] clamped to 1..{MAX_PAGE_SIZE}
      # @return [Page] decoded rows, each with a "status" key
      def jobs(filter, cursor: nil, limit: PAGE_SIZE)
        limit = limit.clamp(1, MAX_PAGE_SIZE)
        where, params = job_conditions(filter)

        if cursor
          created_at, id = cursor
          params.push(created_at, id)
          where << "(decent_jobs.created_at, decent_jobs.id) < ($#{params.size - 1}::timestamptz, $#{params.size}::uuid)"
        end

        params << limit + 1
        rows = query(<<~SQL, params).map { |row| @store.decode(row).merge("status" => row.fetch("status")) }
          SELECT decent_jobs.*, (#{STATUS_SQL}) AS status FROM decent_jobs
          #{"WHERE #{where.join(" AND ")}" unless where.empty?}
          ORDER BY decent_jobs.created_at DESC, decent_jobs.id DESC LIMIT $#{params.size}
        SQL
        page(rows, limit)
      end

      # @param filter [Hash{String => Object}] same keys as {#jobs}
      # @return [Array<String>] ids of at most 10000 matching jobs, newest first
      def job_ids(filter)
        where, params = job_conditions(filter)
        query("SELECT id FROM decent_jobs #{"WHERE #{where.join(" AND ")}" unless where.empty?} ORDER BY created_at DESC, id DESC LIMIT 10000", params).map { |row| row.fetch("id") }
      end

      # @param filter [Hash{String => Object}] same keys as {#jobs}
      # @return [Integer] number of matching jobs
      def job_count(filter)
        where, params = job_conditions(filter)
        query("SELECT count(*) AS count FROM decent_jobs #{"WHERE #{where.join(" AND ")}" unless where.empty?}", params).first.fetch("count").to_i
      end

      # @param id [String] job UUID
      # @return [Hash{String => Object}, nil] the decoded row with "status",
      #   or nil when no job has this id
      def job(id)
        row = query("SELECT decent_jobs.*, (#{STATUS_SQL}) AS status FROM decent_jobs WHERE id = $1", [id]).first
        return unless row
        @store.decode(row).merge("status" => row.fetch("status"))
      end

      # @param job_id [String] job UUID
      # @return [Array<Hash{String => Object}>] attempts, newest first
      def executions(job_id)
        @store.executions(job_id).reverse
      end

      # @param job_id [String] job UUID
      # @return [Array<Hash{String => Object}>] the job's concurrency claims
      #   (scope, state, created_at, updated_at), oldest first
      def claims(job_id)
        query("SELECT scope, state, created_at, updated_at FROM decent_job_concurrency_claims WHERE job_id = $1 ORDER BY created_at", [job_id]).map do |row|
          row.to_h.merge("created_at" => Time.parse(row.fetch("created_at")), "updated_at" => Time.parse(row.fetch("updated_at")))
        end
      end

      # ----- batches --------------------------------------------------------

      # One page of batches, newest first, with job counts.
      #
      # @param cursor [Array(String, String), nil] see {#jobs}
      # @param limit [Integer] clamped to 1..{MAX_PAGE_SIZE}
      # @return [Page] rows with total, succeeded, discarded, and pending
      # @note Counts the jobs of each batch on the page with a lateral join.
      def batches(cursor: nil, limit: PAGE_SIZE)
        limit = limit.clamp(1, MAX_PAGE_SIZE)
        params = []
        where = +""

        if cursor
          params.push(*cursor)
          where = "WHERE (b.created_at, b.id) < ($1::timestamptz, $2::uuid)"
        end

        params << limit + 1
        rows = query(<<~SQL, params).map { |row| decode_batch(row) }
          SELECT b.*, counts.* FROM decent_job_batches b
          CROSS JOIN LATERAL (
            SELECT count(*) AS total,
              count(*) FILTER (WHERE state = 'succeeded') AS succeeded,
              count(*) FILTER (WHERE state = 'discarded') AS discarded,
              count(*) FILTER (WHERE finished_at IS NULL) AS pending
            FROM decent_jobs WHERE batch_id = b.id
          ) counts
          #{where}
          ORDER BY b.created_at DESC, b.id DESC LIMIT $#{params.size}
        SQL
        page(rows, limit)
      end

      # @param id [String] batch UUID
      # @return [Hash{String => Object}, nil] the decoded batch row, or nil
      def batch(id)
        row = query("SELECT * FROM decent_job_batches WHERE id = $1", [id]).first
        decode_batch(row) if row
      end

      # @param id [String] batch UUID
      # @param callbacks [Boolean] true for the batch's callback jobs, false
      #   for its member jobs
      # @return [Array<Hash{String => Object}>] at most 500 jobs, oldest first
      def batch_jobs(id, callbacks: false)
        column = callbacks ? "batch_callback_id" : "batch_id"

        query("SELECT decent_jobs.*, (#{STATUS_SQL}) AS status FROM decent_jobs WHERE #{column} = $1 ORDER BY created_at, id LIMIT 500", [id]).map do |row|
          @store.decode(row).merge("status" => row.fetch("status"))
        end
      end

      # ----- schedules ------------------------------------------------------

      # @param configuration [Configuration] the source of cron entries
      # @return [Array<Hash{Symbol => Object}>] entry, enabled, last_job,
      #   next_at, and properties for each cron entry. next_at is the
      #   exception object when the entry cannot compute its next time.
      def schedules(configuration = DecentJob.configuration)
        entries = configuration.cron_entries(database: @database)
        last = CronEntry.last_jobs_by_key(entries)

        entries.map do |entry|
          last_job = last[entry.key.to_s]
          {entry: entry, enabled: entry.enabled?, last_job: last_job,
           next_at: safe_next_at(entry, last_job), properties: entry.display_properties}
        end
      end

      # @param key [String] cron entry key
      # @param configuration [Configuration]
      # @return [Hash{Symbol => Object}, nil] see {#schedules}; nil when no
      #   entry has this key
      def schedule(key, configuration = DecentJob.configuration)
        schedules(configuration).find { |item| item.fetch(:entry).key.to_s == key }
      end

      # @param key [String] cron entry key
      # @param limit [Integer] number of rows
      # @return [Array<Hash{String => Object}>] jobs of the entry, newest
      #   occurrence first; manual runs without cron_at come last
      def schedule_jobs(key, limit: PAGE_SIZE)
        query("SELECT decent_jobs.*, (#{STATUS_SQL}) AS status FROM decent_jobs WHERE cron_key = $1 ORDER BY cron_at DESC NULLS LAST, created_at DESC, id DESC LIMIT $2", [key, limit]).map do |row|
          @store.decode(row).merge("status" => row.fetch("status"))
        end
      end

      # ----- processes ------------------------------------------------------

      # @return [Array<Hash{String => Object}>] every decent_job_processes row,
      #   with parsed state and active from {ProcessRecord::ACTIVE_SQL}
      def processes
        query("SELECT *, (#{ProcessRecord::ACTIVE_SQL}) AS active FROM decent_job_processes ORDER BY created_at, id").map do |row|
          {"id" => row.fetch("id"), "state" => JSON.parse(row.fetch("state")), "heartbeat_mode" => row.fetch("heartbeat_mode"),
           "created_at" => Time.parse(row.fetch("created_at")), "updated_at" => Time.parse(row.fetch("updated_at")),
           "active" => row.fetch("active") == "t"}
        end
      end

      # @return [Hash{String => Integer}] unfinished job count per locked_by_id.
      #   Advisory-only claims without locked_by_id are not counted.
      def running_by_process
        query("SELECT locked_by_id, count(*) AS count FROM decent_jobs WHERE locked_by_id IS NOT NULL AND finished_at IS NULL GROUP BY locked_by_id").to_h do |row|
          [row.fetch("locked_by_id"), row.fetch("count").to_i]
        end
      end

      # ----- cleaner --------------------------------------------------------

      # @return [Array<Hash{String => Object}>] up to 200 groups of discarded
      #   jobs by job_class and error class, largest first. error_class is ""
      #   when the job has no error class.
      def discard_groups
        query(<<~SQL).map { |row| row.to_h.merge("count" => row.fetch("count").to_i, "oldest" => Time.parse(row.fetch("oldest")), "newest" => Time.parse(row.fetch("newest"))) }
          SELECT job_class, COALESCE(error->>'class', '') AS error_class, count(*) AS count,
            min(finished_at) AS oldest, max(finished_at) AS newest
          FROM decent_jobs WHERE state = 'discarded'
          GROUP BY job_class, error_class ORDER BY count DESC, job_class LIMIT 200
        SQL
      end

      # @return [Hash{String => Object}] succeeded and discarded counts of
      #   finished jobs, and oldest finished_at (nil when none)
      def cleaner_counts
        row = query(<<~SQL).first
          SELECT count(*) FILTER (WHERE state = 'succeeded') AS succeeded,
            count(*) FILTER (WHERE state = 'discarded') AS discarded,
            min(finished_at) AS oldest
          FROM decent_jobs WHERE finished_at IS NOT NULL
        SQL
        {"succeeded" => row.fetch("succeeded").to_i, "discarded" => row.fetch("discarded").to_i,
         "oldest" => row["oldest"] && Time.parse(row.fetch("oldest"))}
      end

      # ----- performance ----------------------------------------------------

      # @param from [Time] start of the range, inclusive
      # @param to [Time] end of the range, exclusive
      # @param job_class [String, nil] limit to one class; nil for all
      # @return [Hash{Symbol => Object}] summary, classes, and queues from
      #   {ExecutionMetrics}
      def performance(from:, to:, job_class: nil)
        metrics = ExecutionMetrics.new(database: @database)
        {summary: metrics.summary(from: from, to: to, job_class: job_class),
         classes: metrics.grouped_by(:job_class, from: from, to: to, job_class: job_class),
         queues: metrics.grouped_by(:queue_name, from: from, to: to, job_class: job_class)}
      end

      # Log-scale latency histogram of finished attempts.
      #
      # Values are clamped to at least 0.5 ms so LOG never sees zero.
      #
      # @param from [Time] attempts scheduled at or after this time
      # @param to [Time] attempts scheduled before this time
      # @param metric [String] "execution" (duration), "queue" (started_at
      #   minus scheduled_at), or "total" (both)
      # @param job_class [String, nil] limit to one class; nil for all
      # @param buckets [Integer] number of buckets
      # @return [Array<Hash{String => Object}>] bucket, count, and upper bound
      #   in seconds. Empty buckets are missing.
      def latency_histogram(from:, to:, metric:, job_class: nil, buckets: 24)
        expression = case metric
        when "queue" then "EXTRACT(EPOCH FROM started_at - scheduled_at)"
        when "total" then "EXTRACT(EPOCH FROM started_at - scheduled_at) + duration"
        else "duration"
        end

        params = [from.iso8601(6), to.iso8601(6), buckets]
        class_filter = ""

        if job_class
          params << job_class
          class_filter = "AND job_class = $4"
        end

        query(<<~SQL, params).map { |row| {"bucket" => row.fetch("bucket").to_i, "count" => row.fetch("count").to_i, "upper" => row.fetch("upper").to_f} }
          WITH values AS (
            SELECT GREATEST(#{expression}, 0.0005) AS v FROM decent_job_executions
            WHERE scheduled_at >= $1 AND scheduled_at < $2 AND duration IS NOT NULL #{class_filter}
          ), bounds AS (
            SELECT LOG(MIN(v)) AS lo, LOG(GREATEST(MAX(v), MIN(v) * 1.0001)) AS hi FROM values
          )
          SELECT width_bucket(LOG(v), lo, hi, $3) AS bucket, count(*) AS count,
            POWER(10, lo + (hi - lo) * width_bucket(LOG(v), lo, hi, $3) / $3) AS upper
          FROM values, bounds GROUP BY bucket, lo, hi ORDER BY bucket
        SQL
      end

      # ----- helpers --------------------------------------------------------

      private

      # Builds WHERE fragments with numbered placeholders. Every value is a
      # bind parameter. Search text escapes LIKE wildcards and is cut to 200
      # characters.
      def job_conditions(filter)
        where = []
        params = []

        add = lambda do |sql, value|
          params << value
          where << sql.gsub("?", "$#{params.size}")
        end

        statuses = Array(filter["status"]).map(&:to_s) & STATUSES
        add.call("(#{STATUS_SQL}) = ANY(?::text[])", encode_array(statuses)) unless statuses.empty?
        add.call("decent_jobs.queue_name = ?", filter["queue"]) if present?(filter["queue"])
        add.call("decent_jobs.job_class = ?", filter["job_class"]) if present?(filter["job_class"])
        add.call("? = ANY(decent_jobs.labels)", filter["label"]) if present?(filter["label"])
        add.call("decent_jobs.cron_key = ?", filter["cron_key"]) if present?(filter["cron_key"])
        add.call("decent_jobs.batch_id = ?::uuid", filter["batch_id"]) if uuid?(filter["batch_id"])
        add.call("decent_jobs.finished_at >= ?::timestamptz", filter["finished_after"]) if time?(filter["finished_after"])
        add.call("decent_jobs.finished_at < ?::timestamptz", filter["finished_before"]) if time?(filter["finished_before"])

        if present?(filter["q"])
          term = filter["q"].to_s.strip[0, 200]

          if uuid?(term)
            add.call("(decent_jobs.id = ?::uuid OR decent_jobs.active_job_id = ?::uuid OR decent_jobs.batch_id = ?::uuid)", term)
          else
            pattern = "%#{term.gsub(/[\\%_]/) { |char| "\\#{char}" }}%"
            add.call(<<~SQL.strip, pattern)
              (decent_jobs.job_class ILIKE ? OR decent_jobs.queue_name ILIKE ? OR decent_jobs.payload::text ILIKE ?
                OR decent_jobs.error::text ILIKE ? OR array_to_string(decent_jobs.labels, ' ') ILIKE ?
                OR decent_jobs.cron_key ILIKE ?)
            SQL
          end
        end

        [where, params]
      end

      # The caller fetches limit + 1 rows. The extra row only shows that
      # another page exists.
      def page(rows, limit)
        more = rows.size > limit
        rows = rows.first(limit)
        cursor = more && rows.last && [rows.last.fetch("created_at").utc.iso8601(6), rows.last.fetch("id")]
        Page.new(rows: rows, next_cursor: cursor || nil)
      end

      # Returns the error instead of raising, so one bad cron entry does not
      # break the schedules page.
      def safe_next_at(entry, last_job)
        previously = last_job && (last_job["cron_at"] || last_job["created_at"])
        entry.next_at(previously_at: previously)
      rescue => error
        error
      end

      def decode_batch(row)
        row.to_h do |key, value|
          decoded = if value.nil? then nil
          elsif key == "properties" then JSON.parse(value)
          elsif key.end_with?("_at") then Time.parse(value)
          elsif %w[total succeeded discarded pending generation callback_priority].include?(key) then value.to_i
          else value
          end

          [key, decoded]
        end
      end

      def decode_counts(row)
        row.to_h do |key, value|
          [key, if key == "queue_name"
                  value
                else
                  ((key == "next_at") ? (value && Time.parse(value)) : value.to_i)
                end]
        end
      end

      def encode_array(values)
        PG::TextEncoder::Array.new.encode(values)
      end

      def present?(value)
        value.is_a?(String) && !value.strip.empty?
      end

      def uuid?(value)
        value.is_a?(String) && value.match?(/\A\h{8}-\h{4}-\h{4}-\h{4}-\h{12}\z/)
      end

      def time?(value)
        present?(value) && !!Time.iso8601(value)
      rescue ArgumentError
        false
      end

      def query(sql, params = [])
        @database.with_connection { |connection| connection.exec_params(sql, params).to_a }
      end
    end
  end
end
