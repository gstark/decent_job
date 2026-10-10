# frozen_string_literal: true

require "json"
require "time"

module DecentJob
  class Dashboard
    # Read-only SQL for dashboard pages. Every list is bounded and uses keyset
    # pagination on (created_at, id) so ties never skip or repeat rows.
    class Queries
      STATUSES = %w[queued scheduled retried running succeeded discarded].freeze
      PAGE_SIZE = 50
      MAX_PAGE_SIZE = 200

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

      Page = Struct.new(:rows, :next_cursor)

      def initialize(database)
        @database = database
        @store = Store.new(database)
      end

      # ----- overview -------------------------------------------------------

      def status_counts
        counts = STATUSES.to_h { |status| [status, 0] }

        query("SELECT (#{STATUS_SQL}) AS status, count(*) AS count FROM decent_jobs GROUP BY 1").each do |row|
          counts[row.fetch("status")] = row.fetch("count").to_i
        end

        counts
      end

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

      def recent_discards(limit: 8)
        query("SELECT * FROM decent_jobs WHERE state = 'discarded' ORDER BY finished_at DESC, id DESC LIMIT $1", [limit]).map { |row| @store.decode(row) }
      end

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

      def filter_options
        query(<<~SQL).first.to_h { |key, value| [key, value ? PG::TextDecoder::Array.new.decode(value) : []] }
          SELECT
            (SELECT array_agg(DISTINCT job_class ORDER BY job_class) FROM decent_jobs) AS job_classes,
            (SELECT array_agg(DISTINCT queue_name ORDER BY queue_name) FROM decent_jobs) AS queues,
            (SELECT array_agg(DISTINCT label ORDER BY label) FROM decent_jobs, unnest(labels) AS label) AS labels,
            (SELECT array_agg(DISTINCT cron_key ORDER BY cron_key) FROM decent_jobs WHERE cron_key IS NOT NULL) AS cron_keys
        SQL
      end

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

      def job_ids(filter)
        where, params = job_conditions(filter)
        query("SELECT id FROM decent_jobs #{"WHERE #{where.join(" AND ")}" unless where.empty?} ORDER BY created_at DESC, id DESC LIMIT 10000", params).map { |row| row.fetch("id") }
      end

      def job_count(filter)
        where, params = job_conditions(filter)
        query("SELECT count(*) AS count FROM decent_jobs #{"WHERE #{where.join(" AND ")}" unless where.empty?}", params).first.fetch("count").to_i
      end

      def job(id)
        row = query("SELECT decent_jobs.*, (#{STATUS_SQL}) AS status FROM decent_jobs WHERE id = $1", [id]).first
        return unless row
        @store.decode(row).merge("status" => row.fetch("status"))
      end

      def executions(job_id)
        @store.executions(job_id).reverse
      end

      def claims(job_id)
        query("SELECT scope, state, created_at, updated_at FROM decent_job_concurrency_claims WHERE job_id = $1 ORDER BY created_at", [job_id]).map do |row|
          row.to_h.merge("created_at" => Time.parse(row.fetch("created_at")), "updated_at" => Time.parse(row.fetch("updated_at")))
        end
      end

      # ----- batches --------------------------------------------------------

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

      def batch(id)
        row = query("SELECT * FROM decent_job_batches WHERE id = $1", [id]).first
        decode_batch(row) if row
      end

      def batch_jobs(id, callbacks: false)
        column = callbacks ? "batch_callback_id" : "batch_id"

        query("SELECT decent_jobs.*, (#{STATUS_SQL}) AS status FROM decent_jobs WHERE #{column} = $1 ORDER BY created_at, id LIMIT 500", [id]).map do |row|
          @store.decode(row).merge("status" => row.fetch("status"))
        end
      end

      # ----- schedules ------------------------------------------------------

      def schedules(configuration = DecentJob.configuration)
        entries = configuration.cron_entries(database: @database)
        last = CronEntry.last_jobs_by_key(entries)

        entries.map do |entry|
          last_job = last[entry.key.to_s]
          {entry: entry, enabled: entry.enabled?, last_job: last_job,
           next_at: safe_next_at(entry, last_job), properties: entry.display_properties}
        end
      end

      def schedule(key, configuration = DecentJob.configuration)
        schedules(configuration).find { |item| item.fetch(:entry).key.to_s == key }
      end

      def schedule_jobs(key, limit: PAGE_SIZE)
        query("SELECT decent_jobs.*, (#{STATUS_SQL}) AS status FROM decent_jobs WHERE cron_key = $1 ORDER BY cron_at DESC NULLS LAST, created_at DESC, id DESC LIMIT $2", [key, limit]).map do |row|
          @store.decode(row).merge("status" => row.fetch("status"))
        end
      end

      # ----- processes ------------------------------------------------------

      def processes
        query("SELECT *, (#{ProcessRecord::ACTIVE_SQL}) AS active FROM decent_job_processes ORDER BY created_at, id").map do |row|
          {"id" => row.fetch("id"), "state" => JSON.parse(row.fetch("state")), "heartbeat_mode" => row.fetch("heartbeat_mode"),
           "created_at" => Time.parse(row.fetch("created_at")), "updated_at" => Time.parse(row.fetch("updated_at")),
           "active" => row.fetch("active") == "t"}
        end
      end

      def running_by_process
        query("SELECT locked_by_id, count(*) AS count FROM decent_jobs WHERE locked_by_id IS NOT NULL AND finished_at IS NULL GROUP BY locked_by_id").to_h do |row|
          [row.fetch("locked_by_id"), row.fetch("count").to_i]
        end
      end

      # ----- cleaner --------------------------------------------------------

      def discard_groups
        query(<<~SQL).map { |row| row.to_h.merge("count" => row.fetch("count").to_i, "oldest" => Time.parse(row.fetch("oldest")), "newest" => Time.parse(row.fetch("newest"))) }
          SELECT job_class, COALESCE(error->>'class', '') AS error_class, count(*) AS count,
            min(finished_at) AS oldest, max(finished_at) AS newest
          FROM decent_jobs WHERE state = 'discarded'
          GROUP BY job_class, error_class ORDER BY count DESC, job_class LIMIT 200
        SQL
      end

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

      def performance(from:, to:, job_class: nil)
        metrics = ExecutionMetrics.new(database: @database)
        {summary: metrics.summary(from: from, to: to, job_class: job_class),
         classes: metrics.grouped_by(:job_class, from: from, to: to, job_class: job_class),
         queues: metrics.grouped_by(:queue_name, from: from, to: to, job_class: job_class)}
      end

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

      def page(rows, limit)
        more = rows.size > limit
        rows = rows.first(limit)
        cursor = more && rows.last && [rows.last.fetch("created_at").utc.iso8601(6), rows.last.fetch("id")]
        Page.new(rows: rows, next_cursor: cursor || nil)
      end

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
