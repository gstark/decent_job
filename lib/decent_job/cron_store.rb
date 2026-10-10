# frozen_string_literal: true

module DecentJob
  class CronStore
    def initialize(database)
      @database = database
      @store = Store.new(database)
    end

    def enabled?(key, default:)
      @database.with_connection do |connection|
        row = connection.exec_params("SELECT enabled FROM decent_job_cron_settings WHERE cron_key = $1", [key.to_s]).first
        row ? row.fetch("enabled") == "t" : default
      end
    end

    def set_enabled(key, enabled)
      @database.with_connection do |connection|
        connection.exec_params(<<~SQL, [key.to_s, enabled])
          INSERT INTO decent_job_cron_settings (cron_key, enabled) VALUES ($1, $2)
          ON CONFLICT (cron_key) DO UPDATE SET enabled = EXCLUDED.enabled
        SQL
      end

      enabled
    end

    def jobs(key)
      @database.with_connection do |connection|
        connection.exec_params("SELECT * FROM decent_jobs WHERE cron_key = $1 ORDER BY cron_at DESC NULLS LAST, created_at DESC, id DESC", [key.to_s]).map { |row| @store.decode(row) }
      end
    end

    def last_jobs_by_key(keys, before: nil)
      return {} if keys.empty?

      params = [PG::TextEncoder::Array.new.encode(keys.map(&:to_s))]

      cutoff = if before
        params << before.getutc.iso8601(6)
        "AND cron_at <= $2"
      else
        ""
      end

      @database.with_connection do |connection|
        connection.exec_params(<<~SQL, params).to_h { |row| [row.fetch("cron_key"), @store.decode(row)] }
          SELECT latest.* FROM unnest($1::text[]) AS keys(cron_key)
          CROSS JOIN LATERAL (
            SELECT * FROM decent_jobs WHERE cron_key = keys.cron_key #{cutoff}
            ORDER BY cron_at DESC NULLS LAST, created_at DESC, id DESC LIMIT 1
          ) AS latest
        SQL
      end
    end
  end
end
