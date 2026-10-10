# frozen_string_literal: true

module DecentJob
  # Reads and writes the SQL state of cron entries: persistent enable and
  # disable overrides, and the job history of each cron entry.
  #
  # {CronEntry} creates a new store for each call through its private
  # +store+ method. {CronEntry.last_jobs_by_key} creates one store per
  # database. The store keeps no state of its own. Each method checks out a
  # connection from the pool for one statement and returns it.
  #
  # Overrides live in +decent_job_cron_settings+, keyed by +cron_key+. History
  # is the set of +decent_jobs+ rows with a matching +cron_key+. A row with a
  # null +cron_at+ is a manual run. History is only as long as the job records
  # are kept: removing rows also removes the history that deduplication and
  # restart catch-up use.
  #
  # See docs/recurring-jobs.md.
  class CronStore
    # @param database [Database] the connection pool that holds the cron tables
    # @api private
    def initialize(database)
      @database = database
      @store = Store.new(database)
    end

    # Reads the persistent override for a cron entry.
    #
    # @param key [Symbol, String] the cron entry key
    # @param default [Boolean] the result when no override row exists
    # @return [Boolean] the stored override, or +default+ when there is none
    # @api private
    def enabled?(key, default:)
      @database.with_connection do |connection|
        row = connection.exec_params("SELECT enabled FROM decent_job_cron_settings WHERE cron_key = $1", [key.to_s]).first
        row ? row.fetch("enabled") == "t" : default
      end
    end

    # Stores a persistent override for a cron entry. The override applies to
    # every process and every entry instance that uses this database.
    #
    # @param key [Symbol, String] the cron entry key
    # @param enabled [Boolean] true to enable, false to disable
    # @return [Boolean] +enabled+
    # @note Writes one row with an upsert. It does not use a transaction of its own.
    # @api private
    def set_enabled(key, enabled)
      @database.with_connection do |connection|
        connection.exec_params(<<~SQL, [key.to_s, enabled])
          INSERT INTO decent_job_cron_settings (cron_key, enabled) VALUES ($1, $2)
          ON CONFLICT (cron_key) DO UPDATE SET enabled = EXCLUDED.enabled
        SQL
      end
      enabled
    end

    # Returns every stored job of a cron entry, newest occurrence first.
    # Manual runs (null +cron_at+) come after all scheduled runs.
    #
    # @param key [Symbol, String] the cron entry key
    # @return [Array<Hash{String => Object}>] decoded +decent_jobs+ rows
    # @note The query has no limit. It loads the full history.
    # @api private
    def jobs(key)
      @database.with_connection do |connection|
        connection.exec_params("SELECT * FROM decent_jobs WHERE cron_key = $1 ORDER BY cron_at DESC NULLS LAST, created_at DESC, id DESC", [key.to_s]).map { |row| @store.decode(row) }
      end
    end

    # Returns the latest stored job for each key in one query.
    #
    # Rows sort by +cron_at+ with nulls last, so the latest scheduled run wins
    # over a newer manual run. With +before+, only rows with a non-null
    # +cron_at+ at or before that time qualify. {CronManager} uses +before+ to
    # exclude future-dated rows from the restart catch-up snapshot.
    #
    # @param keys [Array<Symbol, String>] cron entry keys
    # @param before [Time, nil] inclusive upper bound for +cron_at+, or nil for no bound
    # @return [Hash{String => Hash{String => Object}}] decoded rows keyed by
    #   string cron key; a key with no matching row is absent
    # @api private
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
