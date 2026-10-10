# frozen_string_literal: true

module DecentJob
  class Schema
    MIGRATIONS_PATH = File.expand_path("../../db/migrations", __dir__)

    def initialize(database)
      @database = database
    end

    def migrated?
      # A savepoint protects callers' transactions when metadata is absent or
      # concurrently dropped. Resolve the table through migrate's search_path.
      versions = @database.transaction do |connection|
        connection.exec("SELECT version FROM decent_job_schema_migrations").column_values(0).map(&:to_i)
      end

      Dir[File.join(MIGRATIONS_PATH, "*.sql")].all? do |path|
        versions.include?(File.basename(path).split("_").first.to_i)
      end
    rescue PG::UndefinedTable
      false
    end

    def migrate
      @database.transaction do |connection|
        # Serialize installation before even creating the migrations table.
        connection.exec("SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || '.decent_job_schema', 0))")

        # Check first: CREATE TABLE IF NOT EXISTS raises a NOTICE on every rerun.
        unless connection.exec("SELECT to_regclass('decent_job_schema_migrations')").getvalue(0, 0)
          connection.exec("CREATE TABLE decent_job_schema_migrations (version integer PRIMARY KEY)")
        end

        versions = connection.exec("SELECT version FROM decent_job_schema_migrations").column_values(0).map(&:to_i)

        Dir[File.join(MIGRATIONS_PATH, "*.sql")].sort.each do |path|
          version = File.basename(path).split("_").first.to_i
          next if versions.include?(version)

          connection.exec(File.read(path))
          connection.exec_params("INSERT INTO decent_job_schema_migrations VALUES ($1)", [version])
        end
      end

      self
    end
  end
end
