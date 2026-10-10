# frozen_string_literal: true

module DecentJob
  # Installs and checks DecentJob's SQL schema.
  #
  # {DecentJob.install}, {DecentJob.migrated?}, and the `migrate` CLI command
  # create it. Migrations are the SQL files in db/migrations, applied in file
  # name order. The integer prefix of each file name is its version, recorded
  # in the decent_job_schema_migrations table. Tables resolve through the
  # connection's search_path, so one database can hold separate installations
  # in separate schemas.
  #
  # The object holds no connection. Each call borrows one from the
  # {Database} pool for one transaction.
  class Schema
    # Absolute path of the packaged SQL migrations.
    MIGRATIONS_PATH = File.expand_path("../../db/migrations", __dir__)

    # @param database [Database] the pool that owns the target schema
    def initialize(database)
      @database = database
    end

    # Tells whether every packaged migration version is recorded.
    #
    # Extra versions in the table, for example from a newer gem, do not make
    # the result false. The check never creates tables.
    #
    # @return [Boolean] false when the metadata table or any packaged version
    #   is missing
    # @raise [PG::Error] for database errors other than a missing metadata table
    def migrated?
      # A savepoint protects callers' transactions when metadata is absent or
      # concurrently dropped. Resolve the table through migrate's search_path.
      versions = @database.transaction do |connection|
        connection.exec("SELECT version FROM decent_job_schema_migrations").column_values(0).map(&:to_i)
      end

      self.class.migrations.keys.all? { |version| versions.include?(version) }
    rescue PG::UndefinedTable
      false
    end

    # Applies every packaged migration that is not recorded yet.
    #
    # @return [Schema] self
    # @raise [PG::Error] when a migration fails; the transaction rolls back
    #   all migrations from this call
    # @note All migrations run in one transaction. See {.apply} for the lock.
    def migrate
      @database.transaction { |connection| self.class.apply(connection) }
      self
    end

    # Maps each packaged migration version to its SQL file path.
    #
    # @return [Hash{Integer => String}] versions in ascending order
    def self.migrations
      Dir[File.join(MIGRATIONS_PATH, "*.sql")].sort.to_h { |path| [File.basename(path).split("_").first.to_i, path] }
    end

    # Applies packaged migrations that are not recorded yet on one connection.
    #
    # {#migrate} calls this inside its own transaction. Active Record
    # migrations from the `decent_job:migration` Rails generator call it with
    # the migration's raw connection, inside the migration's transaction.
    #
    # @param connection [PG::Connection] a connection inside an open transaction
    # @param through [Integer, nil] the highest version to apply, or nil for all
    #   packaged versions
    # @return [void]
    # @raise [PG::Error] when a migration fails
    # @note A transaction-scoped advisory lock, keyed by the current schema
    #   name, serializes concurrent installers, so two processes cannot apply
    #   the same version. The caller must hold a transaction, or the lock ends
    #   with the first statement.
    def self.apply(connection, through: nil)
      # Serialize installation before even creating the migrations table.
      connection.exec("SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || '.decent_job_schema', 0))")

      # Check first: CREATE TABLE IF NOT EXISTS raises a NOTICE on every rerun.
      unless connection.exec("SELECT to_regclass('decent_job_schema_migrations')").getvalue(0, 0)
        connection.exec("CREATE TABLE decent_job_schema_migrations (version integer PRIMARY KEY)")
      end

      versions = connection.exec("SELECT version FROM decent_job_schema_migrations").column_values(0).map(&:to_i)

      migrations.each do |version, path|
        next if versions.include?(version) || (through && version > through)

        connection.exec(File.read(path))
        connection.exec_params("INSERT INTO decent_job_schema_migrations VALUES ($1)", [version])
      end
    end
  end
end
