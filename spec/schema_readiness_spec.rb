# frozen_string_literal: true

require "spec_helper"

describe "schema readiness" do
  include DatabaseSpec

  after { DecentJob.reset! }

  it "returns false for an absent metadata table without installing anything" do
    _(DecentJob::Schema.new(database).migrated?).must_equal false
    database.with_connection do |connection|
      _(connection.exec("SELECT tablename FROM pg_tables WHERE schemaname = current_schema()").column_values(0)).must_be_empty
    end
  end

  it "preserves a caller transaction when the metadata table is missing" do
    database.transaction do |connection|
      connection.exec("CREATE TABLE application_data (value integer)")
      connection.exec("INSERT INTO application_data VALUES (42)")
      _(DecentJob::Schema.new(database).migrated?).must_equal false
      _(connection.exec("SELECT value FROM application_data").getvalue(0, 0)).must_equal "42"
    end
    database.with_connection do |connection|
      _(connection.exec("SELECT value FROM application_data").getvalue(0, 0)).must_equal "42"
    end
  end

  it "requires every packaged SQL version and allows additional future versions" do
    schema = DecentJob::Schema.new(database)
    database.with_connection { |connection| connection.exec("CREATE TABLE decent_job_schema_migrations (version integer PRIMARY KEY)") }
    _(schema.migrated?).must_equal false
    versions = Dir[File.join(DecentJob::Schema::MIGRATIONS_PATH, "*.sql")].sort.map { |path| File.basename(path).split("_").first.to_i }
    _(versions).wont_be_empty
    database.with_connection do |connection|
      versions.drop(1).each { |version| connection.exec_params("INSERT INTO decent_job_schema_migrations VALUES ($1)", [version]) }
      _(schema.migrated?).must_equal false
      connection.exec_params("INSERT INTO decent_job_schema_migrations VALUES ($1)", [versions.first])
      _(schema.migrated?).must_equal true
      connection.exec_params("INSERT INTO decent_job_schema_migrations VALUES ($1)", [versions.max + 1])
      _(schema.migrated?).must_equal true
      connection.exec_params("DELETE FROM decent_job_schema_migrations WHERE version = $1", [versions.last])
      _(schema.migrated?).must_equal false
    end
  end

  it "observes a fully installed schema in a read-only transaction without an advisory lock" do
    schema = DecentJob::Schema.new(database)
    schema.migrate
    database.transaction do |connection|
      connection.exec("SET TRANSACTION READ ONLY")
      _(schema.migrated?).must_equal true
      _(connection.exec("SELECT count(*) FROM pg_locks WHERE pid = pg_backend_pid() AND locktype = 'advisory'").getvalue(0, 0)).must_equal "0"
    end
  end

  it "uses explicit database selection without opening the global database" do
    DecentJob.configuration.database_url = nil
    _(DecentJob.migrated?(database: database)).must_equal false
    DecentJob::Schema.new(database).migrate
    _(DecentJob.migrated?(database: database)).must_equal true
    _(DecentJob.instance_variable_get(:@database)).must_be_nil
  end

  it "uses the configured global database by default" do
    DecentJob.configure { |config| config.database_url = @connection_options }
    _(DecentJob.migrated?).must_equal false
    DecentJob::Schema.new(database).migrate
    _(DecentJob.migrated?).must_equal true
  end

  it "propagates malformed metadata errors while preserving the caller transaction" do
    database.transaction do |connection|
      connection.exec("CREATE TABLE decent_job_schema_migrations (wrong_column integer)")
      _ { DecentJob::Schema.new(database).migrated? }.must_raise PG::UndefinedColumn
      _(connection.exec("SELECT 42").getvalue(0, 0)).must_equal "42"
    end
  end

  it "propagates a connection failure" do
    database.close
    _ { DecentJob::Schema.new(database).migrated? }.must_raise DecentJob::ClosedDatabase
  end

  it "preserves caller writes when the metadata table is dropped before its query" do
    admin = @admin
    metadata = "#{admin.quote_ident(@schema_name)}.decent_job_schema_migrations"
    admin.exec("CREATE TABLE #{metadata} (version integer PRIMARY KEY)")
    database.transaction do |connection|
      connection.exec("CREATE TABLE application_data (value integer)")
      connection.exec("INSERT INTO application_data VALUES (42)")
      connection.define_singleton_method(:exec) do |sql, *args, &block|
        admin.exec("DROP TABLE #{metadata}") if sql == "SELECT version FROM decent_job_schema_migrations"
        super(sql, *args, &block)
      end
      begin
        _(DecentJob::Schema.new(database).migrated?).must_equal false
        _(connection.exec("SELECT value FROM application_data").getvalue(0, 0)).must_equal "42"
      ensure
        connection.singleton_class.remove_method(:exec)
      end
    end
  end

  it "propagates metadata permission errors without aborting the caller transaction" do
    role = @admin.quote_ident("decent_job_readiness_#{SecureRandom.hex(8)}")
    @admin.exec("CREATE ROLE #{role}")
    begin
      @admin.exec("GRANT USAGE ON SCHEMA #{@admin.quote_ident(@schema_name)} TO #{role}")
      database.with_connection { |connection| connection.exec("CREATE TABLE decent_job_schema_migrations (version integer PRIMARY KEY)") }
      database.transaction do |connection|
        connection.exec("SET LOCAL ROLE #{role}")
        _ { DecentJob::Schema.new(database).migrated? }.must_raise PG::InsufficientPrivilege
        _(connection.exec("SELECT 42").getvalue(0, 0)).must_equal "42"
      end
    ensure
      @admin.exec("DROP OWNED BY #{role}")
      @admin.exec("DROP ROLE #{role}")
    end
  end

  it "follows installation search_path resolution including quoted schema names" do
    schema = DecentJob::Schema.new(database)
    schema.migrate
    database.with_connection do |connection|
      connection.set_notice_processor {}
      quoted = connection.quote_ident("Readiness \" Schema")
      connection.exec("CREATE SCHEMA #{quoted}")
      begin
        connection.exec("SET search_path TO #{quoted}")
        _(schema.migrated?).must_equal false
        connection.exec("SET search_path TO #{quoted}, #{connection.quote_ident(@schema_name)}")
        _(schema.migrated?).must_equal true
        connection.exec("CREATE TABLE #{quoted}.decent_job_schema_migrations (version integer PRIMARY KEY)")
        _(schema.migrated?).must_equal false
      ensure
        connection.exec("SET search_path TO #{connection.quote_ident(@schema_name)}")
        connection.exec("DROP SCHEMA #{quoted} CASCADE")
      end
    end
  end
end
