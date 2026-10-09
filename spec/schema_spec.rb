# frozen_string_literal: true

require "spec_helper"

describe "schema installation" do
  include DatabaseSpec

  it "installs jobs and execution history without PostgreSQL extensions" do
    DecentJob::Schema.new(database).migrate
    database.with_connection do |connection|
      tables = connection.exec("SELECT tablename FROM pg_tables WHERE schemaname = current_schema()").column_values(0)
      _(tables).must_include "decent_jobs"
      _(tables).must_include "decent_job_executions"
      _(connection.exec("SELECT version FROM decent_job_schema_migrations ORDER BY version").column_values(0)).must_equal ["1", "2", "3", "4", "5"]
    end
  end

  it "can be installed repeatedly without losing records" do
    schema = DecentJob::Schema.new(database)
    schema.migrate
    schema.migrate
    database.with_connection do |connection|
      _(connection.exec("SELECT count(*) FROM decent_job_schema_migrations").getvalue(0, 0)).must_equal "5"
    end
  end

  it "serializes concurrent installers" do
    database
    workers = 3.times.map { Thread.new { DecentJob::Schema.new(database).migrate } }
    workers.each(&:value)
    database.with_connection do |connection|
      _(connection.exec("SELECT count(*) FROM decent_job_schema_migrations").getvalue(0, 0)).must_equal "5"
    end
  end

  it "upgrades the original schema without losing queued jobs" do
    id = SecureRandom.uuid
    database.with_connection do |connection|
      connection.exec(File.read(File.join(DecentJob::Schema::MIGRATIONS_PATH, "001_create_jobs.sql")))
      connection.exec("CREATE TABLE decent_job_schema_migrations (version integer PRIMARY KEY)")
      connection.exec("INSERT INTO decent_job_schema_migrations VALUES (1)")
      connection.exec_params(<<~SQL, [id, SecureRandom.uuid])
        INSERT INTO decent_jobs (id, active_job_id, queue_name, job_class, payload, scheduled_at)
        VALUES ($1, $2, 'default', 'ExampleJob', '{"arguments":[42]}', clock_timestamp())
      SQL
    end
    DecentJob::Schema.new(database).migrate
    row = DecentJob::Store.new(database).find(id)
    _(row.fetch("payload")).must_equal({"arguments" => [42]})
    _(row.fetch("labels")).must_equal []
    _(row.fetch("concurrency_keys")).must_equal []
    _(row.fetch("state")).must_equal "ready"
  end

  it "rolls back all migration changes on failure" do
    database.with_connection { |connection| connection.exec("CREATE TABLE decent_job_executions (existing integer)") }
    _ { DecentJob::Schema.new(database).migrate }.must_raise PG::DuplicateTable
    database.with_connection do |connection|
      _(connection.exec("SELECT to_regclass('decent_jobs')").getvalue(0, 0)).must_be_nil
      _(connection.exec("SELECT to_regclass('decent_job_schema_migrations')").getvalue(0, 0)).must_be_nil
      connection.exec("DROP TABLE decent_job_executions")
    end
    DecentJob::Schema.new(database).migrate
    database.with_connection do |connection|
      _(connection.exec("SELECT version FROM decent_job_schema_migrations ORDER BY version").column_values(0)).must_equal ["1", "2", "3", "4", "5"]
    end
  end
end
