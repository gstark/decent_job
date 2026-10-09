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
      _(connection.exec("SELECT version FROM decent_job_schema_migrations").column_values(0)).must_equal ["1"]
    end
  end

  it "can be installed repeatedly without losing records" do
    schema = DecentJob::Schema.new(database)
    schema.migrate
    schema.migrate
    database.with_connection do |connection|
      _(connection.exec("SELECT count(*) FROM decent_job_schema_migrations").getvalue(0, 0)).must_equal "1"
    end
  end

  it "serializes concurrent installers" do
    database
    workers = 3.times.map { Thread.new { DecentJob::Schema.new(database).migrate } }
    workers.each(&:value)
    database.with_connection do |connection|
      _(connection.exec("SELECT count(*) FROM decent_job_schema_migrations").getvalue(0, 0)).must_equal "1"
    end
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
      _(connection.exec("SELECT version FROM decent_job_schema_migrations").column_values(0)).must_equal ["1"]
    end
  end
end
