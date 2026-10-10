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
      _(connection.exec("SELECT version FROM decent_job_schema_migrations ORDER BY version").column_values(0)).must_equal ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"]
    end
  end

  it "can be installed repeatedly without losing records" do
    schema = DecentJob::Schema.new(database)
    schema.migrate
    schema.migrate

    database.with_connection do |connection|
      _(connection.exec("SELECT count(*) FROM decent_job_schema_migrations").getvalue(0, 0)).must_equal "11"
    end
  end

  it "serializes concurrent installers" do
    database
    workers = 3.times.map { Thread.new { DecentJob::Schema.new(database).migrate } }
    workers.each(&:value)

    database.with_connection do |connection|
      _(connection.exec("SELECT count(*) FROM decent_job_schema_migrations").getvalue(0, 0)).must_equal "11"
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
    _(row.fetch("performed_at")).must_be_nil
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
      _(connection.exec("SELECT version FROM decent_job_schema_migrations ORDER BY version").column_values(0)).must_equal ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"]
    end
  end

  it "indexes finished records in cleanup cursor order" do
    DecentJob::Schema.new(database).migrate

    database.with_connection do |connection|
      %w[decent_jobs_finished decent_job_batches_finished].each do |name|
        definition = connection.exec_params("SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND indexname = $1", [name]).first
        _(definition).wont_be_nil
        _(definition.fetch("indexdef")).must_include "(finished_at, id)"
        _(definition.fetch("indexdef")).must_include "WHERE (finished_at IS NOT NULL)"
      end
    end
  end

  [1, 7].each do |version|
    it "upgrades version #{version} historical attempts without inventing snapshots or measured duration" do
      started_at = Time.utc(2026, 1, 1)
      finished_at = started_at + 20
      later_at = started_at + 40
      scenarios = {
        queued_retry: {state: "ready", execution_state: "retried", execution_finished_at: finished_at},
        running: {state: "running", execution_state: "running", performed: true},
        succeeded: {state: "succeeded", execution_state: "succeeded", finished_at: finished_at, execution_finished_at: finished_at + 0.001, performed: true},
        discarded: {state: "discarded", execution_state: "discarded", finished_at: finished_at, execution_finished_at: finished_at + 0.001, performed: true},
        force_discarded: {state: "discarded", execution_state: "discarded", finished_at: finished_at, execution_finished_at: finished_at, performed: true},
        cancelled_pending_retry: {state: "discarded", execution_state: "retried", finished_at: later_at, execution_finished_at: finished_at},
        cancelled_pending_manual_retry: {state: "discarded", execution_state: "discarded", finished_at: later_at, execution_finished_at: finished_at},
        running_with_old_attempt: {state: "running", execution_state: "retried", execution_finished_at: finished_at},
        terminal_without_finish: {state: "discarded", execution_state: "discarded"}
      }.transform_values { |scenario| scenario.merge(id: SecureRandom.uuid) }

      database.with_connection do |connection|
        connection.exec("CREATE TABLE decent_job_schema_migrations (version integer PRIMARY KEY)")

        Dir[File.join(DecentJob::Schema::MIGRATIONS_PATH, "*.sql")].sort.first(version).each_with_index do |path, index|
          connection.exec(File.read(path))
          connection.exec_params("INSERT INTO decent_job_schema_migrations VALUES ($1)", [index + 1])
        end

        scenarios.each_value do |scenario|
          execution_id = SecureRandom.uuid
          connection.exec_params(<<~SQL, [scenario.fetch(:id), SecureRandom.uuid, scenario.fetch(:state), execution_id, scenario[:finished_at]&.iso8601(6)])
            INSERT INTO decent_jobs (id, active_job_id, queue_name, job_class, payload, scheduled_at, state, execution_id, finished_at)
            VALUES ($1, $2, 'changed', 'ChangedJob', '{"executions":5}', clock_timestamp(), $3, $4, $5)
          SQL
          connection.exec_params(<<~SQL, [execution_id, scenario.fetch(:id), scenario.fetch(:execution_state), started_at.iso8601(6), scenario[:execution_finished_at]&.iso8601(6)])
            INSERT INTO decent_job_executions (id, job_id, process_id, state, started_at, finished_at)
            VALUES ($1, $2, 1, $3, $4, $5)
          SQL
        end
      end

      DecentJob::Schema.new(database).migrate

      scenarios.each do |name, scenario|
        store = DecentJob::Store.new(database)
        execution = store.executions(scenario.fetch(:id)).first
        %w[payload scheduled_at job_class queue_name duration].each { |key| _(execution.fetch(key)).must_be_nil }

        if scenario[:performed]
          _(store.find(scenario.fetch(:id)).fetch("performed_at")).must_equal execution.fetch("started_at"), name
        else
          _(store.find(scenario.fetch(:id)).fetch("performed_at")).must_be_nil name
        end
      end
    end
  end
end
