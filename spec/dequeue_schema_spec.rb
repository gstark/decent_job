# frozen_string_literal: true

require "spec_helper"
require "support/jobs"

describe "dequeue creation indexes" do
  include DatabaseSpec

  after { DecentJob.reset! }

  def index_definitions
    database.with_connection do |connection|
      connection.exec("SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = current_schema() AND tablename = 'decent_jobs'").to_h { |row| row.values_at("indexname", "indexdef") }
    end
  end

  it "installs global and queue creation indexes while retaining deadline indexes" do
    DecentJob::Schema.new(database).migrate
    indexes = index_definitions
    {"decent_jobs_creation_available" => "(priority, created_at, id)",
     "decent_jobs_queue_creation_available" => "(queue_name, priority, created_at, id)",
     "decent_jobs_available" => "(priority, scheduled_at, id)",
     "decent_jobs_queue_available" => "(queue_name, priority, scheduled_at, id)"}.each do |name, columns|
      _(indexes[name]).wont_be_nil
      _(indexes.fetch(name)).must_include columns
      _(indexes.fetch(name)).must_include "WHERE (state = ANY (ARRAY['ready'::text, 'running'::text]))"
    end
    DecentJob::Schema.new(database).migrate
    _(index_definitions).must_equal indexes
  end

  it "executes against version nine before upgrading without changing jobs or existing indexes" do
    database.with_connection do |connection|
      connection.exec("CREATE TABLE decent_job_schema_migrations (version integer PRIMARY KEY)")
      Dir[File.join(DecentJob::Schema::MIGRATIONS_PATH, "*.sql")].sort.first(9).each_with_index do |path, index|
        connection.exec(File.read(path))
        connection.exec_params("INSERT INTO decent_job_schema_migrations VALUES ($1)", [index + 1])
      end
    end
    RecordingJob.queue_adapter = DecentJob::Adapter.new(database: database)
    RecordingJob.events = Queue.new
    first = RecordingJob.perform_later("before migration")
    pending = RecordingJob.perform_later("after migration")
    original = DecentJob::Store.new(database).find(pending.provider_job_id)
    DecentJob.configuration.queue_select_limit = 1
    performer = DecentJob::Performer.new(database: database)
    _(performer.perform_one.job_id).must_equal first.provider_job_id
    _(RecordingJob.events.pop(true)).must_equal "before migration"
    previous_indexes = index_definitions
    DecentJob::Schema.new(database).migrate
    _(DecentJob::Store.new(database).find(pending.provider_job_id)).must_equal original
    _(index_definitions.slice(*previous_indexes.keys)).must_equal previous_indexes
    _((index_definitions.keys - previous_indexes.keys).sort).must_equal %w[decent_jobs_creation_available decent_jobs_queue_creation_available]
    _(performer.perform_one.job_id).must_equal pending.provider_job_id
    _(RecordingJob.events.pop(true)).must_equal "after migration"
    DecentJob::Schema.new(database).migrate
    database.with_connection do |connection|
      _(connection.exec("SELECT max(version) FROM decent_job_schema_migrations").getvalue(0, 0)).must_equal "10"
    end
  end
end
