# frozen_string_literal: true

require "active_record/railtie"
require "decent_job"
require "json"
require "rails/generators"

class RailsMigrationHostApplication < Rails::Application
  config.root = ENV.fetch("HOST_ROOT")
  config.eager_load = false
  config.secret_key_base = "host migration secret" * 6
  config.logger = Logger.new(File::NULL)
  config.active_record.schema_format = ENV.fetch("HOST_SCHEMA_FORMAT").to_sym
  config.active_record.dump_schema_after_migration = true
end

RailsMigrationHostApplication.initialize!
Rails.application.load_tasks

def decent_job_migrated?
  database = DecentJob::Database.new(url: JSON.parse(ENV.fetch("HOST_CONNECTION"), symbolize_names: true), pool_size: 1)
  DecentJob.migrated?(database: database)
ensure
  database&.close
end

Rails::Generators.invoke("decent_job:migration", [], destination_root: Rails.root.to_s)
migrations = Dir[Rails.root.join("db/migrate/*.rb").to_s].map { |path| File.basename(path) }

Rake::Task["db:migrate"].invoke
db_config = ActiveRecord::Base.connection_db_config
schema = File.read(ActiveRecord::Tasks::DatabaseTasks.schema_dump_path(db_config))
migrated = decent_job_migrated?

# Rebuild the schema from the dump alone. The dump creates the schema.
ActiveRecord::Base.connection.drop_schema(db_config.configuration_hash.fetch(:schema_search_path))
Rake::Task["db:schema:load"].invoke

puts "HOST_RESULT:#{JSON.generate(
  migrations: migrations,
  migrated: migrated,
  schema_has_jobs: schema.include?("decent_jobs"),
  reloaded_migrated: decent_job_migrated?,
  pending: ActiveRecord::Base.connection_pool.migration_context.needs_migration?
)}"
