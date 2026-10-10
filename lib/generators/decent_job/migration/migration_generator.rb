# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record/migration"
require "decent_job"

module DecentJob
  module Generators
    # Writes an Active Record migration that installs DecentJob's tables.
    #
    # Run `bin/rails generate decent_job:migration` once to install, and again
    # after a gem upgrade that adds SQL migrations. Each generated migration
    # calls {Schema.apply} with the newest packaged version as its upper bound,
    # so a migration applies the same SQL even after later upgrades. Rails then
    # records the tables in db/schema.rb or db/structure.sql, and
    # {SchemaDump} keeps the decent_job_schema_migrations rows in the dump.
    #
    # Constraints: the migration runs on the Active Record database. Pass
    # `--database` when DecentJob uses a different database of a multi-database
    # application. The generated migration has no down method, because the SQL
    # migrations are not reversible.
    #
    # See docs/rails-integration.md.
    class MigrationGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration

      source_root File.expand_path("templates", __dir__)

      class_option :database, type: :string, aliases: %i[--db],
        desc: "The database for the migration. Defaults to the primary database."

      # Writes db/migrate/<timestamp>_decent_job_schema_v<version>.rb.
      #
      # @return [void]
      # @raise [Rails::Generators::Error] when a migration for this version
      #   already exists
      def create_migration_file
        @version = DecentJob::Schema.migrations.keys.max
        migration_template "migration.rb.tt", File.join(db_migrate_path, "decent_job_schema_v#{@version}.rb")
      end
    end
  end
end
