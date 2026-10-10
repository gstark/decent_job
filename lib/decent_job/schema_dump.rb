# frozen_string_literal: true

module DecentJob
  # Keeps DecentJob's recorded migration versions in Rails schema dumps.
  #
  # Rails dumps table definitions but no table rows. Without the rows of
  # decent_job_schema_migrations, a database built by `db:schema:load` or
  # `db:prepare` has DecentJob's tables, but {Schema#migrated?} returns false
  # and {Schema#migrate} tries to create the tables again.
  #
  # {Railtie} prepends {Ruby} to ActiveRecord::SchemaDumper for db/schema.rb,
  # and {Sql} to the PostgreSQL adapter for db/structure.sql. Each appends one
  # INSERT statement for the recorded versions. Nothing is appended when the
  # table is not in the dumped database, for example when DecentJob uses a
  # separate database.
  #
  # See docs/rails-integration.md.
  module SchemaDump
    TABLE = "decent_job_schema_migrations"

    # Builds the INSERT statement for the recorded versions.
    #
    # @param connection [ActiveRecord::ConnectionAdapters::PostgreSQLAdapter]
    # @return [String, nil] the statement, or nil when the table is missing or
    #   empty
    # @api private
    def self.insert_sql(connection)
      return unless connection.table_exists?(TABLE)

      versions = connection.select_values("SELECT version FROM #{TABLE} ORDER BY version")
      return if versions.empty?

      "INSERT INTO #{TABLE} (version) VALUES #{versions.map { |version| "(#{Integer(version)})" }.join(", ")} ON CONFLICT DO NOTHING"
    end

    # Appends the versions to db/schema.rb as an execute call.
    module Ruby
      private

      def trailer(stream)
        sql = SchemaDump.insert_sql(@connection) unless ignored?(TABLE)
        stream.puts "\n  execute #{sql.inspect}" if sql
        super
      end
    end

    # Appends the versions to db/structure.sql after Rails' own versions.
    module Sql
      # @api private
      def dump_schema_versions
        [super, SchemaDump.insert_sql(self)&.then { |sql| "#{sql};\n" }].compact.join("\n")
      end
    end
  end
end
