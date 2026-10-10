# frozen_string_literal: true

require "fileutils"
require "open3"
require "spec_helper"
require "tmpdir"
require "yaml"

describe "Rails migration generator" do
  include DatabaseSpec

  before { @directory = Dir.mktmpdir("dj-rails-migration-", "/tmp") }

  after { FileUtils.remove_entry(@directory) }

  def run_host(schema_format)
    FileUtils.mkdir_p(File.join(@directory, "config"))
    File.write(File.join(@directory, "config/database.yml"), YAML.dump(
      "test" => @connection_options.except(:options).transform_keys(&:to_s)
        .merge("adapter" => "postgresql", "database" => @connection_options.fetch(:dbname), "schema_search_path" => @schema_name)
    ))

    environment = {"HOST_CONNECTION" => JSON.generate(@connection_options), "HOST_ROOT" => @directory,
                   "HOST_SCHEMA_FORMAT" => schema_format, "RAILS_ENV" => "test", "DATABASE_URL" => nil}
    output, error, status = Open3.capture3(environment, RbConfig.ruby, "-Ilib", "integration/support/rails_migration_app.rb")
    _(status.exitstatus).must_equal 0, "Host failed: #{error}\n#{output}"
    JSON.parse(output.lines.find { |line| line.start_with?("HOST_RESULT:") }.delete_prefix("HOST_RESULT:"))
  end

  %w[ruby sql].each do |schema_format|
    it "installs the tables and keeps the versions through a #{schema_format} schema dump" do
      result = run_host(schema_format)
      version = DecentJob::Schema.migrations.keys.max
      _(result.fetch("migrations").size).must_equal 1
      _(result.fetch("migrations").first).must_match(/\A\d{14}_decent_job_schema_v#{version}\.rb\z/)
      _(result.fetch("migrated")).must_equal true
      _(result.fetch("schema_has_jobs")).must_equal true
      _(result.fetch("reloaded_migrated")).must_equal true
      _(result.fetch("pending")).must_equal false
    end
  end
end
