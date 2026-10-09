# frozen_string_literal: true

require "active_job"
require "json"
require "minitest/autorun"
require "pg"
require "securerandom"

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "decent_job" if File.exist?(File.expand_path("../lib/decent_job.rb", __dir__))

ActiveJob::Base.logger = Logger.new(File::NULL)
# Runtime events are covered with explicit capture sinks in logging specs.
DecentJob::LogSubscriber.loggers.replace([Logger.new(File::NULL)])
DecentJob::LogSubscriber.reset_logger

module DatabaseSpec
  def before_setup
    super
    @schema_name = "decent_job_spec_#{SecureRandom.hex(8)}"
    @admin = PG.connect(ENV.fetch("DECENT_JOB_TEST_DATABASE_URL", "postgres:///postgres"))
    @admin.set_notice_processor {}
    @admin.exec("CREATE SCHEMA #{@admin.quote_ident(@schema_name)}")
    @connection_options = @admin.conninfo_hash.slice(:host, :port, :dbname, :user, :password)
      .merge(options: "-c search_path=#{@schema_name}")
  end

  def after_teardown
    @database&.close
    if @admin
      @admin.exec("DROP SCHEMA #{@admin.quote_ident(@schema_name)} CASCADE")
      @admin.finish
    end
    super
  end

  def database
    @database ||= DecentJob::Database.new(url: @connection_options, pool_size: 3)
  end
end
