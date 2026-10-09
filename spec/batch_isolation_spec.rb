# frozen_string_literal: true

require "spec_helper"

class BatchForeignChildJob < ActiveJob::Base
  def perform
  end
end

class BatchIsolationCallbackJob < ActiveJob::Base
  before_enqueue { ActiveJob.perform_all_later(BatchForeignChildJob.new) }

  def perform(*)
  end
end

describe "batch database isolation" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @foreign_schema = "decent_job_foreign_#{SecureRandom.hex(8)}"
    @admin.exec("CREATE SCHEMA #{@admin.quote_ident(@foreign_schema)}")
    @foreign_database = DecentJob::Database.new(url: @connection_options.merge(options: "-c search_path=#{@foreign_schema}"))
    DecentJob::Schema.new(@foreign_database).migrate
    BatchForeignChildJob.queue_adapter = DecentJob::Adapter.new(database: @foreign_database)
    BatchIsolationCallbackJob.queue_adapter = DecentJob::Adapter.new(database: database)
  end

  after do
    @foreign_database&.close
    @admin.exec("DROP SCHEMA #{@admin.quote_ident(@foreign_schema)} CASCADE") if @foreign_schema
  end

  it "rejects a foreign-database child captured through perform_all_later before writing it" do
    _ { DecentJob::Batch.enqueue(database: database, on_finish: BatchIsolationCallbackJob) }.must_raise ArgumentError
    [database, @foreign_database].each do |pool|
      pool.with_connection { |connection| _(connection.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0)).must_equal "0" }
    end
  end

  it "loads GlobalID batch properties using the batch's database" do
    referenced = DecentJob::Batch.new(database: database, description: "reference")
    referenced.save
    parent = DecentJob::Batch.new(database: database, reference: referenced)
    parent.save
    DecentJob::Current.with(database: @foreign_database) do
      restored = DecentJob::Batch.find(parent.id, database: database)
      _(restored.properties.fetch(:reference).id).must_equal referenced.id
      _(restored.properties.fetch(:reference).database).must_be_same_as database
      _(restored.display_attributes.fetch("properties").fetch(:reference).id).must_equal referenced.id
      _(DecentJob::Current.execution.fetch(:database)).must_be_same_as @foreign_database
    end
  end
end
