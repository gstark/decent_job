# frozen_string_literal: true

require "spec_helper"

class TransactionVisibilityJob < ActiveJob::Base
  def perform
  end
end

describe "enqueue visibility in PostgreSQL transactions" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    TransactionVisibilityJob.queue_adapter = DecentJob::Adapter.new(database: database, execution_mode: :external)
    @observer = PG.connect(@connection_options)
  end

  after { @observer.finish }

  def visible_count
    @observer.exec("SELECT count(*) FROM decent_jobs").getvalue(0, 0).to_i
  end

  it "keeps caller-owned transaction writes invisible until their explicit commit" do
    database.with_connection do |connection|
      connection.exec("BEGIN")
      job = TransactionVisibilityJob.perform_later
      _(job.provider_job_id).wont_be_nil
      _(visible_count).must_equal 0
      connection.exec("COMMIT")
      _(visible_count).must_equal 1
    ensure
      connection.exec("ROLLBACK") unless connection.transaction_status == PG::PQTRANS_IDLE
    end
  end

  it "removes jobs when the caller rolls back a transaction opened through pg" do
    database.with_connection do |connection|
      connection.exec("BEGIN")
      TransactionVisibilityJob.perform_later
      _(visible_count).must_equal 0
      connection.exec("ROLLBACK")
      _(visible_count).must_equal 0
    ensure
      connection.exec("ROLLBACK") unless connection.transaction_status == PG::PQTRANS_IDLE
    end
  end
end
