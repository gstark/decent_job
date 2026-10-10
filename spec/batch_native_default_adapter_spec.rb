# frozen_string_literal: true

require "spec_helper"

class DefaultAdapterMemberJob < DecentJob::Base
  def perform(value)
  end
end

class DefaultAdapterCallbackJob < DecentJob::Base
  def perform(batch, options)
  end
end

describe "batches with native jobs on the default adapter" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    DecentJob.configure { |config| config.database_url = @connection_options }
  end

  after do
    DecentJob.reset!
  end

  it "accepts members and callbacks that share the global default adapter" do
    batch = DecentJob::Batch.enqueue(on_finish: DefaultAdapterCallbackJob, description: "defaults") do
      DefaultAdapterMemberJob.perform_later("a")
    end
    _(batch.persisted?).must_equal true
    _(batch.job_instances.map(&:class)).must_equal [DefaultAdapterMemberJob]
  end
end
