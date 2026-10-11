# frozen_string_literal: true

require "spec_helper"

describe "enqueue connection configuration" do
  before { @environment = %w[DECENT_JOB_DATABASE_URL DATABASE_URL].to_h { |name| [name, ENV.delete(name)] } }

  after do
    @environment.each_key { |name| ENV.delete(name) }
    @environment.each { |name, value| ENV[name] = value if value }
  end

  def rails(configuration, options = {}, active_record: true)
    configuration.apply_rails_config(options, environment: "production", logger: nil, active_record: active_record)
  end

  it "defaults to the Active Record connection only in a Rails host with Active Record" do
    _(DecentJob::Configuration.new.connection).must_equal :own
    _(rails(DecentJob::Configuration.new, {}, active_record: false).connection).must_equal :own
    _(rails(DecentJob::Configuration.new).connection).must_equal :active_record
  end

  it "defaults to the own connection when database_url is explicit" do
    _(rails(DecentJob::Configuration.new, {database_url: "postgres:///other"}).connection).must_equal :own

    configuration = DecentJob::Configuration.new
    configuration.database_url = "postgres:///other"
    _(rails(configuration).connection).must_equal :own

    ENV["DECENT_JOB_DATABASE_URL"] = "postgres:///other"
    _(rails(DecentJob::Configuration.new).connection).must_equal :own
  end

  it "does not count the DATABASE_URL fallback as an explicit database" do
    ENV["DATABASE_URL"] = "postgres:///application"
    configuration = rails(DecentJob::Configuration.new)
    _(configuration.database_url).must_equal "postgres:///application"
    _(configuration.connection).must_equal :active_record
  end

  it "lets explicit and Rails settings override the defaults" do
    _(rails(DecentJob::Configuration.new, {connection: "own"}).connection).must_equal :own
    _(rails(DecentJob::Configuration.new, {connection: :active_record, database_url: "postgres:///other"}).connection).must_equal :active_record

    configuration = rails(DecentJob::Configuration.new, {connection: :own})
    configuration.connection = :active_record
    _(configuration.connection).must_equal :active_record
    configuration.connection = nil
    _(configuration.connection).must_equal :own

    [:shared, "Own", 1].each do |value|
      _ { configuration.connection = value }.must_raise ArgumentError
    end
  end

  it "rejects the Active Record connection without Active Record" do
    configuration = DecentJob::Configuration.new
    configuration.connection = :active_record
    _(configuration.valid?).must_equal false
    _(configuration.errors.full_messages).must_include "connection active_record requires Active Record"
    _(configuration.database_configured?).must_equal true
  end
end
