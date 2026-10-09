# frozen_string_literal: true

require "spec_helper"
require "decent_job/batch_properties"

class BatchPropertiesRecord
  include GlobalID::Identification

  attr_reader :id

  def initialize(id)
    @id = id
  end

  def self.find(id)
    raise KeyError, "record no longer exists" if id == "missing"
    new(id)
  end

  def to_global_id(options = {})
    super(options.merge(app: "decent-job-spec"))
  end
end

describe DecentJob::BatchProperties do
  it "round trips symbol and string keys through JSON storage" do
    properties = {:account => "example", "count" => 3, :nested => {:enabled => true, "items" => [nil, false, {name: "item"}]}}
    serialized = DecentJob::BatchProperties.dump(properties)

    _(serialized).must_equal ActiveJob::Arguments.serialize([properties]).first
    _(DecentJob::BatchProperties.load(JSON.parse(JSON.generate(serialized)))).must_equal properties
  end

  it "round trips dates, times, decimals, symbols, and nested arrays" do
    properties = {date: Date.new(2026, 10, 9), time: Time.utc(2026, 10, 9, 13, 24, 35),
                  nested: [[DateTime.new(2026, 10, 9, 13, 24, 35), BigDecimal("12.30"), :complete]]}
    serialized = DecentJob::BatchProperties.dump(properties)

    _(DecentJob::BatchProperties.load(JSON.parse(JSON.generate(serialized)))).must_equal properties
  end

  it "resolves GlobalID records through their real locator" do
    properties = {record: BatchPropertiesRecord.new("42")}
    serialized = DecentJob::BatchProperties.dump(properties)
    restored = DecentJob::BatchProperties.load(JSON.parse(JSON.generate(serialized)))

    _(serialized.fetch("record").fetch("_aj_globalid")).must_equal "gid://decent-job-spec/BatchPropertiesRecord/42"
    _(restored.fetch(:record)).must_be_kind_of BatchPropertiesRecord
    _(restored.fetch(:record).id).must_equal "42"
  end

  it "round trips an empty hash" do
    _(DecentJob::BatchProperties.load(DecentJob::BatchProperties.dump({}))).must_equal({})
  end

  it "does not mutate the input hash or serialized payload" do
    properties = {nested: [{"value" => "original"}]}
    original = Marshal.load(Marshal.dump(properties))
    serialized = DecentJob::BatchProperties.dump(properties)
    saved_payload = Marshal.load(Marshal.dump(serialized))
    restored = DecentJob::BatchProperties.load(serialized)
    restored[:nested] << "new"

    _(properties).must_equal original
    _(serialized).must_equal saved_payload
  end

  [nil, [], "properties", 42, true].each do |properties|
    it "rejects #{properties.inspect} instead of serializing non-hash properties" do
      error = _ { DecentJob::BatchProperties.dump(properties) }.must_raise ArgumentError

      _(error.message).must_include "Hash"
    end
  end

  it "rejects unsupported nested values" do
    _ { DecentJob::BatchProperties.dump(record: Object.new) }.must_raise DecentJob::SerializationError
  end

  it "displays decoded properties when records can be loaded" do
    properties = {:date => Date.new(2026, 10, 9), "nested" => [{state: :done}]}

    _(DecentJob::BatchProperties.display(DecentJob::BatchProperties.dump(properties))).must_equal properties
  end

  it "raises during load when a GlobalID record no longer exists" do
    serialized = DecentJob::BatchProperties.dump(record: BatchPropertiesRecord.new("missing"))

    _ { DecentJob::BatchProperties.load(serialized) }.must_raise DecentJob::DeserializationError
  end

  it "displays raw properties when a GlobalID record no longer exists" do
    serialized = DecentJob::BatchProperties.dump(record: BatchPropertiesRecord.new("missing"))

    _(DecentJob::BatchProperties.display(serialized)).must_be_same_as serialized
  end

  it "displays raw properties when the GlobalID model no longer exists" do
    serialized = {"record" => {"_aj_globalid" => "gid://decent-job-spec/RemovedBatchPropertiesRecord/42"}, "_aj_symbol_keys" => ["record"]}

    _ { DecentJob::BatchProperties.load(serialized) }.must_raise DecentJob::DeserializationError
    _(DecentJob::BatchProperties.display(serialized)).must_be_same_as serialized
  end
end
