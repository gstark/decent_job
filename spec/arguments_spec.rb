# frozen_string_literal: true

require "bigdecimal"
require "spec_helper"

describe DecentJob::Arguments do
  values = [
    nil, true, false, 1, 1.5, "text", :symbol, BigDecimal("1.23"),
    Date.new(2026, 10, 9), DateTime.new(2026, 10, 9, 12, 30, 15.5r), Time.utc(2026, 10, 9, 12, 30, 15, 123456.789r),
    1..5, (1.0...2.0), String, DecentJob::Base,
    [1, [:nested]], {"string" => 1, :symbol => {deep: [:a]}}
  ]

  it "round-trips every supported type" do
    serialized = DecentJob::Arguments.serialize(values)
    _(JSON.parse(JSON.generate(serialized))).must_equal serialized
    _(DecentJob::Arguments.deserialize(serialized)).must_equal values
  end

  it "reads values that Active Job stored and writes values Active Job can read" do
    _(DecentJob::Arguments.deserialize(ActiveJob::Arguments.serialize(values))).must_equal values
    without_modules = values.grep_v(Module)
    _(ActiveJob::Arguments.deserialize(DecentJob::Arguments.serialize(without_modules).map { |value| rename_serializers(value) })).must_equal without_modules
  end

  it "keeps keyword hashes flagged for keyword splats" do
    hash = Hash.ruby2_keywords_hash({amount: 1})
    restored = DecentJob::Arguments.deserialize(DecentJob::Arguments.serialize([hash])).first
    _(Hash.ruby2_keywords_hash?(restored)).must_equal true
    _(Hash.ruby2_keywords_hash?(DecentJob::Arguments.deserialize(DecentJob::Arguments.serialize([{amount: 1}])).first)).must_equal false
  end

  it "round-trips host types when ActiveSupport is loaded" do
    require "active_support/core_ext/numeric/time"
    require "active_support/core_ext/time/zones"
    zoned = Time.utc(2026, 10, 9, 12).in_time_zone("America/Chicago")
    indifferent = ActiveSupport::HashWithIndifferentAccess.new("a" => 1)
    restored = DecentJob::Arguments.deserialize(DecentJob::Arguments.serialize([5.minutes, zoned, indifferent]))
    _(restored[0]).must_equal 5.minutes
    _(restored[1]).must_equal zoned
    _(restored[1].time_zone.name).must_equal "America/Chicago"
    _(restored[2]).must_be_kind_of ActiveSupport::HashWithIndifferentAccess
    _(restored[2][:a]).must_equal 1
  end

  it "round-trips GlobalID records when GlobalID is loaded" do
    require "globalid"
    GlobalID.app ||= "decent-job-spec"

    record_class = Class.new do
      include GlobalID::Identification

      attr_reader :id

      def self.name = "ArgumentsSpecRecord"

      def self.find(id) = new(id)

      def initialize(id) = @id = id

      def ==(other) = other.is_a?(self.class) && other.id == id
    end

    Object.const_set(:ArgumentsSpecRecord, record_class)
    serialized = DecentJob::Arguments.serialize([record_class.new("7")])
    _(serialized).must_equal [{"_aj_globalid" => "gid://#{GlobalID.app}/ArgumentsSpecRecord/7"}]
    _(DecentJob::Arguments.deserialize(serialized)).must_equal [record_class.new("7")]
  ensure
    Object.send(:remove_const, :ArgumentsSpecRecord) if Object.const_defined?(:ArgumentsSpecRecord)
  end

  it "rejects unsupported values and reserved or non-string keys" do
    _ { DecentJob::Arguments.serialize([Object.new]) }.must_raise DecentJob::SerializationError
    _ { DecentJob::Arguments.serialize([{"_aj_symbol_keys" => 1}]) }.must_raise DecentJob::SerializationError
    _ { DecentJob::Arguments.serialize([{1 => 2}]) }.must_raise DecentJob::SerializationError
    _ { DecentJob::Arguments.serialize([Class.new]) }.must_raise DecentJob::SerializationError
  end

  it "wraps deserialization failures" do
    error = _ { DecentJob::Arguments.deserialize([{"_aj_serialized" => "Missing::Serializer"}]) }.must_raise DecentJob::DeserializationError
    _(error.message).must_include "Missing::Serializer"
  end

  it "uses custom serializers before built-in ones" do
    point = Struct.new(:x, :y)
    serializer = Object.new
    serializer.define_singleton_method(:name) { "PointSerializer" }
    serializer.define_singleton_method(:serialize?) { |value| value.is_a?(point) }
    serializer.define_singleton_method(:serialize) { |value| {"_aj_serialized" => name, "x" => value.x, "y" => value.y} }
    serializer.define_singleton_method(:deserialize) { |hash| point.new(hash["x"], hash["y"]) }
    DecentJob::Arguments.add_serializers(serializer)
    _(DecentJob::Arguments.deserialize(DecentJob::Arguments.serialize([point.new(1, 2)]))).must_equal [point.new(1, 2)]
  end

  def rename_serializers(value)
    case value
    when Array then value.map { |item| rename_serializers(item) }
    when Hash
      value.to_h do |key, item|
        item = item.sub("DecentJob::Arguments::", "ActiveJob::Serializers::") if key == "_aj_serialized"
        [key, rename_serializers(item)]
      end
    else value
    end
  end
end
