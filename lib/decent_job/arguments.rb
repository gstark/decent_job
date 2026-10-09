# frozen_string_literal: true

require "bigdecimal"
require "date"
require "time"
require "decent_job/support"

module DecentJob
  class SerializationError < ArgumentError; end

  class DeserializationError < StandardError
    def initialize(message = "Error while trying to deserialize arguments: #{$!&.message}")
      super
      set_backtrace($!.backtrace) if $!
    end
  end

  # Encodes job arguments as JSON-safe values. The markers match Active Job's,
  # so values that Active Job stored remain readable.
  module Arguments
    OBJECT_KEY = "_aj_serialized"
    GLOBALID_KEY = "_aj_globalid"
    SYMBOL_KEYS_KEY = "_aj_symbol_keys"
    RUBY2_KEYWORDS_KEY = "_aj_ruby2_keywords"
    INDIFFERENT_KEY = "_aj_hash_with_indifferent_access"
    RESERVED_KEYS = [GLOBALID_KEY, SYMBOL_KEYS_KEY, RUBY2_KEYWORDS_KEY, OBJECT_KEY, INDIFFERENT_KEY]
      .flat_map { |key| [key, key.to_sym] }.freeze

    # A serializer handles one class. Its name is stored with each value.
    class Serializer
      attr_reader :name, :klass

      def initialize(name, klass, dump:, load:, aliases: [])
        @name = name
        @klass = klass
        @aliases = aliases
        @dump = dump
        @load = load
      end

      def names
        [name, *@aliases]
      end

      def serialize?(argument)
        resolved = klass.is_a?(Proc) ? klass.call : klass
        !resolved.nil? && argument.is_a?(resolved)
      end

      def serialize(argument)
        {OBJECT_KEY => name}.merge(@dump.call(argument))
      end

      def deserialize(hash)
        @load.call(hash)
      end
    end

    @serializers = []
    @serializers_by_name = {}

    class << self
      # Custom serializers respond to name, serialize?, serialize and
      # deserialize. Later serializers take precedence.
      def add_serializers(*serializers)
        serializers.flatten.each do |serializer|
          @serializers.unshift(serializer)
          names = serializer.respond_to?(:names) ? serializer.names : [serializer.name]
          names.each { |name| @serializers_by_name[name] = serializer }
        end
      end

      def serialize(arguments)
        arguments.map { |argument| serialize_argument(argument) }
      end

      def deserialize(arguments)
        arguments.map { |argument| deserialize_argument(argument) }
      rescue
        raise DeserializationError
      end

      def serialize_argument(argument)
        case argument
        when nil, true, false, Integer, Float
          argument
        when String
          argument.instance_of?(String) ? argument : serialize_object(argument, fallback: argument)
        when Array
          argument.map { |item| serialize_argument(item) }
        when Hash
          if indifferent_hash?(argument)
            serialize_hash(argument).merge(INDIFFERENT_KEY => true)
          else
            marker = Hash.ruby2_keywords_hash?(argument) ? RUBY2_KEYWORDS_KEY : SYMBOL_KEYS_KEY
            serialize_hash(argument).merge(marker => argument.keys.grep(Symbol).map(&:name))
          end
        else
          if global_id?(argument)
            serialize_global_id(argument)
          else
            serialize_object(argument)
          end
        end
      end

      private

      def serialize_object(argument, fallback: nil)
        serializer = @serializers.find { |candidate| candidate.serialize?(argument) }
        return serializer.serialize(argument) if serializer
        return fallback if fallback

        raise SerializationError, "Unsupported argument type: #{argument.class.name}"
      end

      def serialize_hash(hash)
        hash.each_with_object({}) do |(key, value), result|
          result[serialize_key(key)] = serialize_argument(value)
        end
      end

      def serialize_key(key)
        raise SerializationError, "Can't serialize a Hash with reserved key #{key.inspect}" if RESERVED_KEYS.include?(key)
        return key if key.is_a?(String)
        return key.name if key.is_a?(Symbol)

        raise SerializationError, "Only string and symbol hash keys may be serialized as job arguments, but #{key.inspect} is a #{key.class}"
      end

      def deserialize_argument(argument)
        case argument
        when nil, true, false, String, Integer, Float
          argument
        when Array
          argument.map { |item| deserialize_argument(item) }
        when Hash
          if argument.size == 1 && argument.key?(GLOBALID_KEY)
            raise ArgumentError, "GlobalID is not loaded" unless defined?(::GlobalID::Locator)

            ::GlobalID::Locator.locate(argument.fetch(GLOBALID_KEY))
          elsif argument.key?(OBJECT_KEY)
            name = argument.fetch(OBJECT_KEY)
            serializer = @serializers_by_name[name] || raise(ArgumentError, "Serializer #{name} is not known")
            serializer.deserialize(argument)
          else
            deserialize_hash(argument)
          end
        else
          raise ArgumentError, "Can only deserialize primitive arguments: #{argument.inspect}"
        end
      end

      def deserialize_hash(hash)
        result = hash.transform_values { |value| deserialize_argument(value) }
        if result.delete(INDIFFERENT_KEY)
          defined?(::ActiveSupport::HashWithIndifferentAccess) ? ::ActiveSupport::HashWithIndifferentAccess.new(result) : result
        elsif (symbol_keys = result.delete(SYMBOL_KEYS_KEY))
          symbolize(result, symbol_keys)
        elsif (symbol_keys = result.delete(RUBY2_KEYWORDS_KEY))
          Hash.ruby2_keywords_hash(symbolize(result, symbol_keys))
        else
          result
        end
      end

      def symbolize(hash, symbol_keys)
        hash.transform_keys { |key| symbol_keys.include?(key) ? key.to_sym : key }
      end

      def indifferent_hash?(argument)
        defined?(::ActiveSupport::HashWithIndifferentAccess) && argument.is_a?(::ActiveSupport::HashWithIndifferentAccess)
      end

      def global_id?(argument)
        defined?(::GlobalID::Identification) && argument.is_a?(::GlobalID::Identification)
      end

      def serialize_global_id(argument)
        {GLOBALID_KEY => argument.to_global_id.to_s}
      rescue => error
        raise SerializationError, "Unable to serialize #{argument.class} without an id. (Maybe you forgot to call save?)" if error.class.name == "URI::GID::MissingModelIdError" # standard:disable Style/ClassEqualityComparison

        raise
      end
    end

    def self.builtin(name, klass, dump:, load:)
      Serializer.new("DecentJob::Arguments::#{name}", klass, aliases: ["ActiveJob::Serializers::#{name}"], dump: dump, load: load)
    end
    private_class_method :builtin

    # Later entries win, so list general classes before their subclasses.
    add_serializers(
      builtin("RangeSerializer", Range,
        dump: ->(range) { {"begin" => serialize_argument(range.begin), "end" => serialize_argument(range.end), "exclude_end" => range.exclude_end?} },
        load: ->(hash) { Range.new(*deserialize([hash["begin"], hash["end"]]), hash["exclude_end"]) }),
      builtin("ModuleSerializer", Module,
        dump: lambda { |constant|
          raise SerializationError, "Serializing an anonymous class is not supported" unless constant.name

          {"value" => constant.name}
        },
        load: ->(hash) { Support.constantize(hash.fetch("value")) }),
      builtin("BigDecimalSerializer", BigDecimal, dump: ->(value) { {"value" => value.to_s} }, load: ->(hash) { BigDecimal(hash.fetch("value")) }),
      builtin("DateSerializer", Date, dump: ->(date) { {"value" => date.iso8601} }, load: ->(hash) { Date.iso8601(hash.fetch("value")) }),
      builtin("DateTimeSerializer", DateTime, dump: ->(time) { {"value" => time.iso8601(9)} }, load: ->(hash) { DateTime.iso8601(hash.fetch("value")) }),
      builtin("TimeSerializer", Time, dump: ->(time) { {"value" => time.iso8601(9)} }, load: ->(hash) { Time.iso8601(hash.fetch("value")) }),
      builtin("SymbolSerializer", Symbol, dump: ->(symbol) { {"value" => symbol.name} }, load: ->(hash) { hash.fetch("value").to_sym }),
      # Host types activate only when the host has loaded them.
      builtin("DurationSerializer", -> { ::ActiveSupport::Duration if defined?(::ActiveSupport::Duration) },
        dump: ->(duration) { {"value" => duration.value, "parts" => serialize(duration.parts.to_a)} },
        load: ->(hash) { ::ActiveSupport::Duration.new(hash.fetch("value"), deserialize(hash.fetch("parts")).to_h) }),
      builtin("TimeWithZoneSerializer", -> { ::ActiveSupport::TimeWithZone if defined?(::ActiveSupport::TimeWithZone) },
        dump: ->(time) { {"value" => time.iso8601(9), "time_zone" => time.time_zone.tzinfo.name} },
        load: lambda { |hash|
          time = Time.iso8601(hash.fetch("value"))
          time.respond_to?(:in_time_zone) ? time.in_time_zone(hash["time_zone"]) : time
        })
    )
  end
end
