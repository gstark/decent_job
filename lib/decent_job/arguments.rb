# frozen_string_literal: true

require "bigdecimal"
require "date"
require "time"
require "uri"
require "decent_job/support"

module DecentJob
  # Raised when a job argument or batch property cannot be encoded. It is an
  # ArgumentError, as ActiveJob::SerializationError is.
  class SerializationError < ArgumentError; end

  # Raised when stored arguments or batch properties cannot be decoded, for
  # example when a serializer or GlobalID record is gone. It keeps the
  # message and backtrace of the error that is being handled ($!), so raise it
  # from a rescue block. {Integrations::Native#deserialization_error} returns
  # this class.
  class DeserializationError < StandardError
    # @param message [String] defaults to a message built from $!
    def initialize(message = "Error while trying to deserialize arguments: #{$!&.message}")
      super
      set_backtrace($!.backtrace) if $!
    end
  end

  # Encodes job arguments as JSON-safe values. The markers match Active Job's,
  # so values that Active Job stored remain readable.
  #
  # {Base::Core#serialize} and {Base::Core#deserialize_arguments_if_needed}
  # call it for native jobs. {BatchProperties} uses it for batch properties.
  #
  # Native values store DecentJob serializer names, for example
  # "DecentJob::Arguments::SymbolSerializer". Reading also accepts the
  # "ActiveJob::Serializers::*" names. Active Job cannot read the DecentJob
  # names. ActiveSupport and GlobalID types are encoded only when the host
  # has loaded them; this module never loads them.
  #
  # The serializer registry is process global. Register serializers at boot,
  # before threads enqueue or run jobs, because registration is not
  # synchronized.
  #
  # See the Arguments section of docs/native-jobs.md.
  module Arguments
    OBJECT_KEY = "_aj_serialized"
    GLOBALID_KEY = "_aj_globalid"
    SYMBOL_KEYS_KEY = "_aj_symbol_keys"
    RUBY2_KEYWORDS_KEY = "_aj_ruby2_keywords"
    INDIFFERENT_KEY = "_aj_hash_with_indifferent_access"
    RESERVED_KEYS = [GLOBALID_KEY, SYMBOL_KEYS_KEY, RUBY2_KEYWORDS_KEY, OBJECT_KEY, INDIFFERENT_KEY]
      .flat_map { |key| [key, key.to_sym] }.freeze

    # A serializer handles one class. Its name is stored with each value.
    # Used for the built-in serializers. Custom serializers can be any object
    # with the same duck type.
    class Serializer
      attr_reader :name, :klass

      # @param name [String] the name stored under "_aj_serialized"
      # @param klass [Class, Proc] the handled class, or a Proc that returns
      #   it or nil when the class is not loaded
      # @param dump [Proc] value to Hash of extra keys
      # @param load [Proc] stored Hash to value
      # @param aliases [Array<String>] other names accepted when reading
      def initialize(name, klass, dump:, load:, aliases: [])
        @name = name
        @klass = klass
        @aliases = aliases
        @dump = dump
        @load = load
      end

      # @return [Array<String>] the name and its aliases
      def names
        [name, *@aliases]
      end

      # @param argument [Object]
      # @return [Boolean] true when argument is an instance of the class
      def serialize?(argument)
        resolved = klass.is_a?(Proc) ? klass.call : klass
        !resolved.nil? && argument.is_a?(resolved)
      end

      # @param argument [Object]
      # @return [Hash{String => Object}] the value with the "_aj_serialized" marker
      def serialize(argument)
        {OBJECT_KEY => name}.merge(@dump.call(argument))
      end

      # @param hash [Hash{String => Object}] a value from {#serialize}
      # @return [Object]
      def deserialize(hash)
        @load.call(hash)
      end
    end

    @serializers = []
    @serializers_by_name = {}

    class << self
      # Serializes values no registered serializer handles. It responds to
      # serialize(value) and deserialize(hash). The Active Job bridge sets it to
      # ActiveJob::Serializers so application serializers keep working.
      attr_accessor :fallback

      # Registers serializers.
      #
      # Custom serializers respond to name, serialize?, serialize and
      # deserialize. Later serializers take precedence.
      #
      # @param serializers [Array<#name, #serialize?, #serialize, #deserialize>]
      #   nested arrays are flattened. A serializer that responds to names is
      #   also found by its aliases.
      # @return [void]
      # @note Not thread safe. Call it at boot.
      def add_serializers(*serializers)
        serializers.flatten.each do |serializer|
          @serializers.unshift(serializer)
          names = serializer.respond_to?(:names) ? serializer.names : [serializer.name]
          names.each { |name| @serializers_by_name[name] = serializer }
        end
      end

      # @param arguments [Array] the perform arguments
      # @return [Array] JSON-safe values
      # @raise [SerializationError] when a value has no serializer
      def serialize(arguments)
        arguments.map { |argument| serialize_argument(argument) }
      end

      # @param arguments [Array] values from {.serialize}
      # @return [Array] the Ruby values
      # @raise [DeserializationError] when any value fails to load. The
      #   original error is in its message and backtrace.
      def deserialize(arguments)
        arguments.map { |argument| deserialize_argument(argument) }
      rescue
        raise DeserializationError
      end

      # Encodes one value.
      #
      # Plain strings pass through. A String subclass goes to the serializers
      # and falls back to the plain string. Hashes record which keys were
      # symbols and whether the hash is a ruby2_keywords hash, so perform gets
      # keywords back. An object with decent_job_global_id (a {Batch}) is
      # stored as a GlobalID without the globalid gem.
      #
      # @param argument [Object]
      # @return [Object] a JSON-safe value
      # @raise [SerializationError] when the value has no serializer, a hash
      #   uses a reserved key, or a key is not a String or Symbol
      def serialize_argument(argument)
        case argument
        when nil, true, false, Integer, Float
          argument
        when String
          argument.instance_of?(String) ? argument : serialize_object(argument, fallback_value: argument)
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
          if argument.respond_to?(:decent_job_global_id)
            {GLOBALID_KEY => argument.decent_job_global_id}
          elsif global_id?(argument)
            serialize_global_id(argument)
          else
            serialize_object(argument)
          end
        end
      end

      private

      # Tries the registry, then the fallback. fallback_value is returned
      # when neither handles the value, also when the fallback raises.
      def serialize_object(argument, fallback_value: nil)
        serializer = @serializers.find { |candidate| candidate.serialize?(argument) }
        return serializer.serialize(argument) if serializer

        begin
          return fallback.serialize(argument) if fallback
        rescue => error
          raise SerializationError, error.message unless fallback_value
        end
        return fallback_value if fallback_value

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
            locate(argument.fetch(GLOBALID_KEY))
          elsif argument.key?(OBJECT_KEY)
            name = argument.fetch(OBJECT_KEY)
            serializer = @serializers_by_name[name]
            return serializer.deserialize(argument) if serializer
            return fallback.deserialize(argument) if fallback

            raise ArgumentError, "Serializer #{name} is not known"
          else
            deserialize_hash(argument)
          end
        else
          raise ArgumentError, "Can only deserialize primitive arguments: #{argument.inspect}"
        end
      end

      # Without GlobalID, find the record through its class's find method.
      def locate(gid)
        return ::GlobalID::Locator.locate(gid) if defined?(::GlobalID::Locator)

        match = %r{\Agid://[^/]+/([^/]+)/([^/?]+)}.match(gid) || raise(ArgumentError, "Invalid GlobalID #{gid}")
        Support.constantize(match[1]).find(URI.decode_www_form_component(match[2]))
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

    # Builds a built-in serializer that also reads the Active Job name.
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
