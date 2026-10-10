# frozen_string_literal: true

require "decent_job/arguments"

module DecentJob
  # Encodes batch properties for decent_job_batches.properties.
  #
  # Properties use the job argument encoding ({Arguments}), so they support
  # the same types as job arguments, including symbols and GlobalID objects.
  # {Batch} calls it. It does not depend on Active Job, but Active Job
  # serializers act as a fallback when loaded.
  module BatchProperties
    # @param properties [Hash] the custom properties
    # @return [Hash] the JSON-ready encoded form
    # @raise [ArgumentError] when properties is not a Hash
    # @raise [SerializationError] when a value has no serializer
    def self.dump(properties)
      raise ArgumentError, "Properties must be a Hash" unless properties.is_a?(Hash)

      Arguments.serialize([properties]).first
    end

    # @param serialized_properties [Hash] the decoded JSON column
    # @return [Hash] the properties with Ruby values
    # @raise [DeserializationError] when a class or GlobalID target is missing
    # @note A nested batch GlobalID loads from the current execution database
    #   ({Batch.default_database}). {Batch} sets it to the batch's database
    #   first.
    def self.load(serialized_properties)
      Arguments.deserialize([serialized_properties]).first
    end

    # Loads properties for display without failing.
    #
    # @param serialized_properties [Hash] the decoded JSON column
    # @return [Hash] the loaded properties, or the serialized form when they
    #   cannot load
    def self.display(serialized_properties)
      load(serialized_properties)
    rescue DeserializationError
      serialized_properties
    end
  end
end
