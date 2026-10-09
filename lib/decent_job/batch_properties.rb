# frozen_string_literal: true

require "decent_job/arguments"

module DecentJob
  module BatchProperties
    def self.dump(properties)
      raise ArgumentError, "Properties must be a Hash" unless properties.is_a?(Hash)

      Arguments.serialize([properties]).first
    end

    def self.load(serialized_properties)
      Arguments.deserialize([serialized_properties]).first
    end

    def self.display(serialized_properties)
      load(serialized_properties)
    rescue DeserializationError
      serialized_properties
    end
  end
end
