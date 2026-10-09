# frozen_string_literal: true

require "active_job"
require "active_job/base"

module DecentJob
  module BatchProperties
    def self.dump(properties)
      raise ArgumentError, "Properties must be a Hash" unless properties.is_a?(Hash)

      ActiveJob::Arguments.serialize([properties]).first
    end

    def self.load(serialized_properties)
      ActiveJob::Arguments.deserialize([serialized_properties]).first
    end

    def self.display(serialized_properties)
      load(serialized_properties)
    rescue ActiveJob::DeserializationError
      serialized_properties
    end
  end
end
