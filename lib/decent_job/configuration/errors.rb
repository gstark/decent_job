# frozen_string_literal: true

module DecentJob
  class Configuration
    class Errors
      def initialize
        @messages = {}
      end

      def initialize_copy(original)
        super
        @messages = original.to_h
      end

      def add(field, message)
        (@messages[field.to_sym] ||= []) << message
      end

      def [](field)
        @messages.fetch(field.to_sym, [])
      end

      def empty?
        @messages.empty?
      end

      def to_h
        @messages.transform_values { |messages| messages.map(&:dup) }
      end

      def full_messages
        @messages.flat_map { |field, messages| messages.map { |message| "#{field} #{message}" } }
      end
    end
  end
end
