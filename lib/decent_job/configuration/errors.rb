# frozen_string_literal: true

module DecentJob
  class Configuration
    # Collects validation messages by field.
    #
    # {Configuration#valid?} creates a fresh collection on each run and
    # {Validator} fills it. It is a small stand-in for the Active Model errors
    # API, so the core does not depend on Active Model. A copy owns its own
    # message arrays.
    #
    # Not thread-safe. Each configuration owns its collection.
    #
    # See docs/configuration.md.
    class Errors
      def initialize
        @messages = {}
      end

      # Copies the message arrays, so a copy can change without touching the
      # original.
      def initialize_copy(original)
        super

        @messages = original.to_h
      end

      # Records one message for a field.
      #
      # @param field [Symbol, String] the setting name; stored as a Symbol
      # @param message [String] the message, without the field name
      # @return [Array<String>] the messages for the field
      def add(field, message)
        (@messages[field.to_sym] ||= []) << message
      end

      # @param field [Symbol, String] the setting name
      # @return [Array<String>] the messages for the field, or an empty array
      #   when the field has none
      # @note Returns a copy, so changing it does not change the errors.
      def [](field)
        @messages.fetch(field.to_sym, []).dup
      end

      # @return [Boolean] true when no field has a message
      def empty?
        @messages.empty?
      end

      # @return [Hash{Symbol => Array<String>}] a copy of all messages by field
      def to_h
        @messages.transform_values { |messages| messages.map(&:dup) }
      end

      # @return [Array<String>] each message prefixed with its field name, for
      #   example "max_threads must be a positive integer"
      def full_messages
        @messages.flat_map { |field, messages| messages.map { |message| "#{field} #{message}" } }
      end
    end
  end
end
