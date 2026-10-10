# frozen_string_literal: true

module DecentJob
  class CronSchedule
    class Natural
      # One parsed clause of a natural schedule: a calendar field, or an
      # hour/minute pair. The {Natural} grammar rules create components, and
      # {Compiler} merges them.
      #
      # +key+ is one of :second, :hour, :monthday, :month, :weekday, :time, or
      # :zone. +values+ always has two items. For :time it is [hour, minute].
      # For other keys the second item is nil.
      # A value is an Integer, an Array, or cron text such as "*/15" or "1-5".
      #
      # Priority is per coordinate: intervals supply defaults, while explicit
      # minutes override those defaults. +weak+ and +strong+ are nil, true
      # (all coordinates), or a coordinate index (0 for hour, 1 for minute).
      # In a merge, a strong coordinate wins, then the side that is not weak.
      # Two unranked values are unioned.
      class Component
        attr_reader :key, :values, :weak, :strong

        # @param key [Symbol] the field key
        # @param first [Object] the field value, or the hour for :time
        # @param second [Object, nil] the minute for :time, otherwise nil
        # @param weak [Boolean, Integer, nil] coordinates that yield in a merge
        # @param strong [Boolean, Integer, nil] coordinates that win in a merge
        # @api private
        def initialize(key, first, second = nil, weak: nil, strong: nil)
          @key = key
          @values = [first, second]
          @weak = weak
          @strong = strong
        end

        # @return [Boolean] true when the component has a weak or strong marker
        # @api private
        def graded?
          !weak.nil? || !strong.nil?
        end

        # Merges two components of the same key, coordinate by coordinate.
        # The result has no markers.
        #
        # @param other [Component] the other component
        # @return [Component] a new component
        # @raise [ArgumentError] when a whole-hour point meets a strong minute
        # @api private
        def merge(other)
          combined = values.each_index.map { |index| combine(other, index) }
          self.class.new(key, *combined)
        end

        # Returns the hour range of a time span such as "from 9 to 17". A
        # strong minute merged with it covers the hours before the end hour.
        #
        # @return [Array(Integer, Integer), nil] the start and end hours, or nil
        #   when this is not a :time with minute 0 or its hour is not a range
        # @raise [ArgumentError] when this is a single whole-hour point
        # @api private
        def hour_range
          value = values[0]
          return unless key == :time && values[1] == 0
          # The reference cannot combine whole-hour points with a stronger
          # minute constraint. Reject it through the public parse contract.
          raise ArgumentError, "incompatible whole-hour point" unless value.is_a?(String)
          match = /\A(\d+)-(\d+)\z/.match(value)
          match&.captures&.map(&:to_i)
        end

        private

        def combine(other, index)
          left = values[index]
          right = other.values[index]
          return left if right.nil?
          return right if left.nil?
          range = if index == 0
            if other.strong == 1
              hour_range
            elsif strong == 1
              other.hour_range
            end
          end
          if range
            first, last = range
            return (first == last - 1) ? first : "#{first}-#{last - 1}"
          end
          return left if strong == index || strong == true
          return right if other.strong == index || other.strong == true
          return left if other.weak == index || other.weak == true
          return right if weak == index || weak == true
          return ["*"] if left == "*" && right == "*"
          ((left == "*") ? [] : Array(left)) + ((right == "*") ? [] : Array(right))
        end
      end
    end
  end
end
