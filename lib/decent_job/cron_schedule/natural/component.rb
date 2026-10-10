# frozen_string_literal: true

module DecentJob
  class CronSchedule
    class Natural
      # A calendar field, or an hour/minute pair. Priority is per coordinate:
      # intervals supply defaults, while explicit minutes override those defaults.
      class Component
        attr_reader :key, :values, :weak, :strong

        def initialize(key, first, second = nil, weak: nil, strong: nil)
          @key = key
          @values = [first, second]
          @weak = weak
          @strong = strong
        end

        def graded?
          !weak.nil? || !strong.nil?
        end

        def merge(other)
          combined = values.each_index.map { |index| combine(other, index) }
          self.class.new(key, *combined)
        end

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
