# frozen_string_literal: true

module DecentJob
  class CronSchedule
    # Parses bounded cron fields without permitting unbounded range expansion.
    class Field
      MONTHS = %w[jan feb mar apr may jun jul aug sep oct nov dec].freeze
      WEEKDAYS = %w[sun mon tue wed thu fri sat].freeze
      BOUNDS = {seconds: [0, 59], minutes: [0, 59], hours: [0, 23], monthdays: [1, 31], months: [1, 12], weekdays: [0, 6]}.freeze
      TOKENS = {
        seconds: "[0-5]?\\d", minutes: "[0-5]?\\d", hours: "(?:2[0-4]|[01]?[0-9])",
        monthdays: "(?:-?(?:3[01]|[12][0-9]|0?[1-9])|last|l)",
        months: "(?:1[0-2]|0?[1-9]|#{MONTHS.join("|")})",
        weekdays: "(?:[0-7]|#{WEEKDAYS.join("|")})"
      }.freeze

      def initialize(kind, expression)
        @kind = kind
        @min, @max = BOUNDS.fetch(kind)
        @expression = expression
      end

      def values
        raise ArgumentError, "empty cron field" if @expression.empty?
        parts = @expression.split(",").reject(&:empty?)
        raise ArgumentError, "empty cron field" if parts.empty?
        result = parts.flat_map { |part| parse_part(part) }
        return (result.empty? ? nil : result.uniq.sort) if @kind == :weekdays
        return nil if result.include?(nil)
        result.map { |value| (@kind == :hours && value == 24) ? 0 : value }.uniq.sort
      end

      private

      def parse_part(part)
        token = TOKENS.fetch(@kind)

        if @kind == :weekdays && (match = /\A(#{token})(?:#(-?[1-5]|last|l)|%(\d+)(?:\+(\d+))?)\z/i.match(part))
          day = number(match[1]) % 7
          return [[day, number(match[2])]] if match[2]
          modulo = match[3].to_i
          raise ArgumentError, "weekday modulo must be positive" if modulo.zero?
          return [[day, [modulo, match[4].to_i]]]
        end

        match = /\A(?:(\*)|(#{token})(?:-(#{token}))?)?(?:\/(\d{1,2}))?\z/i.match(part)
        raise ArgumentError, "invalid cron field #{part.inspect}" unless match && !part.empty?
        first = match[2] && number(match[2])
        last = match[3] && number(match[3])
        step = match[4]&.to_i
        raise ArgumentError, "cron step must be positive" if step == 0
        return weekday_values(first, last, step) if @kind == :weekdays
        return [nil] if first.nil? && step.nil?
        return [first] if first && last.nil? && step.nil?
        first ||= @min
        last = @max if last.nil? || (last < 0 && first > 0)
        return [nil] if first == @min && last == @max && (step || 1) == 1
        raise ArgumentError, "mixed positive and negative bounds" if first < 0 && last > 0
        lower, upper = (first < 0) ? [-@max, -1] : [@min, @max]
        expanded = []
        current = first

        (2 * @max + 1).times do
          expanded << current
          break if current == last
          current += 1

          if current > upper
            current = lower
            last -= upper + 1 if last > upper
          end
        end

        raise ArgumentError, "invalid cron range" unless expanded.last == last
        expanded.each_with_index.filter_map { |value, index| value if index % (step || 1) == 0 }
      end

      def weekday_values(first, last, step)
        values = if step
          ((first || 0)..(last || first || 6)).step(step).to_a
        elsif last
          (first..((first > last) ? last + 7 : last)).to_a
        elsif first
          [first]
        else
          []
        end

        values.map { |value| [value % 7] }
      end

      def number(value)
        normalized = value.downcase
        return -1 if %w[l last].include?(normalized)
        return MONTHS.index(normalized) + 1 if @kind == :months && MONTHS.include?(normalized)
        return WEEKDAYS.index(normalized) if @kind == :weekdays && WEEKDAYS.include?(normalized)
        Integer(normalized, 10)
      end
    end
  end
end
