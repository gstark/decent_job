# frozen_string_literal: true

module DecentJob
  class CronSchedule
    # Parses one numeric cron field into its allowed values. Parsing is
    # bounded: range expansion stops after 2 * max + 1 steps, and steps have at
    # most two digits.
    #
    # {CronSchedule} creates one field per part while it parses, and also uses
    # a weekday field to test whether a trailing part is a zone.
    #
    # Accepted syntax: "*", values, names (jan-dec, sun-sat), ranges, steps,
    # and lists. Ranges wrap, so hours "22-2" gives 22, 23, 0, 1, 2. Hour 24
    # means 0 and weekday 7 means 0. Month days accept negative values and
    # "L" or "last" (-1). Weekdays also accept "#n" (nth weekday, -1 or "L" for
    # the last) and "%m+o" (every m weeks with offset o).
    #
    # See docs/cron-compatibility.md.
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

      # @param kind [Symbol] a key of {BOUNDS}
      # @param expression [String] the field text
      # @raise [KeyError] when +kind+ is unknown
      # @api private
      def initialize(kind, expression)
        @kind = kind
        @min, @max = BOUNDS.fetch(kind)
        @expression = expression
      end

      # Expands the field.
      #
      # @return [Array<Integer>, Array<Array>, nil] sorted unique values; for
      #   weekdays, [day] or [day, modifier] pairs where the modifier is an
      #   Integer for "#" or [modulo, offset] for "%". Nil means any value. A
      #   "*" or a full range with step 1 anywhere in a non-weekday list gives nil.
      # @raise [ArgumentError] when the field is empty or invalid, or a step or
      #   modulo is zero
      # @api private
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

      # Weekday ranges wrap past Saturday ("fri-mon"). A step without an end
      # covers only the start day, unlike the other kinds.
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
