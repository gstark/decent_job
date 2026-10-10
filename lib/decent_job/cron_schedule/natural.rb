# frozen_string_literal: true

require "decent_job/cron_schedule/natural/compiler"
require "decent_job/cron_schedule/natural/component"
require "decent_job/cron_schedule/natural/time_parser"
require "decent_job/cron_schedule/natural/vocabulary"

module DecentJob
  class CronSchedule
    # Converts the natural scheduling vocabulary into the same numeric cron
    # representation used by the calendar search. Parsing is bounded and ordered;
    # failed alternatives rewind the cursor without modifying collected fields.
    class Natural
      include TimeParser
      include Vocabulary

      def self.compile(expression)
        new(expression).compile
      end

      def initialize(expression)
        @source = expression.strip
        @position = 0
      end

      def compile
        return if @source.empty? || @source.length > 256 || duration?
        components = list(:element, /(?:[ \t]+|[ \t]*,[ \t]*)/)
        return unless components && @position == @source.length
        Compiler.new(components).compile
      end

      private

      def duration?
        # Duration parsing precedes natural scheduling in GoodJob. In
        # particular, "3 h 5" must not become an hour point and an ignored
        # hour-count clause. Scan duration tokens before the natural grammar.
        number = /(?:\d+\.\d*|(?:\d*\.)?\d+)/
        unit = /(?i:y(?:ears?)?|w(?:eeks?)?|d(?:ays?)?|h(?:ours?)?|s(?:ec(?:ond)?)?s?)|M|months?|m(?:in(?:ute)?s?)?/
        token = /[-+]?#{number} *(?:#{unit})/
        @source.match?(/\A(?:#{token}(?:[ \t,]+|(?i:and))*)*(?:#{token}|[-+]?#{number})\z/)
      end

      def take(pattern)
        match = pattern.match(@source, @position)
        return unless match && match.begin(0) == @position
        @position = match.end(0)
        match[0]
      end

      def attempt(method = nil)
        start = @position
        result = method ? send(method) : yield
        @position = start unless result
        result
      end

      def choice(*methods)
        methods.each do |method|
          result = attempt(method)
          return result if result
        end

        nil
      end

      def list(method, separator)
        first = attempt(method)
        return unless first
        result = [first]

        loop do
          item = attempt do
            take(separator) && attempt(method)
          end

          break unless item
          result << item
        end

        result
      end

      def keyword(word)
        take(/[ \t]*#{word}[ \t]+/i)
      end

      def component(key, value, second = nil, **priority)
        Component.new(key, value, second, **priority)
      end

      def element
        choice(:every, :from, :at, :timezone, :on)
      end

      def every
        return unless keyword("every")
        objects = list(:every_object, CONJUNCTION)
        objects&.first
      end

      def every_object
        choice(:every_weekdays, :every_monthdays, :interval, :named_interval, :single_interval)
      end

      def every_weekdays
        list(:weekday_range, CONJUNCTION)&.first
      end

      def weekday_range
        choice(:weekday_span, :weekdays)
      end

      def weekdays
        days = list(:weekday, JOIN)
        component(:weekday, days) if days
      end

      def weekday
        name = take(WEEKDAY_PATTERN)
        WEEKDAYS.index { |day| day.start_with?(name[0, 3].downcase) } if name
      end

      def weekday_span
        first = attempt(:weekday)
        return if first.nil? || !take(RANGE)
        last = attempt(:weekday)
        component(:weekday, "#{first}-#{last}") unless last.nil?
      end

      def ordinal
        value = take(ORDINAL_PATTERN)
        ORDINALS[value.downcase] if value
      end

      def every_monthdays
        days = list(:ordinal, JOIN)
        return unless days && take(/[ \t]+of the month/)
        component(:monthday, days.first)
      end

      def interval
        count = take(/\d+/) || "1"
        unit = take(/[ \t]*(?:seconds?|minutes?|hours?|days?|months?|sec|min|s|m|h|d|M)/)
        return unless unit
        unit = unit.strip
        value = (count == "1") ? "*" : "*/#{count}"

        case unit
        when /\A(?:M|mon)/ then component(:month, value)
        when /\Ad/ then component(:monthday, value, weak: true)
        when /\Ah/ then component(:time, value, 0, weak: 1)
        when /\Am/ then component(:time, "*", value, strong: 1)
        when /\As/ then component(:second, value)
        end
      end

      def named_interval
        component(:weekday, "1-5", weak: true) if take(/weekday/i)
      end

      def single_interval
        name = take(/(?:1[ \t]+)?(?:week|year)/)
        return unless name

        if name.end_with?("year")
          [component(:month, 1, weak: true), component(:monthday, 1, weak: true)]
        else
          component(:weekday, 0, weak: true)
        end
      end

      def from
        keyword("from")
        list(:from_object, CONJUNCTION)
      end

      def from_object
        choice(:weekday_span, :monthday_span, :time_span)
      end

      def monthday_span
        keyword("the")
        first = attempt(:ordinal)
        return unless first && keyword("to")
        keyword("the")
        last = attempt(:ordinal)
        component(:monthday, "#{first}-#{last}") if last
      end

      def on
        return unless keyword("on")
        list(:on_object, /[ \t]*and[ \t]+/i)
      end

      def on_object
        choice(:on_days, :weekdays, :on_minutes, :on_ordinals, :on_clock_unit)
      end

      def on_days
        return unless keyword("days?")
        days = list(:monthday, JOIN)
        days&.map { |day| component(:monthday, day) }
      end

      def monthday
        value = take(/3[0-1]|[0-2]?[0-9]/)
        value&.to_i
      end

      def on_minutes
        return unless keyword("minute")
        minute = take(/[0-5]?[0-9]/)
        return unless minute
        minutes = [minute]

        loop do
          value = attempt do
            if take(JOIN)
              keyword("minute")
              take(/[0-5]?[0-9]/)
            end
          end

          break unless value
          minutes << value
        end

        component(:time, "*", minutes.join(","), strong: 1)
      end

      def on_ordinals
        list(:the_ordinals, JOIN)
      end

      def the_ordinals
        return unless keyword("the")
        list(:ordinal, JOIN)&.map { |day| component(:monthday, day) }
      end

      def on_clock_unit
        text = take(/[ \t]*the[ \t]+(?:hour|minute)[ \t]*/i)
        return unless text
        text.match?(/hour/i) ? component(:time, 0, "*", strong: 0) : component(:time, "*", "*", strong: 1)
      end

      def timezone
        take(/(?:in|on)[ \t]+/i)
        value = take(/[A-Z][a-zA-Z0-9+-]+(?:\/[A-Z][a-zA-Z0-9+\-_]+){0,2}/) || take(/Z|UTC/) || take(/[-+](?:[01][0-9]|2[0-4])(?::?(?:00|15|30|45))?/)
        component(:zone, value) if value
      end
    end
  end
end
