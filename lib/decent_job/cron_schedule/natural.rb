# frozen_string_literal: true

require "decent_job/cron_schedule/natural/compiler"
require "decent_job/cron_schedule/natural/component"
require "decent_job/cron_schedule/natural/time_parser"
require "decent_job/cron_schedule/natural/vocabulary"

module DecentJob
  class CronSchedule
    # Converts a natural-language schedule into numeric cron text. It
    # reproduces the Fugit 1.12.1 grammar, including its odd results.
    #
    # {CronSchedule#initialize} calls {.compile} only after numeric parsing
    # fails. Each call creates a new parser, so there is no shared state.
    #
    # The parser is a hand-written recursive descent parser over +@source+
    # with a cursor, +@position+. Parsing is bounded (input is at most 256
    # characters) and ordered: +choice+ tries alternatives in order, and a
    # failed alternative rewinds the cursor. Rules return {Component} objects
    # (or arrays of them), and {Compiler} merges them into cron fields. The
    # +at+, +from+, and +in+ keywords are optional.
    #
    # Examples (input, then compiled output):
    #
    # - "every 15 minutes" gives "*/15 * * * *".
    # - "every weekday at 5pm" gives "0 17 * * 1-5".
    # - "every month on days 1,15 at 10:00" gives "0 10 1,15 * *".
    # - "every day at noon in Europe/Berlin" gives "0 12 * * * Europe/Berlin".
    # - "every 30 seconds" gives "*/30 * * * * *" (six fields).
    # - "every 15 minutes from 9 to 17" gives "*/15 9-16 * * *". The end hour
    #   is excluded.
    # - "every day at 9:30 and 17:45" gives "30 9 * * *". Only the first time
    #   group is kept.
    # - "every 2 hours and 5 minutes" gives "0 */2 * * *". Only the first
    #   "every" object is kept.
    # - "every first and fifteenth of the month" gives "0 0 1 * *". Only the
    #   first month day is kept.
    # - "every day at hours 8 and 12" gives "0 0 * * *". Hour points are
    #   parsed but never emitted.
    # - "3 hours" gives nil (a standalone duration).
    # - "every minute at 5" raises ArgumentError (a reference error).
    #
    # See docs/cron-compatibility.md.
    class Natural
      include TimeParser
      include Vocabulary

      # Compiles a natural-language schedule.
      #
      # @param expression [String] the phrase; surrounding whitespace is ignored
      # @return [String, nil] numeric cron text with five or six fields and an
      #   optional zone, or nil when the phrase is empty, longer than 256
      #   characters, a standalone duration, or not in the grammar. A
      #   named time with no hour, such as "at Noon", gives text with an empty
      #   hour field, which {CronSchedule} then rejects.
      # @raise [ArgumentError] when the phrase parses but the reference would
      #   fail on it, such as incompatible time clauses
      # @api private
      def self.compile(expression)
        new(expression).compile
      end

      def initialize(expression)
        @source = expression.strip
        @position = 0
      end

      # @return [String, nil] see {.compile}
      # @raise [ArgumentError] see {.compile}
      # @api private
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

      # Matches +pattern+ only at the cursor and advances past it. Returns the
      # matched text, or nil without moving the cursor.
      def take(pattern)
        match = pattern.match(@source, @position)
        return unless match && match.begin(0) == @position
        @position = match.end(0)
        match[0]
      end

      # Runs one rule and rewinds the cursor when the rule returns nil or false.
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

      # Parses one or more items joined by +separator+. A separator without a
      # following item is not consumed.
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

      # "every a and b" parses all objects but keeps only the first, as the
      # reference does.
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

      # "every 1st and 15th of the month" keeps only the first day, as the
      # reference does.
      def every_monthdays
        days = list(:ordinal, JOIN)
        return unless days && take(/[ \t]+of the month/)
        component(:monthday, days.first)
      end

      # "every N unit" becomes a step on the matching calendar field, not a
      # fixed duration. "M" is month and "m" is minute. Weak and strong
      # markers let explicit times override the interval defaults.
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
