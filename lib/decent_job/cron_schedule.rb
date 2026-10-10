# frozen_string_literal: true

require "date"
require "decent_job/cron_schedule/field"
require "decent_job/cron_schedule/natural"
require "decent_job/cron_schedule/zone"

module DecentJob
  # Parses a cron expression and finds its occurrences. It accepts numeric
  # cron with five or six fields (seconds first) and the natural-language
  # phrases of {Natural}. It matches Fugit 1.12.1 and et-orbi 1.4.0, as
  # pinned by GoodJob, except for the corrections in the doc.
  #
  # {CronEntry} parses its +cron+ param on each call; nothing caches the
  # result. A schedule is immutable after parsing and is safe to share across
  # threads. It reads +ENV["TZ"]+ and +Time.zone+ on each search, so it
  # follows changes to them.
  #
  # Field values are sorted integer arrays, or nil for "any value". See
  # {Field#values}. Weekdays are [day, modifier] pairs.
  #
  # Time zones: an optional last field names an IANA zone or a numeric
  # offset. Without it, {Zone} uses the implicit local zone: a numeric +TZ+
  # offset, then a +TZ+ IANA name, then +Time.zone+, then the native Ruby
  # local zone. All field matching uses wall time in that zone. Month and day
  # jumps land on local midnight through {Zone#at}. In a DST fold, that is
  # the earlier instant. In a DST gap, it is the first instant after the gap.
  # Hour, minute, and second steps add real seconds, so a skipped wall time
  # never matches and a repeated wall time can match twice. {#next_at}
  # suppresses the second match only when its wall time equals the search
  # origin's wall time (see there).
  #
  # See docs/cron-compatibility.md.
  class CronSchedule
    # Each alias expands to a five-field expression. A zone suffix can follow
    # the alias.
    ALIASES = {
      "@yearly" => "0 0 1 1 *", "@annually" => "0 0 1 1 *", "@monthly" => "0 0 1 * *",
      "@weekly" => "0 0 * * 0", "@daily" => "0 0 * * *", "@midnight" => "0 0 * * *",
      "@noon" => "0 12 * * *", "@hourly" => "0 * * * *"
    }.freeze
    # Longest length of each month, indexed by month number. February has 29
    # so that a schedule for February 29 stays valid.
    MAXIMUM_MONTH_DAYS = [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31].freeze

    # @return [String] the expression as given for numeric input, or the
    #   generated numeric cron for natural input
    attr_reader :original

    # Parses an expression without raising.
    #
    # @param expression [String, CronSchedule, Object] the expression; a
    #   schedule is returned as it is
    # @return [CronSchedule, nil] the schedule, or nil when the input is not a
    #   valid expression (also for a non-String, such as a Time)
    def self.parse(expression)
      return expression if expression.is_a?(self)

      new(expression)
    rescue ArgumentError
      nil
    end

    # Parses numeric cron first. When that fails, it compiles the input with
    # {Natural} and parses the result.
    #
    # @param expression [String] a numeric or natural-language expression
    # @raise [ArgumentError] when the input is not a String or not a valid
    #   expression, or when a natural phrase hits a reference error
    def initialize(expression)
      raise ArgumentError, "cron expression must be a string" unless expression.is_a?(String)

      @original = expression

      begin
        parse_numeric(expression)
      rescue ArgumentError
        normalized = Natural.compile(expression)
        raise ArgumentError, "invalid cron expression" unless normalized

        parse_numeric(normalized)
        @original = normalized
      end
    end

    # Tests one instant against all fields in the schedule's zone. Subsecond
    # parts are ignored. The fold suppression of {#next_at} does not apply.
    #
    # @param time [Time, #to_time] the instant to test
    # @return [Boolean] true when every field matches
    def match?(time)
      local = @zone.local(time.to_time)
      includes?(@months, local.month) && day_matches?(local) && includes?(@hours, local.hour) && includes?(@minutes, local.min) && includes?(@seconds, local.sec)
    end

    # Finds the first occurrence strictly after the origin, with one-second
    # resolution.
    #
    # The search skips a candidate in a DST fold when its wall time in the
    # schedule's zone equals the origin's wall time but has a different UTC
    # offset. This stops "30 1 * * *" from firing again at 01:30 standard
    # time right after it fired at 01:30 daylight time. The origin's wall time
    # comes from the implicit local zone for a default search, and from
    # +Time#getlocal+ (the process zone) for an explicit +after+. So with an
    # explicit +after+, the skip applies only when the schedule's zone has
    # the same wall time as the process zone.
    #
    # @param after [Time, #to_time, nil] the search origin, or nil for the
    #   current time
    # @return [Time] the occurrence as a UTC instant
    # @raise [ArgumentError] when no occurrence is found within 2048 search steps
    def next_at(after: nil)
      source = (after || Time.now).to_time
      # Default searches originate in the implicit local zone before applying
      # the expression's zone; explicit origins retain their input semantics.
      origin = after ? source.getlocal : Zone.new(nil).local(source)
      original_wall = origin.strftime("%F|%T")
      original_zone = @zone.local(source).strftime("%z|%Z")
      cursor = Time.at(source.to_i + 1).utc

      2048.times do
        local = @zone.local(cursor)

        if !includes?(@months, local.month)
          date = Date.new(local.year, local.month, 1) >> 1
          cursor = @zone.at(date.year, date.month)
        elsif !day_matches?(local)
          date = Date.new(local.year, local.month, local.day) + 1
          cursor = @zone.at(date.year, date.month, date.day)
        elsif !includes?(@hours, local.hour)
          cursor += (60 - local.min) * 60 - local.sec
        elsif !includes?(@minutes, local.min)
          cursor += 60 - local.sec
        elsif !includes?(@seconds, local.sec)
          next_second = @seconds.find { |second| second > local.sec }
          cursor += next_second ? next_second - local.sec : 60 - local.sec + @seconds.first
        elsif local.strftime("%F|%T") == original_wall && local.strftime("%z|%Z") != original_zone
          cursor += 1
        else
          return cursor.utc
        end
      end

      raise ArgumentError, "cron expression has no reachable occurrence: #{@original.inspect}"
    end

    # Returns the occurrences strictly inside +range+. Both ends are excluded,
    # also for an inclusive range, as in the reference.
    #
    # @param range [Range<Time>] the time range
    # @return [Array<Time>] UTC instants in ascending order
    # @raise [ArgumentError] when a search finds no reachable occurrence
    def within(range)
      result = []
      cursor = range.begin

      loop do
        cursor = next_at(after: cursor)
        break if cursor >= range.end

        result << cursor
      end

      result
    end

    private

    # Sets the zone and field values. Five fields get a "0" seconds field. A
    # trailing "&" on the monthday or weekday field sets +@day_and+, which
    # requires both day fields to match instead of either one.
    def parse_numeric(expression)
      normalized = expression.strip

      if normalized.start_with?("@")
        name, suffix = normalized.split(/\s+/, 2)
        normalized = [ALIASES.fetch(name) { raise ArgumentError, "invalid cron alias" }, suffix].compact.join(" ")
      end

      raise ArgumentError, "invalid cron expression" if normalized.match?(/[\r\n]/)

      # Whitespace is allowed before trailing commas and between repeated
      # commas, but never between a list separator and its following value.
      normalized = normalized.gsub(/,(?:[ \t]*,)+/, ",")
      raise ArgumentError, "invalid comma spacing" if normalized.match?(/[ \t]+,[^ \t&]/)

      parts = normalized.gsub(/[ \t]+,/, ",").split(/[ \t]+/)
      zone_name = parts.pop if parts.size > 5 && timezone_suffix?(parts.last)
      @zone = Zone.new(zone_name)
      raise ArgumentError, "expected five or six cron fields" unless [5, 6].include?(parts.size)

      parts.unshift("0") if parts.size == 5
      @day_and = parts[3].end_with?("&") || parts[5].end_with?("&")
      parts[3] = parts[3].delete_suffix("&")
      parts[5] = parts[5].delete_suffix("&")
      @seconds, @minutes, @hours, @monthdays, @months, @weekdays = Field::BOUNDS.keys.zip(parts).map { |kind, part| Field.new(kind, part).values }
      compact_days
    end

    # The last of six or seven parts is a zone only when it is not a valid
    # weekday field, so "0 0 * * * 1" stays a six-field expression.
    def timezone_suffix?(part)
      Field.new(:weekdays, part.delete_suffix("&")).values
      false
    rescue ArgumentError
      true
    end

    def includes?(values, value)
      values.nil? || values.include?(value)
    end

    # Drops months that none of the month days can reach, and month days that
    # no month can reach. It raises when nothing is left, so "0 0 30 2 *" fails
    # at parse time instead of during a search.
    def compact_days
      return unless @months && @monthdays

      @months = @months.select { |month| @monthdays.any? { |day| day <= MAXIMUM_MONTH_DAYS[month] } }
      @monthdays = @monthdays.select { |day| @months.any? { |month| day <= MAXIMUM_MONTH_DAYS[month] } }
      raise ArgumentError, "cron expression has no possible calendar day" if @months.empty? || @monthdays.empty?
    end

    # Negative month days count back from the last day (-1 is the last day).
    # A weekday modifier is an Integer for "#" (nth weekday of the month,
    # negative from the end) or [modulo, offset] for "%" (week number since
    # Monday, December 31, 2018). With both day fields restricted, either
    # one can match unless "&" was given.
    def day_matches?(time)
      last_day = Date.new(time.year, time.month, -1).day
      monthday = @monthdays.nil? || @monthdays.any? { |day| time.day == ((day < 0) ? last_day + day + 1 : day) }

      weekday = @weekdays.nil? || @weekdays.any? do |day, modifier|
        next false unless day == time.wday

        case modifier
        when Array then @zone.reference_week(time) % modifier[0] == modifier[1] % modifier[0]
        when Integer then [(time.day - 1).div(7) + 1, -((last_day - time.day).div(7) + 1)].include?(modifier)
        else true
        end
      end

      return monthday || weekday if @monthdays && @weekdays && !@day_and

      monthday && weekday
    end
  end
end
