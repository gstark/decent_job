# frozen_string_literal: true

require "date"
require "decent_job/cron_schedule/field"
require "decent_job/cron_schedule/natural"
require "decent_job/cron_schedule/zone"

module DecentJob
  class CronSchedule
    ALIASES = {
      "@yearly" => "0 0 1 1 *", "@annually" => "0 0 1 1 *", "@monthly" => "0 0 1 * *",
      "@weekly" => "0 0 * * 0", "@daily" => "0 0 * * *", "@midnight" => "0 0 * * *",
      "@noon" => "0 12 * * *", "@hourly" => "0 * * * *"
    }.freeze
    MAXIMUM_MONTH_DAYS = [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31].freeze

    attr_reader :original

    def self.parse(expression)
      return expression if expression.is_a?(self)
      new(expression)
    rescue ArgumentError
      nil
    end

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

    def match?(time)
      local = @zone.local(time.to_time)
      includes?(@months, local.month) && day_matches?(local) && includes?(@hours, local.hour) && includes?(@minutes, local.min) && includes?(@seconds, local.sec)
    end

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

    def timezone_suffix?(part)
      Field.new(:weekdays, part.delete_suffix("&")).values
      false
    rescue ArgumentError
      true
    end

    def includes?(values, value)
      values.nil? || values.include?(value)
    end

    def compact_days
      return unless @months && @monthdays
      @months = @months.select { |month| @monthdays.any? { |day| day <= MAXIMUM_MONTH_DAYS[month] } }
      @monthdays = @monthdays.select { |day| @months.any? { |month| day <= MAXIMUM_MONTH_DAYS[month] } }
      raise ArgumentError, "cron expression has no possible calendar day" if @months.empty? || @monthdays.empty?
    end

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
