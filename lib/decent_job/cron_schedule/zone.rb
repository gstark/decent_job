# frozen_string_literal: true

require "decent_job/cron_schedule/local_zone"
require "tzinfo"

module DecentJob
  class CronSchedule
    # Converts between UTC instants and wall time for one schedule zone.
    #
    # {CronSchedule} creates one zone per schedule. The zone is an explicit
    # numeric offset, an explicit IANA zone from TZInfo, or the implicit local
    # zone (name nil). The implicit zone is resolved again on each call
    # through {LocalZone}: a numeric +TZ+ offset, then a +TZ+ IANA name, then
    # +Time.zone+, then the native Ruby local zone. It never changes
    # +ENV["TZ"]+ or other process state.
    #
    # See docs/cron-compatibility.md.
    class Zone
      # @param name [String, nil] an IANA name such as "Europe/Berlin", an
      #   offset such as "+05:30" or "-0030" (minutes 00, 15, 30, or 45), or
      #   nil for the implicit local zone
      # @raise [ArgumentError] when the name is not a valid zone or offset
      # @api private
      def initialize(name)
        @local = name.nil?

        if name&.match?(/\A[+-]/)
          match = /\A([+-])([01]\d|2[0-3]):?(00|15|30|45)\z/.match(name)
          raise ArgumentError, "invalid timezone" unless match
          @offset = (match[2].to_i * 3600 + match[3].to_i * 60) * ((match[1] == "-") ? -1 : 1)
        elsif name
          raise ArgumentError, "invalid timezone" unless name.match?(/\A[A-Z][a-zA-Z0-9+-]+(?:\/[A-Z][a-zA-Z0-9+\-_]+){0,2}\z/)
          @timezone = TZInfo::Timezone.get(name)
        end
      rescue TZInfo::InvalidTimezoneIdentifier
        raise ArgumentError, "invalid timezone"
      end

      # @param time [Time] an instant
      # @return [Time] the same instant with the wall time of this zone
      # @api private
      def local(time)
        offset = @local ? LocalZone.offset : @offset
        return time.getlocal(offset) if offset
        timezone = @local ? LocalZone.timezone : @timezone
        timezone ? timezone.to_local(time) : time.getlocal
      end

      # Returns the first instant of a local wall hour.
      #
      # In a DST fold, it returns the earlier instant. In a DST gap, it returns
      # the transition instant, which is the first valid instant after the gap.
      #
      # @param year [Integer] the local year
      # @param month [Integer] the local month
      # @param day [Integer] the local day
      # @param hour [Integer] the local hour
      # @return [Time] the instant (UTC, or with the fixed offset of this zone)
      # @raise [ArgumentError] when TZInfo reports a gap without a transition
      # @api private
      def at(year, month, day = 1, hour = 0)
        offset = @local ? LocalZone.offset : @offset
        return Time.new(year, month, day, hour, 0, 0, offset) if offset
        wall = Time.utc(year, month, day, hour)
        timezone = @local ? LocalZone.timezone : @timezone
        return LocalZone.at(wall) unless timezone
        periods = timezone.periods_for_local(wall)

        if periods.empty?
          # A skipped wall time begins at the transition, which need not land
          # on an hour or even a minute boundary in historical timezone data.
          transition = timezone.transitions_up_to(wall + 172_800, wall - 172_800).find do |change|
            previous_wall = change.at.to_i + change.previous_offset.observed_utc_offset
            next_wall = change.at.to_i + change.offset.observed_utc_offset
            wall.to_i >= previous_wall && wall.to_i < next_wall
          end

          raise ArgumentError, "timezone has no valid local time" unless transition
          return Time.at(transition.at.to_i).utc
        end

        Time.at(wall.to_i - periods.first.observed_utc_offset).utc
      end

      # Counts whole weeks from Monday, December 31, 2018, the et-orbi 1.4.0
      # reference, for the weekday "%" modifier. It compares local noons so
      # that DST changes do not shift the day count.
      #
      # @param time [Time] a wall time in this zone
      # @return [Integer] the week number; negative before the reference
      # @api private
      def reference_week(time)
        noon = at(time.year, time.month, time.day, 12)
        ((noon - at(2018, 12, 31, 12)) / 86_400).floor.div(7)
      end
    end
  end
end
