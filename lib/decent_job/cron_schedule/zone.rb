# frozen_string_literal: true

require "decent_job/cron_schedule/local_zone"
require "tzinfo"

module DecentJob
  class CronSchedule
    class Zone
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

      def local(time)
        offset = @local ? LocalZone.offset : @offset
        return time.getlocal(offset) if offset
        timezone = @local ? LocalZone.timezone : @timezone
        timezone ? timezone.to_local(time) : time.getlocal
      end

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

      def reference_week(time)
        noon = at(time.year, time.month, time.day, 12)
        ((noon - at(2018, 12, 31, 12)) / 86_400).floor.div(7)
      end
    end
  end
end
