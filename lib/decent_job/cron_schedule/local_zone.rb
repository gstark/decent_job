# frozen_string_literal: true

module DecentJob
  class CronSchedule
    # Resolves the implicit local zone of a schedule without changing process
    # state. {Zone} calls it on each conversion, so changes to +ENV["TZ"]+ and
    # +Time.zone+ apply at once. Native Ruby conversion stays the fallback
    # for POSIX +TZ+ rules (such as "EST5EDT") and the host zone setting.
    #
    # See docs/cron-compatibility.md.
    class LocalZone
      # @return [Integer, nil] the UTC offset in seconds when +TZ+ is a numeric
      #   offset such as "+05:30" or "-3"; nil otherwise
      # @api private
      def self.offset
        match = /\A([+-])([01]?\d|2[0-3])(?::?([0-5]\d))?\z/.match(ENV["TZ"].to_s)
        return unless match

        (match[2].to_i * 3600 + match[3].to_i * 60) * ((match[1] == "-") ? -1 : 1)
      end

      # @return [TZInfo::Timezone, nil] the zone named by +TZ+ (with an optional
      #   leading ":"; "Z" means UTC), else the TZInfo zone of +Time.zone+, else
      #   nil for native conversion
      # @api private
      def self.timezone
        name = ENV["TZ"]&.delete_prefix(":")
        name = "UTC" if name == "Z"
        environment_timezone = begin
          TZInfo::Timezone.get(name) if name && !name.empty?
        rescue TZInfo::InvalidTimezoneIdentifier
          nil
        end
        environment_timezone || (Time.zone&.tzinfo if Time.respond_to?(:zone))
      end

      # Finds the instant for a wall time in the native Ruby local zone.
      #
      # It tries the UTC offsets in effect two days before, at, and two days
      # after the wall time. In a fold it returns the earliest match.
      #
      # @param wall [Time] a UTC Time whose fields hold the local wall time
      # @return [Time] the instant in UTC
      # @api private
      def self.at(wall)
        native = Time.local(wall.year, wall.month, wall.day, wall.hour)
        offsets = [-172_800, 0, 172_800].map { |delta| (native + delta).getlocal.utc_offset }.uniq
        matches = offsets.map { |offset| Time.at(wall.to_i - offset).utc }
          .select { |time| local_seconds(time) == wall.to_i }
        return matches.min unless matches.empty?

        # The wall time lies in a gap. Find its transition rather than retain
        # Time.local's normalization, which may overshoot the gap boundary.
        lower = native.to_i - 172_800
        upper = native.to_i + 172_800
        while upper - lower > 1
          middle = (lower + upper).div(2)
          if local_seconds(Time.at(middle)) >= wall.to_i
            upper = middle
          else
            lower = middle
          end
        end
        Time.at(upper).utc
      end

      def self.local_seconds(time)
        time.to_i + time.getlocal.utc_offset
      end
      private_class_method :local_seconds
    end
  end
end
