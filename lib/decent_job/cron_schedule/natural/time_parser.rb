# frozen_string_literal: true

module DecentJob
  class CronSchedule
    class Natural
      module TimeParser
        private

        def at
          keyword("at")
          list(:time_object, Vocabulary::JOIN)
        end

        def time_object
          choice(:named_time, :digital_time, :simple_time, :points)
        end

        def meridian
          value = take(/[ \t]*(?:am|AM|pm|PM|[Nn]oon|[Mm]idday|[Mm]idnight)/)
          value&.strip&.downcase
        end

        def adjusted_time(hour, minute, suffix)
          raise ArgumentError, "invalid named afternoon hour" if hour.nil? && suffix == "pm"

          hour = if suffix == "midnight" && hour == 12
            24
          elsif suffix == "pm" && hour < 12
            hour + 12
          elsif suffix == "am" && hour == 12
            0
          else
            hour
          end

          component(:time, hour, minute)
        end

        def digital_time
          value = take(/(?:2[0-4]|[0-1]?[0-9]):[0-5][0-9]/i)
          return unless value
          hour, minute = value.split(":").map(&:to_i)
          adjusted_time(hour, minute, meridian)
        end

        def simple_time
          value = take(/24|23|22|21|20|19|18|17|16|15|14|13|12|11|10|9|8|7|6|5|4|3|2|1|0/)
          adjusted_time(value.to_i, 0, meridian) if value
        end

        def named_time
          name = take(Vocabulary::HOUR_PATTERN)
          return unless name
          take(/[ \t]*dark/i)
          minute_name = attempt { take(/[ \t]+/) && take(Vocabulary::MINUTE_PATTERN) }
          # The reference recognizes names without regard to case, then looks
          # up their original spelling. A missing hour can still combine with
          # another time clause, so defer validation until cron compilation.
          hour = Vocabulary::HOUR_NAMES[name]
          minute = minute_name ? Vocabulary::MINUTE_NAMES.fetch(minute_name, minute_name) : 0
          suffix = meridian
          adjusted_time(hour, minute, suffix)
        end

        def points
          list(:point, Vocabulary::CONJUNCTION)
        end

        def point
          unit = take(/(?:minutes?|mins?|seconds?|secs?|hours?|hou|h)[ \t]+/i)
          return unless unit
          values = list(:count, Vocabulary::JOIN)
          return unless values

          case unit.downcase[0]
          when "m" then component(:time, "*", values, strong: 1)
          when "s" then component(:second, values)
          else component(:hour, values)
          end
        end

        def count
          value = take(/\d+/)
          value&.to_i
        end

        def time_span
          first = attempt(:time_object)
          return unless first && keyword("to")
          last = attempt(:time_object)
          return unless last

          unless first.is_a?(Component) && last.is_a?(Component) && first.values[1] == last.values[1]
            raise ArgumentError, "natural time range minutes must agree"
          end

          component(:time, "#{first.values[0]}-#{last.values[0]}", 0, strong: 0)
        end
      end
    end
  end
end
