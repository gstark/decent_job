# frozen_string_literal: true

module DecentJob
  class CronSchedule
    class Natural
      module Vocabulary
        SMALL_NUMBERS = %w[zero one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen].freeze

        MINUTE_NAMES = begin
          names = SMALL_NUMBERS + %w[twenty thirty forty fifty].flat_map do |tens|
            [tens] + SMALL_NUMBERS[1..9].map { |ones| "#{tens}-#{ones}" }
          end

          names.each_with_index.to_h.merge("o'clock" => 0, "hundred" => 0).freeze
        end

        HOUR_NAMES = SMALL_NUMBERS[0..12].each_with_index.to_h.merge("midnight" => 0, "oh" => 0, "noon" => 12).freeze
        ORDINAL_NAMES = %w[
          first second third fourth fifth sixth seventh eighth ninth tenth
          eleventh twelfth thirteenth fourteenth fifteenth sixteenth seventeenth
          eighteenth nineteenth twentieth twenty-first twenty-second twenty-third
          twenty-fourth twenty-fifth twenty-sixth twenty-seventh twenty-eighth
          twenty-ninth thirtieth thirty-first
        ].freeze

        ORDINALS = begin
          suffixes = {1 => "st", 2 => "nd", 3 => "rd", 21 => "st", 22 => "nd", 23 => "rd", 31 => "st"}
          days = (1..31).to_h { |day| ["#{day}#{suffixes[day] || "th"}", day] }
          # The pinned grammar also accepts 21th, 22th, and 23th.
          days.merge!((4..30).to_h { |day| ["#{day}th", day] })
          days["last"] = "L"
          ORDINAL_NAMES.each_with_index { |name, index| days[name] = index + 1 }
          days.freeze
        end

        WEEKDAYS = %w[sunday monday tuesday wednesday thursday friday saturday].freeze
        WEEKDAY_PATTERN = /(?:#{(WEEKDAYS + WEEKDAYS.map { |day| day[0, 3] }).join("|")})(?=$|[-, \t])/i
        ORDINAL_PATTERN = /#{ORDINALS.keys.join("|")}/i
        HOUR_PATTERN = /#{HOUR_NAMES.keys.join("|")}/i
        MINUTE_PATTERN = /#{MINUTE_NAMES.keys.join("|")}/i
        JOIN = /[ \t]*(?:,[ \t]*)?(?:(?:and|or)[ \t]+|,[ \t]*)/i
        CONJUNCTION = /[ \t]*(?:and|or)[ \t]+/i
        RANGE = /[ \t]*-[ \t]*|[ \t]+(?:to|through)[ \t]+/i
      end
    end
  end
end
