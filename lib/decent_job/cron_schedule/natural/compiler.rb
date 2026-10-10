# frozen_string_literal: true

module DecentJob
  class CronSchedule
    class Natural
      # Merges the {Component} list of a parsed natural schedule into one
      # numeric cron string. {Natural#compile} creates one compiler per phrase.
      #
      # Rules that match the reference:
      #
      # - Components with the same calendar key merge with {Component#merge}.
      # - A calendar field without a time clause runs at 00:00.
      # - A month without a day runs on day 1.
      # - Only the first group of times that share an hour set is kept.
      # - The seconds field is left out when it is "0", which gives five fields.
      # - An :hour component (from "hours 8") is never read, so it has no effect.
      class Compiler
        # @param components [Array<Component, Array>] parsed components; nested
        #   arrays are flattened
        # @raise [ArgumentError] when two components cannot merge
        # @api private
        def initialize(components)
          @fields = {}
          @times = []

          components.flatten.each do |component|
            if component.key == :time
              @times << component
            else
              existing = @fields[component.key]
              @fields[component.key] = existing ? existing.merge(component) : component
            end
          end

          if @fields[:monthday] || @fields[:weekday] || @fields[:month]
            @times << Component.new(:time, 0, 0) if @times.empty?

            if @fields[:month] && !@fields[:monthday] && !@fields[:weekday]
              @fields[:monthday] = Component.new(:monthday, 1)
            end
          end
        end

        # @return [String] cron text: seconds (when not "0"), minutes, hours,
        #   month days, months, weekdays, and an optional zone. Numbers sort
        #   first, then text values such as "*/2" or "L".
        # @raise [ArgumentError] when the time clauses are incompatible
        # @api private
        def compile
          hours, minutes = first_time_group
          fields = [field(:second, "0"), minutes, hours, field(:monthday), field(:month), field(:weekday)]

          if @fields[:zone]
            fields << field(:zone)
          end

          if fields.first == ["0"]
            fields.shift
          end

          fields.map { |values| values.uniq.sort_by { |value| value.is_a?(Numeric) ? value : 999_999 }.join(",") }.join(" ")
        end

        private

        def field(key, default = "*")
          component = @fields[key]
          component ? Array(component.values[0]) : [default]
        end

        # Merges each graded time into a neighbor (the previous one, or the
        # next one when it is first). Then it groups minutes by their hour set
        # and returns the first group as [hours, minutes].
        def first_time_group
          return [["*"], ["*"]] if @times.empty?

          times = @times.dup

          while times.size > 1 && (index = times.index(&:graded?))
            neighbor = (index == 0) ? 1 : index - 1
            graded = times[index]
            other = times.delete_at(neighbor)
            times[times.index(graded)] = graded.merge(other)
          end

          by_minute = {}

          times.each do |time|
            hours, minutes = time.values.map { |value| Array(value) }
            minutes.each { |minute| (by_minute[minute] ||= []).concat(hours) }
          end

          by_hours = {}

          by_minute.each do |minute, hours|
            (by_hours[hours.sort] ||= []) << minute
          end

          by_hours.first
        rescue ArgumentError
          # Mixed symbolic and numeric hours cannot form a reference time group.
          raise ArgumentError, "incompatible natural schedule times"
        end
      end
    end
  end
end
