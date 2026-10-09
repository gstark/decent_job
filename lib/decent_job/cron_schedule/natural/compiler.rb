# frozen_string_literal: true

module DecentJob
  class CronSchedule
    class Natural
      class Compiler
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

        def compile
          hours, minutes = first_time_group
          fields = [field(:second, "0"), minutes, hours, field(:monthday), field(:month), field(:weekday)]
          fields << field(:zone) if @fields[:zone]
          fields.shift if fields.first == ["0"]
          fields.map { |values| values.uniq.sort_by { |value| value.is_a?(Numeric) ? value : 999_999 }.join(",") }.join(" ")
        end

        private

        def field(key, default = "*")
          component = @fields[key]
          component ? Array(component.values[0]) : [default]
        end

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
