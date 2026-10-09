# frozen_string_literal: true

module DecentJob
  class QueueSelection
    attr_reader :queues

    def self.pools(expression, default_threads:)
      unless default_threads.is_a?(Integer) && default_threads.positive?
        raise ArgumentError, "default_threads must be a positive integer"
      end
      unless expression.is_a?(String) && !expression.strip.empty?
        raise ArgumentError, "queue pools must be a nonempty string"
      end

      expression.split(";", -1).map do |pool|
        parts = pool.split(":", -1).map(&:strip)
        raise ArgumentError, "invalid queue pool: #{pool.inspect}" if parts.length > 2

        threads = default_threads
        if parts.length == 2
          unless parts.last.match?(/\A[0-9]+\z/) && parts.last.to_i.positive?
            raise ArgumentError, "pool threads must be a positive integer"
          end
          threads = parts.last.to_i
        end

        {selection: new(parts.first), threads: threads}
      end
    end

    def initialize(expression = "*")
      unless expression.is_a?(String) && !expression.strip.empty?
        raise ArgumentError, "queue selection must be a nonempty string"
      end

      expression = expression.strip
      @all = expression == "*"
      @exclude = expression.start_with?("-")
      @ordered = expression.start_with?("+")
      expression = expression[1..] if exclude? || ordered?
      @queues = @all ? [] : expression.split(",", -1).map(&:strip)
      if !@all && (@queues.empty? || @queues.any? { |queue| queue.empty? || queue.match?(/[;:*]|\A[+-]/) })
        raise ArgumentError, "invalid queue selection: #{expression.inspect}"
      end
      @queues.each(&:freeze)
      @queues.freeze
    end

    def exclude?
      @exclude
    end

    def ordered?
      @ordered
    end

    def include?(queue_name)
      return true if @all

      exclude? ? !queues.include?(queue_name) : queues.include?(queue_name)
    end
  end
end
