# frozen_string_literal: true

module DecentJob
  # An immutable, parsed queue selection expression.
  #
  # {Worker} builds one per pool through {.pools}. {Claim} uses it to build
  # the queue filter and order of its SQL. {Worker#wake} uses {#include?} to
  # route hints. {Configuration} uses {.pools} to validate queue recipes and
  # to size the connection pool.
  #
  # Syntax:
  # - "*" selects every queue.
  # - "a,b" selects the listed queues in any order.
  # - "+a,b" selects the listed queues and claims from them in list order,
  #   before job priority.
  # - "-a,b" selects every queue except the listed ones.
  #
  # Queue names cannot be empty, contain ";", ":", or "*", or start with "+"
  # or "-".
  #
  # See docs/scheduling.md.
  class QueueSelection
    # @return [Array<String>] the frozen queue names, empty for "*"
    attr_reader :queues

    # Parses a pool expression into selections and thread counts.
    #
    # Pools are separated by ";". Each pool is a selection with an optional
    # ":threads" suffix, for example "urgent:2;-urgent".
    #
    # @param expression [String] the pool expression
    # @param default_threads [Integer] thread count for a pool without a suffix
    # @return [Array<Hash{Symbol => Object}>] one hash per pool with
    #   +:selection+ [QueueSelection] and +:threads+ [Integer]
    # @raise [ArgumentError] if the expression or a thread count is invalid
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

    # @param expression [String] a selection without a thread count
    # @raise [ArgumentError] if the expression is empty or a queue name is invalid
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

    # @return [Boolean] true for a "-" expression
    def exclude?
      @exclude
    end

    # @return [Boolean] true for a "+" expression
    def ordered?
      @ordered
    end

    # @param queue_name [String]
    # @return [Boolean] whether this selection claims from the queue
    def include?(queue_name)
      return true if @all

      exclude? ? !queues.include?(queue_name) : queues.include?(queue_name)
    end
  end
end
