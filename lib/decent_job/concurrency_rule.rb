# frozen_string_literal: true

require "date"

module DecentJob
  class ConcurrencyRule
    Resolved = Data.define(:scope, :kind, :value, :total_limit, :enqueue_limit, :enqueue_limit_configured, :perform_limit, :enqueue_throttle, :perform_throttle)
    KEY_TYPES = [String, Symbol, Numeric, Date, Time, TrueClass, FalseClass].freeze

    def initialize(label: nil, apply_label: false, key: nil, total_limit: nil, enqueue_limit: nil, perform_limit: nil, enqueue_throttle: nil, perform_throttle: nil)
      if apply_label && !label.respond_to?(:call) && normalize_label(label).nil?
        raise ArgumentError, "apply_label: true requires a label"
      end

      @label = copy_configuration(label)
      @apply_label = apply_label
      @key = copy_configuration(key)
      @limits = copy_configuration([total_limit, enqueue_limit, perform_limit])
      @throttles = copy_configuration([enqueue_throttle, perform_throttle])
      freeze
    end

    def applied_label(job)
      normalize_label(evaluate(job, @label)) if @apply_label
    end

    def resolve(job, stage: nil)
      label = normalize_label(evaluate(job, @label))
      if label
        labels = job.respond_to?(:decent_job_labels) ? Array(job.decent_job_labels) : []
        return nil unless labels.any? { |value| normalize_label(value) == label }

        kind, value = "label", label
      else
        key = evaluate(job, @key)
        if !key.nil? && KEY_TYPES.none? { |type| key.is_a?(type) }
          raise TypeError, "Concurrency key must be a scalar; was a #{key.class}"
        end
        blank_key = key.nil? || key == false || (key.is_a?(String) && key.strip.empty?)
        return nil if blank_key && @key.respond_to?(:call)

        kind, value = blank_key ? ["all", nil] : ["key", key.to_s.dup.freeze]
      end

      enqueue = stage.nil? || stage == :enqueue
      perform = stage.nil? || stage == :perform
      enqueue_limit = resolve_limit(job, :enqueue_limit, @limits[1]) if enqueue
      perform_limit = resolve_limit(job, :perform_limit, @limits[2]) if perform
      use_total = stage.nil? || (stage == :enqueue && enqueue_limit.nil?) || (stage == :perform && perform_limit.nil?)
      Resolved.new(
        scope: (value.nil? ? "all" : "#{kind}:#{value}").freeze,
        kind: kind,
        value: value,
        total_limit: (resolve_limit(job, :total_limit, @limits[0]) if use_total),
        enqueue_limit: enqueue_limit,
        enqueue_limit_configured: !@limits[1].nil?,
        perform_limit: perform_limit,
        enqueue_throttle: (resolve_throttle(job, :enqueue_throttle, @throttles[0]) if enqueue),
        perform_throttle: (resolve_throttle(job, :perform_throttle, @throttles[1]) if perform)
      )
    end

    private

    def evaluate(job, value)
      value.respond_to?(:call) ? job.instance_exec(&value) : value
    end

    def normalize_label(value)
      label = value.to_s.strip
      label.freeze unless label.empty?
    end

    def resolve_limit(job, name, configuration)
      value = evaluate(job, configuration)
      return value if value.nil? || (value.is_a?(Integer) && value >= 0)

      raise ArgumentError, "#{name} must be a nonnegative Integer or nil"
    end

    def resolve_throttle(job, name, configuration)
      value = evaluate(job, configuration)
      return if value.nil?

      if value.is_a?(Array) && value.size == 2
        count, seconds = value
        if count.is_a?(Integer) && count >= 0 && seconds.is_a?(Numeric) && seconds.real? && seconds.finite? && seconds.positive?
          return [count, seconds].freeze
        end
      end

      raise ArgumentError, "#{name} must be [nonnegative Integer, positive finite Numeric seconds] or nil"
    end

    def copy_configuration(value)
      case value
      when Array
        value.map { |item| copy_configuration(item) }.freeze
      when String, Date, Time
        value.dup.freeze
      else
        value
      end
    end
  end
end
