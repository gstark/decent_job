# frozen_string_literal: true

require "date"

module DecentJob
  # One immutable concurrency rule, as declared on a job class.
  #
  # {JobExtensions::Concurrency} creates rules from
  # decent_job_concurrency_rule and decent_job_control_concurrency_with, and
  # calls {#resolve} for each job. {ConcurrencyControls} and
  # {PerformControls} read the {Resolved} values.
  #
  # Any setting can be a callable. A callable runs with instance_exec on the
  # job each time the rule resolves. The constructor copies and freezes
  # String, Date, Time, and Array settings, so later changes by the caller do
  # not change the rule. The rule itself is frozen and safe to share between
  # threads.
  #
  # A label rule applies only to jobs that carry the label. A key rule
  # applies to jobs with that key. A rule with no label and a static blank key
  # has scope "all", which matches every job in decent_jobs.
  class ConcurrencyRule
    # A rule resolved for one job and stage.
    #
    # scope is the claim and lock name: "label:<value>", "key:<value>", or
    # "all". kind is "label", "key", or "all". value is the label or key
    # string, or nil for "all". Limits are Integers or nil when not set or not
    # resolved for the stage. Throttles are frozen [count, seconds] arrays or
    # nil. enqueue_limit_configured is true when the rule declares an
    # enqueue_limit, even if its callable returns nil. This matches GoodJob's
    # enqueue_limit_flag (spec/concurrency_parity_spec.rb pins it).
    Resolved = Data.define(:scope, :kind, :value, :total_limit, :enqueue_limit, :enqueue_limit_configured, :perform_limit, :enqueue_throttle, :perform_throttle)
    # Scalar types that a concurrency key can have.
    KEY_TYPES = [String, Symbol, Numeric, Date, Time, TrueClass, FalseClass].freeze

    # @param label [String, #call, nil] the label that selects jobs
    # @param apply_label [Boolean] true to add the label to each job of the class
    # @param key [Object, #call, nil] a scalar key, or a callable that returns one
    # @param total_limit [Integer, #call, nil] the limit for any stage that has
    #   no stage-specific limit
    # @param enqueue_limit [Integer, #call, nil] the maximum of unfinished jobs
    #   that are not executing, checked at enqueue
    # @param perform_limit [Integer, #call, nil] the maximum of executing jobs
    # @param enqueue_throttle [Array(Integer, Numeric), #call, nil] [count,
    #   seconds]: at most count enqueues in a rolling window
    # @param perform_throttle [Array(Integer, Numeric), #call, nil] [count,
    #   seconds]: at most count jobs started in a rolling window
    # @raise [ArgumentError] when apply_label is true and label is blank
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

    # @param job [Object] the job instance
    # @return [String, nil] the label to add to the job, or nil when
    #   apply_label is false or the label is blank
    def applied_label(job)
      normalize_label(evaluate(job, @label)) if @apply_label
    end

    # Resolves the rule for a job.
    #
    # @param job [Object] the job instance
    # @param stage [Symbol, nil] :enqueue or :perform resolves only that
    #   stage's limits and throttle. :metadata resolves only the scope. nil
    #   resolves everything.
    # @return [Resolved, nil] nil when the rule does not apply: the job lacks
    #   the label, or a callable key returns a blank value
    # @raise [TypeError] when the key is not one of {KEY_TYPES}
    # @raise [ArgumentError] when a limit or throttle value is invalid
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
