# frozen_string_literal: true

require "decent_job/concurrency_controls"
require "decent_job/concurrency_rule"
require "decent_job/current"
require "decent_job/job_extensions/labels"
require "decent_job/perform_controls"
require "decent_job/support"

module DecentJob
  module JobExtensions
    # Concurrency rules, keys and perform checks. Each framework decides when
    # _decent_job_concurrency_before_enqueue runs: it must follow every other
    # enqueue callback, because callbacks can change labels and arguments that
    # the rules read. {Base#before_adapter_enqueue} calls it for native jobs.
    # ActiveJobExtensions::Concurrency keeps it as the last before_enqueue.
    #
    # Enqueue admission happens in {ConcurrencyControls#enqueue}, inside the
    # adapter's insert transaction. Perform checks happen in a before_perform
    # callback through {PerformControls#check}. A perform check that fails
    # raises {ConcurrencyExceededError}, and the job retries with no attempt
    # limit and :polynomially_longer waits.
    #
    # Class DSL:
    # - decent_job_control_concurrency_with(**options): one rule keyed by the
    #   job. The key defaults to the class name; key: nil disables the rule.
    # - decent_job_concurrency_rule(**options): adds a {ConcurrencyRule} that
    #   can match a key, a label, or all jobs.
    # Both take the {ConcurrencyRule} options: label:, apply_label:, key:,
    # total_limit:, enqueue_limit:, perform_limit:, enqueue_throttle: and
    # perform_throttle:. Values can be procs that run with instance_exec on
    # the job.
    #
    # See the "Labels, concurrency, and pauses" section of README.md and
    # docs/locking.md.
    module Concurrency
      # Include Labels first so this module's label methods take precedence.
      def self.append_features(base)
        base.include(Labels) unless base <= Labels
        super
      end

      def self.included(base)
        base.prepend(Enqueue)
        base.extend(ClassMethods)
        Support.class_setting(base, :decent_job_concurrency_config, {}.freeze)
        Support.class_setting(base, :decent_job_concurrency_rules, [].freeze)
        base.retry_on ConcurrencyExceededError, attempts: Float::INFINITY, wait: :polynomially_longer

        base.before_perform do |job|
          context = Current.execution

          if job.queue_adapter.is_a?(Adapter) && context && context[:job_id] == job.job_id
            PerformControls.new(context.fetch(:database)).check(job, context)
          end
        end
      end

      # Prepended so it wraps every other enqueue implementation.
      module Enqueue
        # Turns an admission rejection into a failed enqueue.
        #
        # The flag starts false, so admission is skipped when the concurrency
        # callback never runs (a callback aborted, or Active Job skipped it).
        #
        # @param options [Hash] see {Base::Core#enqueue}
        # @return [Object, false] the job, or false when a callback aborted,
        #   the adapter rejected it, or a concurrency limit rejected it. A
        #   limit rejection sets enqueue_error to the integration's error
        #   class.
        def enqueue(options = {})
          @decent_job_enqueue_concurrency_enabled = false
          super
        rescue ConcurrencyControls::AdmissionRejected => error
          self.successfully_enqueued = false
          self.enqueue_error = Integrations.for_job(self).enqueue_error(EnqueueError.new(error.message))
          false
        end
      end

      # The concurrency DSL. See the module header for the options.
      module ClassMethods
        # Replaces the class's keyed concurrency configuration.
        #
        # @param configuration [Hash] {ConcurrencyRule} options. key: defaults
        #   to the class name.
        # @return [void]
        def decent_job_control_concurrency_with(**configuration)
          self.decent_job_concurrency_config = configuration.dup.freeze
        end

        # Appends a concurrency rule for this class and its subclasses.
        #
        # @param configuration [Hash] {ConcurrencyRule} options
        # @return [void]
        # @raise [ArgumentError] when the options are not valid
        def decent_job_concurrency_rule(**configuration)
          self.decent_job_concurrency_rules = (decent_job_concurrency_rules + [ConcurrencyRule.new(**configuration)]).freeze
        end
      end

      # Sets the concurrency key directly, instead of the configured key.
      attr_writer :decent_job_concurrency_key

      # Adds the labels of rules with apply_label: true to the job labels.
      #
      # @return [Array<String>]
      # @api private
      def decent_job_applied_labels
        super + self.class.decent_job_concurrency_rules.filter_map { |rule| rule.applied_label(self) }
      end

      # @return [Boolean] true when bulk enqueue must admit this job on its
      #   own, with its enqueue callbacks. Including this extension opts in.
      #   {Base} overrides it, because every native job includes it.
      # @api private
      def decent_job_concurrency_controlled?
        true
      end

      # @return [Boolean] false when enqueue admission must be skipped: the
      #   concurrency callback did not run in this enqueue, or the adapter is
      #   not a DecentJob {Adapter}
      # @api private
      def decent_job_enqueue_concurrency_enabled?
        @decent_job_enqueue_concurrency_enabled != false
      end

      # Returns the job's concurrency key.
      #
      # The key comes from the key: option of
      # decent_job_control_concurrency_with, or the class name when there is
      # none. It is also computed for classes without that configuration, as
      # in GoodJob.
      #
      # @return [String, nil] the key, or nil when it resolves blank
      # @raise [TypeError] when the key is not a scalar
      def decent_job_concurrency_key
        return @decent_job_concurrency_key if @decent_job_concurrency_key

        deserialize_arguments_if_needed
        configuration = self.class.decent_job_concurrency_config
        key = configuration.fetch(:key, self.class.name)
        ConcurrencyRule.new(key: key).resolve(self, stage: :metadata)&.value
      end

      # Resolves every rule that applies to this job.
      #
      # @param stage [Symbol, nil] :enqueue, :perform or :metadata. A stage
      #   resolves only the limits for that stage. Nil resolves all of them.
      # @return [Array<ConcurrencyRule::Resolved>] the keyed configuration
      #   rule first, then the class rules that match
      # @api private
      def decent_job_resolved_concurrency_rules(stage: nil)
        deserialize_arguments_if_needed
        rules = self.class.decent_job_concurrency_rules.filter_map { |rule| rule.resolve(self, stage: stage) }
        configuration = self.class.decent_job_concurrency_config

        if !configuration.empty? && !decent_job_concurrency_key.nil?
          rules.unshift(ConcurrencyRule.new(**configuration.merge(key: decent_job_concurrency_key)).resolve(self, stage: stage))
        end

        rules
      end

      # Adds "decent_job_concurrency_key" and "decent_job_concurrency_keys" to
      # the payload. The store copies the keys to decent_jobs.concurrency_keys.
      #
      # @return [Hash{String => Object}]
      def serialize
        key = decent_job_concurrency_key
        keys = decent_job_resolved_concurrency_rules(stage: :metadata).select { |rule| rule.kind == "key" }.map(&:value)
        super.merge(
          "decent_job_concurrency_key" => key,
          "decent_job_concurrency_keys" => [key, *keys].compact.uniq
        )
      end

      # Keeps the stored key, so a retry uses the key from the first enqueue.
      #
      # @param job_data [Hash{String => Object}]
      # @return [void]
      def deserialize(job_data)
        super
        @decent_job_concurrency_key = job_data["decent_job_concurrency_key"] if job_data.key?("decent_job_concurrency_key")
      end

      private

      # Turns admission on for a DecentJob adapter and freezes the key, so
      # later reads do not resolve it again. A job that enqueues itself from
      # its own execution (a retry) keeps its stored key.
      def _decent_job_concurrency_before_enqueue
        @decent_job_enqueue_concurrency_enabled = queue_adapter.is_a?(Adapter)
        return if Current.execution&.fetch(:job_id, nil) == job_id

        if @decent_job_enqueue_concurrency_enabled && !self.class.decent_job_concurrency_config.empty?
          @decent_job_concurrency_key ||= decent_job_concurrency_key
        end
      end
    end
  end
end
