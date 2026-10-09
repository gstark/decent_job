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
    # enqueue callback.
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

      module Enqueue
        def enqueue(options = {})
          @decent_job_enqueue_concurrency_enabled = false
          super
        rescue ConcurrencyControls::AdmissionRejected => error
          self.successfully_enqueued = false
          self.enqueue_error = Integrations.for_job(self).enqueue_error(EnqueueError.new(error.message))
          false
        end
      end

      module ClassMethods
        def decent_job_control_concurrency_with(**configuration)
          self.decent_job_concurrency_config = configuration.dup.freeze
        end

        def decent_job_concurrency_rule(**configuration)
          self.decent_job_concurrency_rules = (decent_job_concurrency_rules + [ConcurrencyRule.new(**configuration)]).freeze
        end
      end

      attr_writer :decent_job_concurrency_key

      def decent_job_applied_labels
        super + self.class.decent_job_concurrency_rules.filter_map { |rule| rule.applied_label(self) }
      end

      def decent_job_enqueue_concurrency_enabled?
        @decent_job_enqueue_concurrency_enabled != false
      end

      def decent_job_concurrency_key
        return @decent_job_concurrency_key if @decent_job_concurrency_key

        deserialize_arguments_if_needed
        configuration = self.class.decent_job_concurrency_config
        key = configuration.fetch(:key, self.class.name)
        ConcurrencyRule.new(key: key).resolve(self, stage: :metadata)&.value
      end

      def decent_job_resolved_concurrency_rules(stage: nil)
        deserialize_arguments_if_needed
        rules = self.class.decent_job_concurrency_rules.filter_map { |rule| rule.resolve(self, stage: stage) }
        configuration = self.class.decent_job_concurrency_config
        if !configuration.empty? && !decent_job_concurrency_key.nil?
          rules.unshift(ConcurrencyRule.new(**configuration.merge(key: decent_job_concurrency_key)).resolve(self, stage: stage))
        end
        rules
      end

      def serialize
        key = decent_job_concurrency_key
        keys = decent_job_resolved_concurrency_rules(stage: :metadata).select { |rule| rule.kind == "key" }.map(&:value)
        super.merge(
          "decent_job_concurrency_key" => key,
          "decent_job_concurrency_keys" => [key, *keys].compact.uniq
        )
      end

      def deserialize(job_data)
        super
        @decent_job_concurrency_key = job_data["decent_job_concurrency_key"] if job_data.key?("decent_job_concurrency_key")
      end

      private

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
