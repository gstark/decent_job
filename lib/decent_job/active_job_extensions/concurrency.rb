# frozen_string_literal: true

require "decent_job/active_job_extensions/labels"
require "decent_job/concurrency_controls"
require "decent_job/concurrency_rule"
require "decent_job/perform_controls"

module DecentJob
  module ActiveJobExtensions
    module Concurrency
      extend ActiveSupport::Concern
      include Labels

      class ConcurrencyExceededError < StandardError; end
      class ThrottleExceededError < ConcurrencyExceededError; end

      module Enqueue
        def enqueue(options = {})
          @decent_job_enqueue_concurrency_enabled = false
          super
        rescue ConcurrencyControls::AdmissionRejected => error
          self.successfully_enqueued = false
          self.enqueue_error = ActiveJob::EnqueueError.new(error.message)
          false
        end
      end

      included do
        prepend Enqueue

        class_attribute :decent_job_concurrency_config, instance_accessor: false, default: {}.freeze
        class_attribute :decent_job_concurrency_rules, instance_accessor: false, default: [].freeze
        attr_writer :decent_job_concurrency_key

        retry_on ConcurrencyExceededError, attempts: Float::INFINITY, wait: :polynomially_longer
        before_enqueue :_decent_job_concurrency_before_enqueue
        before_perform do |job|
          context = Current.execution
          if job.class.queue_adapter.is_a?(Adapter) && context && context[:job_id] == job.job_id
            PerformControls.new(context.fetch(:database)).check(job, context)
          end
        end
      end

      class_methods do
        def set_callback(name, *filters, &block)
          result = super
          move_decent_job_enqueue_check_last if name.to_sym == :enqueue
          result
        end

        def decent_job_control_concurrency_with(**configuration)
          self.decent_job_concurrency_config = configuration.dup.freeze
        end

        def decent_job_concurrency_rule(**configuration)
          self.decent_job_concurrency_rules = (decent_job_concurrency_rules + [ConcurrencyRule.new(**configuration)]).freeze
        end

        private

        def move_decent_job_enqueue_check_last
          [self, *descendants].each do |job_class|
            callbacks = job_class._enqueue_callbacks
            marker = callbacks.find { |callback| callback.kind == :before && callback.filter == :_decent_job_concurrency_before_enqueue }
            next if marker.nil? || callbacks.to_a.last.equal?(marker)

            # Preserve conditional copies created by skip_callback, and leave
            # absent markers absent when the callback was skipped entirely.
            callbacks = callbacks.dup
            callbacks.delete(marker)
            callbacks.append(marker)
            job_class._enqueue_callbacks = callbacks
          end
        end
      end

      def decent_job_applied_labels
        super + self.class.decent_job_concurrency_rules.filter_map { |rule| rule.applied_label(self) }
      end

      def decent_job_enqueue_concurrency_enabled?
        @decent_job_enqueue_concurrency_enabled != false
      end

      def decent_job_concurrency_key
        return @decent_job_concurrency_key if defined?(@decent_job_concurrency_key)

        deserialize_arguments_if_needed
        configuration = self.class.decent_job_concurrency_config
        key = configuration.fetch(:key, self.class.name) unless configuration.empty?
        @decent_job_concurrency_key = ConcurrencyRule.new(key: key).resolve(self, stage: :metadata).value
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
        super.merge(
          "decent_job_concurrency_key" => decent_job_concurrency_key,
          "decent_job_concurrency_keys" => decent_job_resolved_concurrency_rules(stage: :metadata).select { |rule| rule.kind == "key" }.map(&:value).uniq
        )
      end

      def deserialize(job_data)
        super
        @decent_job_concurrency_key = job_data["decent_job_concurrency_key"] if job_data.key?("decent_job_concurrency_key")
      end

      private

      def _decent_job_concurrency_before_enqueue
        @decent_job_enqueue_concurrency_enabled = self.class.queue_adapter.is_a?(Adapter)
      end
    end
  end
end
