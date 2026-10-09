# frozen_string_literal: true

module DecentJob
  module ActiveJobExtensions
    module Labels
      extend ActiveSupport::Concern

      included do
        class_attribute :decent_job_labels, instance_accessor: false, instance_predicate: false, default: []
        prepend Serialization
      end

      module Serialization
        def enqueue(options = {})
          self.decent_job_labels = options[:decent_job_labels] if options.key?(:decent_job_labels)
          super
        end

        def serialize
          super.merge("decent_job_labels" => decent_job_labels.dup)
        end

        def deserialize(job_data)
          super
          self.decent_job_labels = job_data["decent_job_labels"]
        end
      end

      # Active Job constructs jobs without arguments when deserializing them.
      # Resolve defaults on first access, after arguments are available.
      def decent_job_labels
        deserialize_arguments_if_needed
        @decent_job_labels ||= normalize_decent_job_labels(Array(self.class.decent_job_labels).map do |label|
          label.respond_to?(:call) ? instance_exec(&label) : label
        end)
        @decent_job_labels.replace(normalize_decent_job_labels(@decent_job_labels + Array(decent_job_applied_labels)))
      end

      def decent_job_labels=(labels)
        @decent_job_labels = normalize_decent_job_labels(labels)
      end

      def decent_job_applied_labels
        []
      end

      private

      def normalize_decent_job_labels(labels)
        Array(labels).compact.map { |label| label.to_s.strip }.reject(&:empty?).uniq
      end
    end
  end
end
