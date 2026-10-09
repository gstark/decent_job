# frozen_string_literal: true

require "decent_job/support"

module DecentJob
  # Extensions shared by native and Active Job classes. They use only the API
  # both frameworks provide: callbacks, retry_on, serialize and deserialize.
  module JobExtensions
    module Labels
      def self.included(base)
        Support.class_setting(base, :decent_job_labels, [])
        base.prepend(Serialization)
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

      # Jobs are constructed without arguments when deserialized. Resolve
      # defaults on first access, after arguments are available.
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
