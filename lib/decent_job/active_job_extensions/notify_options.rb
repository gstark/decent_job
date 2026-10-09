# frozen_string_literal: true

module DecentJob
  module ActiveJobExtensions
    module NotifyOptions
      extend ActiveSupport::Concern

      included do
        class_attribute :decent_job_notify, instance_accessor: false, instance_predicate: false, default: nil
        attr_accessor :decent_job_notify
        prepend Serialization
      end

      module Serialization
        def enqueue(options = {})
          self.decent_job_notify = options[:decent_job_notify] if options.key?(:decent_job_notify)
          super
        end

        def serialize
          super.tap do |payload|
            payload["decent_job_notify"] = decent_job_notify unless decent_job_notify.nil?
          end
        end

        def deserialize(job_data)
          super
          self.decent_job_notify = job_data["decent_job_notify"]
        end
      end
    end
  end
end
