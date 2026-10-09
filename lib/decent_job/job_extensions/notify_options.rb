# frozen_string_literal: true

require "decent_job/support"

module DecentJob
  module JobExtensions
    module NotifyOptions
      def self.included(base)
        Support.class_setting(base, :decent_job_notify, nil)
        base.prepend(Serialization)
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

      attr_accessor :decent_job_notify
    end
  end
end
