# frozen_string_literal: true

require "decent_job/support"

module DecentJob
  module JobExtensions
    # Per-job control of the NOTIFY that enqueue sends. {Base} includes it.
    # Active Job classes include ActiveJobExtensions::NotifyOptions.
    #
    # The class setting decent_job_notify is the default. False turns NOTIFY
    # off; true and nil leave it on. The instance value overrides it when not
    # nil.
    # {Notifier} reads both values when it publishes. The global
    # enable_listen_notify = false wins over a true value.
    #
    # NOTIFY is only a wakeup hint. A job with notify off still runs, after
    # polling or another wakeup finds it.
    #
    # See docs/notifications.md.
    module NotifyOptions
      def self.included(base)
        Support.class_setting(base, :decent_job_notify, nil)
        base.prepend(Serialization)
      end

      # Prepended so it wraps the framework's enqueue and payload methods.
      module Serialization
        # @param options [Hash] enqueue options. :decent_job_notify sets the
        #   instance value before the callbacks run.
        def enqueue(options = {})
          self.decent_job_notify = options[:decent_job_notify] if options.key?(:decent_job_notify)
          super
        end

        # Writes "decent_job_notify" only when the instance value is not nil,
        # so a stored job keeps following the class default.
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

      # @return [Boolean, nil] the instance NOTIFY setting. Nil means the class
      #   default. It does not return the class default itself.
      attr_accessor :decent_job_notify
    end
  end
end
