# frozen_string_literal: true

require "decent_job/job_extensions/concurrency"

module DecentJob
  module ActiveJobExtensions
    # Active Job runs the concurrency check as a named before_enqueue callback,
    # so applications can skip it with skip_callback. The callback stays last
    # when more enqueue callbacks are added later.
    module Concurrency
      ConcurrencyExceededError = DecentJob::ConcurrencyExceededError
      ThrottleExceededError = DecentJob::ThrottleExceededError

      def self.included(base)
        base.include(JobExtensions::Concurrency)
        base.extend(ClassMethods)
        base.before_enqueue :_decent_job_concurrency_before_enqueue
      end

      module ClassMethods
        def set_callback(name, *filters, &block)
          result = super
          move_decent_job_enqueue_check_last if name.to_sym == :enqueue
          result
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
    end
  end
end
