# frozen_string_literal: true

require "decent_job/job_extensions/concurrency"

module DecentJob
  module ActiveJobExtensions
    # Opt-in concurrency limits and throttles for Active Job classes.
    #
    # It wraps {JobExtensions::Concurrency}, which also includes
    # {JobExtensions::Labels}. Active Job runs the concurrency check as a named
    # before_enqueue callback, so applications can skip it with skip_callback.
    # The callback stays last when more enqueue callbacks are added later.
    # {JobExtensions::Concurrency} requires that it follows every other enqueue
    # callback.
    #
    # Loaded only with Active Job. Job classes include it explicitly.
    #
    # See README.md "Labels, concurrency, and pauses".
    module Concurrency
      # Aliases so Active Job code can rescue the errors by their short names.
      ConcurrencyExceededError = DecentJob::ConcurrencyExceededError
      ThrottleExceededError = DecentJob::ThrottleExceededError

      # @param base [Class] an ActiveJob::Base subclass
      # @return [void]
      def self.included(base)
        base.include(JobExtensions::Concurrency)
        base.extend(ClassMethods)
        base.before_enqueue :_decent_job_concurrency_before_enqueue
      end

      # Keeps the concurrency check last in each class's enqueue chain.
      module ClassMethods
        # Adds a callback as ActiveSupport does, then moves the concurrency
        # check back to the end of the enqueue chain.
        #
        # @param name [Symbol] the callback chain name
        # @return [Object] the result of ActiveSupport's set_callback
        # @note Rewrites _enqueue_callbacks on this class and every subclass.
        def set_callback(name, *filters, &block)
          result = super
          move_decent_job_enqueue_check_last if name.to_sym == :enqueue
          result
        end

        private

        # ActiveSupport adds a parent's new callback to every descendant's chain
        # too, so each descendant needs the same reorder.
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
