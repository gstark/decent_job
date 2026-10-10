# frozen_string_literal: true

module DecentJob
  # A small event bus. Every event is also forwarded to ActiveSupport
  # notifications when the host loads them, so Rails subscribers keep working.
  module Notifications
    Event = Data.define(:name, :payload, :duration)

    Subscription = Data.define(:pattern, :callback) do
      def match?(name)
        pattern.is_a?(Regexp) ? pattern.match?(name) : pattern == name
      end
    end

    class SubscriberError < StandardError
      attr_reader :exceptions

      def initialize(exceptions)
        @exceptions = exceptions
        super("Subscriber errors: #{exceptions.map(&:message).join(", ")}")
      end
    end

    class Handle
      def initialize(name, payload, subscriptions)
        @name = name
        @payload = payload
        @subscriptions = subscriptions
        @external = ActiveSupport::Notifications.instrumenter.build_handle(name, payload) if defined?(ActiveSupport::Notifications)
      end

      def start
        @started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        @external&.start
      end

      # Run every subscriber before raising, as ActiveSupport does.
      def finish
        errors = []

        begin
          @external&.finish
        rescue ActiveSupport::Notifications::InstrumentationSubscriberError => error
          errors.concat(error.exceptions)
        rescue => error
          errors << error
        end

        unless @subscriptions.empty?
          duration = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - (@started || 0)) * 1000.0
          event = Event.new(name: @name, payload: @payload, duration: duration)

          @subscriptions.each do |subscription|
            subscription.callback.call(event)
          rescue => error
            errors << error
          end
        end

        raise errors.first if errors.size == 1
        raise SubscriberError, errors unless errors.empty?
      end
    end

    @mutex = Mutex.new
    @subscriptions = [].freeze

    class << self
      def subscribe(pattern, &callback)
        raise ArgumentError, "a subscriber block is required" unless callback

        subscription = Subscription.new(pattern: pattern, callback: callback)
        @mutex.synchronize { @subscriptions = (@subscriptions + [subscription]).freeze }
        subscription
      end

      def unsubscribe(subscription)
        @mutex.synchronize { @subscriptions = (@subscriptions - [subscription]).freeze }
      end

      def subscribed(pattern, callback)
        subscription = subscribe(pattern, &callback)
        yield
      ensure
        unsubscribe(subscription) if subscription
      end

      def build_handle(name, payload)
        Handle.new(name, payload, @subscriptions.select { |subscription| subscription.match?(name) })
      end

      def instrument(name, payload = {})
        handle = build_handle(name, payload)
        handle.start

        begin
          yield payload if block_given?
        rescue Exception => error # standard:disable Lint/RescueException
          payload[:exception] = [error.class.name, error.message]
          payload[:exception_object] = error
          raise
        ensure
          handle.finish
        end
      end
    end
  end
end
