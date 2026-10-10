# frozen_string_literal: true

module DecentJob
  # A small event bus. Every event is also forwarded to ActiveSupport
  # notifications when the host loads them, so Rails subscribers keep working.
  #
  # All event names end in ".decent_job". The full list of events and their
  # payload keys is in docs/instrumentation.md. {LogSubscriber} attaches to
  # this bus when the library loads. It does not see events that something
  # else sends only to ActiveSupport::Notifications.
  #
  # Ownership: the subscription list is a frozen array. {.subscribe} and
  # {.unsubscribe} replace it under a mutex. {.build_handle} reads it without
  # the mutex, so a publisher always sees one complete list.
  #
  # Constraint: callbacks run synchronously on the publishing thread.
  # {.instrument} raises subscriber errors to the publisher. Events that
  # surround job code use {Instrumentation.instrument} instead, which contains
  # those errors.
  #
  # See docs/instrumentation.md.
  module Notifications
    # The value passed to subscriber callbacks.
    #
    # @!attribute name [String] full event name
    # @!attribute payload [Hash{Symbol => Object}] the publisher's hash
    # @!attribute duration [Float] milliseconds between start and finish
    Event = Data.define(:name, :payload, :duration)

    # A subscriber registration. Pass it to {Notifications.unsubscribe}.
    Subscription = Data.define(:pattern, :callback) do
      # @param name [String] full event name
      # @return [Boolean] true when the pattern equals or matches +name+
      def match?(name)
        pattern.is_a?(Regexp) ? pattern.match?(name) : pattern == name
      end
    end

    # Raised by {Handle#finish} when more than one subscriber failed. A single
    # failure is raised as itself.
    class SubscriberError < StandardError
      attr_reader :exceptions

      def initialize(exceptions)
        @exceptions = exceptions
        super("Subscriber errors: #{exceptions.map(&:message).join(", ")}")
      end
    end

    # One start/finish pair for one event.
    #
    # The subscriber list is fixed when the handle is built. Subscribers added
    # later do not receive this event. Use one handle per event on one thread.
    class Handle
      # @param name [String] full event name
      # @param payload [Hash{Symbol => Object}] shared with subscribers, not
      #   copied
      # @param subscriptions [Array<Subscription>] matching subscriptions
      def initialize(name, payload, subscriptions)
        @name = name
        @payload = payload
        @subscriptions = subscriptions
        @external = ActiveSupport::Notifications.instrumenter.build_handle(name, payload) if defined?(ActiveSupport::Notifications)
      end

      # Records the monotonic start time and starts the ActiveSupport handle.
      #
      # @return [void]
      def start
        @started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        @external&.start
      end

      # Calls the ActiveSupport handle and every DecentJob subscriber.
      #
      # Run every subscriber before raising, as ActiveSupport does.
      #
      # @return [void]
      # @raise [StandardError] the error when exactly one subscriber failed
      # @raise [SubscriberError] when more than one subscriber failed
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
      # Adds a subscriber for every event whose name matches +pattern+.
      #
      # @param pattern [String, Regexp] exact event name, or a pattern
      # @yieldparam event [Event] the finished event
      # @return [Subscription] pass to {.unsubscribe}
      # @raise [ArgumentError] when no block is given
      # @note Thread safe. The callback runs on the publishing thread.
      def subscribe(pattern, &callback)
        raise ArgumentError, "a subscriber block is required" unless callback

        subscription = Subscription.new(pattern: pattern, callback: callback)
        @mutex.synchronize { @subscriptions = (@subscriptions + [subscription]).freeze }
        subscription
      end

      # Removes a subscriber. Events already started still reach it.
      #
      # @param subscription [Subscription] the value from {.subscribe}
      # @return [void]
      def unsubscribe(subscription)
        @mutex.synchronize { @subscriptions = (@subscriptions - [subscription]).freeze }
      end

      # Subscribes only while the block runs.
      #
      # @param pattern [String, Regexp] exact event name, or a pattern
      # @param callback [#call] receives each {Event}
      # @yield the code to observe
      # @return [Object] the block result
      def subscribed(pattern, callback)
        subscription = subscribe(pattern, &callback)
        yield
      ensure
        unsubscribe(subscription) if subscription
      end

      # Builds a handle with the subscribers that match +name+ now.
      #
      # @param name [String] full event name
      # @param payload [Hash{Symbol => Object}] shared with subscribers
      # @return [Handle]
      # @api private
      def build_handle(name, payload)
        Handle.new(name, payload, @subscriptions.select { |subscription| subscription.match?(name) })
      end

      # Publishes one event around the optional block.
      #
      # @param name [String] full event name
      # @param payload [Hash{Symbol => Object}] shared with subscribers. On an
      #   exception this method adds :exception ([class name, message]) and
      #   :exception_object.
      # @yieldparam payload [Hash{Symbol => Object}]
      # @return [Object, nil] the block result, or nil without a block
      # @raise [Exception] the block's exception. Subscriber errors are
      #   dropped in that case, so they cannot hide it. ActiveSupport lets
      #   them replace it.
      # @note Subscriber errors after a successful block propagate to the
      #   caller. Each caller decides whether to contain them.
      def instrument(name, payload = {})
        handle = build_handle(name, payload)
        handle.start

        begin
          result = yield payload if block_given?
        rescue Exception => error # standard:disable Lint/RescueException
          payload[:exception] = [error.class.name, error.message]
          payload[:exception_object] = error

          begin
            handle.finish
          rescue
            nil
          end

          raise
        end

        handle.finish
        result
      end
    end
  end
end
