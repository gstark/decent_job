# frozen_string_literal: true

require "logger"
require "decent_job/isolated_state"

module DecentJob
  # Standard library loggers with tags and fan-out. Host loggers that provide
  # their own #tagged or #broadcasts, such as Rails.logger, work the same way.
  #
  # This module exists so the core can tag log lines without Active Support.
  # {Configuration} builds its default stdout logger with {.tagged_logger}.
  # {CLI} uses {.outputs_to?} to avoid a second stdout destination.
  # {LogSubscriber} wraps its destinations in a {BroadcastLogger}.
  #
  # See docs/instrumentation.md.
  module Logging
    # Writes only the message, one per line. Non-string messages are written
    # with #inspect.
    class SimpleFormatter < ::Logger::Formatter
      # @return [String] the formatted line
      def call(_severity, _time, _progname, message)
        "#{message.is_a?(String) ? message : message.inspect}\n"
      end
    end

    # Mixin for a formatter that prefixes each line with "[tag] " for each
    # active tag.
    #
    # Ownership: the tag stack is stored in {IsolatedState}, so each thread
    # or fiber has its own tags. The key includes the formatter's object_id,
    # so two loggers do not share tags.
    module TaggedFormatter
      # @return [String] the formatted line with tag prefixes
      def call(severity, time, progname, message)
        tags = current_tags
        message = "#{tags.map { |tag| "[#{tag}] " }.join}#{message}" unless tags.empty?
        super
      end

      # @return [Array<String>] the tag stack of the current context. The
      #   caller can change it in place.
      def current_tags
        IsolatedState[tags_key] ||= []
      end

      # @param tags [Array<String>] tags to add
      # @return [Array<String>] the added tags
      def push_tags(tags)
        current_tags.concat(tags)
        tags
      end

      # @param count [Integer] number of tags to remove from the end
      # @return [Array<String>] the removed tags
      def pop_tags(count)
        current_tags.pop(count)
      end

      private

      def tags_key
        @tags_key ||= :"decent_job_logging_tags_#{object_id}"
      end
    end

    # Mixin for a ::Logger whose formatter includes {TaggedFormatter}. It
    # gives the logger the same #tagged block API as Rails loggers.
    module Tagged
      # Adds tags for the lines logged in the block.
      #
      # @param tags [Array<#to_s>] nil and empty tags are skipped
      # @yieldparam logger [::Logger] this logger
      # @return [Object] the block result
      # @note The tags are removed when the block exits, also on an exception.
      def tagged(*tags)
        pushed = formatter.push_tags(tags.flatten.compact.map(&:to_s).reject(&:empty?))
        begin
          yield self
        ensure
          formatter.pop_tags(pushed.size)
        end
      end
    end

    # Sends each log call to every logger in a list.
    #
    # The constructor copies the logger list. A change to
    # {LogSubscriber.loggers} takes effect only after
    # {LogSubscriber.reset_logger} builds a new broadcast logger.
    class BroadcastLogger
      # @return [Array<Object>] the destination loggers
      attr_reader :broadcasts

      # @param loggers [Array<Object>] destinations that respond to the
      #   standard level methods
      def initialize(*loggers)
        @broadcasts = loggers
      end

      %i[debug info warn error fatal unknown].each do |level|
        define_method(level) do |*arguments, &block|
          @broadcasts.each { |logger| logger.public_send(level, *arguments, &block) }
          true
        end
      end
    end

    module_function

    # Builds a standard library logger with tag support.
    #
    # @param output [IO, String] an IO or a file path, as for ::Logger.new
    # @return [::Logger] a logger extended with {Tagged}
    def tagged_logger(output)
      logger = ::Logger.new(output)
      logger.formatter = SimpleFormatter.new.extend(TaggedFormatter)
      logger.extend(Tagged)
    end

    # Tells whether a logger already writes to +output+.
    #
    # Matches by device or file name, recursing through broadcast loggers.
    #
    # @param logger [Object] a ::Logger, a broadcast logger, or another logger
    # @param output [IO, String] the IO or file path to look for
    # @return [Boolean] false when the logger has no readable device
    def outputs_to?(logger, output)
      return logger.broadcasts.any? { |destination| outputs_to?(destination, output) } if logger.respond_to?(:broadcasts)

      device = logger.instance_variable_get(:@logdev)
      return false unless device

      source = (device.respond_to?(:filename) && device.filename) || device.dev
      target = output.respond_to?(:path) ? output.path : output
      !source.nil? && (source.equal?(output) || source == target)
    rescue IOError
      false
    end
  end
end
