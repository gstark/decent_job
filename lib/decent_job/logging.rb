# frozen_string_literal: true

require "logger"
require "decent_job/isolated_state"

module DecentJob
  # Standard library loggers with tags and fan-out. Host loggers that provide
  # their own #tagged or #broadcasts, such as Rails.logger, work the same way.
  module Logging
    class SimpleFormatter < ::Logger::Formatter
      def call(_severity, _time, _progname, message)
        "#{message.is_a?(String) ? message : message.inspect}\n"
      end
    end

    module TaggedFormatter
      def call(severity, time, progname, message)
        tags = current_tags
        message = "#{tags.map { |tag| "[#{tag}] " }.join}#{message}" unless tags.empty?
        super
      end

      def current_tags
        IsolatedState[tags_key] ||= []
      end

      def push_tags(tags)
        current_tags.concat(tags)
        tags
      end

      def pop_tags(count)
        current_tags.pop(count)
      end

      private

      def tags_key
        @tags_key ||= :"decent_job_logging_tags_#{object_id}"
      end
    end

    module Tagged
      def tagged(*tags)
        pushed = formatter.push_tags(tags.flatten.compact.map(&:to_s).reject(&:empty?))
        begin
          yield self
        ensure
          formatter.pop_tags(pushed.size)
        end
      end
    end

    class BroadcastLogger
      attr_reader :broadcasts

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

    def tagged_logger(output)
      logger = ::Logger.new(output)
      logger.formatter = SimpleFormatter.new.extend(TaggedFormatter)
      logger.extend(Tagged)
    end

    # Matches by device or file name, recursing through broadcast loggers.
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
