# frozen_string_literal: true

module DecentJob
  class Configuration
    class Validator
      def initialize(configuration, errors)
        @configuration = configuration
        @errors = errors
      end

      def validate
        validate_handled_exceptions
        %i[retry_on_unhandled_error enable_cron].each do |field|
          @errors.add(field, "must be true or false") unless [true, false].include?(@configuration.public_send(field))
        end
        unless EXECUTION_MODES.include?(@configuration.execution_mode)
          @errors.add(:execution_mode, "must be #{EXECUTION_MODES.join(", ")}")
        end
        validate_queues
        unless finite_real?(@configuration.poll_interval)
          @errors.add(:poll_interval, "must be a finite real number")
        end
        timeout = @configuration.shutdown_timeout
        unless finite_real?(timeout) && timeout >= 0
          @errors.add(:shutdown_timeout, "must be nonnegative and finite")
        end
        reporter = @configuration.on_thread_error
        unless reporter.nil? || reporter.respond_to?(:call)
          @errors.add(:on_thread_error, "must be callable or nil")
        end
        period = @configuration.cron_graceful_restart_period
        if !period.nil? && !(finite_real?(period) && period.positive?)
          @errors.add(:cron_graceful_restart_period, "must be a finite positive number or nil")
        end
        policy = @configuration.preserve_job_records
        unless [true, false, :on_unhandled_error].include?(policy) || policy.respond_to?(:call)
          @errors.add(:preserve_job_records, "must be true, false, :on_unhandled_error, or callable")
        end
        validate_cron
        @errors
      end

      private

      def finite_real?(value)
        value.is_a?(Numeric) && value.real? && value.finite?
      end

      def validate_handled_exceptions
        values = @configuration.handled_exceptions
        valid = values.is_a?(Array) && values.all? do |value|
          (value.is_a?(Class) && value <= Exception) || (value.is_a?(String) && !value.empty?)
        end
        @errors.add(:handled_exceptions, "must be an Array of exception classes or class names") unless valid
      end

      def validate_queues
        threads = @configuration.max_threads
        unless threads.is_a?(Integer) && threads.positive?
          @errors.add(:max_threads, "must be a positive integer")
        end
        queues = @configuration.queues
        unless queues.is_a?(String) && !queues.strip.empty?
          @errors.add(:queues, "must be a nonempty string")
          return
        end
        queues.split("|", -1).map(&:strip).each do |group|
          if group.empty?
            @errors.add(:queues, "groups must not be empty")
          else
            # Validate the expression independently of an invalid default thread count.
            QueueSelection.pools(group, default_threads: 1)
          end
        rescue ArgumentError => error
          @errors.add(:queues, error.message)
        end
      end

      def validate_cron
        unless @configuration.cron.is_a?(Hash)
          @errors.add(:cron, "must be a Hash")
          return
        end
        @configuration.cron.each do |key, attributes|
          unless attributes.is_a?(Hash)
            @errors.add(:cron, "entry #{key.inspect} must be a Hash")
            next
          end
          entry = CronEntry.new(attributes.merge(key: key))
          next if entry.valid?

          entry.errors.each do |field, messages|
            configured_class = " #{entry.job_class.inspect}" if field == :class
            messages.each { |message| @errors.add(:cron, "entry #{key.inspect} #{field}#{configured_class} #{message}") }
          end
        rescue => error
          @errors.add(:cron, "entry #{key.inspect} is invalid: #{error.message}")
        end
      end
    end
  end
end
