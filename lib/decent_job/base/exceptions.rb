# frozen_string_literal: true

module DecentJob
  class Base
    # rescue_from, retry_on and discard_on with Active Job semantics. A job that
    # handles an error records its outcome so the runtime can report it.
    module Exceptions
      Handler = Data.define(:classes, :callback) do
        def handles?(exception)
          classes.any? do |klass|
            klass = Support.constantize(klass) if klass.is_a?(String)
            exception.is_a?(klass)
          end
        end
      end

      module ClassMethods
        def rescue_from(*classes, with: nil, &block)
          callback = with || block
          raise ArgumentError, "rescue_from needs a handler: pass with: or a block" unless callback
          classes.each do |klass|
            next if klass.is_a?(String) || (klass.is_a?(Class) && klass <= Exception)

            raise ArgumentError, "#{klass.inspect} must be an Exception class or a class name"
          end

          self.rescue_handlers = rescue_handlers + [Handler.new(classes: classes.freeze, callback: callback)]
        end

        def retry_on(*exceptions, wait: 3, attempts: 5, queue: nil, priority: nil, jitter: JITTER_DEFAULT, &block)
          rescue_from(*exceptions) do |error|
            executions = executions_for(exceptions)
            if attempts == :unlimited || executions < attempts
              retry_job(wait: determine_delay(wait, executions, jitter), queue: queue, priority: priority, error: error)
            else
              record_outcome(:retry_stopped, error)
              if block
                block.call(self, error)
                run_after_discard_procs(error)
              else
                run_after_discard_procs(error)
                raise error
              end
            end
          end
        end

        def discard_on(*exceptions, &block)
          rescue_from(*exceptions) do |error|
            record_outcome(:discarded, error)
            block&.call(self, error)
            run_after_discard_procs(error)
          end
        end

        def after_discard(&block)
          raise ArgumentError, "after_discard needs a block" unless block

          self.after_discard_procs = after_discard_procs + [block]
        end
      end

      JITTER_DEFAULT = Object.new
      private_constant :JITTER_DEFAULT

      def self.included(base)
        base.extend(ClassMethods)
      end

      # The last matching handler wins. An unhandled error falls back to its
      # cause. A handled error is returned, as Active Job does.
      def rescue_with_handler(exception, visited = [])
        return if exception.nil? || visited.include?(exception)

        visited << exception
        handler = self.class.rescue_handlers.reverse.find { |candidate| candidate.handles?(exception) }
        return rescue_with_handler(exception.cause, visited) unless handler

        callback = handler.callback
        callback = method(callback) if callback.is_a?(Symbol) || callback.is_a?(String)
        callback.arity.zero? ? instance_exec(&callback) : instance_exec(exception, &callback)
        exception
      end

      def retry_job(options = {})
        record_outcome(:retried, options[:error])
        scheduled_at, queue_name, priority = self.scheduled_at, self.queue_name, self.priority
        begin
          enqueue(options.slice(:wait, :wait_until, :queue, :priority))
        ensure
          self.scheduled_at, self.queue_name, self.priority = scheduled_at, queue_name, priority
        end
      end

      private

      def determine_delay(wait, executions, jitter)
        jitter = (jitter == JITTER_DEFAULT) ? self.class.retry_jitter : (jitter || 0.0)
        case wait
        when :polynomially_longer
          delay = executions**4
          delay + jitter_for(delay, jitter) + 2
        when Proc
          wait.call(executions)
        when Numeric, ->(value) { value.respond_to?(:to_i) && !value.is_a?(String) && !value.is_a?(Symbol) }
          delay = wait.to_i
          delay + jitter_for(delay, jitter)
        else
          raise ArgumentError, "Couldn't determine a delay based on #{wait.inspect}"
        end
      end

      def jitter_for(delay, jitter)
        jitter.zero? ? 0.0 : Kernel.rand * delay * jitter
      end

      def executions_for(exceptions)
        return executions unless exception_executions

        key = exceptions.to_s
        exception_executions[key] = (exception_executions[key] || 0) + 1
      end

      def run_after_discard_procs(exception)
        errors = []
        self.class.after_discard_procs.each do |block|
          instance_exec(self, exception, &block)
        rescue => error
          errors << error
        end
        raise errors.last unless errors.empty?
      end

      def record_outcome(event, error)
        @decent_job_outcome = [event, error].freeze
      end
    end
  end
end
