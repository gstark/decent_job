# frozen_string_literal: true

module DecentJob
  class Base
    # rescue_from, retry_on and discard_on with Active Job semantics. A job that
    # handles an error records its outcome so the runtime can report it.
    #
    # {Core#perform_now} calls {#rescue_with_handler} when an error escapes
    # perform. retry_on and discard_on are rescue_from handlers, so the order
    # rules are the same for all three. Handlers live in the inherited
    # rescue_handlers class setting. Each call stores a new frozen array.
    #
    # The outcome goes to {Core#decent_job_outcome}. {Integrations::Native}
    # turns it into the execution state and error_event: :retried becomes
    # "retried", and :discarded and :retry_stopped become "discarded".
    #
    # Differences from Active Job: no report: option, and retry_jitter
    # defaults to 0.0.
    #
    # See docs/native-jobs.md.
    module Exceptions
      # One rescue_from registration.
      Handler = Data.define(:classes, :callback) do
        # @param exception [Exception] the error
        # @return [Boolean] true when the error is an instance of one of the
        #   classes. Class names are resolved on each call, so the class can
        #   load after the handler is defined.
        def handles?(exception)
          classes.any? do |klass|
            klass = Support.constantize(klass) if klass.is_a?(String)
            exception.is_a?(klass)
          end
        end
      end

      # The error handling DSL for job classes.
      module ClassMethods
        # Registers a handler for errors that escape perform.
        #
        # The last matching handler wins, so define general classes first. A
        # handled error does not raise from perform_now.
        #
        # @param classes [Array<Class, String>] Exception subclasses or class names
        # @param with [Symbol, String, Proc, nil] a method name or proc to call
        #   instead of a block
        # @yieldparam error [Exception] the error, when the handler takes an
        #   argument. The handler runs with instance_exec on the job.
        # @return [void]
        # @raise [ArgumentError] when there is no handler, or a class is not an
        #   Exception subclass or a String
        def rescue_from(*classes, with: nil, &block)
          callback = with || block
          raise ArgumentError, "rescue_from needs a handler: pass with: or a block" unless callback

          classes.each do |klass|
            next if klass.is_a?(String) || (klass.is_a?(Class) && klass <= Exception)

            raise ArgumentError, "#{klass.inspect} must be an Exception class or a class name"
          end

          self.rescue_handlers = (rescue_handlers + [Handler.new(classes: classes.freeze, callback: callback)]).freeze
        end

        # Enqueues the job again when one of the errors escapes perform.
        #
        # Each retry_on call counts attempts in exception_executions, keyed by
        # its exception list. When the count reaches attempts, the retry stops.
        # Then the block runs if there is one, and the after_discard blocks
        # run. Without a block, the error is raised from perform_now.
        #
        # @param exceptions [Array<Class, String>] the errors to retry
        # @param wait [Numeric, #to_i, Proc, Symbol] the delay in seconds. A
        #   Proc gets the attempt count and returns seconds; jitter does not
        #   apply to it. :polynomially_longer waits attempts**4 + 2 seconds
        #   plus jitter. Default: 3.
        # @param attempts [Integer, Float, Symbol] the total attempts for this
        #   handler, or :unlimited. Default: 5.
        # @param queue [String, Symbol, nil] queue name part for the retry, or
        #   nil to keep the queue. Default: nil.
        # @param priority [Integer, nil] priority for the retry, or nil to keep
        #   it. Default: nil.
        # @param jitter [Float, nil] the fraction of the delay to add at random.
        #   Nil means 0.0. Default: the class retry_jitter.
        # @yieldparam job [Base] the job, when attempts run out
        # @yieldparam error [Exception] the error, when attempts run out
        # @return [void]
        # @note A retry writes the job row again through {#retry_job} and
        #   records the outcome as :retried. When attempts run out, the outcome
        #   is :retry_stopped.
        # @note retry_on does not check wait. A Symbol other than
        #   :polynomially_longer, or a String, raises ArgumentError from the
        #   handler when the error first occurs.
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

        # Ends the job without a retry when one of the errors escapes perform.
        #
        # @param exceptions [Array<Class, String>] the errors to discard
        # @yieldparam job [Base] the job
        # @yieldparam error [Exception] the error
        # @return [void]
        # @note Records the outcome as :discarded. The block runs before the
        #   after_discard blocks.
        def discard_on(*exceptions, &block)
          rescue_from(*exceptions) do |error|
            record_outcome(:discarded, error)
            block&.call(self, error)
            run_after_discard_procs(error)
          end
        end

        # Registers a block that runs when the job ends without a retry.
        #
        # It runs after an unhandled error, a discard_on, and a retry_on that
        # ran out of attempts. It does not run after a plain rescue_from
        # handler. All blocks run even when one raises; then the last error is
        # raised.
        #
        # @yieldparam job [Base] the job. The block also runs with
        #   instance_exec on the job.
        # @yieldparam error [Exception] the error
        # @return [void]
        # @raise [ArgumentError] when there is no block
        def after_discard(&block)
          raise ArgumentError, "after_discard needs a block" unless block

          self.after_discard_procs = (after_discard_procs + [block]).freeze
        end
      end

      # Marks "jitter: not given", because nil is a valid value that means 0.0.
      JITTER_DEFAULT = Object.new
      private_constant :JITTER_DEFAULT

      def self.included(base)
        base.extend(ClassMethods)
      end

      # Finds and runs the handler for an error.
      #
      # The last matching handler wins. An unhandled error falls back to its
      # cause. A handled error is returned, as Active Job does.
      #
      # @param exception [Exception, nil] the error
      # @param visited [Array<Exception>] errors already checked, to stop
      #   cycles in the cause chain
      # @return [Exception, nil] the error that a handler handled (the error
      #   or one of its causes), or nil when no handler matched
      # @raise [Exception] any error that the handler raises
      # @api private
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

      # Enqueues this job again with the same job_id. retry_on calls it.
      #
      # The scheduled time, queue and priority are restored afterward, so
      # options apply only to the new enqueue. Inside the job's own execution,
      # {Adapter} updates the existing row instead of inserting a new one.
      #
      # @param options [Hash] :wait, :wait_until, :queue and :priority as in
      #   {Core#set}, and :error [Exception, nil] for the outcome
      # @return [Base, false] see {Core#enqueue}
      # @note Records the outcome as :retried before the enqueue, also when
      #   the enqueue then fails. Enqueue callbacks run.
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

      # Returns the retry delay in seconds. Matches Active Job's formula.
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

      # Increments and returns the attempt count for one retry_on call. The
      # key is exceptions.to_s, as in Active Job, so stored counts carry over
      # between the two frameworks.
      def executions_for(exceptions)
        return executions unless exception_executions

        key = exceptions.to_s
        exception_executions[key] = (exception_executions[key] || 0) + 1
      end

      # Runs every after_discard block, then raises the last error that a
      # block raised. One failing block does not skip the others.
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
