# frozen_string_literal: true

module DecentJob
  class Base
    # Enqueue and perform callbacks with Active Job ordering, without
    # ActiveSupport::Callbacks.
    #
    # {Core#enqueue} runs the :enqueue chain and {Core#perform_now} runs the
    # :perform chain. Extensions in {JobExtensions} add callbacks through the
    # same class methods.
    #
    # Each callback wraps the callbacks defined after it. A before callback
    # that throws :abort halts the chain, skips later callbacks, and makes
    # run_callbacks return false. After callbacks do not run when the chain
    # halts. The code after the continuation call in an around callback still
    # runs.
    #
    # Differences from Active Job: filters must be method names or procs, not
    # callback objects. The only options are if: and unless:.
    #
    # See docs/native-jobs.md.
    module Callbacks
      KINDS = %i[enqueue perform].freeze

      # One registered callback. Instances are frozen and shared by subclasses.
      Callback = Data.define(:stage, :filter, :conditions) do
        # @param job [Base] the job that runs the chain
        # @return [Boolean] true when every if: condition is truthy and every
        #   unless: condition is falsy
        def applies?(job)
          conditions.fetch(:if).all? { |condition| Callbacks.evaluate(job, condition) } &&
            conditions.fetch(:unless).none? { |condition| Callbacks.evaluate(job, condition) }
        end
      end

      # Evaluates an if: or unless: condition.
      #
      # @param job [Base] the job
      # @param condition [Symbol, String, Proc, #call] a method name sent to the
      #   job, a proc run with instance_exec (the job is passed when the proc
      #   takes arguments), or any object that responds to call(job)
      # @return [Object] the condition value
      # @api private
      def self.evaluate(job, condition)
        case condition
        when Symbol, String then job.send(condition)
        when Proc then condition.arity.zero? ? job.instance_exec(&condition) : job.instance_exec(job, &condition)
        else condition.call(job)
        end
      end

      # Calls one callback filter.
      #
      # A method name filter is sent to the job, with the continuation as its
      # block for around callbacks. A proc filter runs with instance_exec and
      # gets the job and the continuation, cut to the proc's arity. An around
      # proc must take two arguments to call the continuation, as in Active
      # Job.
      #
      # @param job [Base] the job
      # @param filter [Symbol, String, Proc] the filter
      # @param continuation [Proc, nil] runs the rest of the chain, for around
      #   callbacks only
      # @return [Object] the filter value
      # @api private
      def self.invoke(job, filter, continuation = nil)
        case filter
        when Symbol, String
          continuation ? job.send(filter, &continuation) : job.send(filter)
        else
          arguments = continuation ? [job, continuation] : [job]
          arguments = arguments.first(filter.arity) if filter.arity >= 0
          job.instance_exec(*arguments, &filter)
        end
      end

      # The callback DSL: before_enqueue, after_enqueue, around_enqueue,
      # before_perform, after_perform and around_perform.
      #
      # Each DSL method takes one or more filters and an optional block:
      #
      #   before_perform :load_account, if: -> { arguments.first }
      #   around_perform { |job, block| block.call }
      #
      # Filters are method names (Symbol or String) or procs. The options are:
      # - if: [Symbol, String, Proc, #call, Array] conditions that must all be
      #   truthy. Default: none.
      # - unless: [Symbol, String, Proc, #call, Array] conditions that must all
      #   be falsy. Default: none.
      #
      # The DSL methods raise ArgumentError when there is no filter, when a
      # filter is not a method name or proc, or when an option is unknown.
      module ClassMethods
        KINDS.each do |kind|
          %i[before after around].each do |stage|
            define_method(:"#{stage}_#{kind}") do |*filters, **options, &block|
              add_callback(kind, stage, *filters, **options, &block)
            end
          end
        end

        # Returns the full chain for a kind, parent callbacks first.
        #
        # Parent callbacks are read on each run, so definitions added to a parent
        # after a subclass exists still apply to the subclass.
        #
        # @param kind [Symbol] :enqueue or :perform
        # @return [Array<Callback>] the chain in run order
        # @api private
        def callbacks_for(kind)
          inherited = (superclass.respond_to?(:callbacks_for) ? superclass.callbacks_for(kind) : [])
          inherited + own_callbacks.fetch(kind, [])
        end

        private

        # Appends callbacks for this class only. A block is added after the
        # positional filters. Each kind's list is replaced, not mutated, so a
        # chain that is running keeps its snapshot.
        def add_callback(kind, stage, *filters, **options, &block)
          filters << block if block
          raise ArgumentError, "a callback filter or block is required" if filters.empty?
          unless filters.all? { |filter| filter.is_a?(Symbol) || filter.is_a?(String) || filter.is_a?(Proc) }
            raise ArgumentError, "callback filters must be method names or procs"
          end
          unknown = options.keys - %i[if unless]
          raise ArgumentError, "unknown callback options: #{unknown.join(", ")}" unless unknown.empty?

          conditions = {if: Array(options[:if]), unless: Array(options[:unless])}.freeze
          callbacks = filters.map { |filter| Callback.new(stage: stage, filter: filter, conditions: conditions) }
          own_callbacks[kind] = (own_callbacks.fetch(kind, []) + callbacks).freeze
        end

        def own_callbacks
          @own_callbacks ||= {}
        end
      end

      def self.included(base)
        base.extend(ClassMethods)
      end

      # Runs the callback chain for a kind around a block.
      #
      # @param kind [Symbol] :enqueue or :perform
      # @yieldreturn [Object] the value of the wrapped work
      # @return [Object, false] the block value, or false when a before
      #   callback threw :abort. The block value is nil when an around
      #   callback did not call its continuation.
      # @note Callbacks that do not match their if: or unless: conditions are
      #   skipped. Exceptions from callbacks or the block propagate.
      def run_callbacks(kind, &block)
        chain = self.class.callbacks_for(kind)
        value = nil
        halted = false
        run = lambda do |index|
          if index == chain.size
            value = block.call
            return
          end

          callback = chain[index]
          next run.call(index + 1) unless callback.applies?(self)

          case callback.stage
          when :before
            continued = catch(:abort) do
              Callbacks.invoke(self, callback.filter)
              true
            end
            continued ? run.call(index + 1) : halted = true
          when :around
            Callbacks.invoke(self, callback.filter, -> { run.call(index + 1) })
          when :after
            run.call(index + 1)
            Callbacks.invoke(self, callback.filter) unless halted
          end
        end
        run.call(0)
        halted ? false : value
      end
    end
  end
end
