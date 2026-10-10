# frozen_string_literal: true

module DecentJob
  class Base
    # Enqueue and perform callbacks with Active Job ordering. Each callback wraps
    # the callbacks defined after it. A before callback that throws :abort halts
    # the chain, skips later callbacks, and makes run_callbacks return false.
    module Callbacks
      KINDS = %i[enqueue perform].freeze

      Callback = Data.define(:stage, :filter, :conditions) do
        def applies?(job)
          conditions.fetch(:if).all? { |condition| Callbacks.evaluate(job, condition) } &&
            conditions.fetch(:unless).none? { |condition| Callbacks.evaluate(job, condition) }
        end
      end

      def self.evaluate(job, condition)
        case condition
        when Symbol, String then job.send(condition)
        when Proc then condition.arity.zero? ? job.instance_exec(&condition) : job.instance_exec(job, &condition)
        else condition.call(job)
        end
      end

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

      module ClassMethods
        KINDS.each do |kind|
          %i[before after around].each do |stage|
            define_method(:"#{stage}_#{kind}") do |*filters, **options, &block|
              add_callback(kind, stage, *filters, **options, &block)
            end
          end
        end

        # Parent callbacks are read on each run, so definitions added to a parent
        # after a subclass exists still apply to the subclass.
        def callbacks_for(kind)
          inherited = (superclass.respond_to?(:callbacks_for) ? superclass.callbacks_for(kind) : [])
          inherited + own_callbacks.fetch(kind, [])
        end

        private

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
