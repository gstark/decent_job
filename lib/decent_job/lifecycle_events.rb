# frozen_string_literal: true

require "decent_job/execution_state"
require "decent_job/instrumentation"

module DecentJob
  # Defers worker and cron lifecycle notifications until the outermost
  # lifecycle transition has released its locks.
  #
  # Allocation happens under lifecycle locks; observation happens after the
  # outermost transition releases them. {Adapter}, {ResourceShutdown}, and the
  # runtime transitions in {ForkSafety} wrap their work in {.defer}. {Worker}
  # and {CronManager} call {.record}. Delivery goes through
  # {Instrumentation.instrument}, so observer errors go to the receipt's
  # on_error callback.
  #
  # Events recorded here: scheduler_create_pool, scheduler_shutdown_start,
  # scheduler_shutdown, scheduler_shutdown_kill, scheduler_restart_pools, and
  # cron_manager_start (each with the .decent_job suffix). {.record} adds
  # process_id (OS PID) and thread_name to every payload.
  #
  # Ownership: state lives in {ExecutionState} and belongs to one execution
  # context (thread or fiber) in one process. A scope created by another
  # context or before a fork is ignored. Immutable snapshots also survive host
  # share_with without lending another context our pending observations.
  #
  # Invariant: while observers for a resource run, new events for that same
  # resource are dropped. An observer that restarts its worker therefore
  # cannot cause endless creation events.
  #
  # See docs/instrumentation.md, section "Lifecycle observations".
  module LifecycleEvents
    # The :receipts field holds Receipt objects in the :lifecycle_events scope
    # and the resources being observed in the :lifecycle_observers scope.
    Scope = Data.define(:pid, :owner, :receipts)
    Receipt = Data.define(:resource, :name, :payload, :database, :on_error)
    private_constant :Scope, :Receipt

    # Runs the block and delivers the events it records after the block ends.
    #
    # Nested calls in the same context join the outer scope, so delivery
    # happens once, after the outermost block.
    #
    # @yield the lifecycle transition, which can hold mutexes
    # @return [Object] the block result
    # @note Deferred events are delivered in an ensure clause, also when the
    #   block raises or returns early.
    # @api private
    def self.defer
      return yield if owned_scope

      receipts = []

      begin
        ExecutionState.with(:lifecycle_events, Scope.new(pid: Process.pid, owner: IsolatedState.context, receipts: [].freeze)) do
          yield
        ensure
          receipts = owned_scope&.receipts || []
        end
      ensure
        receipts.each { |receipt| deliver(receipt) }
      end
    end

    # Records a lifecycle event, or delivers it now when no {.defer} scope is
    # open in this context.
    #
    # @param resource [Object] the worker or cron manager. Used to suppress
    #   nested events for the same resource.
    # @param name [String] event name without the ".decent_job" suffix
    # @param payload [Hash{Symbol => Object}] event fields. Hashes, arrays,
    #   and strings are copied, so later changes by the caller do not leak in.
    # @param database [Database, nil] passed to {Instrumentation.instrument}
    # @param on_error [#call, nil] observer error callback. nil drops observer
    #   errors. It does not fall back to the global callback.
    # @return [void]
    # @api private
    def self.record(resource, name, payload, database: nil, on_error: nil)
      return if observing?(resource)

      receipt = Receipt.new(resource: resource, name: name,
        payload: detach(payload).merge(process_id: Process.pid, thread_name: (Thread.current.name || Thread.current.object_id).to_s),
        database: database, on_error: on_error)

      if (scope = owned_scope)
        ExecutionState[:lifecycle_events] = scope.with(receipts: (scope.receipts + [receipt]).freeze)
      else
        deliver(receipt)
      end
    end

    # @param resource [Object] a worker or cron manager
    # @return [Boolean] true while this context runs observers for +resource+
    # @api private
    def self.observing?(resource)
      scope = ExecutionState[:lifecycle_observers]
      owned?(scope) && scope.receipts.include?(resource)
    end

    def self.deliver(receipt)
      return if observing?(receipt.resource)

      scope = ExecutionState[:lifecycle_observers]
      resources = owned?(scope) ? scope.receipts : []
      current = Scope.new(pid: Process.pid, owner: IsolatedState.context, receipts: (resources + [receipt.resource]).freeze)

      ExecutionState.with(:lifecycle_observers, current) do
        Instrumentation.instrument("#{receipt.name}.decent_job", receipt.payload, database: receipt.database, on_error: receipt.on_error) {}
      end
    end
    private_class_method :deliver

    def self.detach(value)
      case value
      when Hash then value.transform_values { |item| detach(item) }
      when Array then value.map { |item| detach(item) }
      when String then value.dup
      else value
      end
    end
    private_class_method :detach

    def self.owned_scope
      scope = ExecutionState[:lifecycle_events]
      scope if owned?(scope)
    end
    private_class_method :owned_scope

    def self.owned?(scope)
      scope && scope.pid == Process.pid && scope.owner.equal?(IsolatedState.context)
    end
    private_class_method :owned?
  end
end
