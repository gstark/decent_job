# frozen_string_literal: true

require "decent_job/execution_state"
require "decent_job/instrumentation"

module DecentJob
  # Allocation happens under lifecycle locks; observation happens after the
  # outermost transition releases them. Immutable snapshots also survive host
  # share_with without lending another context our pending observations.
  module LifecycleEvents
    Scope = Data.define(:pid, :owner, :receipts)
    Receipt = Data.define(:resource, :name, :payload, :database, :on_error)
    private_constant :Scope, :Receipt

    def self.defer
      return yield if owned_scope

      receipts = []
      begin
        ExecutionState.with(:lifecycle_events, Scope.new(pid: Process.pid, owner: ActiveSupport::IsolatedExecutionState.context, receipts: [].freeze)) do
          yield
        ensure
          receipts = owned_scope&.receipts || []
        end
      ensure
        receipts.each { |receipt| deliver(receipt) }
      end
    end

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

    def self.observing?(resource)
      scope = ExecutionState[:lifecycle_observers]
      owned?(scope) && scope.receipts.include?(resource)
    end

    def self.deliver(receipt)
      return if observing?(receipt.resource)
      scope = ExecutionState[:lifecycle_observers]
      resources = owned?(scope) ? scope.receipts : []
      current = Scope.new(pid: Process.pid, owner: ActiveSupport::IsolatedExecutionState.context, receipts: (resources + [receipt.resource]).freeze)
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
      scope && scope.pid == Process.pid && scope.owner.equal?(ActiveSupport::IsolatedExecutionState.context)
    end
    private_class_method :owned?
  end
end
