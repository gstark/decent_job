# frozen_string_literal: true

module DecentJob
  # Raised when a lifecycle call races a runtime transition, such as a reset
  # or configuration change, that {ForkSafety#with_runtime_transition} holds.
  class LifecycleInProgress < StandardError; end

  # A handle for one stop transition of a {Worker} or {CronManager}.
  #
  # The resource's +begin_shutdown+ signals its threads and returns this
  # handle under the resource lock. The caller then waits outside all locks.
  # {DecentJob} and {Adapter} first signal every resource, then wait on all
  # handles with one shared monotonic deadline.
  #
  # A stop transition captures one run. Waiting never reads the owner's newer
  # threads, so an explicit start can safely race a completed run's drain.
  #
  # Protocol: the handle calls private resource methods with send.
  # - +drain_shutdown(snapshot, deadline:, force:)+ joins the captured run.
  # - +shutdown_snapshot_stopped?(snapshot)+ and +observe_lifecycle+ are
  #   needed only when +observe_shutdown+ or +lifecycle_pools+ are set.
  # - {#restart} calls +start_run+ when +lifecycle_pools+ is set, else +start+.
  #
  # See docs/shutdown.md.
  class ResourceShutdown
    # +revision+ is the resource lifecycle revision that this stop created,
    # or nil when +begin_shutdown+ failed. +resource+ is the worker or cron
    # manager.
    attr_reader :revision, :resource

    # Converts a timeout into a monotonic deadline.
    #
    # @param timeout [Numeric, nil] seconds, or nil for no limit
    # @return [Float, nil] a CLOCK_MONOTONIC time in seconds, or nil for no limit
    # @raise [ArgumentError] if timeout is negative, infinite, or not numeric
    def self.deadline(timeout)
      if !timeout.nil? && (!timeout.is_a?(Numeric) || !timeout.real? || !timeout.finite? || timeout.negative?)
        raise ArgumentError, "timeout must be a finite nonnegative number"
      end

      Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout if timeout
    end

    # @param deadline [Float, nil] from {.deadline}
    # @return [Float, nil] seconds left, never negative, or nil for no limit
    def self.remaining(deadline)
      deadline && [deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC), 0].max
    end

    # Calls +begin_shutdown+ on every resource.
    #
    # @param resources [Array<Worker, CronManager>]
    # @return [Array<ResourceShutdown>] one handle per resource. A resource
    #   whose +begin_shutdown+ raised gets a handle that raises the error from
    #   {#wait}, so the other resources still get signaled.
    def self.capture_all(resources)
      resources.map do |resource|
        resource.begin_shutdown
      rescue Exception => error # standard:disable Lint/RescueException
        new(resource, nil, nil, failure: error)
      end
    end

    # Waits on every handle, also after one raises.
    #
    # @param handles [Array<ResourceShutdown>]
    # @param deadline [Float, nil] shared deadline from {.deadline}
    # @param force [Boolean] kill remaining work when the deadline expires
    # @param timeout [Numeric, nil] the original timeout, for event payloads only
    # @return [Boolean] true when every resource drained
    # @raise [Exception] the first error from any handle, after all waits
    def self.wait_all(handles, deadline:, force: false, timeout: nil)
      failure = nil

      stopped = handles.map do |handle|
        handle.wait(deadline: deadline, force: force, timeout: timeout)
      rescue Exception => error # standard:disable Lint/RescueException
        failure ||= error
        false
      end.all?

      raise failure if failure

      stopped
    end

    # Waits on handles before a restart and reports restart failures.
    #
    # @param handles [Array<ResourceShutdown>] handles to wait on
    # @param restart_handles [Array<ResourceShutdown>] handles that the caller
    #   will restart. They get a restart event when the wait fails.
    # @param options [Hash] passed to {.wait_all}
    # @return [Boolean] true when every resource drained and a restart can go on
    # @raise [Exception] the first error from {.wait_all}
    def self.prepare_restart(handles, restart_handles: handles, **options)
      stopped = wait_all(handles, **options)
      restart_handles.each { |handle| handle.restart_failed(:incomplete) } unless stopped
      stopped
    rescue Exception => error # standard:disable Lint/RescueException
      restart_handles.each { |handle| handle.restart_failed(:failed, error: error) }
      raise
    end

    # @param resource [Worker, CronManager] the owner of the run
    # @param revision [Integer, nil] the revision this stop created
    # @param snapshot [Object] resource-specific run state. Only the resource
    #   reads it.
    # @param failure [Exception, nil] an error to raise from {#wait}
    # @param lifecycle_pools [Array<Hash>] per-pool event payloads. Empty for
    #   resources without pool events.
    # @param observe_shutdown [Boolean] emit scheduler_shutdown_start and
    #   scheduler_shutdown events. {Worker} sets it when the run was active.
    def initialize(resource, revision, snapshot, failure: nil, lifecycle_pools: [], observe_shutdown: false)
      @resource = resource
      @revision = revision
      @snapshot = snapshot
      @failure = failure
      @lifecycle_pools = lifecycle_pools
      @observe_shutdown = observe_shutdown
    end

    # Waits for the captured run to drain.
    #
    # @param deadline [Float, nil] see {.wait_all}
    # @param force [Boolean] see {.wait_all}
    # @param timeout [Numeric, nil] see {.wait_all}
    # @return [Boolean] true when the captured run drained
    # @raise [Exception] the capture failure, or a fatal error of the run
    def wait(deadline: nil, force: false, timeout: nil)
      raise @failure if @failure

      began = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      payload = {timeout: timeout, force: force}

      if @observe_shutdown
        stopped = resource.send(:shutdown_snapshot_stopped?, @snapshot)
        observe("scheduler_shutdown_start", payload.merge(stopped: stopped, draining: !stopped))
      end

      resource.send(:drain_shutdown, @snapshot, deadline: deadline, force: force)
    rescue Exception => error # standard:disable Lint/RescueException
      payload[:error] = error if payload
      raise
    ensure
      if @observe_shutdown && began
        stopped = resource.send(:shutdown_snapshot_stopped?, @snapshot)
        observe("scheduler_shutdown", payload.merge(stopped: stopped, draining: !stopped,
          operation_duration: Process.clock_gettime(Process::CLOCK_MONOTONIC) - began))
      end
    end

    # Starts the resource again if nothing else started or stopped it since
    # this handle was captured.
    #
    # @return [Object, false] the start result, or false when the revision
    #   changed and the start was refused
    # @note Emits scheduler_restart_pools events after the start, outside
    #   lifecycle locks.
    def restart
      LifecycleEvents.defer do
        began = Process.clock_gettime(Process::CLOCK_MONOTONIC)

        if @lifecycle_pools.empty?
          result = resource.start(expected_revision: revision)
        else
          result, new_run_id = resource.send(:start_run, expected_revision: revision)
          result = false unless new_run_id
        end

        observe_restart(result ? :restarted : :refused, new_run_id: new_run_id,
          operation_duration: Process.clock_gettime(Process::CLOCK_MONOTONIC) - began)
        result
      end
    rescue Exception => error # standard:disable Lint/RescueException
      observe_restart(:failed, error: error)
      raise
    end

    # Reports a restart that did not happen.
    #
    # @param outcome [Symbol] :incomplete, :failed, or :refused
    # @param details [Hash] extra event fields, for example +error:+
    # @return [false]
    def restart_failed(outcome, **details)
      observe_restart(outcome, **details)
      false
    end

    private

    def observe_restart(outcome, **details)
      @lifecycle_pools.each do |pool|
        resource.send(:observe_lifecycle, "scheduler_restart_pools", pool.merge(
          old_run_id: pool[:run_id], new_run_id: nil, restarted: outcome == :restarted,
          outcome: outcome, **details
        ))
      end
    end

    def observe(name, payload)
      @lifecycle_pools.each { |pool| resource.send(:observe_lifecycle, name, pool.merge(payload)) }
    end
  end
end
