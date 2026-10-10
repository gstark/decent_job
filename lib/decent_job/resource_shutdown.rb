# frozen_string_literal: true

module DecentJob
  class LifecycleInProgress < StandardError; end

  # A stop transition captures one run. Waiting never reads the owner's newer
  # threads, so an explicit start can safely race a completed run's drain.
  class ResourceShutdown
    attr_reader :revision, :resource

    def self.deadline(timeout)
      if !timeout.nil? && (!timeout.is_a?(Numeric) || !timeout.real? || !timeout.finite? || timeout.negative?)
        raise ArgumentError, "timeout must be a finite nonnegative number"
      end

      Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout if timeout
    end

    def self.remaining(deadline)
      deadline && [deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC), 0].max
    end

    def self.capture_all(resources)
      resources.map do |resource|
        resource.begin_shutdown
      rescue Exception => error # standard:disable Lint/RescueException
        new(resource, nil, nil, failure: error)
      end
    end

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

    def self.prepare_restart(handles, restart_handles: handles, **options)
      stopped = wait_all(handles, **options)
      restart_handles.each { |handle| handle.restart_failed(:incomplete) } unless stopped
      stopped
    rescue Exception => error # standard:disable Lint/RescueException
      restart_handles.each { |handle| handle.restart_failed(:failed, error: error) }
      raise
    end

    def initialize(resource, revision, snapshot, failure: nil, lifecycle_pools: [], observe_shutdown: false)
      @resource = resource
      @revision = revision
      @snapshot = snapshot
      @failure = failure
      @lifecycle_pools = lifecycle_pools
      @observe_shutdown = observe_shutdown
    end

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
