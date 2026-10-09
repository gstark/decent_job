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

    def self.wait_all(handles, deadline:, force: false)
      failure = nil
      stopped = handles.map do |handle|
        handle.wait(deadline: deadline, force: force)
      rescue Exception => error # standard:disable Lint/RescueException
        failure ||= error
        false
      end.all?
      raise failure if failure
      stopped
    end

    def initialize(resource, revision, snapshot, failure: nil)
      @resource = resource
      @revision = revision
      @snapshot = snapshot
      @failure = failure
    end

    def wait(deadline: nil, force: false)
      raise @failure if @failure
      resource.send(:drain_shutdown, @snapshot, deadline: deadline, force: force)
    end

    def restart
      resource.start(expected_revision: revision)
    end
  end
end
