# frozen_string_literal: true

class RecordingJob < ActiveJob::Base
  class_attribute :events, default: Queue.new

  def perform(value)
    events << value
  end
end

class RetryingJob < RecordingJob
  retry_on RuntimeError, wait: 0, attempts: 2

  def perform(value)
    raise "try again" if executions == 1
    super
  end
end

class DiscardingJob < ActiveJob::Base
  discard_on ArgumentError

  def perform
    raise ArgumentError, "invalid argument"
  end
end

class FailingJob < ActiveJob::Base
  def perform
    raise "unhandled error"
  end
end

class BlockingJob < ActiveJob::Base
  class_attribute :started, default: Queue.new
  class_attribute :release, default: Queue.new

  def perform
    started << true
    release.pop
  end
end
