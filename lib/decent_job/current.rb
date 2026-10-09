# frozen_string_literal: true

module DecentJob
  module Current
    def self.execution
      Thread.current[:decent_job_execution]
    end

    def self.with(execution)
      previous = self.execution
      Thread.current[:decent_job_execution] = execution
      yield
    ensure
      Thread.current[:decent_job_execution] = previous
    end
  end
end
