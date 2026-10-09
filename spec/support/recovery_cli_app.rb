# frozen_string_literal: true

require "json"

module RecoveryProcessBody
  def perform(started, completed)
    File.write(started, provider_job_id)
    if ENV["DECENT_JOB_RECOVERY_SPEC_BLOCK"]
      sleep 60
    else
      File.write(completed, JSON.generate(provider_job_id: provider_job_id, executions: executions))
    end
  end
end

class RecoveryProcessJob < ActiveJob::Base
  include RecoveryProcessBody
  include DecentJob::ActiveJobExtensions::InterruptErrors
end

class RecoveryProcessRetryJob < RecoveryProcessJob
  retry_on DecentJob::InterruptError, wait: 3600, attempts: 3
end

class RecoveryProcessDiscardJob < RecoveryProcessJob
  discard_on DecentJob::InterruptError
end

class RecoveryProcessPlainJob < ActiveJob::Base
  include RecoveryProcessBody
end

class RecoveryProcessErrorJob < ActiveJob::Base
  def perform(completed, fail_execution)
    raise NotImplementedError, "process job not implemented" if fail_execution
    File.write(completed, provider_job_id)
  end
end

if ENV["DECENT_JOB_RECOVERY_SPEC_CONNECTION"]
  ActiveJob::Base.logger = Logger.new(File::NULL)
  [RecoveryProcessJob, RecoveryProcessRetryJob, RecoveryProcessDiscardJob, RecoveryProcessPlainJob, RecoveryProcessErrorJob].each do |klass|
    klass.queue_adapter = :decent_job
  end
  DecentJob.configure do |config|
    config.database_url = JSON.parse(ENV.fetch("DECENT_JOB_RECOVERY_SPEC_CONNECTION"))
    config.max_threads = 1
    config.poll_interval = 0.02
    config.enable_listen_notify = false
    config.cleanup_interval_jobs = false
    config.cleanup_interval_seconds = false
    config.on_thread_error = lambda do |error|
      File.open(ENV.fetch("DECENT_JOB_RECOVERY_SPEC_ERRORS"), "a") do |file|
        file.puts JSON.generate(class: error.class.name, message: error.message)
      end
    end
  end
end
