# frozen_string_literal: true

require "active_job/railtie"
require "decent_job"
require "json"
require "stringio"

class RailsCliHostJob < ActiveJob::Base
  def perform
    RailsCliChildJob.perform_later
  end
end

class RailsCliChildJob < ActiveJob::Base
  def perform
    max_threads = DecentJob.configuration.max_threads
    Rails.application.config.decent_job.max_threads = 3
    DecentJob.configure_rails(Rails.application)
    File.write(ENV.fetch("HOST_RESULT"), JSON.generate(
      max_threads: max_threads,
      reapplied_max_threads: DecentJob.configuration.max_threads,
      processes: DecentJob::ProcessRecord.all.size
    ))
  end
end

class RailsCliHostApplication < Rails::Application
  config.root = ENV.fetch("HOST_ROOT")
  config.eager_load = false
  config.secret_key_base = "host CLI secret" * 6
  config.logger = Logger.new(StringIO.new)
  config.active_job.queue_adapter = :decent_job
  config.decent_job.database_url = JSON.parse(ENV.fetch("HOST_CONNECTION"), symbolize_names: true)
  config.decent_job.execution_mode = :async_all
  config.decent_job.max_threads = 2
  config.decent_job.poll_interval = 0.02
  config.after_initialize do
    File.write(ENV.fetch("HOST_BOOT_RESULT"), JSON.generate(DecentJob.started?))
  end
end

RailsCliHostApplication.initialize!
