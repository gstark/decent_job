# frozen_string_literal: true

require "decent_job/active_job"
require "json"

class AdvisoryClaimProcessJob < ActiveJob::Base
  class_attribute :visits, default: []
  class_attribute :nested_results, default: []

  def perform(value)
    visits << value
    return unless value == "outer" && visits.count("outer") == 1

    performer = DecentJob::Performer.new(database: self.class.queue_adapter.database)
    nested_results << performer.perform_one(job_id: provider_job_id)&.state
    nested_results << performer.perform_one&.state
  end
end

if ENV["ADVISORY_CLAIM_PROCESS_CONNECTION"]
  DecentJob.configure do |config|
    config.database_url = JSON.parse(ENV.fetch("ADVISORY_CLAIM_PROCESS_CONNECTION"), symbolize_names: true)
    config.enable_listen_notify = false
    config.logger = Logger.new(File::NULL)
  end
  ActiveJob::Base.logger = Logger.new(File::NULL)
  AdvisoryClaimProcessJob.queue_adapter = DecentJob::Adapter.new(execution_mode: :external)
  result = DecentJob::Performer.new.perform_one(job_id: ENV.fetch("ADVISORY_CLAIM_PROCESS_JOB"))
  puts "ADVISORY_RESULT:#{JSON.generate(state: result.state, visits: AdvisoryClaimProcessJob.visits, nested_results: AdvisoryClaimProcessJob.nested_results)}"
  DecentJob.shutdown
end
