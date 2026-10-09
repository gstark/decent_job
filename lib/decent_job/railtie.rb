# frozen_string_literal: true

require "decent_job"

module DecentJob
  prepare_rails!

  class Railtie < ::Rails::Railtie
    config.decent_job = ActiveSupport::OrderedOptions.new

    # Load the Active Job bridge only for applications that use Active Job.
    ActiveSupport.on_load(:active_job) { require "decent_job/active_job" }

    initializer "decent_job.configure", before: "active_job.set_configs" do |application|
      DecentJob.configure_rails(application, validate: false)
    end

    config.after_initialize do |application|
      DecentJob.configure_rails(application)
      DecentJob.finish_rails_initialization!
    end
  end
end
