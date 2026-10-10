# frozen_string_literal: true

require "decent_job"

module DecentJob
  # Marks the runtime as not ready. Adapters created during Rails boot wait
  # for {Railtie}'s after_initialize hook before they start async workers.
  prepare_rails!

  # Connects DecentJob to a Rails application.
  #
  # {HostIntegration#load_railtie_with_rails} requires this file when Rails is
  # present. The Railtie adds `config.decent_job`, applies it in two passes,
  # and then starts eligible async adapters:
  #
  # 1. Before Active Job reads its settings, it applies the options without
  #    validation. Autoloaders are not ready yet, and cron validation can load
  #    job classes.
  # 2. After initialization, it applies the options again with validation and
  #    calls {HostIntegration#finish_rails_initialization!}.
  #
  # See docs/rails-integration.md.
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
