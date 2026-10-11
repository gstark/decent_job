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
  # When Active Job loads, {.warn_unless_atomic_enqueue} checks that enqueues
  # inside Active Record transactions are atomic or deferred.
  #
  # See docs/rails-integration.md.
  class Railtie < ::Rails::Railtie
    config.decent_job = ActiveSupport::OrderedOptions.new

    # Load the Active Job bridge only for applications that use Active Job.
    ActiveSupport.on_load(:active_job) { require "decent_job/active_job" }

    # Keep DecentJob's migration versions in Rails schema dumps.
    ActiveSupport.on_load(:active_record) do
      require "active_record/schema_dumper"
      require "decent_job/schema_dump"
      ActiveRecord::SchemaDumper.prepend(DecentJob::SchemaDump::Ruby)
    end

    ActiveSupport.on_load(:active_record_postgresqladapter) do
      require "decent_job/schema_dump"
      prepend DecentJob::SchemaDump::Sql
    end

    initializer "decent_job.configure", before: "active_job.set_configs" do |application|
      DecentJob.configure_rails(application, validate: false)
    end

    config.after_initialize do |application|
      DecentJob.configure_rails(application)
      ActiveSupport.on_load(:active_job) { DecentJob::Railtie.warn_unless_atomic_enqueue(self) }
      DecentJob.finish_rails_initialization!
    end

    # Logs a warning when Active Job enqueues through DecentJob's own pool
    # without transaction deferral.
    #
    # In :own mode an enqueue inside an Active Record transaction commits at
    # once, so a worker can run the job before the application commits, and
    # a rollback leaves the job queued. The gem does not change
    # enqueue_after_transaction_commit itself. Applications without Active
    # Record get no warning, because they have no transaction to defer to.
    #
    # Active Job 8.1 reads enqueue_after_transaction_commit only from job
    # classes and ignores config.active_job, so the check reads ApplicationJob
    # when the application defines it, and ActiveJob::Base otherwise.
    #
    # @param job_class [Class] ActiveJob::Base, from the on_load hook
    # @return [void]
    # @note Can autoload ApplicationJob.
    # @api private
    def self.warn_unless_atomic_enqueue(job_class)
      return unless defined?(::ActiveRecord::Railtie) && job_class._queue_adapter.is_a?(DecentJob::Adapter)
      return unless DecentJob.configuration.connection == :own

      job_class = ::ApplicationJob if Object.const_defined?(:ApplicationJob) && job_class > ::ApplicationJob
      return if job_class.enqueue_after_transaction_commit

      DecentJob.configuration.logger.warn(
        "DecentJob enqueues through its own connection (connection: :own) and enqueue_after_transaction_commit is false. " \
        "Jobs enqueued in an Active Record transaction commit at once and survive a rollback. " \
        "Use config.decent_job.connection = :active_record, or set enqueue_after_transaction_commit = true."
      )
    end
  end
end
