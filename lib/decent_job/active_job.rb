# frozen_string_literal: true

# The Active Job bridge. Load it with require "decent_job/active_job", with
# config.active_job.queue_adapter = :decent_job, or through the Railtie.
#
# This file and everything under decent_job/active_job_extensions load only
# when the application uses Active Job. The core gem must never require them.
# Loading this file:
# - registers {DecentJob::ActiveJobExtensions::Integration}, so the engine can
#   run Active Job classes next to native jobs;
# - makes {DecentJob::Batch} a GlobalID model, so batches can be job arguments;
# - adds the batch and cron enqueue checks to every ActiveJob::Base subclass;
# - sends unknown argument types to ActiveJob::Serializers.
#
# Concurrency, Labels, InterruptErrors, and NotifyOptions are opt-in. Job
# classes include them explicitly.

require "active_job"
require "decent_job"
require "decent_job/active_job_extensions/batches"
require "decent_job/active_job_extensions/concurrency"
require "decent_job/active_job_extensions/cron"
require "decent_job/active_job_extensions/integration"
require "decent_job/active_job_extensions/interrupt_errors"
require "decent_job/active_job_extensions/labels"
require "decent_job/active_job_extensions/notify_options"

module DecentJob
  # Active Job versions of the {JobExtensions} modules. Loaded only with
  # Active Job. See docs/rails-integration.md.
  module ActiveJobExtensions
    # Active Job stores batch arguments through GlobalID, under the same app
    # name that native jobs write.
    #
    # Native jobs write "gid://decent-job/..." through
    # {Batch#decent_job_global_id}. This module makes the globalid gem use the
    # same app name, so both job kinds can read each other's batch arguments.
    module BatchGlobalID
      # @param options [Hash] GlobalID options. An explicit :app wins.
      # @return [GlobalID]
      def to_global_id(options = {})
        super({app: "decent-job"}.merge(options))
      end
    end
  end
end

ActiveJob::QueueAdapters.autoload :DecentJobAdapter, "active_job/queue_adapters/decent_job_adapter"
DecentJob::Integrations.register(DecentJob::ActiveJobExtensions::Integration.new)
DecentJob::Batch.include(GlobalID::Identification)
DecentJob::Batch.prepend(DecentJob::ActiveJobExtensions::BatchGlobalID)
ActiveJob::Base.include DecentJob::ActiveJobExtensions::Batches
ActiveJob::Base.include DecentJob::ActiveJobExtensions::Cron
ActiveJob.singleton_class.prepend DecentJob::ActiveJobExtensions::CronBulk
# After ActiveJob::Base loads, which loads the serializers' dependencies.
DecentJob::Arguments.fallback = ActiveJob::Serializers
