# frozen_string_literal: true

# The Active Job bridge. Load it with require "decent_job/active_job", with
# config.active_job.queue_adapter = :decent_job, or through the Railtie.

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
  module ActiveJobExtensions
    # Active Job stores batch arguments through GlobalID, under the same app
    # name that native jobs write.
    module BatchGlobalID
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
