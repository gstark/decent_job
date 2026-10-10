# frozen_string_literal: true

require "decent_job/active_job"

module ActiveJob
  module QueueAdapters
    # The name Active Job looks up for config.active_job.queue_adapter =
    # :decent_job. All behavior lives in {DecentJob::Adapter}.
    #
    # Active Job autoloads this file on first lookup. Requiring it loads
    # decent_job/active_job, so it runs only when Active Job is present.
    #
    # See docs/rails-integration.md.
    class DecentJobAdapter < DecentJob::Adapter
    end
  end
end
