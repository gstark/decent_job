# frozen_string_literal: true

require "decent_job/job_extensions/notify_options"

module DecentJob
  module ActiveJobExtensions
    # Opt-in module that controls whether an Active Job enqueue sends NOTIFY.
    # Same module as {JobExtensions::NotifyOptions}.
    #
    # Loaded only with Active Job. Job classes include it explicitly.
    #
    # See docs/notifications.md.
    NotifyOptions = JobExtensions::NotifyOptions
  end
end
