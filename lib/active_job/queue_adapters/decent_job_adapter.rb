# frozen_string_literal: true

require "decent_job"

module ActiveJob
  module QueueAdapters
    class DecentJobAdapter < DecentJob::Adapter
    end
  end
end
