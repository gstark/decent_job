# frozen_string_literal: true

require "json"
require "time"

module DecentJob
  # PostgreSQL delivers these hints only when the caller's transaction commits.
  module Notifier
    MAX_PAYLOAD_BYTES = 7999

    def self.notify(connection, state = nil)
      payload = state ? JSON.generate(state) : ""
      # Queue names are unbounded. Preserve persistence and wake all queues when
      # the routing hint would exceed PostgreSQL's NOTIFY payload limit.
      payload = "" if payload.bytesize > MAX_PAYLOAD_BYTES
      connection.exec_params("SELECT pg_notify('decent_job', $1)", [payload])
    end

    def self.notify_jobs(connection, jobs)
      return unless DecentJob.configuration.enable_listen_notify

      now = Time.now

      groups = jobs.select { |job| enabled?(job) }.group_by do |job|
        state = {queue_name: job.queue_name}
        state[:scheduled_at] = job.scheduled_at.utc.iso8601(6) if job.scheduled_at && job.scheduled_at > now
        state
      end

      groups.each { |state, entries| notify(connection, state.merge(count: entries.size)) }
    end

    def self.enabled?(job)
      return true unless job.respond_to?(:decent_job_notify)

      job.decent_job_notify != false && !(job.decent_job_notify.nil? && job.class.decent_job_notify == false)
    end
    private_class_method :enabled?
  end
end
