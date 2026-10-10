# frozen_string_literal: true

require "json"
require "time"

module DecentJob
  # Publishes wakeup hints on the "decent_job" channel with pg_notify.
  #
  # PostgreSQL delivers these hints only when the caller's transaction commits.
  # A rollback publishes nothing. A hint is not a record of work: workers also
  # find jobs by polling, so a lost or merged hint only delays a job. The
  # {Worker} listener parses the payload and accepts only the keys queue_name,
  # count, fanout, and scheduled_at.
  #
  # {Store} calls {.notify_jobs} after it inserts or reschedules jobs, and
  # {.notify} for broad maintenance wakeups.
  #
  # See docs/notifications.md.
  module Notifier
    # PostgreSQL rejects NOTIFY payloads of 8000 bytes or more.
    MAX_PAYLOAD_BYTES = 7999

    # Sends one hint on a caller-owned connection.
    #
    # @param connection [PG::Connection] the caller's connection. Use the
    #   connection of the transaction that wrote the jobs.
    # @param state [Hash{Symbol => Object}, nil] routing hint, sent as JSON.
    #   nil sends an empty payload, which wakes every pool.
    # @return [PG::Result]
    # @note Does not check {Configuration#enable_listen_notify} or per-job
    #   settings. Writes a NOTIFY that PostgreSQL sends at commit.
    def self.notify(connection, state = nil)
      payload = state ? JSON.generate(state) : ""
      # Queue names are unbounded. Preserve persistence and wake all queues when
      # the routing hint would exceed PostgreSQL's NOTIFY payload limit.
      payload = "" if payload.bytesize > MAX_PAYLOAD_BYTES
      connection.exec_params("SELECT pg_notify('decent_job', $1)", [payload])
    end

    # Sends one hint per (queue_name, future scheduled_at) group of jobs.
    #
    # Jobs that are due now have no scheduled_at in the hint. Each hint
    # carries the number of jobs in its group as count. Arguments are never
    # sent.
    #
    # @param connection [PG::Connection] the connection of the transaction
    #   that wrote the jobs
    # @param jobs [Array<#queue_name, #scheduled_at>] accepted jobs
    # @return [void]
    # @note Does nothing when {Configuration#enable_listen_notify} is false.
    #   Skips jobs whose decent_job_notify setting is false. A nil instance
    #   setting uses the class setting.
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
