# frozen_string_literal: true

# Event messages adapted from GoodJob; see LICENSE for the upstream notice.

require "active_support/broadcast_logger"
require "active_support/log_subscriber"
require "active_support/tagged_logging"

module DecentJob
  # Logs the public notification vocabulary without serializing job arguments.
  class LogSubscriber < ActiveSupport::LogSubscriber
    def finished_timer_task(event)
      exception = event.payload[:error]
      return unless exception

      error do
        "DecentJob error: #{exception.class}: #{exception}\n #{exception.backtrace}"
      end
    end

    def finished_job_task(event)
      exception = event.payload[:error]
      return unless exception

      error do
        "DecentJob error: #{exception.class}: #{exception}\n #{exception.backtrace}"
      end
    end

    def scheduler_create_pool(event)
      max_threads = event.payload[:max_threads]
      performer_name = event.payload[:performer_name]
      process_id = event.payload[:process_id]

      info(tags: [process_id]) do
        "DecentJob #{DecentJob::VERSION} started scheduler with queues=#{performer_name} max_threads=#{max_threads}."
      end
    end

    def cron_manager_start(event)
      cron_entries = event.payload[:cron_entries]
      cron_jobs_count = cron_entries.size

      info do
        "DecentJob started cron with #{cron_jobs_count} #{"job".pluralize(cron_jobs_count)}."
      end
    end

    def scheduler_shutdown_start(event)
      process_id = event.payload[:process_id]

      info(tags: [process_id]) do
        "DecentJob shutting down scheduler..."
      end
    end

    def scheduler_shutdown(event)
      process_id = event.payload[:process_id]

      info(tags: [process_id]) do
        (event.payload[:stopped] == false) ? "DecentJob scheduler is still draining." : "DecentJob scheduler is shut down."
      end
    end

    def scheduler_shutdown_kill(event)
      process_id = event.payload[:process_id]

      warn(tags: [process_id]) do
        active_job_ids = event.payload.fetch(:active_job_ids, [])
        if active_job_ids.any?
          "DecentJob scheduler has been killed. The following Active Jobs were interrupted: #{active_job_ids.join(" ")}"
        else
          "DecentJob scheduler has been killed."
        end
      end
    end

    def scheduler_restart_pools(event)
      process_id = event.payload[:process_id]

      info(tags: [process_id]) do
        (event.payload[:restarted] == false) ? "DecentJob scheduler restart #{event.payload[:outcome]}." : "DecentJob scheduler has restarted."
      end
    end

    def cluster_start(event)
      subprocesses = event.payload[:subprocesses]

      info do
        "DecentJob #{DecentJob::VERSION} started supervisor with subprocesses=#{subprocesses}."
      end
    end

    def cluster_spawn(event)
      pid = event.payload[:pid]

      info do
        "DecentJob supervisor forked subprocess (PID: #{pid})."
      end
    end

    def cluster_reap(event)
      pid = event.payload[:pid]
      status = event.payload[:status]
      signal = event.payload[:signal]

      info do
        exit_description = if signal
          "signal: #{::Signal.signame(signal) || signal}"
        else
          "exit status: #{status.inspect}"
        end
        "DecentJob supervisor reaped subprocess (PID: #{pid}, #{exit_description})."
      end
    end

    def cluster_backoff(event)
      restart_count = event.payload[:restart_count]
      delay = event.payload[:delay]

      warn do
        "DecentJob supervisor is delaying the replacement of a repeatedly exiting subprocess by #{delay} seconds (consecutive unhealthy exits: #{restart_count})."
      end
    end

    def cluster_shutdown(event)
      info do
        "DecentJob supervisor is shut down."
      end
    end

    def subprocess_start(event)
      pid = event.payload[:pid]

      info do
        "DecentJob started subprocess (PID: #{pid})."
      end
    end

    def subprocess_shutdown_start(event)
      pid = event.payload[:pid]

      info do
        "DecentJob shutting down subprocess (PID: #{pid})..."
      end
    end

    def subprocess_shutdown(event)
      pid = event.payload[:pid]

      info do
        "DecentJob subprocess is shut down (PID: #{pid})."
      end
    end

    def perform_job(event)
      job = event.payload[:job]
      process_id = event.payload[:process_id]
      thread_name = event.payload[:thread_name]

      info(tags: [process_id, thread_name]) do
        "Executed DecentJob #{job.id}"
      end
    end

    def notifier_listen(event)
      info do
        "Notifier subscribed with LISTEN"
      end
    end

    def notifier_notified(event)
      payload = event.payload[:payload]

      debug do
        "Notifier received payload: #{payload}"
      end
    end

    def notifier_notify_error(event)
      exception = event.payload[:error]

      error do
        "Notifier errored: #{exception.class}: #{exception}\n #{exception.backtrace}"
      end
    end

    def notifier_unlisten(event)
      info do
        "Notifier unsubscribed with UNLISTEN"
      end
    end

    def cleanup_preserved_jobs(event)
      timestamp = event.payload[:timestamp]
      destroyed_records_count = event.payload[:destroyed_records_count]

      info do
        "DecentJob destroyed #{destroyed_records_count} preserved job execution #{"records".pluralize(destroyed_records_count)} finished before #{timestamp}."
      end
    end

    def enqueue_concurrency_limit_exceeded(event)
      job = event.payload[:job]
      key = event.payload[:key]
      limit = event.payload[:limit]

      info do
        "Aborted enqueue of #{job.class.name} (Job ID: #{job.job_id}) because the concurrency key '#{key}' has reached its enqueue limit of #{limit} #{"job".pluralize(limit)}"
      end
    end

    def enqueue_concurrency_throttle_exceeded(event)
      job = event.payload[:job]
      key = event.payload[:key]
      limit = event.payload[:limit]

      info do
        "Aborted enqueue of #{job.class.name} (Job ID: #{job.job_id}) because the concurrency key '#{key}' has reached its throttle limit of #{limit} #{"job".pluralize(limit)}"
      end
    end

    def systemd_watchdog_start(event)
      interval = event.payload[:interval]

      info do
        "Pinging systemd watchdog every #{interval.round(1)} seconds"
      end
    end

    def systemd_watchdog_error(event)
      exception = event.payload[:error]

      error do
        "Error pinging systemd: #{exception.class}: #{exception}\n #{exception.backtrace}"
      end
    end

    def logger
      self.class.logger
    end

    class << self
      # Changing this array opts into explicit destinations. Configuration changes
      # only replace the default destination while the array remains untouched.
      def loggers
        unless defined?(@loggers)
          @default_logger = DecentJob.logger
          @loggers = [@default_logger]
          @explicit_loggers = false
        end
        @explicit_loggers ||= @loggers != [@default_logger]
        unless @explicit_loggers || @default_logger.equal?(DecentJob.logger)
          @default_logger = DecentJob.logger
          @loggers.replace([@default_logger])
          reset_logger
        end
        @loggers
      end

      def logger
        destinations = loggers
        @logger ||= ActiveSupport::BroadcastLogger.new(*destinations)
      end

      def reset_logger
        @logger = nil
      end
    end

    private

    def tag_logger(*tags, &block)
      tags = ["DecentJob", *tags].compact
      leaf_loggers(self.class.loggers).inject(block) do |inner, destination|
        formatter = destination.formatter if destination.respond_to?(:formatter)
        if destination.respond_to?(:tagged) && formatter.respond_to?(:current_tags)
          destination_tags = formatter.current_tags.include?("ActiveJob") ? ["ActiveJob", *tags] : tags
          proc { destination.tagged(*destination_tags, &inner) }
        else
          inner
        end
      end.call
    end

    # BroadcastLogger delegates tagged blocks to each child. Scope tags on the
    # leaves directly, then let the original broadcast write the event once.
    def leaf_loggers(destinations)
      destinations.flat_map do |destination|
        destination.is_a?(ActiveSupport::BroadcastLogger) ? leaf_loggers(destination.broadcasts) : [destination]
      end.uniq(&:object_id)
    end

    %i[info debug warn error fatal unknown].each do |level|
      define_method(level) do |progname = nil, tags: [], &block|
        tag_logger(*tags) { logger.public_send(level, progname, &block) }
      end
    end

    # ActiveSupport reports subscriber exceptions through the same logger. A
    # failed destination must not turn that reporting into a runtime failure.
    def log_exception(name, exception)
      super
    rescue
      nil
    end
  end
end
