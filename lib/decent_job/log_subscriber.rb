# frozen_string_literal: true

# Event messages adapted from GoodJob; see LICENSE for the upstream notice.

require "decent_job/logging"
require "decent_job/notifications"
require "decent_job/support"

module DecentJob
  # Logs the public notification vocabulary without serializing job arguments.
  #
  # Each public handler method has the name of the event it logs, without the
  # ".decent_job" suffix. {#call} dispatches on that name and ignores events
  # with no handler. The library calls {.attach_to} with :decent_job when it
  # loads, so one instance listens on {Notifications}. Exception messages and
  # backtraces can still contain application data.
  #
  # Ownership: handlers run synchronously on the thread that published the
  # event. Class-level state (the logger list and the broadcast logger) is not
  # guarded by a mutex. Change it at boot, or from one thread.
  #
  # Constraint: a handler failure must never reach the runtime. {#call} logs
  # it and returns.
  #
  # See docs/instrumentation.md.
  class LogSubscriber
    # Logs a failed polling wakeup at error level. Uses :error. Silent when
    # :error is nil.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def finished_timer_task(event)
      exception = event.payload[:error]
      return unless exception

      error do
        "DecentJob error: #{exception.class}: #{exception}\n #{exception.backtrace}"
      end
    end

    # Logs a performer call that raised, at error level. Uses :error. Silent
    # when :error is nil.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def finished_job_task(event)
      exception = event.payload[:error]
      return unless exception

      error do
        "DecentJob error: #{exception.class}: #{exception}\n #{exception.backtrace}"
      end
    end

    # Logs a new execution pool at info level. Uses :max_threads,
    # :performer_name, and :process_id.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def scheduler_create_pool(event)
      max_threads = event.payload[:max_threads]
      performer_name = event.payload[:performer_name]
      process_id = event.payload[:process_id]

      info(tags: [process_id]) do
        "DecentJob #{DecentJob::VERSION} started scheduler with queues=#{performer_name} max_threads=#{max_threads}."
      end
    end

    # Logs cron startup at info level. Uses the size of :cron_entries.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def cron_manager_start(event)
      cron_entries = event.payload[:cron_entries]
      cron_jobs_count = cron_entries.size

      info do
        "DecentJob started cron with #{cron_jobs_count} #{Support.pluralize(cron_jobs_count, "job")}."
      end
    end

    # Logs the start of a pool shutdown at info level. Uses :process_id.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def scheduler_shutdown_start(event)
      process_id = event.payload[:process_id]

      info(tags: [process_id]) do
        "DecentJob shutting down scheduler..."
      end
    end

    # Logs the end of a pool shutdown at info level. Uses :process_id and
    # :stopped. stopped: false means the pool is still draining.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def scheduler_shutdown(event)
      process_id = event.payload[:process_id]

      info(tags: [process_id]) do
        (event.payload[:stopped] == false) ? "DecentJob scheduler is still draining." : "DecentJob scheduler is shut down."
      end
    end

    # Logs a forced shutdown at warn level. Uses :process_id and
    # :active_job_ids.
    #
    # @param event [Notifications::Event]
    # @return [void]
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

    # Logs a pool restart at info level. Uses :process_id, :restarted, and
    # :outcome.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def scheduler_restart_pools(event)
      process_id = event.payload[:process_id]

      info(tags: [process_id]) do
        (event.payload[:restarted] == false) ? "DecentJob scheduler restart #{event.payload[:outcome]}." : "DecentJob scheduler has restarted."
      end
    end

    # Logs supervisor startup at info level. Uses :subprocesses.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def cluster_start(event)
      subprocesses = event.payload[:subprocesses]

      info do
        "DecentJob #{DecentJob::VERSION} started supervisor with subprocesses=#{subprocesses}."
      end
    end

    # Logs a forked subprocess at info level. Uses :pid.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def cluster_spawn(event)
      pid = event.payload[:pid]

      info do
        "DecentJob supervisor forked subprocess (PID: #{pid})."
      end
    end

    # Logs a reaped subprocess at info level. Uses :pid, :status, and :signal.
    # A signal is shown by name when Ruby knows it.
    #
    # @param event [Notifications::Event]
    # @return [void]
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

    # Logs a delayed subprocess replacement at warn level. Uses :restart_count
    # and :delay (seconds).
    #
    # @param event [Notifications::Event]
    # @return [void]
    def cluster_backoff(event)
      restart_count = event.payload[:restart_count]
      delay = event.payload[:delay]

      warn do
        "DecentJob supervisor is delaying the replacement of a repeatedly exiting subprocess by #{delay} seconds (consecutive unhealthy exits: #{restart_count})."
      end
    end

    # Logs supervisor shutdown at info level. Uses no payload keys.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def cluster_shutdown(event)
      info do
        "DecentJob supervisor is shut down."
      end
    end

    # Logs subprocess startup at info level. Uses :pid.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def subprocess_start(event)
      pid = event.payload[:pid]

      info do
        "DecentJob started subprocess (PID: #{pid})."
      end
    end

    # Logs the start of a subprocess shutdown at info level. Uses :pid.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def subprocess_shutdown_start(event)
      pid = event.payload[:pid]

      info do
        "DecentJob shutting down subprocess (PID: #{pid})..."
      end
    end

    # Logs the end of a subprocess shutdown at info level. Uses :pid.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def subprocess_shutdown(event)
      pid = event.payload[:pid]

      info do
        "DecentJob subprocess is shut down (PID: #{pid})."
      end
    end

    # Logs a finished attempt at info level. Uses :job (a {Job} snapshot),
    # :process_id, and :thread_name. Arguments are not logged.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def perform_job(event)
      job = event.payload[:job]
      process_id = event.payload[:process_id]
      thread_name = event.payload[:thread_name]

      info(tags: [process_id, thread_name]) do
        "Executed DecentJob #{job.id}"
      end
    end

    # Logs a completed LISTEN at info level. Uses no payload keys.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def notifier_listen(event)
      info do
        "Notifier subscribed with LISTEN"
      end
    end

    # Logs a received NOTIFY payload at debug level. Uses the raw :payload
    # string, which can be nil.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def notifier_notified(event)
      payload = event.payload[:payload]

      debug do
        "Notifier received payload: #{payload}"
      end
    end

    # Logs a listener connection or task failure at error level. Uses :error.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def notifier_notify_error(event)
      exception = event.payload[:error]

      error do
        "Notifier errored: #{exception.class}: #{exception}\n #{exception.backtrace}"
      end
    end

    # Logs a completed UNLISTEN at info level. Uses no payload keys.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def notifier_unlisten(event)
      info do
        "Notifier unsubscribed with UNLISTEN"
      end
    end

    # Logs a cleanup run at info level. Uses :destroyed_records_count, its
    # job, execution, and batch counts, and the cutoff :timestamp.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def cleanup_preserved_jobs(event)
      payload = event.payload
      counts = {%w[job jobs] => payload[:destroyed_jobs_count], %w[execution executions] => payload[:destroyed_executions_count],
                %w[batch batches] => payload[:destroyed_batches_count]}
      details = counts.filter_map { |nouns, count| "#{count} #{Support.pluralize(count, *nouns)}" if count }.join(", ")
      total = payload[:destroyed_records_count]

      info do
        "DecentJob destroyed #{total} preserved #{Support.pluralize(total, "record")}#{" (#{details})" unless details.empty?} finished before #{payload[:timestamp]}."
      end
    end

    # Logs a rejected enqueue at info level. Uses :job (the original job
    # instance), :key, and :limit.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def enqueue_concurrency_limit_exceeded(event)
      job = event.payload[:job]
      key = event.payload[:key]
      limit = event.payload[:limit]

      info do
        "Aborted enqueue of #{job.class.name} (Job ID: #{job.job_id}) because the concurrency key '#{key}' has reached its enqueue limit of #{limit} #{Support.pluralize(limit, "job")}"
      end
    end

    # Logs a throttled enqueue at info level. Uses :job (the original job
    # instance), :key, and :limit.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def enqueue_concurrency_throttle_exceeded(event)
      job = event.payload[:job]
      key = event.payload[:key]
      limit = event.payload[:limit]

      info do
        "Aborted enqueue of #{job.class.name} (Job ID: #{job.job_id}) because the concurrency key '#{key}' has reached its throttle limit of #{limit} #{Support.pluralize(limit, "job")}"
      end
    end

    # Logs the watchdog interval at info level. Uses :interval (seconds).
    #
    # @param event [Notifications::Event]
    # @return [void]
    def systemd_watchdog_start(event)
      interval = event.payload[:interval]

      info do
        "Pinging systemd watchdog every #{interval.round(1)} seconds"
      end
    end

    # Logs a failed systemd ping at error level. Uses :error.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def systemd_watchdog_error(event)
      exception = event.payload[:error]

      error do
        "Error pinging systemd: #{exception.class}: #{exception}\n #{exception.backtrace}"
      end
    end

    # @return [Logging::BroadcastLogger] the shared class-level logger
    def logger
      self.class.logger
    end

    # Dispatches an event to the handler with the same name.
    #
    # Handlers swallow their own failures: logging must never fail the runtime.
    #
    # @param event [Notifications::Event]
    # @return [void]
    def call(event)
      handler = event.name.split(".").first.to_sym
      public_send(handler, event) if self.class.handlers.include?(handler)
    rescue => error
      log_exception(event.name, error)
    end

    class << self
      # @return [Array<Symbol>] the event names this class can log
      def handlers
        @handlers ||= public_instance_methods(false) - %i[logger call]
      end

      # @return [Array<LogSubscriber>] one instance per attached namespace
      def subscribers
        attachments.values.map(&:first)
      end

      # Subscribes a new instance to every event that ends in ".namespace".
      #
      # @param namespace [Symbol, String] for example :decent_job
      # @return [void]
      # @note Does nothing when the namespace is already attached.
      def attach_to(namespace)
        return if attachments.key?(namespace)

        subscriber = new
        subscription = Notifications.subscribe(/\.#{Regexp.escape(namespace.to_s)}\z/) { |event| subscriber.call(event) }
        attachments[namespace] = [subscriber, subscription]
      end

      # Removes the subscription that {.attach_to} made.
      #
      # @param namespace [Symbol, String]
      # @return [void]
      def detach_from(namespace)
        _subscriber, subscription = attachments.delete(namespace)
        Notifications.unsubscribe(subscription) if subscription
      end

      # The destination list. Callers can change it in place.
      #
      # Changing this array opts into explicit destinations. Configuration changes
      # only replace the default destination while the array remains untouched.
      # An empty array turns off log output, not notifications.
      #
      # @return [Array<Object>] the live, mutable destination list
      # @note After a change, call {.reset_logger} to rebuild {.logger}.
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

      # @return [Logging::BroadcastLogger] a cached logger that writes to each
      #   destination in {.loggers}
      def logger
        destinations = loggers
        @logger ||= Logging::BroadcastLogger.new(*destinations)
      end

      # Drops the cached broadcast logger. It does not restore the default
      # destination.
      #
      # @return [void]
      def reset_logger
        @logger = nil
      end

      private

      def attachments
        @attachments ||= {}
      end
    end

    private

    # Adds "DecentJob" and the given tags on each leaf destination that has
    # a tag stack, then runs the block once. When the destination already has
    # an "ActiveJob" tag, "ActiveJob" is pushed again before "DecentJob". This
    # matches the GoodJob output format.
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
        destination.respond_to?(:broadcasts) ? leaf_loggers(destination.broadcasts) : [destination]
      end.uniq(&:object_id)
    end

    %i[info debug warn error fatal unknown].each do |level|
      define_method(level) do |progname = nil, tags: [], &block|
        tag_logger(*tags) { logger.public_send(level, progname, &block) }
      end
    end

    # Report handler failures through the same logger. A failed destination
    # must not turn that reporting into a runtime failure.
    def log_exception(name, exception)
      logger.error("Could not log #{name.inspect} event. #{exception.class}: #{exception.message} #{exception.backtrace}")
    rescue
      nil
    end
  end
end
