# frozen_string_literal: true

require "decent_job/concurrency_claims"

module DecentJob
  # Enforces enqueue-time concurrency limits and throttles, and takes the
  # scope locks that enqueue paths need.
  #
  # {Adapter} wraps each insert in {#enqueue}. {BatchStore} and {CronEntry}
  # call {#lock_scopes} before they persist captured jobs. {Batch} reaches it
  # through {BatchStore#lock_scopes}.
  # {Job#retry_job} passes {#scopes_for} to {ConcurrencyClaims#synchronize}.
  # Perform-time limits belong to {PerformControls}.
  #
  # Scope locks use the same advisory key as {ConcurrencyClaims}. They are
  # transaction-scoped, so they hold until the enclosing transaction ends.
  # Locks are always taken in sorted scope order to avoid deadlocks.
  #
  # See the "Labels, concurrency, and pauses" section of README.md.
  class ConcurrencyControls
    # Raised by {#enqueue} when a limit or throttle rejects the job.
    # {JobExtensions::Concurrency} rescues it and makes enqueue return false.
    class AdmissionRejected < StandardError; end

    # @param job [Object] a job instance
    # @return [Boolean] true when bulk enqueue must admit the job on its own:
    #   a class that includes the concurrency extension and sets limits or
    #   rules. Plain Active Job classes lack the extension.
    def self.controlled?(job)
      job.respond_to?(:decent_job_concurrency_controlled?) && job.decent_job_concurrency_controlled?
    end

    # @param database [Database] the pool for all queries
    def initialize(database)
      @database = database
    end

    # Checks the job's enqueue rules and runs the insert in the same
    # transaction, under the scope locks.
    #
    # The lock serializes concurrent enqueues in a scope, so the count and the
    # insert cannot race.
    #
    # @param job [Object] the job instance
    # @yield the insert. It runs only when every rule admits the job.
    # @return [Object] the block result
    # @raise [AdmissionRejected] when a limit or throttle is exceeded
    # @raise [EnqueueError] when a PG::Error occurs, also from the block
    # @note Skips all checks when the job reports
    #   decent_job_enqueue_concurrency_enabled? as false, for example when
    #   its concurrency enqueue callback did not run.
    # @note Emits enqueue_concurrency_limit_exceeded.decent_job or
    #   enqueue_concurrency_throttle_exceeded.decent_job before a rejection.
    def enqueue(job)
      return yield if job.respond_to?(:decent_job_enqueue_concurrency_enabled?) && !job.decent_job_enqueue_concurrency_enabled?

      rules = job.respond_to?(:decent_job_resolved_concurrency_rules) ? job.decent_job_resolved_concurrency_rules(stage: :enqueue) : []

      @database.transaction do |connection|
        rules.map(&:scope).uniq.sort.each do |scope|
          connection.exec_params("SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || '.decent_job.concurrency.' || $1, 0))", [scope])
        end

        rules.each { |rule| check_enqueue(connection, rule, job) }
        yield
      rescue PG::Error => error
        raise EnqueueError, error.message
      end
    end

    # @param jobs [Array<Object>] job instances
    # @return [Array<String>] the concurrency scopes of jobs whose enqueue
    #   concurrency is enabled. The list can contain duplicates.
    def scopes_for(jobs)
      jobs.flat_map do |job|
        next [] unless job.respond_to?(:decent_job_resolved_concurrency_rules)
        next [] if job.respond_to?(:decent_job_enqueue_concurrency_enabled?) && !job.decent_job_enqueue_concurrency_enabled?
        job.decent_job_resolved_concurrency_rules(stage: :metadata).map(&:scope)
      end
    end

    # Takes the scope locks for jobs in sorted order.
    #
    # @param jobs [Array<Object>] job instances
    # @return [void]
    # @note Call inside a {Database#transaction}. Outside a transaction each
    #   lock releases as soon as its statement ends.
    def lock_scopes(jobs)
      @database.with_connection do |connection|
        scopes_for(jobs).uniq.sort.each do |scope|
          connection.exec_params("SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || '.decent_job.concurrency.' || $1, 0))", [scope])
        end
      end
    end

    private

    # Counts unfinished jobs in the scope against enqueue_limit or total_limit.
    # When an enqueue_limit is configured, executing jobs do not count. This
    # holds even when a callable enqueue_limit returns nil and total_limit is
    # used instead, as in GoodJob's enqueue_limit_flag. The
    # "all" kind matches every job in the table. The throttle counts
    # decent_jobs rows created in the last throttle window, so deleted job
    # records do not count.
    def check_enqueue(connection, rule, job)
      scope = case rule.kind
      when "label" then "labels @> ARRAY[$1]::text[]"
      when "key" then "concurrency_keys @> ARRAY[$1]::text[]"
      else "($1::text IS NULL)"
      end

      limit = rule.enqueue_limit || rule.total_limit

      if limit
        performing_filter = rule.enqueue_limit_configured ? "AND NOT (#{ConcurrencyClaims::EXECUTION_LOCKED_SQL})" : ""
        count = connection.exec_params("SELECT count(*) FROM decent_jobs WHERE #{scope} AND state IN ('ready', 'running') #{performing_filter}", [rule.value]).getvalue(0, 0).to_i

        if count >= limit
          rejection_event("limit", job, rule, limit, count)
          raise AdmissionRejected, "enqueue concurrency limit exceeded for #{rule.scope}"
        end
      end

      if rule.enqueue_throttle
        limit, seconds = rule.enqueue_throttle
        count = connection.exec_params("SELECT count(*) FROM decent_jobs WHERE #{scope} AND created_at > clock_timestamp() - $2::double precision * interval '1 second'", [rule.value, seconds]).getvalue(0, 0).to_i

        if count >= limit
          rejection_event("throttle", job, rule, limit, count, window: seconds)
          raise AdmissionRejected, "enqueue throttle exceeded for #{rule.scope}"
        end
      end
    end

    def rejection_event(type, job, rule, limit, count, **extensions)
      payload = {job: job, key: rule.value, scope: rule.scope, kind: rule.kind, limit: limit, count: count, **extensions}
      Instrumentation.instrument("enqueue_concurrency_#{type}_exceeded.decent_job", payload, database: @database) { nil }
    end
  end
end
