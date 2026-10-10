# frozen_string_literal: true

require "decent_job/concurrency_claims"

module DecentJob
  # Raised in perform when a concurrency or throttle rule must delay a job.
  # Jobs with concurrency rules retry these without an attempt limit.
  class ConcurrencyExceededError < StandardError; end

  # Raised in perform when a perform throttle must delay a job.
  class ThrottleExceededError < ConcurrencyExceededError; end

  # Enforces perform-time concurrency limits and throttles for one attempt.
  #
  # {JobExtensions::Concurrency} calls {#check} from before_perform, only when
  # a {Performer} runs the job with this DecentJob adapter. The state lives in
  # decent_job_concurrency_claims through {ConcurrencyClaims}.
  #
  # A job over a limit gets a 'waiting' claim and raises
  # {ConcurrencyExceededError}. Its retry_on handler reschedules it with
  # backoff. When a slot opens, {ConcurrencyClaims} makes the oldest waiter
  # due at once, so it does not wait for its full backoff.
  #
  # See the "Labels, concurrency, and pauses" section of README.md.
  class PerformControls
    # @param database [Database] the pool for all queries
    def initialize(database)
      @claims = ConcurrencyClaims.new(database)
    end

    # Grants the attempt a slot in each limited scope, or makes it wait.
    #
    # Rules are checked in order. The first rule over its limit makes the job
    # wait in that scope; the first throttled rule releases every grant of
    # the attempt. The check does nothing when the attempt no longer owns the
    # job, for example after a force-discard.
    #
    # @param job [Object] the job instance
    # @param context [Hash] execution context with :id and :execution_id
    # @return [void]
    # @raise [ConcurrencyExceededError] when a perform or total limit is reached
    # @raise [ThrottleExceededError] when a perform throttle is exceeded
    # @note The error is raised after the claim transaction commits, so the
    #   waiting claim or released grant persists.
    def check(job, context)
      rules = job.decent_job_resolved_concurrency_rules(stage: :perform).select { |rule| rule.perform_limit || rule.total_limit || rule.perform_throttle }
      return if rules.empty?
      exceeded = nil

      @claims.synchronize(context.fetch(:id), scopes: rules.map(&:scope)) do |connection|
        current = connection.exec_params("SELECT finished_at, execution_id FROM decent_jobs WHERE id = $1", [context.fetch(:id)]).first
        next unless current && current["finished_at"].nil? && current["execution_id"] == context.fetch(:execution_id)

        rules.each do |rule|
          limit = rule.perform_limit || rule.total_limit

          if limit && running_count(connection, rule, context) >= limit
            @claims.wait(connection, rule.scope, context)
            exceeded = ConcurrencyExceededError
            break
          end

          @claims.grant(connection, rule.scope, context) if limit

          if rule.perform_throttle && throttled?(connection, rule, job)
            @claims.release(connection, context)
            exceeded = ThrottleExceededError
            break
          end
        end
      end

      raise exceeded if exceeded
    end

    private

    def predicate(rule)
      case rule.kind
      when "label" then "decent_jobs.labels @> ARRAY[$1]::text[]"
      when "key" then "decent_jobs.concurrency_keys @> ARRAY[$1]::text[]"
      else "($1::text IS NULL)"
      end
    end

    # Counts other executing jobs in the scope, under any lock strategy. Jobs
    # with a non-granted claim in this scope do not count: they wait for a
    # slot, or their attempt is still unwinding after a wait.
    def running_count(connection, rule, context)
      connection.exec_params(<<~SQL, [rule.value, context.fetch(:id), rule.scope]).getvalue(0, 0).to_i
        SELECT count(*) FROM decent_jobs
        WHERE #{predicate(rule)} AND decent_jobs.id <> $2
          AND decent_jobs.state IN ('running', 'ready')
          AND #{ConcurrencyClaims::EXECUTION_LOCKED_SQL}
          AND NOT EXISTS (
            SELECT 1 FROM decent_job_concurrency_claims
            WHERE job_id = decent_jobs.id AND scope = $3 AND state <> 'granted'
          )
      SQL
    end

    # Takes the earliest count executions started in the window, without
    # throttle-rejected attempts, and allows the job when it is among them.
    # It compares active_job_id, so a retry of an admitted job stays allowed.
    # The current attempt's execution row already exists and counts.
    def throttled?(connection, rule, job)
      limit, seconds = rule.perform_throttle
      allowed = connection.exec_params(<<~SQL, [rule.value, seconds, limit, ThrottleExceededError.name]).column_values(0)
        SELECT decent_jobs.active_job_id FROM decent_job_executions
        JOIN decent_jobs ON decent_jobs.id = decent_job_executions.job_id
        WHERE #{predicate(rule)}
          AND decent_job_executions.started_at > clock_timestamp() - $2::double precision * interval '1 second'
          AND (decent_job_executions.error IS NULL OR decent_job_executions.error->>'class' <> $4)
        ORDER BY decent_job_executions.started_at, decent_job_executions.id LIMIT $3
      SQL
      !allowed.include?(job.job_id)
    end
  end
end
