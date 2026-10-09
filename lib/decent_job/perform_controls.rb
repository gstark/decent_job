# frozen_string_literal: true

require "decent_job/concurrency_claims"

module DecentJob
  class PerformControls
    def initialize(database)
      @claims = ConcurrencyClaims.new(database)
    end

    def check(job, context)
      rules = job.decent_job_resolved_concurrency_rules(stage: :perform).select { |rule| rule.perform_limit || rule.total_limit || rule.perform_throttle }
      return if rules.empty?
      exceeded = nil
      @claims.synchronize(context.fetch(:id), scopes: rules.map(&:scope)) do |connection|
        rules.each do |rule|
          limit = rule.perform_limit || rule.total_limit
          if limit && running_count(connection, rule, context) >= limit
            @claims.wait(connection, rule.scope, context)
            exceeded = ActiveJobExtensions::Concurrency::ConcurrencyExceededError
            break
          end
          @claims.grant(connection, rule.scope, context) if limit
          if rule.perform_throttle && throttled?(connection, rule, job)
            @claims.release(connection, context)
            exceeded = ActiveJobExtensions::Concurrency::ThrottleExceededError
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

    def running_count(connection, rule, context)
      connection.exec_params(<<~SQL, [rule.value, context.fetch(:id), rule.scope]).getvalue(0, 0).to_i
        SELECT count(*) FROM decent_jobs
        WHERE #{predicate(rule)} AND decent_jobs.id <> $2
          AND decent_jobs.state IN ('running', 'ready')
          AND #{ConcurrencyClaims::JOB_LOCKED_SQL}
          AND NOT EXISTS (
            SELECT 1 FROM decent_job_concurrency_claims
            WHERE job_id = decent_jobs.id AND scope = $3 AND state <> 'granted'
          )
      SQL
    end

    def throttled?(connection, rule, job)
      limit, seconds = rule.perform_throttle
      allowed = connection.exec_params(<<~SQL, [rule.value, seconds, limit, ActiveJobExtensions::Concurrency::ThrottleExceededError.name]).column_values(0)
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
