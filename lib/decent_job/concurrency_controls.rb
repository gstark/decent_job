# frozen_string_literal: true

require "decent_job/concurrency_claims"

module DecentJob
  class ConcurrencyControls
    class AdmissionRejected < StandardError; end

    def initialize(database)
      @database = database
    end

    def enqueue(job)
      return yield if job.respond_to?(:decent_job_enqueue_concurrency_enabled?) && !job.decent_job_enqueue_concurrency_enabled?

      rules = job.respond_to?(:decent_job_resolved_concurrency_rules) ? job.decent_job_resolved_concurrency_rules(stage: :enqueue) : []
      @database.transaction do |connection|
        rules.map(&:scope).uniq.sort.each do |scope|
          connection.exec_params("SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || '.decent_job.concurrency.' || $1, 0))", [scope])
        end
        rules.each { |rule| check_enqueue(connection, rule) }
        yield
      rescue PG::Error => error
        raise ActiveJob::EnqueueError, error.message
      end
    end

    private

    def check_enqueue(connection, rule)
      scope = case rule.kind
      when "label" then "labels @> ARRAY[$1]::text[]"
      when "key" then "concurrency_keys @> ARRAY[$1]::text[]"
      else "($1::text IS NULL)"
      end
      limit = rule.enqueue_limit || rule.total_limit
      if limit
        performing_filter = rule.enqueue_limit ? "AND NOT (#{ConcurrencyClaims::JOB_LOCKED_SQL})" : ""
        count = connection.exec_params("SELECT count(*) FROM decent_jobs WHERE #{scope} AND state IN ('ready', 'running') #{performing_filter}", [rule.value]).getvalue(0, 0).to_i
        raise AdmissionRejected, "enqueue concurrency limit exceeded for #{rule.scope}" if count >= limit
      end
      if rule.enqueue_throttle
        limit, seconds = rule.enqueue_throttle
        count = connection.exec_params("SELECT count(*) FROM decent_jobs WHERE #{scope} AND created_at > clock_timestamp() - $2::double precision * interval '1 second'", [rule.value, seconds]).getvalue(0, 0).to_i
        raise AdmissionRejected, "enqueue throttle exceeded for #{rule.scope}" if count >= limit
      end
    end
  end
end
