ALTER TABLE decent_job_executions
  ADD COLUMN payload jsonb,
  ADD COLUMN scheduled_at timestamptz,
  ADD COLUMN job_class text,
  ADD COLUMN queue_name text,
  ADD COLUMN duration double precision CHECK (duration >= 0 AND duration < 'Infinity'::double precision);

ALTER TABLE decent_jobs ADD COLUMN performed_at timestamptz;

-- Historical attempts have no trustworthy snapshot or monotonic measurement.
-- A terminal job can still point at an attempt completed before a pending
-- retry was cancelled. Only matching outcomes finished with this job establish
-- its start time: normal completion writes the attempt after the job, while
-- forced discard writes identical finish times.
UPDATE decent_jobs AS jobs SET performed_at = executions.started_at
FROM decent_job_executions AS executions
WHERE jobs.execution_id = executions.id AND jobs.id = executions.job_id
  AND (
    (jobs.state = 'running' AND jobs.finished_at IS NULL
      AND executions.state = 'running' AND executions.finished_at IS NULL)
    OR
    (jobs.state IN ('succeeded', 'discarded') AND executions.state = jobs.state
      AND jobs.finished_at IS NOT NULL AND executions.finished_at IS NOT NULL
      AND executions.finished_at >= jobs.finished_at)
  );
