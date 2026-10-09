CREATE TABLE decent_job_concurrency_claims (
  scope text NOT NULL,
  job_id uuid NOT NULL REFERENCES decent_jobs(id) ON DELETE CASCADE,
  execution_id uuid NOT NULL REFERENCES decent_job_executions(id) ON DELETE CASCADE,
  state text NOT NULL CHECK (state IN ('granted', 'waiting', 'promotion_pending', 'promoted')),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (scope, job_id)
);
CREATE INDEX decent_job_concurrency_claims_job ON decent_job_concurrency_claims(job_id);
CREATE INDEX decent_job_concurrency_claims_waiting ON decent_job_concurrency_claims(scope, created_at, job_id) WHERE state = 'waiting';
