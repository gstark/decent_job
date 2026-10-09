CREATE TABLE decent_job_processes (
  id uuid PRIMARY KEY,
  state jsonb NOT NULL DEFAULT '{}',
  heartbeat_mode text NOT NULL CHECK (heartbeat_mode IN ('advisory', 'heartbeat')),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX decent_job_processes_updated_at ON decent_job_processes (updated_at);
ALTER TABLE decent_jobs ADD COLUMN locked_by_id uuid;
ALTER TABLE decent_jobs ADD COLUMN locked_at timestamptz;
CREATE INDEX decent_jobs_locked_by_id ON decent_jobs (locked_by_id) WHERE locked_by_id IS NOT NULL;
ALTER TABLE decent_job_executions ADD COLUMN process_uuid uuid;
