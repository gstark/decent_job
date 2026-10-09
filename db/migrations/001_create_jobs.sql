CREATE TABLE decent_jobs (
  id uuid PRIMARY KEY,
  active_job_id uuid NOT NULL UNIQUE,
  queue_name text NOT NULL,
  job_class text NOT NULL,
  priority integer NOT NULL DEFAULT 0,
  payload jsonb NOT NULL,
  state text NOT NULL DEFAULT 'ready' CHECK (state IN ('ready', 'running', 'succeeded', 'discarded')),
  scheduled_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  finished_at timestamptz,
  execution_id uuid,
  executions_count integer NOT NULL DEFAULT 0,
  error jsonb
);

CREATE INDEX decent_jobs_available ON decent_jobs (priority, scheduled_at, id)
  WHERE state IN ('ready', 'running');
CREATE INDEX decent_jobs_queue_available ON decent_jobs (queue_name, priority, scheduled_at, id)
  WHERE state IN ('ready', 'running');
CREATE INDEX decent_jobs_finished ON decent_jobs (finished_at) WHERE finished_at IS NOT NULL;

CREATE TABLE decent_job_executions (
  id uuid PRIMARY KEY,
  job_id uuid NOT NULL REFERENCES decent_jobs(id) ON DELETE CASCADE,
  started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  finished_at timestamptz,
  state text NOT NULL DEFAULT 'running' CHECK (state IN ('running', 'succeeded', 'discarded', 'retried', 'interrupted')),
  error jsonb,
  process_id integer NOT NULL
);

CREATE INDEX decent_job_executions_job ON decent_job_executions (job_id, started_at);
