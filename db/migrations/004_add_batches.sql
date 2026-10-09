CREATE TABLE decent_job_batches (
  id uuid PRIMARY KEY,
  description text,
  on_finish text,
  on_success text,
  on_discard text,
  callback_queue_name text,
  callback_priority integer,
  properties jsonb NOT NULL DEFAULT '{}',
  generation integer NOT NULL DEFAULT 0,
  reconciled_at timestamptz,
  enqueued_at timestamptz,
  jobs_finished_at timestamptz,
  finished_at timestamptz,
  discarded_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE decent_jobs ADD COLUMN batch_id uuid REFERENCES decent_job_batches(id) ON DELETE SET NULL;
ALTER TABLE decent_jobs ADD COLUMN batch_callback_id uuid REFERENCES decent_job_batches(id) ON DELETE SET NULL;
ALTER TABLE decent_job_executions ADD COLUMN batch_id uuid REFERENCES decent_job_batches(id) ON DELETE SET NULL;
CREATE INDEX decent_jobs_batch ON decent_jobs(batch_id) WHERE batch_id IS NOT NULL;
CREATE INDEX decent_jobs_batch_callback ON decent_jobs(batch_callback_id) WHERE batch_callback_id IS NOT NULL;

CREATE TABLE decent_job_batch_events (
  batch_id uuid NOT NULL REFERENCES decent_job_batches(id) ON DELETE CASCADE,
  generation integer NOT NULL,
  event text NOT NULL CHECK (event IN ('success', 'discard', 'finish')),
  job_class text NOT NULL,
  queue_name text,
  priority integer,
  job_id uuid REFERENCES decent_jobs(id) ON DELETE SET NULL,
  dispatched_at timestamptz,
  PRIMARY KEY (batch_id, generation, event)
);
CREATE INDEX decent_job_batch_events_pending ON decent_job_batch_events(batch_id) WHERE dispatched_at IS NULL;

-- Remember work still in flight at reopening. Its completion can commit before
-- the new generation becomes visible and must still contribute to that batch.
CREATE TABLE decent_job_batch_pending_jobs (
  batch_id uuid NOT NULL REFERENCES decent_job_batches(id) ON DELETE CASCADE,
  generation integer NOT NULL,
  job_id uuid NOT NULL,
  PRIMARY KEY (batch_id, job_id)
);

-- No batch foreign key: job completion must never wait for a batch-row lock.
-- Append-only inserts also avoid a conflicting row lock during completion.
CREATE TABLE decent_job_batch_updates (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  batch_id uuid NOT NULL,
  generation integer NOT NULL,
  job_id uuid,
  discarded boolean NOT NULL DEFAULT false
);
CREATE INDEX decent_job_batch_updates_batch ON decent_job_batch_updates(batch_id, id);
