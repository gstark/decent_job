ALTER TABLE decent_jobs ADD COLUMN labels text[] NOT NULL DEFAULT '{}';
ALTER TABLE decent_jobs ADD COLUMN concurrency_keys text[] NOT NULL DEFAULT '{}';
CREATE INDEX decent_jobs_labels ON decent_jobs USING gin(labels);
CREATE INDEX decent_jobs_concurrency_keys ON decent_jobs USING gin(concurrency_keys);

CREATE TABLE decent_job_pauses (
  kind text NOT NULL CHECK (kind IN ('queues', 'job_classes', 'labels')),
  key text NOT NULL,
  PRIMARY KEY (kind, key)
);
