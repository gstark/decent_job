ALTER TABLE decent_jobs ADD COLUMN cron_key text, ADD COLUMN cron_at timestamptz;
CREATE UNIQUE INDEX decent_jobs_cron_occurrence_idx ON decent_jobs (cron_key, cron_at)
  WHERE cron_key IS NOT NULL AND cron_at IS NOT NULL;
CREATE INDEX decent_jobs_cron_history_idx ON decent_jobs (cron_key, cron_at DESC NULLS LAST, created_at DESC);
CREATE TABLE decent_job_cron_settings (
  cron_key text PRIMARY KEY,
  enabled boolean NOT NULL
);
