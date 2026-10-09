DROP INDEX decent_jobs_finished;
CREATE INDEX decent_jobs_finished ON decent_jobs (finished_at, id) WHERE finished_at IS NOT NULL;
CREATE INDEX decent_job_batches_finished ON decent_job_batches (finished_at, id) WHERE finished_at IS NOT NULL;
