CREATE INDEX decent_jobs_creation_available ON decent_jobs (priority, created_at, id)
  WHERE state IN ('ready', 'running');
CREATE INDEX decent_jobs_queue_creation_available ON decent_jobs (queue_name, priority, created_at, id)
  WHERE state IN ('ready', 'running');
