-- Where a recording's audio lives, and what has been done to it.
--
-- Until now the interface could say whether a transcript existed, but not
-- whether one was queued, running, or had failed — so a recording that failed
-- to transcribe looked identical to one that simply had not been tried, and
-- neither could be retried.

-- When the recording was last confirmed present in remote storage.
ALTER TABLE recordings ADD COLUMN uploaded_at INTEGER;

CREATE INDEX idx_jobs_by_recording_state ON jobs (recording_id, job_type, state);
