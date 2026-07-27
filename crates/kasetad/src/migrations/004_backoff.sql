-- Retry pacing for the job queue.
--
-- A failed job was requeued and reclaimed immediately, so a permanent failure —
-- a missing worker, a wrong model name — burned its entire attempt budget in
-- seconds. That turns a condition the user could fix into a job that is already
-- dead by the time they notice.

-- Earliest unix second this job may be claimed. Null means immediately.
ALTER TABLE jobs ADD COLUMN next_attempt_at INTEGER;

CREATE INDEX idx_jobs_claimable ON jobs (state, next_attempt_at, enqueue_seq);
