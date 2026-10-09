-- Recordings made from an imported file rather than captured live.
--
-- Two changes. The job queue learns the stage that decodes an uploaded file,
-- and the index learns which recordings are imports and what they came from.
--
-- `job_type` carries a CHECK listing the stages by name, and SQLite cannot
-- alter a CHECK in place, so the table is rebuilt exactly as 007 rebuilt it,
-- with one more name in the list. Leaving the list alone is the mistake 007
-- exists to correct: the stage would be valid everywhere in the code and
-- unstorable in the one place that matters.

CREATE TABLE jobs_new (
    id             TEXT      PRIMARY KEY,
    -- Strictly increasing insertion order, and the only thing the queue orders
    -- by. ULIDs cannot serve this purpose: two minted in the same millisecond
    -- differ only in random bits, so sorting by id would claim jobs out of
    -- order. The Postgres equivalent is BIGSERIAL.
    enqueue_seq    INTEGER NOT NULL,
    recording_id   TEXT      NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
    job_type       TEXT    NOT NULL CHECK (job_type IN (
                       'finalize_recording','transcribe','merge_transcript',
                       'summarize','upload_remote','publish_webhook',
                       'import_media')),
    -- Incremented when a recording is reprocessed, e.g. with a different model.
    -- Results from separate revisions coexist until one is superseded.
    revision       INTEGER NOT NULL DEFAULT 1,
    state          TEXT    NOT NULL CHECK (state IN (
                       'queued','running','succeeded','failed_retryable',
                       'failed_terminal','canceled','superseded')),
    attempt        INTEGER NOT NULL DEFAULT 0,
    max_attempts   INTEGER NOT NULL DEFAULT 3,
    -- PID of the child worker while running. The startup sweep uses the absence
    -- of a live daemon, not this value, to detect orphans; it is kept for
    -- diagnostics.
    worker_pid     INTEGER,
    error_code     TEXT,
    error_message  TEXT,
    started_at     INTEGER,
    created_at     INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    next_attempt_at INTEGER
);

INSERT INTO jobs_new (
    id, enqueue_seq, recording_id, job_type, revision, state, attempt,
    max_attempts, worker_pid, error_code, error_message, started_at,
    created_at, next_attempt_at
)
SELECT
    id, enqueue_seq, recording_id, job_type, revision, state, attempt,
    max_attempts, worker_pid, error_code, error_message, started_at,
    created_at, next_attempt_at
FROM jobs;

DROP TABLE jobs;
ALTER TABLE jobs_new RENAME TO jobs;

-- Recreated because dropping the table dropped them with it. Identical to the
-- definitions 007 recreated.
CREATE UNIQUE INDEX idx_jobs_enqueue_seq ON jobs (enqueue_seq);
CREATE INDEX idx_jobs_state ON jobs (state, enqueue_seq);
CREATE INDEX idx_jobs_recording ON jobs (recording_id, job_type, revision);
CREATE INDEX idx_jobs_claimable ON jobs (state, next_attempt_at, enqueue_seq);
CREATE INDEX idx_jobs_by_recording_state ON jobs (recording_id, job_type, state);

-- Whether the recording was captured or imported. Every existing row was
-- captured: imports did not exist before this migration.
ALTER TABLE recordings ADD COLUMN origin TEXT NOT NULL DEFAULT 'captured'
    CHECK (origin IN ('captured','imported'));

-- The manifest's import source, as JSON. A cache, like the rest of this table:
-- the manifest holds the authoritative copy, and rebuilding the index from
-- storage restores this from it.
ALTER TABLE recordings ADD COLUMN source_json TEXT;

-- Whether an imported recording's kept original is still on this machine.
-- Cleared when removing local copies after backup, or retention, deletes it,
-- so the interface never offers to play a file that is only in the bucket.
ALTER TABLE recordings ADD COLUMN original_local INTEGER NOT NULL DEFAULT 0;
