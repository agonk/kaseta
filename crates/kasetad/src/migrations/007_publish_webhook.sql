-- The hand-off stage could never be queued at all.
--
-- `job_type` has carried a CHECK listing the stages by name since the first
-- migration, and that list was never extended when sending was added. Every
-- attempt to enqueue one failed the constraint before it reached the network,
-- so the stage has never run on any installation — the interface offered a
-- button whose only possible outcome was a database error.
--
-- That is also why no row needs rewriting here, only the constraint. The stage
-- was renamed in the same change that fixed this, and the old name cannot
-- appear in any database: the constraint that broke the feature also made it
-- unstorable.
--
-- SQLite cannot alter a CHECK in place, so the table is rebuilt. Nothing
-- references `jobs`, which is what makes a straight copy safe rather than a
-- careful one.

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
                       'summarize','upload_remote','publish_webhook')),
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
-- definitions they replace; listed here so the finished schema does not depend
-- on reading four earlier migrations to know what a rebuild owes.
CREATE UNIQUE INDEX idx_jobs_enqueue_seq ON jobs (enqueue_seq);
CREATE INDEX idx_jobs_state ON jobs (state, enqueue_seq);
CREATE INDEX idx_jobs_recording ON jobs (recording_id, job_type, revision);
CREATE INDEX idx_jobs_claimable ON jobs (state, next_attempt_at, enqueue_seq);
CREATE INDEX idx_jobs_by_recording_state ON jobs (recording_id, job_type, state);
