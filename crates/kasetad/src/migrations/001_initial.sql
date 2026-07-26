-- Initial Kaseta schema.
--
-- Written the way a multi-user schema would be even though one local user
-- exists: surrogate ULID primary keys, an explicit owner_id on every root
-- entity, and no reliance on SQLite rowid. Moving to a networked database is
-- then a migration rather than a redesign.
--
-- Timestamps are unix seconds (INTEGER) for wall-clock events. Capture timing
-- lives in *_ns columns against CLOCK_BOOTTIME and is never mixed with these.

CREATE TABLE owners (
    id          INTEGER PRIMARY KEY,
    name        TEXT    NOT NULL,
    created_at  INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

INSERT INTO owners (id, name) VALUES (1, 'local');

CREATE TABLE recordings (
    id                TEXT      PRIMARY KEY,
    owner_id          INTEGER NOT NULL REFERENCES owners(id) ON DELETE CASCADE,
    status            TEXT    NOT NULL CHECK (status IN (
                          'recording','finalizing','processing','ready',
                          'partial','failed','canceled')),
    title             TEXT,
    started_at        INTEGER NOT NULL,
    ended_at          INTEGER,
    manifest_version  TEXT    NOT NULL,
    -- The manifest as written by capture, retained verbatim. The normalised
    -- tables below are derived from it; this is the record of what was actually
    -- captured, and survives any later schema change.
    manifest_json     TEXT,
    -- Reading of CLOCK_BOOTTIME when capture began.
    clock_started_ns  INTEGER,
    created_at        INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    updated_at        INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX idx_recordings_owner_started ON recordings (owner_id, started_at DESC);
CREATE INDEX idx_recordings_status ON recordings (status);

CREATE TABLE recording_tracks (
    id             TEXT      PRIMARY KEY,
    recording_id   TEXT      NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
    track_id       TEXT    NOT NULL,
    media_type     TEXT    NOT NULL CHECK (media_type IN ('audio','video')),
    role           TEXT    NOT NULL,
    source_kind    TEXT    NOT NULL,
    node_name      TEXT,
    display_name   TEXT,
    container      TEXT    NOT NULL,
    codec          TEXT    NOT NULL,
    sample_rate_hz INTEGER,
    channels       INTEGER,
    sample_format  TEXT,
    UNIQUE (recording_id, track_id)
);

CREATE TABLE recording_chunks (
    id                  TEXT      PRIMARY KEY,
    recording_id        TEXT      NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
    recording_track_id  TEXT      NOT NULL REFERENCES recording_tracks(id) ON DELETE CASCADE,
    seq                 INTEGER NOT NULL,
    -- Storage-agnostic object key. Never a filesystem path.
    blob_key            TEXT    NOT NULL,
    sha256              TEXT    NOT NULL,
    bytes               INTEGER NOT NULL,
    sample_count        INTEGER,
    boottime_start_ns   INTEGER NOT NULL,
    boottime_end_ns     INTEGER NOT NULL,
    source_pts_start_ns INTEGER,
    source_pts_end_ns   INTEGER,
    discontinuity       INTEGER NOT NULL DEFAULT 0,
    gap_before_ns       INTEGER NOT NULL DEFAULT 0,
    drops_before_chunk  INTEGER NOT NULL DEFAULT 0,
    uploaded_at         INTEGER,
    -- Capture writes each sequence exactly once; a retry that produces
    -- different bytes for the same sequence is a bug, not a new chunk.
    UNIQUE (recording_track_id, seq)
);

CREATE INDEX idx_chunks_recording ON recording_chunks (recording_id, seq);
CREATE INDEX idx_chunks_pending_upload ON recording_chunks (uploaded_at) WHERE uploaded_at IS NULL;

CREATE TABLE jobs (
    id             TEXT      PRIMARY KEY,
    -- Strictly increasing insertion order, and the only thing the queue orders
    -- by. ULIDs cannot serve this purpose: two minted in the same millisecond
    -- differ only in random bits, so sorting by id would claim jobs out of
    -- order. The Postgres equivalent is BIGSERIAL.
    enqueue_seq    INTEGER NOT NULL,
    recording_id   TEXT      NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
    job_type       TEXT    NOT NULL CHECK (job_type IN (
                       'finalize_recording','transcribe','merge_transcript',
                       'summarize','upload_remote')),
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
    created_at     INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE UNIQUE INDEX idx_jobs_enqueue_seq ON jobs (enqueue_seq);
-- Claiming scans queued jobs in insertion order.
CREATE INDEX idx_jobs_state ON jobs (state, enqueue_seq);
CREATE INDEX idx_jobs_recording ON jobs (recording_id, job_type, revision);

CREATE TABLE transcripts (
    id            TEXT      PRIMARY KEY,
    recording_id  TEXT      NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
    revision      INTEGER NOT NULL DEFAULT 1,
    engine_name   TEXT,
    engine_model  TEXT,
    language      TEXT,
    created_at    INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    UNIQUE (recording_id, revision)
);

CREATE TABLE transcript_segments (
    id                TEXT      PRIMARY KEY,
    transcript_id     TEXT      NOT NULL REFERENCES transcripts(id) ON DELETE CASCADE,
    track_id          TEXT    NOT NULL,
    seq               INTEGER NOT NULL,
    -- Positioned on the canonical timeline, already mapped out of chunk-local
    -- sample offsets.
    start_boottime_ns INTEGER NOT NULL,
    end_boottime_ns   INTEGER NOT NULL,
    -- Attribution derived from which track the audio came from, requiring no
    -- speaker model: 'local' | 'remote' | 'unknown'.
    speaker_hint      TEXT,
    -- Set once diarization and voice enrollment exist; null until then.
    speaker_label     TEXT,
    text              TEXT    NOT NULL,
    words_json        TEXT,
    UNIQUE (transcript_id, track_id, seq)
);

CREATE INDEX idx_segments_timeline ON transcript_segments (transcript_id, start_boottime_ns);

-- Full-text search over segments. Postgres tsvector is the eventual equivalent;
-- both are driven from the same segment rows, so search is a reimplementation
-- rather than a data migration.
CREATE VIRTUAL TABLE transcript_search USING fts5 (
    text,
    content = 'transcript_segments',
    content_rowid = 'rowid'
);

CREATE TRIGGER transcript_segments_ai AFTER INSERT ON transcript_segments BEGIN
    INSERT INTO transcript_search (rowid, text) VALUES (new.rowid, new.text);
END;

CREATE TRIGGER transcript_segments_ad AFTER DELETE ON transcript_segments BEGIN
    INSERT INTO transcript_search (transcript_search, rowid, text)
    VALUES ('delete', old.rowid, old.text);
END;

CREATE TRIGGER transcript_segments_au AFTER UPDATE ON transcript_segments BEGIN
    INSERT INTO transcript_search (transcript_search, rowid, text)
    VALUES ('delete', old.rowid, old.text);
    INSERT INTO transcript_search (rowid, text) VALUES (new.rowid, new.text);
END;

CREATE TABLE summaries (
    id             TEXT      PRIMARY KEY,
    recording_id   TEXT      NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
    revision       INTEGER NOT NULL DEFAULT 1,
    provider       TEXT    NOT NULL,
    model          TEXT    NOT NULL,
    -- Structured result: overview, decisions, action items, topic timeline.
    content_json   TEXT    NOT NULL,
    input_tokens   INTEGER,
    output_tokens  INTEGER,
    created_at     INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    UNIQUE (recording_id, revision)
);

-- Key/value configuration that must outlive a config file edit, such as which
-- devices were last used. Secrets are not stored here.
CREATE TABLE settings (
    key         TEXT PRIMARY KEY,
    value_json  TEXT NOT NULL,
    updated_at  INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);
