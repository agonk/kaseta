-- Library metadata that is mutable after capture.
--
-- Capture artifacts in blob storage are the source of truth and are immutable:
-- a manifest records what was captured and must not be rewritten. Anything a
-- user changes afterwards — a title, a deletion — is library metadata and lives
-- here instead.

-- Renaming after the fact does not touch the recording's own notes. The
-- effective title is this override when set, otherwise whatever the manifest
-- recorded at capture time, otherwise a generated fallback.
ALTER TABLE recordings ADD COLUMN title_override TEXT;

-- Deletion is a tombstone first, purge second. Hard-deleting blobs and crashing
-- halfway would let the startup reconciler resurrect a half-deleted recording
-- from whatever survived.
ALTER TABLE recordings ADD COLUMN deleted_at INTEGER;

-- Duration is derived from the manifest at index time so listing does not have
-- to open every manifest.
ALTER TABLE recordings ADD COLUMN duration_ms INTEGER;

CREATE INDEX idx_recordings_live ON recordings (owner_id, started_at DESC)
    WHERE deleted_at IS NULL;
