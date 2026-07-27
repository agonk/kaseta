-- Derived artefacts move into the store, and metadata changes become
-- re-uploadable.
--
-- Transcripts, summaries and a person's chosen title lived only here, so a
-- backup — which copies the blob store — never contained them. They are now
-- written as blobs as well, and this column tracks when that still needs doing
-- or re-doing.
--
-- Two flags, because they answer different questions and are cleared by
-- different work. Conflating them means whichever one is never satisfied keeps
-- the other repeating: a machine with backup switched off would rewrite the
-- same blobs on every startup, waiting for an upload that is never going to
-- happen.
--
-- Set on every existing recording: none of them has a stored transcript,
-- summary or title yet, so all of them need one written and re-uploading.

-- The store is behind the index. Cleared once the artefacts are written.
ALTER TABLE recordings ADD COLUMN derived_dirty INTEGER NOT NULL DEFAULT 1;

-- The bucket is behind the store. Cleared once a backup completes.
ALTER TABLE recordings ADD COLUMN remote_dirty INTEGER NOT NULL DEFAULT 1;

-- Only rows that need work, so neither sweep walks the whole library once its
-- backlog is cleared.
CREATE INDEX IF NOT EXISTS idx_recordings_derived_dirty
    ON recordings (derived_dirty)
    WHERE derived_dirty = 1;

CREATE INDEX IF NOT EXISTS idx_recordings_remote_dirty
    ON recordings (remote_dirty)
    WHERE remote_dirty = 1;
