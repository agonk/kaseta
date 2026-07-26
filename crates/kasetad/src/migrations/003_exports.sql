-- Exported audio, recorded at index time.
--
-- Listing previously discovered exports by scanning storage once per recording,
-- on every request. That put filesystem I/O on the request path and made listing
-- cost grow with the library. What exports exist changes only when a recording
-- is finalised, so it is indexed then and read back as a plain column.

-- Whether a combined stereo mix exists for this recording.
ALTER TABLE recordings ADD COLUMN has_mixed INTEGER NOT NULL DEFAULT 0;

-- The per-track exports, as a JSON array of {track_id, role}. A recording has a
-- handful of tracks and they are always read together, so a column avoids a
-- join and a second query per row.
ALTER TABLE recordings ADD COLUMN tracks_json TEXT;

-- Deletion purges objects before the row is removed. A purge that fails partway
-- must keep its tombstone, or the next reconcile resurrects the recording from
-- whatever survived.
ALTER TABLE recordings ADD COLUMN purge_pending INTEGER NOT NULL DEFAULT 0;
