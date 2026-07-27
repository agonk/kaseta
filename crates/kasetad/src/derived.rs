//! Keeping derived artefacts in the store, not only in the index.
//!
//! A transcript, a summary and a person's chosen title used to live solely in
//! SQLite. Backup copies the store, so none of them reached it: a lost machine
//! left a bucket full of audio and nothing that had been made from it.
//!
//! Everything here writes the store first and the index second. The index can
//! be rebuilt by reading the store; the store cannot be rebuilt by reading the
//! index. Writing in that order means a crash between the two leaves an
//! artefact that reconciliation can pick up, rather than an index row pointing
//! at something that was never written.

use anyhow::{Context, Result};
use kaseta_contracts::derived::{
    LibraryMetadata, SummaryBody, SummaryDocument, TranscriptDocument, TranscriptLine,
    DERIVED_VERSION,
};
use kaseta_contracts::RecordingPrefix;
use rusqlite::OptionalExtension;
use ulid::Ulid;

use crate::blobstore::BlobStore;
use crate::db::Db;

/// The revision every artefact currently carries.
///
/// Reprocessing replaces revision 1 today rather than adding a revision 2. The
/// number is written down anyway so that when that changes, existing documents
/// already say which one they are instead of having to be assumed.
pub const CURRENT_REVISION: u32 = 1;

/// Speaker hints as a reader should see them.
fn speaker_label(hint: Option<&str>) -> String {
    match hint {
        Some("local") => "you".into(),
        Some("remote") => "them".into(),
        _ => "unknown".into(),
    }
}

/// Builds the transcript document from what is indexed.
///
/// Times are rebased from the canonical clock, which counts from boot, onto the
/// recording itself. Boot time is meaningless to a reader and meaningless on
/// another machine, so a document carrying it would not survive the move a
/// backup exists to make possible.
pub fn transcript_document(db: &Db, recording_id: Ulid) -> Result<Option<TranscriptDocument>> {
    let header: Option<(String, Option<String>, Option<String>, Option<i64>, u32)> = db
        .conn()
        .query_row(
            "SELECT t.id, t.engine_model, t.language, r.clock_started_ns, t.revision
             FROM transcripts t
             JOIN recordings r ON r.id = t.recording_id
             WHERE t.recording_id = ?1
             ORDER BY t.revision DESC
             LIMIT 1",
            rusqlite::params![recording_id.to_string()],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .optional()?;

    let Some((transcript_id, engine, language, clock_started_ns, revision)) = header else {
        return Ok(None);
    };

    let mut stmt = db.conn().prepare(
        "SELECT start_boottime_ns, speaker_hint, text, track_id
         FROM transcript_segments
         WHERE transcript_id = ?1
         ORDER BY start_boottime_ns",
    )?;
    let rows = stmt.query_map(rusqlite::params![transcript_id], |r| {
        Ok((
            r.get::<_, i64>(0)? as u64,
            r.get::<_, Option<String>>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
        ))
    })?;
    let raw: Vec<(u64, Option<String>, String, String)> = rows.collect::<Result<_, _>>()?;

    // Capture's clock origin is the right reference. Falling back to the first
    // segment keeps a transcript readable when that origin was never indexed,
    // at the cost of appearing to start at zero.
    let origin = match clock_started_ns {
        Some(ns) if ns > 0 => ns as u64,
        _ => raw.first().map(|(t, _, _, _)| *t).unwrap_or(0),
    };

    Ok(Some(TranscriptDocument {
        version: DERIVED_VERSION.to_string(),
        revision,
        engine,
        language,
        lines: raw
            .into_iter()
            .map(|(start_ns, hint, text, track)| TranscriptLine {
                at_s: start_ns.saturating_sub(origin) as f64 / 1e9,
                speaker: speaker_label(hint.as_deref()),
                text,
                track: Some(track),
            })
            .collect(),
    }))
}

/// Writes the transcript beside the audio it came from.
pub fn publish_transcript(
    store: &dyn BlobStore,
    prefix: &RecordingPrefix,
    document: &TranscriptDocument,
) -> Result<()> {
    let key = prefix.transcript(document.revision);
    let bytes = serde_json::to_vec_pretty(document)?;
    store
        .put(&key, &bytes)
        .with_context(|| format!("writing {key}"))
}

/// Writes the summary beside the transcript it was drawn from.
pub fn publish_summary(
    store: &dyn BlobStore,
    prefix: &RecordingPrefix,
    revision: u32,
    transcript_revision: u32,
    provider: &str,
    model: &str,
    body: &SummaryBody,
) -> Result<()> {
    let document = SummaryDocument {
        version: DERIVED_VERSION.to_string(),
        revision,
        transcript_revision,
        provider: provider.to_string(),
        model: model.to_string(),
        body: body.clone(),
    };
    let key = prefix.summary(revision);
    let bytes = serde_json::to_vec_pretty(&document)?;
    store
        .put(&key, &bytes)
        .with_context(|| format!("writing {key}"))
}

/// Writes what a person changed after the fact.
///
/// Overwrites rather than versions: a title is current state, not a history,
/// and keeping every discarded name someone typed would be a record nobody
/// asked to keep.
pub fn publish_library_metadata(
    store: &dyn BlobStore,
    prefix: &RecordingPrefix,
    metadata: &LibraryMetadata,
) -> Result<()> {
    let key = prefix.library_metadata();
    let bytes = serde_json::to_vec_pretty(metadata)?;
    store
        .put(&key, &bytes)
        .with_context(|| format!("writing {key}"))
}

/// Reads back what a person changed, for rebuilding an index from a store.
///
/// A missing or unreadable document is not an error. The recording is still
/// entirely usable without a title someone chose, and refusing to reconcile the
/// rest of a library because one sidecar is malformed would turn a cosmetic
/// problem into an outage.
pub fn read_library_metadata(
    store: &dyn BlobStore,
    prefix: &RecordingPrefix,
) -> Option<LibraryMetadata> {
    let key = prefix.library_metadata();
    let bytes = store.get(&key).ok()?;
    match serde_json::from_slice(&bytes) {
        Ok(metadata) => Some(metadata),
        Err(e) => {
            tracing::warn!(%key, error = %e, "ignoring an unreadable library sidecar");
            None
        }
    }
}

/// Reads a stored transcript, for rebuilding an index from a store.
pub fn read_transcript(
    store: &dyn BlobStore,
    prefix: &RecordingPrefix,
    revision: u32,
) -> Option<TranscriptDocument> {
    let key = prefix.transcript(revision);
    let bytes = store.get(&key).ok()?;
    match serde_json::from_slice(&bytes) {
        Ok(document) => Some(document),
        Err(e) => {
            tracing::warn!(%key, error = %e, "ignoring an unreadable transcript");
            None
        }
    }
}

/// Reads a stored summary, for rebuilding an index from a store.
pub fn read_summary(
    store: &dyn BlobStore,
    prefix: &RecordingPrefix,
    revision: u32,
) -> Option<SummaryDocument> {
    let key = prefix.summary(revision);
    let bytes = store.get(&key).ok()?;
    match serde_json::from_slice(&bytes) {
        Ok(document) => Some(document),
        Err(e) => {
            tracing::warn!(%key, error = %e, "ignoring an unreadable summary");
            None
        }
    }
}

/// Records that a recording's derived artefacts have changed.
///
/// Sets both flags: something new exists, so it needs writing to the store and
/// then copying to the bucket. They are cleared separately, by the work that
/// actually satisfies each.
pub fn mark_dirty(db: &Db, recording_id: Ulid) -> Result<()> {
    db.conn().execute(
        "UPDATE recordings SET derived_dirty = 1, remote_dirty = 1 WHERE id = ?1",
        rusqlite::params![recording_id.to_string()],
    )?;
    Ok(())
}

/// The artefacts are now in the store.
pub fn clear_store_dirty(db: &Db, recording_id: Ulid) -> Result<()> {
    db.conn().execute(
        "UPDATE recordings SET derived_dirty = 0 WHERE id = ?1",
        rusqlite::params![recording_id.to_string()],
    )?;
    Ok(())
}

/// The store's contents are now in the bucket.
pub fn clear_remote_dirty(db: &Db, recording_id: Ulid) -> Result<()> {
    db.conn().execute(
        "UPDATE recordings SET remote_dirty = 0 WHERE id = ?1",
        rusqlite::params![recording_id.to_string()],
    )?;
    Ok(())
}

pub fn is_dirty(db: &Db, recording_id: Ulid) -> Result<bool> {
    Ok(db
        .conn()
        .query_row(
            "SELECT remote_dirty FROM recordings WHERE id = ?1",
            rusqlite::params![recording_id.to_string()],
            |r| r.get::<_, i64>(0),
        )
        .optional()?
        .unwrap_or(0)
        != 0)
}

fn flagged(db: &Db, column: &str) -> Result<Vec<(Ulid, time::OffsetDateTime)>> {
    let sql = format!(
        "SELECT id, started_at FROM recordings
         WHERE {column} = 1 AND deleted_at IS NULL
         ORDER BY started_at"
    );
    let mut stmt = db.conn().prepare(&sql)?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;

    let mut out = Vec::new();
    for row in rows {
        let (id, started_at) = row?;
        let Ok(id) = Ulid::from_string(&id) else { continue };
        let Ok(started_at) = time::OffsetDateTime::from_unix_timestamp(started_at) else {
            continue;
        };
        out.push((id, started_at));
    }
    Ok(out)
}

/// Recordings whose stored artefacts are behind their index.
pub fn dirty_recordings(db: &Db) -> Result<Vec<(Ulid, time::OffsetDateTime)>> {
    flagged(db, "derived_dirty")
}

/// Recordings whose bucket copy is behind the store.
pub fn remote_dirty_recordings(db: &Db) -> Result<Vec<(Ulid, time::OffsetDateTime)>> {
    flagged(db, "remote_dirty")
}

/// What a heal pass did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Healed {
    pub written: usize,
    pub already_current: usize,
}

/// Writes stored artefacts for recordings that predate them.
///
/// Every recording made before derived artefacts were stored has a transcript
/// and summary that exist only in the index, and a backup that silently omits
/// them. Left to be fixed the next time each one happened to be touched, those
/// backups would stay incomplete indefinitely — so the backlog is worked
/// through once, at startup, rather than waited on.
pub fn heal(store: &dyn BlobStore, db: &Db) -> Result<Healed> {
    let mut healed = Healed::default();

    for (id, started_at) in dirty_recordings(db)? {
        let prefix = RecordingPrefix::new(id, started_at);
        match store_for(store, db, id, &prefix) {
            Ok(true) => {
                healed.written += 1;
                // The store is now current. Whether the bucket is, is a
                // separate question with its own flag — otherwise a machine
                // with backup switched off would rewrite these same blobs on
                // every startup, waiting on an upload that is never coming.
                clear_store_dirty(db, id)?;
            }
            Ok(false) => {
                // Nothing derived exists yet — a recording that was never
                // transcribed. There is nothing to store, so nothing is owed.
                healed.already_current += 1;
                clear_store_dirty(db, id)?;
            }
            Err(e) => {
                tracing::warn!(%id, error = %format!("{e:#}"), "could not store derived artefacts");
            }
        }
    }

    if healed.written > 0 {
        tracing::info!(
            written = healed.written,
            "stored derived artefacts that were only indexed"
        );
    }
    Ok(healed)
}

/// Writes everything derived for one recording into the store.
///
/// Returns whether anything was written: a recording that has been neither
/// transcribed nor renamed has nothing to store, which is not a failure.
///
/// The flag is deliberately not cleared here. Written is not the same as
/// backed up, and clearing on the write would leave a bucket permanently one
/// step behind. Only a completed upload clears it.
pub fn store_for(
    store: &dyn BlobStore,
    db: &Db,
    recording_id: Ulid,
    prefix: &RecordingPrefix,
) -> Result<bool> {
    let mut wrote = false;

    if let Some(document) = transcript_document(db, recording_id)? {
        publish_transcript(store, prefix, &document)?;
        wrote = true;
    }

    if let Some(document) = summary_document(db, recording_id)? {
        let key = prefix.summary(document.revision);
        let bytes = serde_json::to_vec_pretty(&document)?;
        store
            .put(&key, &bytes)
            .with_context(|| format!("writing {key}"))?;
        wrote = true;
    }

    // Republished when a title exists, and also when one used to: a sidecar
    // already in the store still naming a title someone has since cleared is
    // exactly the stale state this repair pass exists to correct.
    let title = title_override(db, recording_id)?;
    if title.is_some() || store.exists(&prefix.library_metadata()).unwrap_or(false) {
        publish_library_metadata(
            store,
            prefix,
            &LibraryMetadata {
                version: DERIVED_VERSION.to_string(),
                title_override: title,
            },
        )?;
        wrote = true;
    }

    Ok(wrote)
}

/// Rebuilds index rows from stored documents.
///
/// The claim that the store is the source of truth is only worth anything if
/// the index can be reconstructed from it. Restoring a bucket onto a fresh
/// machine, or losing the database while keeping the recordings, both end here.
///
/// Only fills gaps. Anything the index already holds is left alone, because a
/// stored document is a copy taken at some past moment and the index is what
/// has been maintained since.
pub fn reindex_from_store(
    store: &dyn BlobStore,
    db: &Db,
    recording_id: Ulid,
    prefix: &RecordingPrefix,
) -> Result<bool> {
    let mut restored = false;

    let has_transcript: bool = db
        .conn()
        .query_row(
            "SELECT 1 FROM transcripts WHERE recording_id = ?1 LIMIT 1",
            rusqlite::params![recording_id.to_string()],
            |_| Ok(true),
        )
        .optional()?
        .unwrap_or(false);

    if !has_transcript {
        if let Some(document) = read_transcript(store, prefix, CURRENT_REVISION) {
            reindex_transcript(db, recording_id, &document)?;
            restored = true;
        }
    }

    let has_summary: bool = db
        .conn()
        .query_row(
            "SELECT 1 FROM summaries WHERE recording_id = ?1 LIMIT 1",
            rusqlite::params![recording_id.to_string()],
            |_| Ok(true),
        )
        .optional()?
        .unwrap_or(false);

    if !has_summary {
        if let Some(document) = read_summary(store, prefix, CURRENT_REVISION) {
            let json = serde_json::to_string(&document.body)?;
            db.conn().execute(
                "INSERT INTO summaries (id, recording_id, revision, provider, model, content_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(recording_id, revision) DO NOTHING",
                rusqlite::params![
                    Ulid::new().to_string(),
                    recording_id.to_string(),
                    document.revision,
                    document.provider,
                    document.model,
                    json
                ],
            )?;
            restored = true;
        }
    }

    if title_override(db, recording_id)?.is_none() {
        if let Some(metadata) = read_library_metadata(store, prefix) {
            if let Some(title) = metadata.title_override.filter(|t| !t.trim().is_empty()) {
                db.conn().execute(
                    "UPDATE recordings SET title_override = ?1 WHERE id = ?2",
                    rusqlite::params![title, recording_id.to_string()],
                )?;
                restored = true;
            }
        }
    }

    Ok(restored)
}

/// Writes a stored transcript back into the index.
///
/// Times return to the canonical clock they were rebased out of. The origin
/// comes from this machine's record of when capture started; a recording
/// restored without one keeps its internal spacing but starts at zero, which is
/// the same compromise reading makes in that situation.
fn reindex_transcript(db: &Db, recording_id: Ulid, document: &TranscriptDocument) -> Result<()> {
    let origin: u64 = db
        .conn()
        .query_row(
            "SELECT clock_started_ns FROM recordings WHERE id = ?1",
            rusqlite::params![recording_id.to_string()],
            |r| r.get::<_, Option<i64>>(0),
        )
        .optional()?
        .flatten()
        .filter(|ns| *ns > 0)
        .unwrap_or(0) as u64;

    let transcript_id = Ulid::new();
    let tx = db.conn().unchecked_transaction()?;

    tx.execute(
        "INSERT INTO transcripts (id, recording_id, revision, engine_name, engine_model, language)
         VALUES (?1, ?2, ?3, 'restored', ?4, ?5)",
        rusqlite::params![
            transcript_id.to_string(),
            recording_id.to_string(),
            document.revision,
            document.engine.as_deref(),
            document.language.as_deref(),
        ],
    )?;

    for (seq, line) in document.lines.iter().enumerate() {
        let start = origin + (line.at_s.max(0.0) * 1e9) as u64;
        let hint = match line.speaker.as_str() {
            "you" => Some("local"),
            "them" => Some("remote"),
            _ => None,
        };
        tx.execute(
            "INSERT INTO transcript_segments
                 (id, transcript_id, track_id, seq, start_boottime_ns, end_boottime_ns,
                  speaker_hint, text)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                Ulid::new().to_string(),
                transcript_id.to_string(),
                line.track.as_deref().unwrap_or("restored"),
                seq as i64,
                start as i64,
                start as i64,
                hint,
                line.text,
            ],
        )?;
    }

    tx.commit()?;
    Ok(())
}

/// Brings already-uploaded recordings back for another pass.
///
/// A finished backup clears the flag, so nothing would revisit a recording
/// whose *rules* changed rather than whose contents did — switching
/// transcription off, or asking to reclaim space after the fact. Both mean work
/// that was correctly deferred is now owed.
pub fn mark_uploaded_for_revisit(db: &Db) -> Result<usize> {
    Ok(db.conn().execute(
        "UPDATE recordings SET remote_dirty = 1
         WHERE deleted_at IS NULL AND uploaded_at IS NOT NULL",
        [],
    )?)
}

/// Queues uploads for recordings whose stored copy has fallen behind.
///
/// Run on a timer rather than from the rename itself, which is what keeps a
/// burst of edits to one recording from becoming a burst of uploads: the flag
/// is set as often as someone likes and read once per pass.
///
/// Does nothing when backup is off. There is nowhere for the copy to fall
/// behind, and the flag stays set for whenever backup is switched on.
pub fn requeue_dirty(db: &Db, settings: &crate::config::Settings) -> Result<usize> {
    if !settings.remote_storage.enabled {
        return Ok(0);
    }

    let mut queued = 0usize;
    for (id, _) in remote_dirty_recordings(db)? {
        if db
            .enqueue_again(id, kaseta_contracts::JobType::UploadRemote, 1)?
            .is_some()
        {
            queued += 1;
        }
    }
    if queued > 0 {
        tracing::info!(queued, "queued backups for changed recordings");
    }
    Ok(queued)
}

/// Builds the summary document from what is indexed.
pub fn summary_document(db: &Db, recording_id: Ulid) -> Result<Option<SummaryDocument>> {
    let row: Option<(u32, String, String, String)> = db
        .conn()
        .query_row(
            "SELECT revision, provider, model, content_json FROM summaries
             WHERE recording_id = ?1 ORDER BY revision DESC LIMIT 1",
            rusqlite::params![recording_id.to_string()],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;

    let Some((revision, provider, model, content_json)) = row else {
        return Ok(None);
    };
    let body: SummaryBody = serde_json::from_str(&content_json)?;

    // Which transcript this was drawn from was not recorded before summaries
    // were stored, so for anything already in the index the current one is the
    // only defensible answer.
    let transcript_revision = db
        .conn()
        .query_row(
            "SELECT revision FROM transcripts WHERE recording_id = ?1
             ORDER BY revision DESC LIMIT 1",
            rusqlite::params![recording_id.to_string()],
            |r| r.get::<_, u32>(0),
        )
        .optional()?
        .unwrap_or(CURRENT_REVISION);

    Ok(Some(SummaryDocument {
        version: DERIVED_VERSION.to_string(),
        revision,
        transcript_revision,
        provider,
        model,
        body,
    }))
}

/// A title someone typed, if they did.
pub fn title_override(db: &Db, recording_id: Ulid) -> Result<Option<String>> {
    Ok(db
        .conn()
        .query_row(
            "SELECT title_override FROM recordings WHERE id = ?1",
            rusqlite::params![recording_id.to_string()],
            |r| r.get::<_, Option<String>>(0),
        )
        .optional()?
        .flatten()
        .filter(|t| !t.trim().is_empty()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blobstore::LocalFsStore;
    use tempfile::TempDir;
    use time::macros::datetime;

    fn prefix() -> RecordingPrefix {
        RecordingPrefix::new(Ulid::new(), datetime!(2026-07-27 09:00:00 UTC))
    }

    fn store() -> (TempDir, LocalFsStore) {
        let dir = TempDir::new().unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();
        (dir, store)
    }

    #[test]
    fn a_title_survives_being_written_and_read_back() {
        let (_dir, store) = store();
        let prefix = prefix();

        publish_library_metadata(
            &store,
            &prefix,
            &LibraryMetadata {
                version: DERIVED_VERSION.into(),
                title_override: Some("Board call".into()),
            },
        )
        .unwrap();

        let back = read_library_metadata(&store, &prefix).unwrap();
        assert_eq!(back.title_override.as_deref(), Some("Board call"));
    }

    /// A recording nobody renamed has no sidecar, which is ordinary rather
    /// than a fault.
    #[test]
    fn an_absent_sidecar_reads_as_nothing() {
        let (_dir, store) = store();
        assert!(read_library_metadata(&store, &prefix()).is_none());
    }

    /// One damaged sidecar must not stop a library reconciling. The recording
    /// is entirely usable without the title someone chose.
    #[test]
    fn a_damaged_sidecar_is_ignored_rather_than_fatal() {
        let (_dir, store) = store();
        let prefix = prefix();
        store
            .put(&prefix.library_metadata(), b"{ not json")
            .unwrap();

        assert!(read_library_metadata(&store, &prefix).is_none());
    }

    /// Inserts a recording with a transcript and a summary indexed.
    fn indexed_recording(db: &Db, id: Ulid, started: time::OffsetDateTime) {
        db.conn()
            .execute(
                "INSERT INTO recordings (id, owner_id, status, started_at, manifest_version,
                                         clock_started_ns)
                 VALUES (?1, 1, 'ready', ?2, 'recording-manifest/v1', 1000000000)",
                rusqlite::params![id.to_string(), started.unix_timestamp()],
            )
            .unwrap();

        let transcript_id = Ulid::new();
        db.conn()
            .execute(
                "INSERT INTO transcripts (id, recording_id, revision, engine_name, engine_model,
                                          language)
                 VALUES (?1, ?2, 1, 'parakeet', 'tdt-0.6b', 'en')",
                rusqlite::params![transcript_id.to_string(), id.to_string()],
            )
            .unwrap();

        for (seq, (offset_ns, hint, text)) in [
            (0u64, "local", "shall we ship on friday"),
            (4_000_000_000u64, "remote", "yes, if the tests pass"),
        ]
        .into_iter()
        .enumerate()
        {
            db.conn()
                .execute(
                    "INSERT INTO transcript_segments
                         (id, transcript_id, track_id, seq, start_boottime_ns, end_boottime_ns,
                          speaker_hint, text)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    rusqlite::params![
                        Ulid::new().to_string(),
                        transcript_id.to_string(),
                        if hint == "local" { "a_local-mic_01" } else { "b_remote-mix_01" },
                        seq as i64,
                        (1_000_000_000u64 + offset_ns) as i64,
                        (1_000_000_000u64 + offset_ns) as i64,
                        hint,
                        text
                    ],
                )
                .unwrap();
        }

        db.conn()
            .execute(
                "INSERT INTO summaries (id, recording_id, revision, provider, model, content_json)
                 VALUES (?1, ?2, 1, 'openrouter', 'some/model', ?3)",
                rusqlite::params![
                    Ulid::new().to_string(),
                    id.to_string(),
                    r#"{"overview":"we agreed to ship","decisions":[],"action_items":[],"topics":[]}"#
                ],
            )
            .unwrap();
    }

    /// The whole point. Losing the index must not lose the transcript, because
    /// the bucket is a copy of the store and the store now holds one.
    #[test]
    fn a_transcript_survives_losing_the_database() {
        let (_dir, store) = store();
        let id = Ulid::new();
        let started = datetime!(2026-07-27 09:00:00 UTC);
        let prefix = RecordingPrefix::new(id, started);

        let original = Db::open_in_memory().unwrap();
        indexed_recording(&original, id, started);
        assert!(store_for(&store, &original, id, &prefix).unwrap());

        // A different machine, or the same one after the index was deleted:
        // the store is all there is.
        let fresh = Db::open_in_memory().unwrap();
        fresh
            .conn()
            .execute(
                "INSERT INTO recordings (id, owner_id, status, started_at, manifest_version,
                                         clock_started_ns)
                 VALUES (?1, 1, 'ready', ?2, 'recording-manifest/v1', 1000000000)",
                rusqlite::params![id.to_string(), started.unix_timestamp()],
            )
            .unwrap();

        assert!(reindex_from_store(&store, &fresh, id, &prefix).unwrap());

        let restored = crate::library::transcript(&fresh, id).unwrap().unwrap();
        assert_eq!(restored.lines.len(), 2);
        assert_eq!(restored.lines[0].speaker, "you");
        assert_eq!(restored.lines[0].text, "shall we ship on friday");
        assert_eq!(restored.lines[1].speaker, "them");
        // Rebased out and back again, so the spacing between speakers holds.
        assert!((restored.lines[1].at_s - 4.0).abs() < 0.001);

        let summary = crate::summarize::load(&fresh, id).unwrap().unwrap();
        assert_eq!(summary.overview, "we agreed to ship");
    }

    /// Re-indexing must not overwrite what the index has maintained since the
    /// stored copy was written.
    #[test]
    fn re_indexing_does_not_replace_what_is_already_indexed() {
        let (_dir, store) = store();
        let id = Ulid::new();
        let started = datetime!(2026-07-27 09:00:00 UTC);
        let prefix = RecordingPrefix::new(id, started);

        let db = Db::open_in_memory().unwrap();
        indexed_recording(&db, id, started);
        store_for(&store, &db, id, &prefix).unwrap();

        // Nothing is missing, so nothing is restored.
        assert!(!reindex_from_store(&store, &db, id, &prefix).unwrap());

        let count: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM transcripts WHERE recording_id = ?1",
                rusqlite::params![id.to_string()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "re-indexing must not duplicate a transcript");
    }

    /// Backup off means there is nowhere to fall behind, and the flag waits.
    #[test]
    fn nothing_is_queued_for_backup_while_backup_is_off() {
        let db = Db::open_in_memory().unwrap();
        let id = Ulid::new();
        indexed_recording(&db, id, datetime!(2026-07-27 09:00:00 UTC));

        let settings = crate::config::Settings::default();
        assert_eq!(requeue_dirty(&db, &settings).unwrap(), 0);
        assert!(is_dirty(&db, id).unwrap(), "the flag must survive for later");
    }

    /// With backup switched off there is no upload to clear the flag, so a
    /// single flag would have this rewriting the same blobs on every launch,
    /// forever, waiting on something that is never coming.
    #[test]
    fn healing_settles_even_when_nothing_will_ever_be_uploaded() {
        let (_dir, store) = store();
        let db = Db::open_in_memory().unwrap();
        let id = Ulid::new();
        indexed_recording(&db, id, datetime!(2026-07-27 09:00:00 UTC));

        let first = heal(&store, &db).unwrap();
        assert_eq!(first.written, 1);

        // A second launch. The store is current, so there is nothing to do.
        let second = heal(&store, &db).unwrap();
        assert_eq!(second.written, 0, "healing must not repeat itself forever");

        // The bucket, however, is still owed a copy.
        assert!(
            is_dirty(&db, id).unwrap(),
            "a written store does not mean an uploaded one"
        );
    }

    /// A rename that is later cleared must not leave the old name in the
    /// bucket. The repair pass has to rewrite the sidecar, not skip it because
    /// there is no longer a title to write.
    #[test]
    fn clearing_a_title_rewrites_the_stored_one() {
        let (_dir, store) = store();
        let db = Db::open_in_memory().unwrap();
        let id = Ulid::new();
        let started = datetime!(2026-07-27 09:00:00 UTC);
        indexed_recording(&db, id, started);
        let prefix = RecordingPrefix::new(id, started);

        db.conn()
            .execute(
                "UPDATE recordings SET title_override = 'Board call' WHERE id = ?1",
                rusqlite::params![id.to_string()],
            )
            .unwrap();
        store_for(&store, &db, id, &prefix).unwrap();
        assert_eq!(
            read_library_metadata(&store, &prefix)
                .unwrap()
                .title_override
                .as_deref(),
            Some("Board call")
        );

        db.conn()
            .execute(
                "UPDATE recordings SET title_override = NULL WHERE id = ?1",
                rusqlite::params![id.to_string()],
            )
            .unwrap();
        store_for(&store, &db, id, &prefix).unwrap();

        assert_eq!(
            read_library_metadata(&store, &prefix).unwrap().title_override,
            None,
            "the bucket must not keep a name that was taken back"
        );
    }

    #[test]
    fn a_summary_is_stored_with_the_transcript_it_came_from() {
        let (_dir, store) = store();
        let prefix = prefix();

        publish_summary(
            &store,
            &prefix,
            1,
            1,
            "openrouter",
            "some/model",
            &SummaryBody {
                overview: "we agreed".into(),
                ..Default::default()
            },
        )
        .unwrap();

        let back = read_summary(&store, &prefix, 1).unwrap();
        assert_eq!(back.transcript_revision, 1);
        assert_eq!(back.model, "some/model");
        assert_eq!(back.body.overview, "we agreed");
    }

    /// The point of the exercise: these land under the recording's own prefix,
    /// which is what the backup uploads.
    #[test]
    fn derived_artefacts_land_under_the_recordings_prefix() {
        let (_dir, store) = store();
        let prefix = prefix();

        publish_transcript(
            &store,
            &prefix,
            &TranscriptDocument {
                version: DERIVED_VERSION.into(),
                revision: 1,
                engine: None,
                language: None,
                lines: vec![],
            },
        )
        .unwrap();
        publish_summary(&store, &prefix, 1, 1, "openrouter", "m", &SummaryBody::default())
            .unwrap();
        publish_library_metadata(&store, &prefix, &LibraryMetadata::default()).unwrap();

        let keys = store.list_prefix(prefix.root().as_str()).unwrap();
        let listed: Vec<&str> = keys.iter().map(|k| k.as_str()).collect();

        assert!(listed.iter().any(|k| k.ends_with("transcripts/v1.json")));
        assert!(listed.iter().any(|k| k.ends_with("summaries/v1.json")));
        assert!(listed.iter().any(|k| k.ends_with("library.json")));
    }
}
