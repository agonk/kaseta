//! The recording library: an index over what capture wrote.
//!
//! Blob storage is the source of truth. A manifest is an immutable record of
//! what was captured and is never rewritten; the database is a derived index
//! that makes listing cheap and holds the things a user can change afterwards —
//! a title, a deletion.
//!
//! That split is what makes the index disposable: delete the database and
//! [`reconcile`] rebuilds it from storage. It is also why renaming does not
//! touch a manifest.

use std::collections::HashSet;

use anyhow::{Context, Result};
use kaseta_contracts::{BlobKey, RecordingManifest};
use rusqlite::{params, OptionalExtension};
use serde::Serialize;
use ulid::Ulid;

use crate::blobstore::BlobStore;
use crate::db::{Db, LOCAL_OWNER_ID};

/// One recording, as the interface sees it.
#[derive(Clone, Debug, Serialize)]
pub struct LibraryItem {
    pub id: Ulid,
    /// The title actually shown: a user's rename if set, otherwise whatever was
    /// recorded at capture time, otherwise a generated fallback.
    pub title: String,
    /// Whether the title came from a rename rather than from capture.
    pub renamed: bool,
    pub status: String,
    #[serde(with = "time::serde::rfc3339")]
    pub started_at: time::OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub ended_at: Option<time::OffsetDateTime>,
    pub duration_ms: i64,
    pub tracks: Vec<LibraryTrack>,
    /// Whether a transcript exists, so the interface can offer to show it.
    pub has_transcript: bool,
    pub has_summary: bool,
    /// Whether the audio is still on this machine, and whether a copy is in
    /// remote storage. Both can be true, and after a retention sweep or an
    /// upload-then-delete neither need be.
    pub audio_local: bool,
    pub audio_remote: bool,
    /// What the pipeline is doing with this recording, if anything.
    pub stages: Vec<StageState>,
    /// Present once a mixed export exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mixed_audio_url: Option<String>,
}

/// One pipeline stage's state for a recording.
///
/// Reported so a failure is visible and retryable rather than looking identical
/// to a stage that was never attempted.
#[derive(Clone, Debug, Serialize)]
pub struct StageState {
    /// `transcribe`, `summarize`, or `upload_remote`.
    pub stage: String,
    /// `queued`, `running`, `succeeded`, `skipped`, `failed`, or `retrying`.
    pub state: String,
    /// Why it failed, or why it was skipped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Whether asking again would achieve anything.
    ///
    /// True for a failure worth retrying and for a skip, which is not a failure
    /// at all — the stage found nothing to do, and correcting whatever caused
    /// that is exactly the case where running it again is the point.
    pub retryable: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct LibraryTrack {
    pub track_id: String,
    pub role: String,
    /// What this track holds, in plain terms.
    pub label: String,
    pub audio_url: String,
}

/// Rebuilds the index from storage.
///
/// Run at startup so a database that was deleted, or that missed a recording
/// because the daemon died mid-write, converges on what storage actually holds.
/// Existing rows are updated rather than duplicated, and user-set titles and
/// tombstones are preserved.
pub fn reconcile(store: &dyn BlobStore, db: &Db) -> Result<usize> {
    let manifests: Vec<BlobKey> = store
        .list_prefix("recordings")?
        .into_iter()
        .filter(|k| k.as_str().ends_with("/manifest.json"))
        .collect();

    let mut indexed = 0usize;
    let mut restored = 0usize;
    let mut seen: HashSet<String> = HashSet::new();

    for key in manifests {
        let bytes = match store.get(&key) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::warn!(%key, error = %format!("{e:#}"), "skipping unreadable manifest");
                continue;
            }
        };
        let manifest: RecordingManifest = match serde_json::from_slice(&bytes) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(%key, %e, "skipping unparseable manifest");
                continue;
            }
        };

        // A tombstoned recording whose objects have not finished being purged
        // must stay deleted rather than being rebuilt from the survivors.
        let tombstoned: bool = db
            .conn()
            .query_row(
                "SELECT deleted_at IS NOT NULL FROM recordings WHERE id = ?1",
                params![manifest.recording_id.to_string()],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(false);
        if tombstoned {
            continue;
        }

        seen.insert(manifest.recording_id.to_string());
        index_recording(store, db, &manifest)?;
        indexed += 1;

        // Whatever was derived from this recording is beside it in the store.
        // Reading it back is what makes a restored bucket a working library
        // rather than a pile of audio: only gaps are filled, so nothing the
        // index has maintained since is overwritten by an older copy.
        let prefix = prefix_for(manifest.recording_id, manifest.started_at);
        match crate::derived::reindex_from_store(store, db, manifest.recording_id, &prefix) {
            Ok(true) => restored += 1,
            Ok(false) => {}
            Err(e) => tracing::warn!(
                id = %manifest.recording_id,
                error = %format!("{e:#}"),
                "could not read back what was derived from this recording"
            ),
        }
    }

    if restored > 0 {
        tracing::info!(restored, "rebuilt transcripts, summaries or titles from storage");
    }
    tracing::info!(indexed, "library reconciled with storage");
    Ok(indexed)
}

/// Inserts or refreshes one recording, preserving anything the user set.
///
/// Which exports exist is resolved here, once, rather than by scanning storage
/// on every list request.
pub fn index_recording(
    store: &dyn BlobStore,
    db: &Db,
    manifest: &RecordingManifest,
) -> Result<()> {
    let prefix = prefix_for(manifest.recording_id, manifest.started_at);
    let exported: Vec<BlobKey> = store
        .list_prefix(prefix.root().as_str())
        .unwrap_or_default()
        .into_iter()
        .filter(|k| k.as_str().contains("/exports/"))
        .collect();

    let has_mixed = exported
        .iter()
        .any(|k| k.as_str().ends_with("/mixed.flac"));

    let tracks: Vec<IndexedTrack> = exported
        .iter()
        .filter_map(|k| {
            let name = k.as_str().rsplit_once('/')?.1;
            let stem = name.strip_suffix(".flac")?;
            (stem != "mixed").then(|| IndexedTrack {
                track_id: stem.to_string(),
                role: describe_track(stem).0.to_string(),
            })
        })
        .collect();

    db.conn().execute(
        "INSERT INTO recordings
             (id, owner_id, status, title, started_at, ended_at, manifest_version,
              clock_started_ns, duration_ms, has_mixed, tracks_json)
         VALUES (?1, ?2, 'ready', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
         ON CONFLICT(id) DO UPDATE SET
             status      = excluded.status,
             ended_at    = excluded.ended_at,
             duration_ms = excluded.duration_ms,
             has_mixed   = excluded.has_mixed,
             tracks_json = excluded.tracks_json,
             updated_at  = strftime('%s','now')",
        params![
            manifest.recording_id.to_string(),
            LOCAL_OWNER_ID,
            manifest.notes.title,
            manifest.started_at.unix_timestamp(),
            manifest.ended_at.map(|t| t.unix_timestamp()),
            manifest.manifest_version,
            manifest.canonical_clock.started_at_ns as i64,
            duration_ms(manifest),
            has_mixed as i64,
            serde_json::to_string(&tracks).unwrap_or_else(|_| "[]".into()),
        ],
    )?;
    Ok(())
}

#[derive(Clone, Debug, Serialize, serde::Deserialize)]
struct IndexedTrack {
    track_id: String,
    role: String,
}

/// Longest track duration, in milliseconds.
///
/// Taken from the audio rather than from wall-clock start and end, which would
/// include time before the first sample and after the last.
fn duration_ms(manifest: &RecordingManifest) -> i64 {
    manifest
        .tracks
        .iter()
        .filter_map(|t| {
            let rate = t.format.sample_rate_hz?;
            if rate == 0 {
                return None;
            }
            Some((t.sample_count() as i64 * 1_000) / rate as i64)
        })
        .max()
        .unwrap_or(0)
}

/// Retries purging recordings whose objects were not fully removed.
///
/// Called at startup: a purge interrupted by a crash or a transient I/O error
/// leaves objects behind, and the tombstone that hides them must not be dropped
/// until they are gone.
pub fn purge_pending(store: &dyn BlobStore, db: &Db) -> Result<usize> {
    let pending: Vec<(String, i64)> = {
        let mut stmt = db.conn().prepare(
            "SELECT id, started_at FROM recordings WHERE purge_pending = 1",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
        rows.collect::<Result<_, _>>()?
    };

    let mut completed = 0usize;
    for (id, started_at) in pending {
        let Ok(ulid) = Ulid::from_string(&id) else { continue };
        let Ok(started_at) = time::OffsetDateTime::from_unix_timestamp(started_at) else {
            continue;
        };
        let prefix = prefix_for(ulid, started_at);

        let mut cleared = true;
        for key in store.list_prefix(prefix.root().as_str())? {
            if store.delete(&key).is_err() {
                cleared = false;
            }
        }
        if cleared {
            db.conn()
                .execute("DELETE FROM recordings WHERE id = ?1", params![id])?;
            completed += 1;
        }
    }
    Ok(completed)
}

/// Every recording that has not been deleted, newest first.
///
/// Reads only the index. Listing must not touch storage: it runs on every page
/// load, and probing the filesystem once per recording put its cost on the
/// request path and grew it with the library.
pub fn list(db: &Db) -> Result<Vec<LibraryItem>> {
    let mut stmt = db.conn().prepare(
        "SELECT id, title, title_override, status, started_at, ended_at,
                duration_ms, has_mixed, tracks_json, uploaded_at
         FROM recordings
         WHERE owner_id = ?1 AND deleted_at IS NULL
         ORDER BY started_at DESC",
    )?;

    let rows = stmt.query_map(params![LOCAL_OWNER_ID], |r| {
        Ok(Row {
            id: r.get(0)?,
            title: r.get(1)?,
            title_override: r.get(2)?,
            status: r.get(3)?,
            started_at: r.get(4)?,
            ended_at: r.get(5)?,
            duration_ms: r.get::<_, Option<i64>>(6)?.unwrap_or(0),
            has_mixed: r.get::<_, i64>(7)? != 0,
            tracks_json: r.get(8)?,
            uploaded_at: r.get(9)?,
        })
    })?;

    let mut items: Vec<LibraryItem> = rows
        .map(|row| row.map_err(Into::into).and_then(into_item))
        .collect::<Result<_>>()?;

    // One query each for the whole page rather than one per row.
    let with_transcripts = ids_in(db, "SELECT DISTINCT recording_id FROM transcripts")?;
    let with_summaries = ids_in(db, "SELECT DISTINCT recording_id FROM summaries")?;
    let stages = stages_by_recording(db)?;

    for item in &mut items {
        let id = item.id.to_string();
        item.has_transcript = with_transcripts.contains(&id);
        item.has_summary = with_summaries.contains(&id);
        item.stages = stages.get(&id).cloned().unwrap_or_default();
    }

    Ok(items)
}

fn ids_in(db: &Db, sql: &str) -> Result<HashSet<String>> {
    let mut stmt = db.conn().prepare(sql)?;
    let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
    Ok(rows.filter_map(|r| r.ok()).collect())
}

/// The latest state of each pipeline stage, per recording.
///
/// Only the newest attempt matters: an earlier failure that has since succeeded
/// is history, not something to show or offer to retry.
/// One recording's stages, for callers that do not want the whole library.
pub fn stages_for(db: &Db, id: Ulid) -> Result<Vec<StageState>> {
    Ok(stages_by_recording(db)?
        .remove(&id.to_string())
        .unwrap_or_default())
}

fn stages_by_recording(db: &Db) -> Result<std::collections::HashMap<String, Vec<StageState>>> {
    let mut stmt = db.conn().prepare(
        "SELECT recording_id, job_type, state, error_message, attempt, max_attempts,
                error_code
         FROM jobs j
         WHERE enqueue_seq = (
             SELECT MAX(enqueue_seq) FROM jobs
             WHERE recording_id = j.recording_id AND job_type = j.job_type
         )",
    )?;

    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, Option<String>>(3)?,
            r.get::<_, i64>(4)?,
            r.get::<_, i64>(5)?,
            r.get::<_, Option<String>>(6)?,
        ))
    })?;

    let mut out: std::collections::HashMap<String, Vec<StageState>> = Default::default();
    for row in rows {
        let (recording_id, stage, state, error, attempt, max_attempts, code) = row?;

        // `failed_retryable` with attempts left is waiting, not finished — the
        // difference decides whether the interface offers a retry or says it is
        // already coming back around.
        let (state, retryable) = match state.as_str() {
            "queued" => ("queued", false),
            "running" => ("running", false),
            // A stage that ran and found nothing to do is not the same as one
            // that did the work. Shown apart so "summaries are off" does not
            // read as "summarised", and offered as runnable so switching the
            // setting on is enough to act on it.
            "succeeded" if code.as_deref() == Some("skipped") => ("skipped", true),
            "succeeded" => ("succeeded", false),
            // Only genuinely coming back around. Once the budget is spent it is
            // a failure, however it is stored — otherwise a dead job shows as
            // "retrying" indefinitely with no way to act on it.
            "failed_retryable" if attempt < max_attempts => ("retrying", false),
            "failed_retryable" | "failed_terminal" => ("failed", true),
            "canceled" => ("failed", true),
            _ => ("succeeded", false),
        };

        out.entry(recording_id).or_default().push(StageState {
            stage,
            state: state.to_string(),
            error: error.filter(|_| state == "failed" || state == "skipped"),
            retryable,
        });
    }
    Ok(out)
}

struct Row {
    id: String,
    title: Option<String>,
    title_override: Option<String>,
    status: String,
    started_at: i64,
    ended_at: Option<i64>,
    duration_ms: i64,
    has_mixed: bool,
    tracks_json: Option<String>,
    uploaded_at: Option<i64>,
}

fn into_item(row: Row) -> Result<LibraryItem> {
    let id = Ulid::from_string(&row.id).context("recording id is not a valid ULID")?;
    let started_at = time::OffsetDateTime::from_unix_timestamp(row.started_at)
        .context("recording has an invalid start time")?;

    let indexed: Vec<IndexedTrack> = row
        .tracks_json
        .as_deref()
        .and_then(|j| serde_json::from_str(j).ok())
        .unwrap_or_default();

    Ok(LibraryItem {
        title: effective_title(&row.title_override, &row.title, started_at),
        renamed: row.title_override.is_some(),
        status: row.status,
        started_at,
        ended_at: row
            .ended_at
            .and_then(|t| time::OffsetDateTime::from_unix_timestamp(t).ok()),
        duration_ms: row.duration_ms,
        has_transcript: false,
        has_summary: false,
        audio_local: row.has_mixed || !indexed.is_empty(),
        audio_remote: row.uploaded_at.is_some(),
        stages: Vec::new(),
        tracks: indexed
            .into_iter()
            .map(|t| LibraryTrack {
                label: describe_track(&t.track_id).1.to_string(),
                audio_url: format!("/api/v1/recordings/{id}/audio/{}.flac", t.track_id),
                track_id: t.track_id,
                role: t.role,
            })
            .collect(),
        mixed_audio_url: row
            .has_mixed
            .then(|| format!("/api/v1/recordings/{id}/audio/mixed.flac")),
        id,
    })
}

pub fn get(db: &Db, id: Ulid) -> Result<Option<LibraryItem>> {
    Ok(list(db)?.into_iter().find(|i| i.id == id))
}

/// A recording always has a name, even if nobody gave it one.
fn effective_title(
    override_title: &Option<String>,
    captured_title: &Option<String>,
    started_at: time::OffsetDateTime,
) -> String {
    if let Some(t) = override_title.as_ref().filter(|t| !t.trim().is_empty()) {
        return t.clone();
    }
    if let Some(t) = captured_title.as_ref().filter(|t| !t.trim().is_empty()) {
        return t.clone();
    }
    format!(
        "Recording {:04}-{:02}-{:02} {:02}:{:02}",
        started_at.year(),
        u8::from(started_at.month()),
        started_at.day(),
        started_at.hour(),
        started_at.minute()
    )
}

fn prefix_for(id: Ulid, started_at: time::OffsetDateTime) -> kaseta_contracts::RecordingPrefix {
    kaseta_contracts::RecordingPrefix::new(id, started_at)
}



/// Maps a track id to something a person can read.
fn describe_track(track_id: &str) -> (&'static str, &'static str) {
    if track_id.contains("local-mic") {
        ("local_mic", "You")
    } else if track_id.contains("remote-mix") {
        ("remote_mix", "Everyone else")
    } else {
        ("unknown", "Audio")
    }
}

/// One line of a transcript, as the interface shows it.
#[derive(Clone, Debug, Serialize)]
pub struct TranscriptLine {
    /// Seconds from the start of the recording, for seeking playback.
    pub at_s: f64,
    /// Who said it: `you`, `them`, or `unknown`.
    pub speaker: String,
    pub text: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct Transcript {
    pub engine: Option<String>,
    pub language: Option<String>,
    pub lines: Vec<TranscriptLine>,
}

/// Reads a recording's transcript as a conversation.
///
/// Times are rebased from the canonical clock, which counts from system boot,
/// onto the recording itself — the only frame of reference a listener has.
pub fn transcript(db: &Db, id: Ulid) -> Result<Option<Transcript>> {
    let header: Option<(String, Option<String>, Option<String>, Option<i64>)> = db
        .conn()
        .query_row(
            "SELECT t.id, t.engine_model, t.language, r.clock_started_ns
             FROM transcripts t
             JOIN recordings r ON r.id = t.recording_id
             WHERE t.recording_id = ?1
             ORDER BY t.revision DESC
             LIMIT 1",
            params![id.to_string()],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;

    let Some((transcript_id, engine, language, clock_started_ns)) = header else {
        return Ok(None);
    };


    let mut stmt = db.conn().prepare(
        "SELECT start_boottime_ns, speaker_hint, text
         FROM transcript_segments
         WHERE transcript_id = ?1
         ORDER BY start_boottime_ns",
    )?;
    let rows = stmt.query_map(params![transcript_id], |r| {
        Ok((
            r.get::<_, i64>(0)? as u64,
            r.get::<_, Option<String>>(1)?,
            r.get::<_, String>(2)?,
        ))
    })?;

    let raw: Vec<(u64, Option<String>, String)> = rows.collect::<Result<_, _>>()?;

    // Capture's clock origin is the right reference. Falling back to the first
    // segment keeps a recording readable if that origin was never indexed,
    // at the cost of the transcript appearing to start at zero.
    let origin = match clock_started_ns {
        Some(ns) if ns > 0 => ns as u64,
        _ => raw.first().map(|(t, _, _)| *t).unwrap_or(0),
    };

    let lines = raw
        .into_iter()
        .map(|(start_ns, hint, text)| TranscriptLine {
            at_s: start_ns.saturating_sub(origin) as f64 / 1e9,
            speaker: match hint.as_deref() {
                Some("local") => "you".into(),
                Some("remote") => "them".into(),
                _ => "unknown".into(),
            },
            text,
        })
        .collect();

    Ok(Some(Transcript {
        engine,
        language,
        lines,
    }))
}

/// Finds recordings whose transcript contains `query`.
///
/// Uses the full-text index rather than scanning, so search stays instant as
/// the library grows.
pub fn search(db: &Db, query: &str) -> Result<Vec<Ulid>> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let mut stmt = db.conn().prepare(
        "SELECT DISTINCT t.recording_id
         FROM transcript_search s
         JOIN transcript_segments seg ON seg.rowid = s.rowid
         JOIN transcripts t ON t.id = seg.transcript_id
         JOIN recordings r ON r.id = t.recording_id
         WHERE transcript_search MATCH ?1 AND r.deleted_at IS NULL",
    )?;

    // Quoted as a phrase so punctuation in the query cannot be read as FTS
    // operator syntax and turn a search into a syntax error.
    let phrase = format!("\"{}\"", trimmed.replace('"', ""));
    let rows = stmt.query_map(params![phrase], |r| r.get::<_, String>(0))?;

    Ok(rows
        .filter_map(|r| r.ok())
        .filter_map(|id| Ulid::from_string(&id).ok())
        .collect())
}

/// Renames a recording without touching its capture record.
///
/// The name is stored beside the recording as well as indexed, so a bucket
/// holds objects someone can recognise rather than a wall of timestamps. The
/// object keys themselves never change: they are the recording's identity, and
/// renaming them would break every reference to it, in the index and in
/// whatever else has already copied them.
pub fn set_title(
    store: &dyn BlobStore,
    db: &Db,
    id: Ulid,
    title: Option<&str>,
) -> Result<bool> {
    // An empty title clears the override rather than storing blankness, so the
    // recording falls back to its captured or generated name.
    let cleaned = title.map(str::trim).filter(|t| !t.is_empty());
    let changed = db.conn().execute(
        "UPDATE recordings SET title_override = ?1, updated_at = strftime('%s','now')
         WHERE id = ?2 AND deleted_at IS NULL",
        params![cleaned, id.to_string()],
    )?;
    if changed == 0 {
        return Ok(false);
    }

    if let Some(started_at) = started_at(db, id)? {
        let prefix = kaseta_contracts::RecordingPrefix::new(id, started_at);
        let metadata = kaseta_contracts::LibraryMetadata {
            version: kaseta_contracts::DERIVED_VERSION.to_string(),
            title_override: cleaned.map(str::to_string),
        };
        if let Err(e) = crate::derived::publish_library_metadata(store, &prefix, &metadata) {
            tracing::warn!(%id, error = %format!("{e:#}"), "could not store the new title");
        }
        // Whether or not the write landed, the stored copy is now behind the
        // index, and a backup taken before the rename still carries the old
        // name. Marking it is what gets both put right.
        crate::derived::mark_dirty(db, id)?;
    }
    Ok(true)
}

/// When a recording started, which its object keys are derived from.
fn started_at(db: &Db, id: Ulid) -> Result<Option<time::OffsetDateTime>> {
    let seconds: Option<i64> = db
        .conn()
        .query_row(
            "SELECT started_at FROM recordings WHERE id = ?1",
            params![id.to_string()],
            |r| r.get(0),
        )
        .optional()?;
    Ok(seconds.and_then(|s| time::OffsetDateTime::from_unix_timestamp(s).ok()))
}

/// Marks a recording deleted and removes its stored objects.
///
/// The tombstone is written first. Purging blobs is not atomic, so a crash
/// partway through would otherwise let the startup reconciler resurrect a
/// half-deleted recording from whatever survived.
pub fn delete(store: &dyn BlobStore, db: &Db, id: Ulid) -> Result<bool> {
    let started_at: Option<i64> = db
        .conn()
        .query_row(
            "SELECT started_at FROM recordings WHERE id = ?1 AND deleted_at IS NULL",
            params![id.to_string()],
            |r| r.get(0),
        )
        .optional()?;

    let Some(started_at) = started_at else {
        return Ok(false);
    };

    // The tombstone and the purge marker go down together, before any object is
    // touched.
    db.conn().execute(
        "UPDATE recordings SET deleted_at = strftime('%s','now'), purge_pending = 1
         WHERE id = ?1",
        params![id.to_string()],
    )?;

    let started_at = time::OffsetDateTime::from_unix_timestamp(started_at)
        .context("recording has an invalid start time")?;
    let prefix = prefix_for(id, started_at);

    let mut purged_everything = true;
    for key in store.list_prefix(prefix.root().as_str())? {
        if let Err(e) = store.delete(&key) {
            tracing::warn!(%key, error = %format!("{e:#}"), "could not remove object");
            purged_everything = false;
        }
    }

    if purged_everything {
        // Nothing survives for the reconciler to find, so the row can go.
        db.conn()
            .execute("DELETE FROM recordings WHERE id = ?1", params![id.to_string()])?;
    } else {
        // An object survived. Dropping the row now would let the next reconcile
        // rebuild the recording from whatever is left and resurrect something
        // the user deleted. The tombstone stays until a retry finishes the job.
        tracing::warn!(%id, "purge incomplete; the recording stays tombstoned");
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn a_rename_wins_over_the_captured_title() {
        let at = datetime!(2026-07-26 19:30:00 UTC);
        assert_eq!(
            effective_title(&Some("Weekly sync".into()), &Some("Captured".into()), at),
            "Weekly sync"
        );
    }

    #[test]
    fn the_captured_title_is_used_when_there_is_no_rename() {
        let at = datetime!(2026-07-26 19:30:00 UTC);
        assert_eq!(effective_title(&None, &Some("Captured".into()), at), "Captured");
    }

    #[test]
    fn an_unnamed_recording_still_has_a_readable_name() {
        let at = datetime!(2026-07-26 19:30:00 UTC);
        assert_eq!(effective_title(&None, &None, at), "Recording 2026-07-26 19:30");
    }

    #[test]
    fn a_blank_rename_falls_back_rather_than_showing_nothing() {
        let at = datetime!(2026-07-26 19:30:00 UTC);
        assert_eq!(
            effective_title(&Some("   ".into()), &Some("Captured".into()), at),
            "Captured"
        );
        assert_eq!(
            effective_title(&Some("".into()), &None, at),
            "Recording 2026-07-26 19:30"
        );
    }

    #[test]
    fn tracks_are_described_in_plain_terms() {
        assert_eq!(describe_track("a_local-mic_01"), ("local_mic", "You"));
        assert_eq!(describe_track("a_remote-mix_01"), ("remote_mix", "Everyone else"));
        assert_eq!(describe_track("something_else"), ("unknown", "Audio"));
    }
}
