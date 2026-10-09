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
use kaseta_contracts::{BlobKey, ImportSource, MediaType, Origin, RecordingManifest};
use rusqlite::{params, OptionalExtension};
use serde::Serialize;
use ulid::Ulid;

use crate::blobstore::BlobStore;
use crate::db::{Db, IndexFields, LOCAL_OWNER_ID};

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
    /// `captured` for a meeting recorded here, `imported` for a file someone
    /// brought. Decides how unattributed lines are named, among other things.
    pub origin: Origin,
    /// What an imported recording was made from. Absent for captured ones,
    /// and for an import still being decoded, whose file has not been looked
    /// at yet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<ImportedFrom>,
    /// The original file, when the person asked for it to be kept.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original: Option<KeptOriginal>,
}

/// Where an imported recording came from, as the interface shows it.
#[derive(Clone, Debug, Serialize)]
pub struct ImportedFrom {
    /// The file's name on the person's machine.
    pub filename: String,
    /// `video` or `audio`.
    pub media_kind: MediaType,
    /// When the file says it was made, if it says.
    #[serde(with = "time::serde::rfc3339::option")]
    pub media_created_at: Option<time::OffsetDateTime>,
    /// The file's size as uploaded.
    pub bytes: u64,
}

/// A kept original file.
///
/// The manifest goes on naming it after the local copy is removed, because
/// the bucket still holds it; `local` says whether this machine can serve it.
#[derive(Clone, Debug, Serialize)]
pub struct KeptOriginal {
    pub filename: String,
    pub media_kind: MediaType,
    /// What it is served as, decided from its content when it was imported.
    pub content_type: String,
    pub bytes: u64,
    pub local: bool,
    /// Plays the file. Present only while it is on this machine.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Saves the file under its original name. Present only while it is on
    /// this machine.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_url: Option<String>,
}

/// One pipeline stage's state for a recording.
///
/// Reported so a failure is visible and retryable rather than looking identical
/// to a stage that was never attempted.
#[derive(Clone, Debug, Serialize)]
pub struct StageState {
    /// `transcribe`, `summarize`, `upload_remote`, or `publish_webhook`.
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

        // An import whose decode has not succeeded may have left a manifest
        // behind from an attempt that was then overtaken or failed. Only its
        // own finalisation decides whether it is a recording, so the
        // reconciler leaves it alone, derived artefacts included.
        if unfinished_import(db, manifest.recording_id)? {
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

/// Whether `id` is an import that has not been finalised: still decoding,
/// failed, or with its latest decode not (yet) a success.
fn unfinished_import(db: &Db, id: Ulid) -> Result<bool> {
    Ok(db
        .conn()
        .query_row(
            "SELECT r.status <> 'ready'
                    OR COALESCE((SELECT state FROM jobs
                                 WHERE recording_id = r.id AND job_type = 'import_media'
                                 ORDER BY enqueue_seq DESC LIMIT 1), 'succeeded') <> 'succeeded'
             FROM recordings r WHERE r.id = ?1 AND r.origin = 'imported'",
            params![id.to_string()],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(false))
}

/// Inserts or refreshes one recording, preserving anything the user set.
///
/// Which exports exist is resolved here, once, rather than by scanning storage
/// on every list request.
///
/// An import that has not finished, still decoding or failed, is left exactly
/// as it is. Only its own finalisation declares an import ready, because only
/// that knows the decode it belongs to is still the one wanted; a manifest
/// found by the reconciler may be what an overtaken attempt left behind.
pub fn index_recording(
    store: &dyn BlobStore,
    db: &Db,
    manifest: &RecordingManifest,
) -> Result<()> {
    let prefix = prefix_for(manifest.recording_id, manifest.started_at);
    let keys = store.list_prefix(prefix.root().as_str()).unwrap_or_default();
    let fields = index_fields(manifest, &keys);

    db.conn().execute(
        "INSERT INTO recordings
             (id, owner_id, status, title, started_at, ended_at, manifest_version,
              clock_started_ns, duration_ms, has_mixed, tracks_json,
              origin, source_json, original_local)
         VALUES (?1, ?2, 'ready', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
         ON CONFLICT(id) DO UPDATE SET
             status         = excluded.status,
             ended_at       = excluded.ended_at,
             duration_ms    = excluded.duration_ms,
             has_mixed      = excluded.has_mixed,
             tracks_json    = excluded.tracks_json,
             origin         = excluded.origin,
             source_json    = excluded.source_json,
             original_local = excluded.original_local,
             updated_at     = strftime('%s','now')
         WHERE NOT (recordings.origin = 'imported'
                    AND recordings.status IN ('processing','failed'))",
        params![
            manifest.recording_id.to_string(),
            LOCAL_OWNER_ID,
            fields.title,
            fields.started_at,
            fields.ended_at,
            fields.manifest_version,
            fields.clock_started_ns,
            fields.duration_ms,
            fields.has_mixed as i64,
            fields.tracks_json,
            fields.origin.as_str(),
            fields.source_json,
            fields.original_local as i64,
        ],
    )?;
    Ok(())
}

/// What the index holds about a recording, from its manifest and the keys
/// under its prefix.
///
/// Pure, so that indexing and an import's finalisation compute the same thing
/// from the same inputs rather than two versions of it that drift apart.
pub fn index_fields(manifest: &RecordingManifest, keys: &[BlobKey]) -> IndexFields {
    let exported: Vec<&BlobKey> = keys
        .iter()
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

    // Present only while the kept original is still in this store. The
    // manifest goes on naming it after local copies are removed, because the
    // bucket still holds it.
    let original_local = manifest
        .source
        .as_ref()
        .and_then(|s| s.original_key.as_ref())
        .is_some_and(|key| keys.contains(key));

    IndexFields {
        title: manifest.notes.title.clone(),
        started_at: manifest.started_at.unix_timestamp(),
        ended_at: manifest.ended_at.map(|t| t.unix_timestamp()),
        manifest_version: manifest.manifest_version.clone(),
        clock_started_ns: manifest.canonical_clock.started_at_ns as i64,
        duration_ms: duration_ms(manifest),
        has_mixed,
        tracks_json: serde_json::to_string(&tracks).unwrap_or_else(|_| "[]".into()),
        origin: manifest.origin(),
        source_json: manifest
            .source
            .as_ref()
            .and_then(|s| serde_json::to_string(s).ok()),
        original_local,
    }
}

/// Whether `key` is an imported recording's kept original,
/// `{prefix}/source/original.{ext}`.
///
/// Matched on the last two segments rather than on a substring, so a track or
/// export can never be mistaken for one whatever it is named.
pub fn is_original(key: &BlobKey) -> bool {
    let mut segments = key.as_str().rsplit('/');
    let name = segments.next().unwrap_or_default();
    name.starts_with("original.") && segments.next() == Some("source")
}

/// Records that a recording's kept original is no longer held locally.
///
/// Called by whatever removed it, straight after: the index's flag is what
/// the interface reads to decide between playing the original and saying it
/// is in the bucket, and it must not outlive the file. The manifest keeps
/// naming the original, because the bucket still holds it.
pub fn forget_local_original(db: &Db, id: Ulid) -> Result<()> {
    db.conn().execute(
        "UPDATE recordings SET original_local = 0, updated_at = strftime('%s','now')
         WHERE id = ?1",
        params![id.to_string()],
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
        // An import's upload is outside the prefix, and goes with it.
        if store.local_root().is_some() && crate::import::remove_staging(store, ulid).is_err() {
            cleared = false;
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
                duration_ms, has_mixed, tracks_json, uploaded_at,
                origin, source_json, original_local
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
            origin: r.get(10)?,
            source_json: r.get(11)?,
            original_local: r.get::<_, i64>(12)? != 0,
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

/// One recording's stages, for tests that assert on a single pipeline.
#[cfg(test)]
pub fn stages_for(db: &Db, id: Ulid) -> Result<Vec<StageState>> {
    Ok(stages_by_recording(db)?
        .remove(&id.to_string())
        .unwrap_or_default())
}

/// The latest state of each pipeline stage, per recording.
///
/// Only the newest attempt matters: an earlier failure that has since succeeded
/// is history, not something to show or offer to retry.
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
    origin: String,
    source_json: Option<String>,
    original_local: bool,
}

fn into_item(row: Row) -> Result<LibraryItem> {
    let id = Ulid::from_string(&row.id).context("recording id is not a valid ULID")?;
    let started_at = time::OffsetDateTime::from_unix_timestamp(row.started_at)
        .context("recording has an invalid start time")?;
    let origin = Origin::parse(&row.origin)
        .with_context(|| format!("recording {id} has an unknown origin {:?}", row.origin))?;

    // A cache of the manifest's field. One that no longer parses costs the
    // provenance line, not the recording, so it is logged and left out.
    let source: Option<ImportSource> = row.source_json.as_deref().and_then(|json| {
        serde_json::from_str(json)
            .map_err(|e| tracing::warn!(%id, %e, "unreadable import source in the index"))
            .ok()
    });
    let original = source.as_ref().and_then(|s| {
        s.original_key.as_ref()?;
        let url = format!("/api/v1/recordings/{id}/original");
        Some(KeptOriginal {
            filename: s.original_filename.clone(),
            media_kind: s.media_kind,
            content_type: s.content_type.clone(),
            bytes: s.original_bytes,
            local: row.original_local,
            download_url: row.original_local.then(|| format!("{url}?download=1")),
            url: row.original_local.then_some(url),
        })
    });

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
        origin,
        source: source.map(|s| ImportedFrom {
            filename: s.original_filename,
            media_kind: s.media_kind,
            media_created_at: s.media_created_at,
            bytes: s.original_bytes,
        }),
        original,
        id,
    })
}

/// Whether a recording was captured here or imported, from the index.
///
/// `None` for a recording the index does not hold.
pub fn origin(db: &Db, id: Ulid) -> Result<Option<Origin>> {
    let raw: Option<String> = db
        .conn()
        .query_row(
            "SELECT origin FROM recordings WHERE id = ?1",
            params![id.to_string()],
            |r| r.get(0),
        )
        .optional()?;
    raw.map(|o| Origin::parse(&o).with_context(|| format!("recording {id} has an unknown origin {o:?}")))
        .transpose()
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
    } else if track_id.starts_with("a_imported_") {
        // One mixed track from a file, carrying every voice in it.
        ("unattributed", "Speaker")
    } else {
        ("unknown", "Audio")
    }
}

/// One line of a transcript, as the interface shows it.
#[derive(Clone, Debug, Serialize)]
pub struct TranscriptLine {
    /// Seconds from the start of the recording, for seeking playback.
    pub at_s: f64,
    /// Who said it: `you`, `them`, or `unknown`. The stored attribution,
    /// for anything that styles or filters by it.
    pub speaker: String,
    /// Who said it, in the words a person reads: `You`, `Them`, `Unknown`,
    /// or `Speaker` in an imported recording. Decided here so the interface
    /// never keeps a mapping of its own that could disagree with the rest.
    pub label: String,
    pub text: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct Transcript {
    /// What kind of recording this came from, which is what decides how a
    /// line nobody attributed is named, here and in a summary.
    pub origin: Origin,
    pub engine: Option<String>,
    pub language: Option<String>,
    pub lines: Vec<TranscriptLine>,
}

/// A transcript as plain text, one line per turn with its time and speaker.
pub fn plain_text(transcript: &Transcript) -> String {
    transcript
        .lines
        .iter()
        .map(|line| {
            let minutes = (line.at_s / 60.0) as u64;
            let seconds = (line.at_s % 60.0) as u64;
            format!("[{minutes:02}:{seconds:02}] {}: {}", line.label, line.text)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Reads a recording's transcript as a conversation.
///
/// Times are rebased from the canonical clock, which counts from system boot,
/// onto the recording itself — the only frame of reference a listener has.
pub fn transcript(db: &Db, id: Ulid) -> Result<Option<Transcript>> {
    type Header = (String, Option<String>, Option<String>, Option<i64>, String);
    let header: Option<Header> = db
        .conn()
        .query_row(
            "SELECT t.id, t.engine_model, t.language, r.clock_started_ns, r.origin
             FROM transcripts t
             JOIN recordings r ON r.id = t.recording_id
             WHERE t.recording_id = ?1
             ORDER BY t.revision DESC
             LIMIT 1",
            params![id.to_string()],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .optional()?;

    let Some((transcript_id, engine, language, clock_started_ns, origin)) = header else {
        return Ok(None);
    };
    let origin = Origin::parse(&origin)
        .with_context(|| format!("recording {id} has an unknown origin {origin:?}"))?;

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
    let clock_origin = match clock_started_ns {
        Some(ns) if ns > 0 => ns as u64,
        _ => raw.first().map(|(t, _, _)| *t).unwrap_or(0),
    };

    let lines = raw
        .into_iter()
        .map(|(start_ns, hint, text)| {
            let speaker = match hint.as_deref() {
                Some("local") => "you",
                Some("remote") => "them",
                _ => "unknown",
            };
            TranscriptLine {
                at_s: start_ns.saturating_sub(clock_origin) as f64 / 1e9,
                speaker: speaker.into(),
                label: kaseta_contracts::speaker::display(origin, speaker).into(),
                text,
            }
        })
        .collect();

    Ok(Some(Transcript {
        origin,
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
///
/// An import being decoded right now is the exception: it is still writing
/// under the prefix, so anything removed here could be written again a
/// moment later. Its row is tombstoned and every other job of it cancelled;
/// the decode notices at its own finalisation, removes what it wrote and then
/// the row. A crash before that leaves the tombstone for the startup purge.
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

    let decoding: bool = db.conn().query_row(
        "SELECT EXISTS (SELECT 1 FROM jobs
                        WHERE recording_id = ?1 AND job_type = 'import_media'
                          AND state = 'running')",
        params![id.to_string()],
        |r| r.get(0),
    )?;
    if decoding {
        db.conn().execute(
            "UPDATE jobs SET state = 'canceled'
             WHERE recording_id = ?1 AND state IN ('queued','failed_retryable')",
            params![id.to_string()],
        )?;
        return Ok(true);
    }

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
    // An import's upload waits outside the prefix, and goes with it.
    if store.local_root().is_some() {
        if let Err(e) = crate::import::remove_staging(store, id) {
            tracing::warn!(%id, error = %format!("{e:#}"), "could not remove the import's upload");
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

    /// Only `{prefix}/source/original.{ext}` is an original. Removing local
    /// copies deletes what this matches, so a false positive loses audio.
    #[test]
    fn only_the_source_original_is_recognised_as_one() {
        let key = |raw: &str| BlobKey::new(raw).unwrap();
        assert!(is_original(&key("recordings/2026/10/09/x/source/original.mp4")));
        assert!(is_original(&key("recordings/2026/10/09/x/source/original.bin")));
        assert!(!is_original(&key("recordings/2026/10/09/x/source/intent.json")));
        assert!(!is_original(&key("recordings/2026/10/09/x/tracks/original.flac")));
        assert!(!is_original(&key("recordings/2026/10/09/x/exports/original.flac")));
        assert!(!is_original(&key("recordings/2026/10/09/x/source/originals.mp4")));
        assert!(!is_original(&key("original.mp4")));
    }

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

    /// An imported file's one track carries every voice, so it is neither
    /// "You" nor "Everyone else".
    #[test]
    fn an_imported_track_is_a_speaker() {
        assert_eq!(
            describe_track(kaseta_contracts::IMPORTED_TRACK_ID),
            ("unattributed", "Speaker")
        );
    }

    use kaseta_contracts::manifest::{ClockDomain, RecordingNotes, Timeline};
    use kaseta_contracts::{
        CanonicalClock, Chunk, ClockKind, ImportSource, MediaType, Origin, RecordingPrefix, Track,
        TrackFormat, TrackId, TrackRole, TrackSource, MANIFEST_VERSION,
    };

    const STARTED: time::OffsetDateTime = datetime!(2026-10-09 08:00:00 UTC);

    /// A one-track manifest: imported when `source` is given, captured
    /// otherwise.
    fn manifest(id: Ulid, source: Option<ImportSource>) -> RecordingManifest {
        let imported = source.is_some();
        let track_id = TrackId::new(if imported {
            kaseta_contracts::IMPORTED_TRACK_ID
        } else {
            "a_local-mic_01"
        })
        .unwrap();
        let prefix = RecordingPrefix::new(id, STARTED);
        RecordingManifest {
            manifest_version: MANIFEST_VERSION.into(),
            recording_id: id,
            started_at: STARTED,
            ended_at: Some(STARTED + time::Duration::seconds(2)),
            canonical_clock: CanonicalClock {
                kind: ClockKind::BoottimeNs,
                started_at_ns: 7_000_000_000,
            },
            timeline: Timeline {
                master_track_id: track_id.clone(),
                nominal_sample_rate_hz: 48_000,
            },
            tracks: vec![Track {
                track_id: track_id.clone(),
                media_type: MediaType::Audio,
                role: if imported { TrackRole::Unattributed } else { TrackRole::LocalMic },
                source: if imported {
                    TrackSource::ImportedFile { stream_index: 1 }
                } else {
                    TrackSource::Microphone {
                        node_name: "alsa_input.test".into(),
                        display_name: "Test".into(),
                    }
                },
                clock_domain: ClockDomain {
                    source_clock: "test".into(),
                    device_clock_id: None,
                },
                format: TrackFormat {
                    container: "flac".into(),
                    codec: "flac".into(),
                    sample_rate_hz: Some(48_000),
                    channels: Some(1),
                    sample_format: Some("s16".into()),
                },
                chunks: vec![Chunk {
                    seq: 0,
                    blob: prefix.chunk(&track_id, 0, "flac"),
                    sha256: "0".repeat(64),
                    bytes: 1,
                    sample_count: Some(96_000),
                    boottime_start_ns: 7_000_000_000,
                    boottime_end_ns: 9_000_000_000,
                    source_pts_start_ns: None,
                    source_pts_end_ns: None,
                    discontinuity: false,
                    gap_before_ns: 0,
                    drops_before_chunk: 0,
                }],
            }],
            notes: RecordingNotes {
                title: Some("Lecture 3".into()),
                ..RecordingNotes::default()
            },
            source,
        }
    }

    fn import_source(original_key: Option<BlobKey>) -> ImportSource {
        ImportSource {
            original_filename: "lecture.mp4".into(),
            original_key,
            original_bytes: 10,
            original_sha256: "ab".repeat(32),
            container: "mov".into(),
            codec: "aac".into(),
            content_type: "video/mp4".into(),
            media_kind: MediaType::Video,
            media_created_at: None,
            imported_at: STARTED,
            duration_s: 2.0,
        }
    }

    fn keys(prefix: &RecordingPrefix, names: &[&str]) -> Vec<BlobKey> {
        names.iter().map(|n| prefix.root().join(n).unwrap()).collect()
    }

    #[test]
    fn a_captured_recording_is_indexed_as_one() {
        let id = Ulid::new();
        let prefix = RecordingPrefix::new(id, STARTED);
        let fields = index_fields(
            &manifest(id, None),
            &keys(&prefix, &["manifest.json", "exports/a_local-mic_01.flac", "exports/mixed.flac"]),
        );

        assert_eq!(fields.origin, Origin::Captured);
        assert_eq!(fields.source_json, None);
        assert!(!fields.original_local);
        assert!(fields.has_mixed);
        assert_eq!(fields.duration_ms, 2_000);
        assert_eq!(fields.clock_started_ns, 7_000_000_000);
        assert_eq!(fields.started_at, STARTED.unix_timestamp());
        assert_eq!(fields.title.as_deref(), Some("Lecture 3"));
        assert_eq!(
            fields.tracks_json,
            r#"[{"track_id":"a_local-mic_01","role":"local_mic"}]"#
        );
    }

    #[test]
    fn an_imported_recording_carries_its_source_into_the_index() {
        let id = Ulid::new();
        let prefix = RecordingPrefix::new(id, STARTED);
        let original = prefix.original("mp4").unwrap();
        let m = manifest(id, Some(import_source(Some(original.clone()))));

        let with_original = index_fields(
            &m,
            &[
                keys(&prefix, &["exports/a_imported_01.flac", "exports/mixed.flac"]),
                vec![original],
            ]
            .concat(),
        );
        assert_eq!(with_original.origin, Origin::Imported);
        assert!(with_original.original_local);
        let cached: ImportSource =
            serde_json::from_str(with_original.source_json.as_deref().unwrap()).unwrap();
        assert_eq!(cached, import_source(m.source.as_ref().unwrap().original_key.clone()));
        assert_eq!(
            with_original.tracks_json,
            r#"[{"track_id":"a_imported_01","role":"unattributed"}]"#
        );

        // Removed locally after backup: the manifest still names it, but it
        // is not here to play.
        let without = index_fields(&m, &keys(&prefix, &["exports/mixed.flac"]));
        assert!(!without.original_local);
    }

    /// The index is disposable: rebuilt from storage, an import must come
    /// back as an import, with its source.
    #[test]
    fn rebuilding_the_index_restores_an_import() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = crate::blobstore::LocalFsStore::new(dir.path()).unwrap();
        let db = Db::open_in_memory().unwrap();
        let id = Ulid::new();
        let prefix = RecordingPrefix::new(id, STARTED);
        let original = prefix.original("mp4").unwrap();
        store.put(&original, b"original bytes").unwrap();
        let m = manifest(id, Some(import_source(Some(original))));

        index_recording(&store, &db, &m).unwrap();

        let (status, origin, source_json, original_local): (String, String, Option<String>, i64) =
            db.conn()
                .query_row(
                    "SELECT status, origin, source_json, original_local FROM recordings
                     WHERE id = ?1",
                    params![id.to_string()],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )
                .unwrap();
        assert_eq!((status.as_str(), origin.as_str()), ("ready", "imported"));
        assert!(source_json.unwrap().contains("lecture.mp4"));
        assert_eq!(original_local, 1);
    }

    /// Only finalisation may declare an import ready. Indexing a manifest left
    /// behind by an attempt that was overtaken must not promote a row that is
    /// still decoding, or one that failed.
    #[test]
    fn indexing_never_promotes_an_unfinished_import() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = crate::blobstore::LocalFsStore::new(dir.path()).unwrap();
        let db = Db::open_in_memory().unwrap();

        for status in ["processing", "failed"] {
            let id = Ulid::new();
            db.create_import(&crate::db::NewImport {
                recording_id: id,
                started_at: STARTED,
                title: "Lecture 3".into(),
            })
            .unwrap();
            db.conn()
                .execute(
                    "UPDATE recordings SET status = ?2 WHERE id = ?1",
                    params![id.to_string(), status],
                )
                .unwrap();

            index_recording(&store, &db, &manifest(id, Some(import_source(None)))).unwrap();

            let (now, source_json): (String, Option<String>) = db
                .conn()
                .query_row(
                    "SELECT status, source_json FROM recordings WHERE id = ?1",
                    params![id.to_string()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            assert_eq!(now, status);
            assert_eq!(source_json, None, "the row is left exactly as it was");
        }
    }

    /// A manifest left by an import that never finalised is not a recording.
    /// The reconciler must not bring it into the library, nor read anything
    /// derived back from beside it.
    #[test]
    fn reconciling_skips_an_unfinished_import() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = crate::blobstore::LocalFsStore::new(dir.path()).unwrap();
        let db = Db::open_in_memory().unwrap();
        let id = Ulid::new();
        db.create_import(&crate::db::NewImport {
            recording_id: id,
            started_at: STARTED,
            title: "Lecture 3".into(),
        })
        .unwrap();
        let m = manifest(id, Some(import_source(None)));
        store
            .put(&RecordingPrefix::new(id, STARTED).manifest(), &serde_json::to_vec(&m).unwrap())
            .unwrap();

        assert_eq!(reconcile(&store, &db).unwrap(), 0);
        let status: String = db
            .conn()
            .query_row("SELECT status FROM recordings WHERE id = ?1", params![id.to_string()], |r| r.get(0))
            .unwrap();
        assert_eq!(status, "processing");

        // Finalised, it is reconciled like any other recording.
        db.conn().execute("UPDATE recordings SET status = 'ready'", []).unwrap();
        db.conn().execute("UPDATE jobs SET state = 'succeeded'", []).unwrap();
        assert_eq!(reconcile(&store, &db).unwrap(), 1);

        // And a rebuilt index, with no rows at all, restores it.
        let fresh = Db::open_in_memory().unwrap();
        assert_eq!(reconcile(&store, &fresh).unwrap(), 1);
    }

    fn staged(store: &crate::blobstore::LocalFsStore, id: Ulid) -> std::path::PathBuf {
        let dir = crate::import::staging_dir(store, id).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("upload.mp4"), b"upload").unwrap();
        dir
    }

    /// Deleting an import that is waiting, or failed, removes it outright,
    /// upload included.
    #[test]
    fn deleting_an_import_that_is_not_decoding_removes_its_upload() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = crate::blobstore::LocalFsStore::new(dir.path()).unwrap();
        let db = Db::open_in_memory().unwrap();
        let id = Ulid::new();
        db.create_import(&crate::db::NewImport {
            recording_id: id,
            started_at: STARTED,
            title: "Lecture 3".into(),
        })
        .unwrap();
        let staging = staged(&store, id);

        assert!(delete(&store, &db, id).unwrap());

        assert!(!staging.exists());
        let rows: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM recordings", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 0);
        let jobs: i64 = db.conn().query_row("SELECT COUNT(*) FROM jobs", [], |r| r.get(0)).unwrap();
        assert_eq!(jobs, 0, "the queued decode went with the recording");
    }

    /// While the decode runs it is still writing, so the deletion is left to
    /// it: the row stays tombstoned and nothing else of it may start.
    #[test]
    fn deleting_an_import_mid_decode_leaves_the_tombstone_to_the_decode() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = crate::blobstore::LocalFsStore::new(dir.path()).unwrap();
        let db = Db::open_in_memory().unwrap();
        let id = Ulid::new();
        db.create_import(&crate::db::NewImport {
            recording_id: id,
            started_at: STARTED,
            title: "Lecture 3".into(),
        })
        .unwrap();
        db.claim_next_job(1).unwrap().unwrap();
        db.enqueue(id, kaseta_contracts::JobType::Transcribe, 1).unwrap();
        let staging = staged(&store, id);

        assert!(delete(&store, &db, id).unwrap());

        let (deleted, pending): (bool, i64) = db
            .conn()
            .query_row(
                "SELECT deleted_at IS NOT NULL, purge_pending FROM recordings WHERE id = ?1",
                params![id.to_string()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert!(deleted);
        assert_eq!(pending, 1);
        assert!(staging.exists(), "the decode is still reading it");
        let states: Vec<String> = db
            .conn()
            .prepare("SELECT state FROM jobs ORDER BY enqueue_seq")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(states, ["running", "canceled"]);
        assert!(!delete(&store, &db, id).unwrap(), "already deleted");

        // A crash before the decode finished: the startup purge completes it.
        db.conn().execute("UPDATE jobs SET state = 'queued'", []).unwrap();
        assert_eq!(purge_pending(&store, &db).unwrap(), 1);
        assert!(!staging.exists());
    }

    /// Gives `id` a transcript of one line per `(hint, text)`, a second apart
    /// from the recording's clock origin.
    fn transcribe(db: &Db, id: Ulid, lines: &[(&str, &str)]) {
        let transcript_id = Ulid::new().to_string();
        db.conn()
            .execute(
                "INSERT INTO transcripts (id, recording_id, revision, engine_name, engine_model,
                                          language)
                 VALUES (?1, ?2, 1, 'parakeet', 'tdt-0.6b', 'en')",
                params![transcript_id, id.to_string()],
            )
            .unwrap();
        for (i, (hint, text)) in lines.iter().enumerate() {
            let start = 7_000_000_000 + i as i64 * 1_000_000_000;
            db.conn()
                .execute(
                    "INSERT INTO transcript_segments
                         (id, transcript_id, track_id, seq, start_boottime_ns, end_boottime_ns,
                          speaker_hint, text)
                     VALUES (?1, ?2, 'a_track', ?3, ?4, ?5, ?6, ?7)",
                    params![Ulid::new().to_string(), transcript_id, i as i64, start, start + 1, hint, text],
                )
                .unwrap();
        }
    }

    /// The interface shows the label it is given and keeps no mapping of its
    /// own, so the label is where captured and imported part ways.
    #[test]
    fn transcript_lines_carry_the_label_a_person_reads() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = crate::blobstore::LocalFsStore::new(dir.path()).unwrap();
        let db = Db::open_in_memory().unwrap();

        let captured = Ulid::new();
        index_recording(&store, &db, &manifest(captured, None)).unwrap();
        transcribe(&db, captured, &[("local", "hi"), ("remote", "hello"), ("unknown", "...")]);

        let t = transcript(&db, captured).unwrap().unwrap();
        assert_eq!(t.origin, Origin::Captured);
        let pairs: Vec<(&str, &str)> =
            t.lines.iter().map(|l| (l.speaker.as_str(), l.label.as_str())).collect();
        assert_eq!(pairs, [("you", "You"), ("them", "Them"), ("unknown", "Unknown")]);
        assert_eq!(t.lines[1].at_s, 1.0);
        assert_eq!(plain_text(&t), "[00:00] You: hi\n[00:01] Them: hello\n[00:02] Unknown: ...");

        let imported = Ulid::new();
        index_recording(&store, &db, &manifest(imported, Some(import_source(None)))).unwrap();
        transcribe(&db, imported, &[("unknown", "welcome"), ("unknown", "to the lecture")]);

        let t = transcript(&db, imported).unwrap().unwrap();
        assert_eq!(t.origin, Origin::Imported);
        // The stored attribution is untouched; only the word shown differs.
        assert!(t.lines.iter().all(|l| l.speaker == "unknown" && l.label == "Speaker"));
        assert_eq!(
            plain_text(&t),
            "[00:00] Speaker: welcome\n[00:01] Speaker: to the lecture"
        );

        let json = serde_json::to_value(&t).unwrap();
        assert_eq!(json["origin"], "imported");
        assert_eq!(json["lines"][0]["label"], "Speaker");
        assert_eq!(json["lines"][0]["speaker"], "unknown");
    }

    /// A captured recording looks exactly as it did: no provenance, no
    /// original.
    #[test]
    fn a_captured_item_has_no_source() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = crate::blobstore::LocalFsStore::new(dir.path()).unwrap();
        let db = Db::open_in_memory().unwrap();
        let id = Ulid::new();
        index_recording(&store, &db, &manifest(id, None)).unwrap();

        let item = get(&db, id).unwrap().unwrap();
        assert_eq!(item.origin, Origin::Captured);
        let json = serde_json::to_value(&item).unwrap();
        assert_eq!(json["origin"], "captured");
        assert!(json.get("source").is_none());
        assert!(json.get("original").is_none());
        assert_eq!(origin(&db, id).unwrap(), Some(Origin::Captured));
        assert_eq!(origin(&db, Ulid::new()).unwrap(), None);
    }

    /// An import says where it came from, and offers its kept original only
    /// while this machine holds it.
    #[test]
    fn an_imported_item_describes_its_file_and_kept_original() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = crate::blobstore::LocalFsStore::new(dir.path()).unwrap();
        let db = Db::open_in_memory().unwrap();
        let id = Ulid::new();
        let prefix = RecordingPrefix::new(id, STARTED);
        let original = prefix.original("mp4").unwrap();
        store.put(&original, b"original bytes").unwrap();
        store.put(&prefix.export("a_imported_01.flac").unwrap(), b"flac").unwrap();
        let mut source = import_source(Some(original.clone()));
        source.media_created_at = Some(datetime!(2025-03-01 14:30:00 UTC));
        index_recording(&store, &db, &manifest(id, Some(source))).unwrap();

        let item = get(&db, id).unwrap().unwrap();
        assert_eq!(item.origin, Origin::Imported);
        let json = serde_json::to_value(&item).unwrap();
        assert_eq!(json["origin"], "imported");
        assert_eq!(json["source"]["filename"], "lecture.mp4");
        assert_eq!(json["source"]["media_kind"], "video");
        assert_eq!(json["source"]["media_created_at"], "2025-03-01T14:30:00Z");
        assert_eq!(json["source"]["bytes"], 10);
        assert_eq!(json["original"]["content_type"], "video/mp4");
        assert_eq!(json["original"]["local"], true);
        assert_eq!(json["original"]["url"], format!("/api/v1/recordings/{id}/original"));
        assert_eq!(
            json["original"]["download_url"],
            format!("/api/v1/recordings/{id}/original?download=1")
        );
        assert_eq!(item.tracks[0].label, "Speaker");

        // Removed after backup: still named, no longer playable here.
        forget_local_original(&db, id).unwrap();
        let json = serde_json::to_value(get(&db, id).unwrap().unwrap()).unwrap();
        assert_eq!(json["original"]["local"], false);
        assert!(json["original"].get("url").is_none());
        assert!(json["original"].get("download_url").is_none());
    }

    /// Not keeping the original leaves the provenance and nothing to offer.
    #[test]
    fn an_import_without_its_original_offers_none() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = crate::blobstore::LocalFsStore::new(dir.path()).unwrap();
        let db = Db::open_in_memory().unwrap();
        let id = Ulid::new();
        index_recording(&store, &db, &manifest(id, Some(import_source(None)))).unwrap();

        let json = serde_json::to_value(get(&db, id).unwrap().unwrap()).unwrap();
        assert_eq!(json["source"]["filename"], "lecture.mp4");
        assert!(json["source"]["media_created_at"].is_null());
        assert!(json.get("original").is_none());
    }

    /// A captured recording indexed again is refreshed as before.
    #[test]
    fn indexing_a_captured_recording_again_refreshes_it() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = crate::blobstore::LocalFsStore::new(dir.path()).unwrap();
        let db = Db::open_in_memory().unwrap();
        let id = Ulid::new();
        let mut m = manifest(id, None);

        index_recording(&store, &db, &m).unwrap();
        m.tracks[0].chunks[0].sample_count = Some(480_000);
        index_recording(&store, &db, &m).unwrap();

        let (duration, origin): (i64, String) = db
            .conn()
            .query_row(
                "SELECT duration_ms, origin FROM recordings WHERE id = ?1",
                params![id.to_string()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(duration, 10_000);
        assert_eq!(origin, "captured");
    }
}
