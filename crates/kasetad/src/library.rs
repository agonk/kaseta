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
    /// Present once a mixed export exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mixed_audio_url: Option<String>,
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

        seen.insert(manifest.recording_id.to_string());
        upsert(db, &manifest)?;
        indexed += 1;
    }

    tracing::info!(indexed, "library reconciled with storage");
    Ok(indexed)
}

/// Inserts or refreshes one recording, preserving anything the user set.
fn upsert(db: &Db, manifest: &RecordingManifest) -> Result<()> {
    let duration_ms = duration_ms(manifest);

    db.conn().execute(
        "INSERT INTO recordings
             (id, owner_id, status, title, started_at, ended_at, manifest_version,
              clock_started_ns, duration_ms)
         VALUES (?1, ?2, 'ready', ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(id) DO UPDATE SET
             status           = excluded.status,
             ended_at         = excluded.ended_at,
             duration_ms      = excluded.duration_ms,
             updated_at       = strftime('%s','now')",
        params![
            manifest.recording_id.to_string(),
            LOCAL_OWNER_ID,
            manifest.notes.title,
            manifest.started_at.unix_timestamp(),
            manifest.ended_at.map(|t| t.unix_timestamp()),
            manifest.manifest_version,
            manifest.canonical_clock.started_at_ns as i64,
            duration_ms,
        ],
    )?;
    Ok(())
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

/// Every recording that has not been deleted, newest first.
pub fn list(store: &dyn BlobStore, db: &Db) -> Result<Vec<LibraryItem>> {
    let rows: Vec<(String, Option<String>, Option<String>, String, i64, Option<i64>, i64)> = {
        let mut stmt = db.conn().prepare(
            "SELECT id, title, title_override, status, started_at, ended_at, duration_ms
             FROM recordings
             WHERE owner_id = ?1 AND deleted_at IS NULL
             ORDER BY started_at DESC",
        )?;
        let rows = stmt.query_map(params![LOCAL_OWNER_ID], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get::<_, Option<i64>>(6)?.unwrap_or(0),
            ))
        })?;
        rows.collect::<Result<_, _>>()?
    };

    let mut items = Vec::with_capacity(rows.len());
    for (id, title, title_override, status, started_at, ended_at, duration_ms) in rows {
        let id = Ulid::from_string(&id).context("recording id is not a valid ULID")?;
        let started_at = time::OffsetDateTime::from_unix_timestamp(started_at)
            .context("recording has an invalid start time")?;

        items.push(LibraryItem {
            id,
            title: effective_title(&title_override, &title, started_at),
            renamed: title_override.is_some(),
            status,
            started_at,
            ended_at: ended_at.and_then(|t| time::OffsetDateTime::from_unix_timestamp(t).ok()),
            duration_ms,
            tracks: tracks_for(store, id, started_at),
            mixed_audio_url: mixed_url(store, id, started_at),
        });
    }
    Ok(items)
}

pub fn get(store: &dyn BlobStore, db: &Db, id: Ulid) -> Result<Option<LibraryItem>> {
    Ok(list(store, db)?.into_iter().find(|i| i.id == id))
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

/// Exported per-track files, discovered from storage rather than assumed.
fn tracks_for(store: &dyn BlobStore, id: Ulid, started_at: time::OffsetDateTime) -> Vec<LibraryTrack> {
    let prefix = prefix_for(id, started_at);
    let Ok(keys) = store.list_prefix(prefix.root().as_str()) else {
        return Vec::new();
    };

    let mut tracks: Vec<LibraryTrack> = keys
        .iter()
        .filter_map(|k| {
            let name = k.as_str().rsplit_once('/')?.1;
            let stem = name.strip_suffix(".flac")?;
            if !k.as_str().contains("/exports/") || stem == "mixed" {
                return None;
            }
            let (role, label) = describe_track(stem);
            Some(LibraryTrack {
                track_id: stem.to_string(),
                role: role.to_string(),
                label: label.to_string(),
                audio_url: format!("/api/v1/recordings/{id}/audio/{stem}.flac"),
            })
        })
        .collect();

    tracks.sort_by(|a, b| a.track_id.cmp(&b.track_id));
    tracks
}

fn mixed_url(store: &dyn BlobStore, id: Ulid, started_at: time::OffsetDateTime) -> Option<String> {
    let prefix = prefix_for(id, started_at);
    let key = prefix.export("mixed.flac").ok()?;
    store
        .exists(&key)
        .ok()?
        .then(|| format!("/api/v1/recordings/{id}/audio/mixed.flac"))
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

/// Renames a recording without touching its capture record.
pub fn set_title(db: &Db, id: Ulid, title: Option<&str>) -> Result<bool> {
    // An empty title clears the override rather than storing blankness, so the
    // recording falls back to its captured or generated name.
    let cleaned = title.map(str::trim).filter(|t| !t.is_empty());
    let changed = db.conn().execute(
        "UPDATE recordings SET title_override = ?1, updated_at = strftime('%s','now')
         WHERE id = ?2 AND deleted_at IS NULL",
        params![cleaned, id.to_string()],
    )?;
    Ok(changed > 0)
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

    db.conn().execute(
        "UPDATE recordings SET deleted_at = strftime('%s','now') WHERE id = ?1",
        params![id.to_string()],
    )?;

    let started_at = time::OffsetDateTime::from_unix_timestamp(started_at)
        .context("recording has an invalid start time")?;
    let prefix = prefix_for(id, started_at);

    for key in store.list_prefix(prefix.root().as_str())? {
        if let Err(e) = store.delete(&key) {
            // The tombstone stands regardless; leftover objects are retried by
            // the next purge rather than blocking the deletion.
            tracing::warn!(%key, error = %format!("{e:#}"), "could not remove object");
        }
    }

    db.conn()
        .execute("DELETE FROM recordings WHERE id = ?1", params![id.to_string()])?;
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
