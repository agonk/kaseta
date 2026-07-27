//! Removing old recordings on a schedule.
//!
//! Off unless asked for. Deleting someone's meetings without being told to is
//! not a sensible default, however much disk it saves.
//!
//! Two policies, because they answer different worries. Deleting outright
//! reclaims everything. Keeping only the derived artefacts — transcript,
//! summary — discards the audio, which is almost all of the size, while leaving
//! what someone actually wants months later.

use anyhow::{Context, Result};
use ulid::Ulid;

use crate::blobstore::BlobStore;
use crate::config::RetentionSettings;
use crate::db::{Db, LOCAL_OWNER_ID};

/// What a sweep did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Swept {
    pub deleted: usize,
    pub audio_removed: usize,
}

/// Applies the retention policy once.
pub fn sweep(
    store: &dyn BlobStore,
    db: &Db,
    settings: &RetentionSettings,
    now: time::OffsetDateTime,
) -> Result<Swept> {
    let Some(keep_days) = settings.keep_days.filter(|d| *d > 0) else {
        return Ok(Swept::default());
    };
    if !settings.enabled {
        return Ok(Swept::default());
    }

    let cutoff = now - time::Duration::days(keep_days as i64);
    let expired = expired_before(db, cutoff)?;

    let mut swept = Swept::default();
    for (id, started_at) in expired {
        if settings.audio_only {
            // The transcript and summary live in the database and are left
            // alone; only the audio, which is nearly all of the size, goes.
            match remove_audio(store, db, id, started_at) {
                Ok(true) => swept.audio_removed += 1,
                Ok(false) => {}
                Err(e) => tracing::warn!(%id, error = %format!("{e:#}"), "could not remove audio"),
            }
        } else {
            match crate::library::delete(store, db, id) {
                Ok(true) => swept.deleted += 1,
                Ok(false) => {}
                Err(e) => tracing::warn!(%id, error = %format!("{e:#}"), "could not delete"),
            }
        }
    }

    if swept != Swept::default() {
        tracing::info!(
            deleted = swept.deleted,
            audio_removed = swept.audio_removed,
            "retention applied"
        );
    }
    Ok(swept)
}

/// Recordings that started before `cutoff` and still exist.
fn expired_before(db: &Db, cutoff: time::OffsetDateTime) -> Result<Vec<(Ulid, time::OffsetDateTime)>> {
    let mut stmt = db.conn().prepare(
        "SELECT id, started_at FROM recordings
         WHERE owner_id = ?1 AND deleted_at IS NULL AND started_at < ?2
         ORDER BY started_at",
    )?;
    let rows = stmt.query_map(
        rusqlite::params![LOCAL_OWNER_ID, cutoff.unix_timestamp()],
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
    )?;

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

/// Removes a recording's audio, keeping the recording itself.
///
/// Chunks and exports go; the manifest and headers stay, so the recording
/// remains identifiable and its transcript keeps its timeline.
fn remove_audio(
    store: &dyn BlobStore,
    db: &Db,
    id: Ulid,
    started_at: time::OffsetDateTime,
) -> Result<bool> {
    let prefix = kaseta_contracts::RecordingPrefix::new(id, started_at);
    let keys = store.list_prefix(prefix.root().as_str())?;

    let mut removed = 0usize;
    for key in keys {
        let path = key.as_str();
        // Metadata is tiny and is what makes the remainder legible; only the
        // audio is worth reclaiming.
        let is_audio = path.ends_with(".flac") || path.ends_with(".wav");
        if !is_audio {
            continue;
        }
        store
            .delete(&key)
            .with_context(|| format!("removing {key}"))?;
        removed += 1;
    }

    if removed > 0 {
        // Reflected in the index so the interface stops offering a player for
        // audio that is no longer there.
        db.conn().execute(
            "UPDATE recordings SET has_mixed = 0, tracks_json = '[]',
                    updated_at = strftime('%s','now')
             WHERE id = ?1",
            rusqlite::params![id.to_string()],
        )?;
    }
    Ok(removed > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blobstore::LocalFsStore;
    use kaseta_contracts::BlobKey;
    use tempfile::TempDir;
    use time::macros::datetime;

    const NOW: time::OffsetDateTime = datetime!(2026-07-27 12:00:00 UTC);

    fn setup() -> (TempDir, LocalFsStore, Db) {
        let dir = TempDir::new().unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();
        let db = Db::open_in_memory().unwrap();
        (dir, store, db)
    }

    /// Inserts a recording with audio and metadata on disk.
    fn recording(store: &LocalFsStore, db: &Db, days_ago: i64) -> Ulid {
        let id = Ulid::new();
        let started = NOW - time::Duration::days(days_ago);

        db.conn()
            .execute(
                "INSERT INTO recordings (id, owner_id, status, started_at, manifest_version,
                                         has_mixed, tracks_json)
                 VALUES (?1, 1, 'ready', ?2, 'recording-manifest/v1', 1, '[{}]')",
                rusqlite::params![id.to_string(), started.unix_timestamp()],
            )
            .unwrap();

        let prefix = kaseta_contracts::RecordingPrefix::new(id, started);
        store.put(&prefix.manifest(), b"{}").unwrap();
        store
            .put(&prefix.export("mixed.flac").unwrap(), b"audio")
            .unwrap();
        store
            .put(
                &BlobKey::new(format!("{}/tracks/a_local-mic_01/000000.flac", prefix.root()))
                    .unwrap(),
                b"chunk",
            )
            .unwrap();
        id
    }

    fn policy(enabled: bool, days: u32, audio_only: bool) -> RetentionSettings {
        RetentionSettings {
            enabled,
            keep_days: Some(days),
            audio_only,
        }
    }

    #[test]
    fn nothing_is_removed_unless_retention_is_switched_on() {
        // Deleting someone's meetings without being asked is never right.
        let (_dir, store, db) = setup();
        recording(&store, &db, 400);

        let swept = sweep(&store, &db, &policy(false, 30, false), NOW).unwrap();
        assert_eq!(swept, Swept::default());
    }

    #[test]
    fn nothing_is_removed_without_a_period() {
        let (_dir, store, db) = setup();
        recording(&store, &db, 400);

        let none = RetentionSettings { enabled: true, keep_days: None, audio_only: false };
        assert_eq!(sweep(&store, &db, &none, NOW).unwrap(), Swept::default());

        // Zero would mean "delete everything immediately", which nobody means.
        let zero = RetentionSettings { enabled: true, keep_days: Some(0), audio_only: false };
        assert_eq!(sweep(&store, &db, &zero, NOW).unwrap(), Swept::default());
    }

    #[test]
    fn only_recordings_past_the_period_are_removed() {
        let (_dir, store, db) = setup();
        let old = recording(&store, &db, 90);
        let recent = recording(&store, &db, 5);

        let swept = sweep(&store, &db, &policy(true, 30, false), NOW).unwrap();
        assert_eq!(swept.deleted, 1);

        let left = crate::library::list(&db).unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].id, recent);
        assert_ne!(left[0].id, old);
    }

    #[test]
    fn keeping_only_transcripts_removes_the_audio_and_keeps_the_recording() {
        let (_dir, store, db) = setup();
        let id = recording(&store, &db, 90);

        let swept = sweep(&store, &db, &policy(true, 30, true), NOW).unwrap();
        assert_eq!(swept.audio_removed, 1);
        assert_eq!(swept.deleted, 0);

        // The recording is still listed — a transcript months later is usually
        // the point — but has no audio to offer.
        let left = crate::library::list(&db).unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].id, id);
        assert!(left[0].mixed_audio_url.is_none());
        assert!(left[0].tracks.is_empty());
    }

    #[test]
    fn keeping_only_transcripts_leaves_the_metadata_readable() {
        let (_dir, store, db) = setup();
        let id = recording(&store, &db, 90);
        let started = NOW - time::Duration::days(90);
        let prefix = kaseta_contracts::RecordingPrefix::new(id, started);

        sweep(&store, &db, &policy(true, 30, true), NOW).unwrap();

        assert!(store.exists(&prefix.manifest()).unwrap(), "the manifest must survive");
        assert!(!store.exists(&prefix.export("mixed.flac").unwrap()).unwrap());
    }

    #[test]
    fn a_sweep_with_nothing_expired_does_nothing() {
        let (_dir, store, db) = setup();
        recording(&store, &db, 1);
        assert_eq!(sweep(&store, &db, &policy(true, 30, false), NOW).unwrap(), Swept::default());
    }

    #[test]
    fn sweeping_twice_is_harmless() {
        let (_dir, store, db) = setup();
        recording(&store, &db, 90);

        let first = sweep(&store, &db, &policy(true, 30, true), NOW).unwrap();
        let second = sweep(&store, &db, &policy(true, 30, true), NOW).unwrap();

        assert_eq!(first.audio_removed, 1);
        assert_eq!(second.audio_removed, 0, "there is nothing left to remove");
    }
}
