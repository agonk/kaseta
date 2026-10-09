//! Clearing staging that nothing will come back for.
//!
//! An import's staging normally goes the moment it is finalised. This sweep,
//! run at startup, catches the rest: a crash between finalising and removing
//! it, an upload abandoned halfway, an upload that committed but whose
//! recording was never created, a recording deleted while it was imported.
//!
//! Staging is kept while its import could still need it: whenever the
//! recording exists and its decode has not succeeded, because Retry decodes
//! from it again. A staging root with no recording is given a day before it
//! goes. An upload in progress has no recording yet either, and the age is
//! what tells the two apart.

use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{params, OptionalExtension};
use ulid::Ulid;

use super::intent::Intent;
use crate::blobstore::BlobStore;
use crate::db::Db;

/// How long staging without a recording is left alone.
pub const ORPHAN_AGE: time::Duration = time::Duration::hours(24);

/// Removes staging roots that are finished with. Returns how many went.
pub fn sweep(store: &dyn BlobStore, db: &Db, now: time::OffsetDateTime) -> Result<usize> {
    let Some(root) = store.local_root() else {
        return Ok(0);
    };
    let imports = root.join("imports");
    let entries = match std::fs::read_dir(&imports) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e).with_context(|| format!("listing {}", imports.display())),
    };

    let mut removed = 0;
    for entry in entries {
        let entry = entry.with_context(|| format!("listing {}", imports.display()))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let id = Ulid::from_string(&name).ok();

        if !finished_with(store, db, id, &path, now)? {
            continue;
        }
        match std::fs::remove_dir_all(&path) {
            Ok(()) => {
                removed += 1;
                tracing::info!(staging = %name, "removed import staging nothing needs");
            }
            Err(e) => tracing::warn!(staging = %name, error = %e, "could not remove import staging"),
        }
    }
    Ok(removed)
}

/// Whether a staging root can go.
fn finished_with(
    store: &dyn BlobStore,
    db: &Db,
    id: Option<Ulid>,
    path: &Path,
    now: time::OffsetDateTime,
) -> Result<bool> {
    if let Some(id) = id {
        let row: Option<(bool, Option<String>)> = db
            .conn()
            .query_row(
                "SELECT r.deleted_at IS NOT NULL,
                        (SELECT state FROM jobs
                         WHERE recording_id = r.id AND job_type = 'import_media'
                         ORDER BY enqueue_seq DESC LIMIT 1)
                 FROM recordings r WHERE r.id = ?1",
                params![id.to_string()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        match row {
            // Deleted: nothing will decode it again.
            Some((true, _)) => return Ok(true),
            // Decoded and finalised; only the cleanup was missed.
            Some((false, Some(state))) if state == "succeeded" => return Ok(true),
            // Still to decode, or failed and waiting for a retry.
            Some((false, _)) => return Ok(false),
            None => {}
        }
    }

    // No recording. Either the upload is still arriving, or it never will.
    let started = id
        .and_then(|id| Intent::read(store, id).ok().flatten())
        .map(|intent| intent.created_at)
        .or_else(|| id.and_then(ulid_time))
        .or_else(|| modified(path));
    Ok(match started {
        Some(started) => now - started > ORPHAN_AGE,
        None => false,
    })
}

/// When a ULID was minted, which for a staging root is when its upload began.
fn ulid_time(id: Ulid) -> Option<time::OffsetDateTime> {
    let ms = i128::from(id.timestamp_ms());
    time::OffsetDateTime::from_unix_timestamp_nanos(ms * 1_000_000).ok()
}

fn modified(path: &Path) -> Option<time::OffsetDateTime> {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .map(time::OffsetDateTime::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blobstore::LocalFsStore;
    use crate::db::NewImport;
    use crate::import::intent::IntentState;
    use kaseta_contracts::ImportStaging;
    use time::macros::datetime;

    const NOW: time::OffsetDateTime = datetime!(2026-10-09 12:00:00 UTC);

    fn setup() -> (tempfile::TempDir, LocalFsStore, Db) {
        let dir = tempfile::TempDir::new().unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();
        (dir, store, Db::open_in_memory().unwrap())
    }

    /// A ULID minted `hours` before `NOW`.
    fn id_aged(hours: i64) -> Ulid {
        let at = NOW - time::Duration::hours(hours);
        Ulid::from_parts((at.unix_timestamp_nanos() / 1_000_000) as u64, rand_bits())
    }

    fn rand_bits() -> u128 {
        Ulid::new().random()
    }

    fn intent(store: &LocalFsStore, id: Ulid, state: IntentState, created_at: time::OffsetDateTime) {
        let intent = Intent {
            state,
            recording_id: id,
            created_at,
            original_filename: "talk.mp4".into(),
            ext: "mp4".into(),
            keep_original: false,
            title: "talk".into(),
            bytes: None,
            sha256: None,
        };
        store
            .put(&ImportStaging::new(id).intent(), &serde_json::to_vec(&intent).unwrap())
            .unwrap();
    }

    fn staging(store: &LocalFsStore, id: Ulid) -> std::path::PathBuf {
        let dir = crate::import::staging_dir(store, id).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A crash right after the directory was created, before the intent.
    #[test]
    fn a_bare_directory_goes_once_it_is_a_day_old() {
        let (_dir, store, db) = setup();
        let old = id_aged(25);
        let young = id_aged(1);
        let old_dir = staging(&store, old);
        let young_dir = staging(&store, young);

        assert_eq!(sweep(&store, &db, NOW).unwrap(), 1);
        assert!(!old_dir.exists());
        assert!(young_dir.exists(), "an upload may still be arriving");
    }

    /// A crash mid-upload: the intent still says uploading, the bytes are in
    /// a `.part` file.
    #[test]
    fn an_abandoned_upload_goes_with_its_partial_file() {
        let (_dir, store, db) = setup();
        let id = Ulid::new();
        let dir = staging(&store, id);
        intent(&store, id, IntentState::Uploading, NOW - time::Duration::hours(30));
        std::fs::write(dir.join("upload.mp4.part"), b"half an upload").unwrap();

        assert_eq!(sweep(&store, &db, NOW).unwrap(), 1);
        assert!(!dir.exists());
    }

    /// A crash between the upload committing and the recording being created.
    /// The intent's own time decides, not the directory name's.
    #[test]
    fn a_committed_upload_without_a_recording_waits_a_day() {
        let (_dir, store, db) = setup();
        let id = Ulid::new();
        let dir = staging(&store, id);
        std::fs::write(dir.join("upload.mp4"), b"whole").unwrap();

        intent(&store, id, IntentState::Uploaded, NOW - time::Duration::hours(2));
        assert_eq!(sweep(&store, &db, NOW).unwrap(), 0);
        assert!(dir.exists());

        intent(&store, id, IntentState::Uploaded, NOW - time::Duration::hours(26));
        assert_eq!(sweep(&store, &db, NOW).unwrap(), 1);
        assert!(!dir.exists());
    }

    /// Retry decodes from staging, so it stays for as long as the import is
    /// unfinished, however old.
    #[test]
    fn staging_stays_while_its_import_is_unfinished() {
        let (_dir, store, db) = setup();
        let id = id_aged(100);
        let dir = staging(&store, id);
        intent(&store, id, IntentState::Uploaded, NOW - time::Duration::hours(100));
        db.create_import(&NewImport {
            recording_id: id,
            started_at: datetime!(2026-10-05 08:00:00 UTC),
            title: "talk".into(),
        })
        .unwrap();

        assert_eq!(sweep(&store, &db, NOW).unwrap(), 0, "queued");
        db.conn()
            .execute("UPDATE jobs SET state = 'failed_terminal'", [])
            .unwrap();
        db.conn()
            .execute("UPDATE recordings SET status = 'failed'", [])
            .unwrap();
        assert_eq!(sweep(&store, &db, NOW).unwrap(), 0, "failed, waiting for a retry");
        assert!(dir.exists());

        // Finalised, with the cleanup missed.
        db.conn()
            .execute("UPDATE jobs SET state = 'succeeded'", [])
            .unwrap();
        assert_eq!(sweep(&store, &db, NOW).unwrap(), 1);
        assert!(!dir.exists());
    }

    #[test]
    fn staging_of_a_deleted_recording_goes_at_once() {
        let (_dir, store, db) = setup();
        let id = Ulid::new();
        let dir = staging(&store, id);
        db.create_import(&NewImport {
            recording_id: id,
            started_at: datetime!(2026-10-09 08:00:00 UTC),
            title: "talk".into(),
        })
        .unwrap();
        db.conn()
            .execute("UPDATE recordings SET deleted_at = 1, purge_pending = 1", [])
            .unwrap();

        assert_eq!(sweep(&store, &db, NOW).unwrap(), 1);
        assert!(!dir.exists());
    }

    /// Something that is not a staging root at all is aged by its own
    /// modification time.
    #[test]
    fn a_stray_directory_is_aged_by_its_mtime() {
        let (_dir, store, db) = setup();
        let stray = store.root().join("imports/not-a-ulid");
        std::fs::create_dir_all(&stray).unwrap();

        assert_eq!(sweep(&store, &db, time::OffsetDateTime::now_utc()).unwrap(), 0);
        let later = time::OffsetDateTime::now_utc() + time::Duration::hours(25);
        assert_eq!(sweep(&store, &db, later).unwrap(), 1);
        assert!(!stray.exists());
    }

    #[test]
    fn no_staging_at_all_is_nothing_to_do() {
        let (_dir, store, db) = setup();
        assert_eq!(sweep(&store, &db, NOW).unwrap(), 0);
    }
}
