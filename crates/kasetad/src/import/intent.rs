//! What the person asked for when they uploaded a file.
//!
//! Written beside the upload in staging, first as `uploading` before a single
//! byte of the file arrives, then rewritten as `uploaded` with the size and
//! digest once the upload has committed. The decode reads its inputs from
//! here rather than from the index, and every field of the manifest that is
//! not measured from the audio comes from here too, so a retry rebuilds
//! exactly the same recording.

use anyhow::{Context, Result};
use kaseta_contracts::{BlobKey, ImportStaging};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::blobstore::BlobStore;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntentState {
    /// The upload is still arriving, or was abandoned partway.
    Uploading,
    /// The upload is complete and its size and digest are known.
    Uploaded,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Intent {
    pub state: IntentState,
    pub recording_id: Ulid,
    /// When the upload began. The recording's start time and its import time
    /// both derive from this, never from the clock at decode time, so a
    /// retried decode names the same moment.
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: time::OffsetDateTime,
    /// The file's name on the person's machine, verbatim.
    pub original_filename: String,
    /// The upload's extension as stored, already reduced to a safe one.
    pub ext: String,
    pub keep_original: bool,
    /// The title the recording gets: the one given, or the file's name.
    pub title: String,
    /// Known once the upload has committed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

impl Intent {
    /// Reads an import's intent, or `None` if it has none.
    pub fn read(store: &dyn BlobStore, id: Ulid) -> Result<Option<Self>> {
        let key = ImportStaging::new(id).intent();
        if !store.exists(&key)? {
            return Ok(None);
        }
        let bytes = store.get(&key)?;
        serde_json::from_slice(&bytes)
            .map(Some)
            .with_context(|| format!("reading {key}"))
    }

    /// The recording's start time: when the upload began, in whole seconds,
    /// because the recording's prefix names it to the second.
    pub fn started_at(&self) -> time::OffsetDateTime {
        let utc = self.created_at.to_offset(time::UtcOffset::UTC);
        utc.replace_nanosecond(0).unwrap_or(utc)
    }

    /// Where the upload itself is in staging.
    pub fn upload_key(&self) -> Result<BlobKey> {
        ImportStaging::new(self.recording_id)
            .upload(&self.ext)
            .context("building the upload's key")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    pub(crate) fn uploaded(id: Ulid) -> Intent {
        Intent {
            state: IntentState::Uploaded,
            recording_id: id,
            created_at: datetime!(2026-10-09 08:00:00.75 UTC),
            original_filename: "Lecture 3.mp4".into(),
            ext: "mp4".into(),
            keep_original: false,
            title: "Lecture 3".into(),
            bytes: Some(10),
            sha256: Some("ab".repeat(32)),
        }
    }

    #[test]
    fn the_start_time_is_the_upload_time_to_the_second() {
        let intent = uploaded(Ulid::new());
        assert_eq!(intent.started_at(), datetime!(2026-10-09 08:00:00 UTC));
    }

    #[test]
    fn an_intent_round_trips_through_storage() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = crate::blobstore::LocalFsStore::new(dir.path()).unwrap();
        let id = Ulid::new();
        assert_eq!(Intent::read(&store, id).unwrap(), None);

        let intent = uploaded(id);
        store
            .put(&ImportStaging::new(id).intent(), &serde_json::to_vec(&intent).unwrap())
            .unwrap();
        assert_eq!(Intent::read(&store, id).unwrap(), Some(intent.clone()));
        assert_eq!(
            intent.upload_key().unwrap().as_str(),
            format!("imports/{id}/upload.mp4")
        );
    }

    /// An intent written before the upload has nothing to say about its size.
    #[test]
    fn an_uploading_intent_has_no_size_yet() {
        let json = format!(
            r#"{{"state":"uploading","recording_id":"{}","created_at":"2026-10-09T08:00:00Z",
                "original_filename":"a.mp3","ext":"mp3","keep_original":true,"title":"a"}}"#,
            Ulid::new()
        );
        let intent: Intent = serde_json::from_str(&json).unwrap();
        assert_eq!(intent.state, IntentState::Uploading);
        assert_eq!(intent.bytes, None);
        assert!(!serde_json::to_string(&intent).unwrap().contains("sha256"));
    }
}
