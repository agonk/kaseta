//! Storage-agnostic blob addressing.
//!
//! Every byte Kaseta persists is addressed by a [`BlobKey`] — an opaque,
//! store-independent object key. Nothing in the contracts, the database, or the
//! worker protocol ever names a filesystem path.
//!
//! The same key resolves against a local directory or an S3/R2 bucket depending
//! on which [`BlobStore`](crate::BlobStore) adapter is configured. Keys are
//! chosen to be valid S3 object keys so the local layout and the bucket layout
//! are byte-identical, which makes migration a copy rather than a translation.

use std::fmt;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Characters permitted in a key segment. Deliberately narrower than what S3
/// accepts: no spaces, no `+`, no unicode, so keys survive shells, URLs, and
/// signed-URL encoding without escaping.
fn is_safe_segment_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')
}

/// An object key, relative to whatever store resolves it.
///
/// Guaranteed on construction to contain only safe characters, to have no empty
/// segments, no leading or trailing `/`, and no `.`/`..` traversal segments.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct BlobKey(String);

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BlobKeyError {
    #[error("blob key is empty")]
    Empty,
    #[error("blob key has an empty segment: {0:?}")]
    EmptySegment(String),
    #[error("blob key contains a traversal segment: {0:?}")]
    Traversal(String),
    #[error("blob key contains an unsafe character {1:?}: {0:?}")]
    UnsafeChar(String, char),
}

impl BlobKey {
    pub fn new(raw: impl Into<String>) -> Result<Self, BlobKeyError> {
        let raw = raw.into();
        if raw.is_empty() {
            return Err(BlobKeyError::Empty);
        }
        for segment in raw.split('/') {
            if segment.is_empty() {
                return Err(BlobKeyError::EmptySegment(raw.clone()));
            }
            if segment == "." || segment == ".." {
                return Err(BlobKeyError::Traversal(raw.clone()));
            }
            if let Some(bad) = segment.chars().find(|c| !is_safe_segment_char(*c)) {
                return Err(BlobKeyError::UnsafeChar(raw.clone(), bad));
            }
        }
        Ok(Self(raw))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Append a segment, validating it the same way as construction.
    pub fn join(&self, segment: &str) -> Result<Self, BlobKeyError> {
        Self::new(format!("{}/{}", self.0, segment))
    }

    /// The prefix this key sits under, used for bucket lifecycle rules and for
    /// deleting a whole recording without enumerating its chunks.
    pub fn parent(&self) -> Option<Self> {
        let (head, _) = self.0.rsplit_once('/')?;
        Self::new(head).ok()
    }
}

impl fmt::Display for BlobKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for BlobKey {
    type Error = BlobKeyError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<BlobKey> for String {
    fn from(value: BlobKey) -> Self {
        value.0
    }
}

/// Identifies one recording and generates every key beneath it.
///
/// The layout is time-partitioned and lexicographically sortable, so listing a
/// bucket yields recordings in chronological order and prefix-based retention
/// rules can expire a whole day or month at once:
///
/// ```text
/// recordings/2026/07/25/20260725T141203Z_01J9S8Q4N4M7X2K6Y8A1B2C3D4/
///     manifest.json
///     tracks/a_local-mic_01/000000.flac
///     tracks/v_screen_01/000000.webm
///     exports/transcript.json
/// ```
///
/// The ULID makes the key collision-free without needing a meeting title, which
/// is not knowable when recording starts. A human-readable UTC timestamp is
/// carried alongside it so a bucket listing is legible without decoding ULIDs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordingPrefix {
    pub id: Ulid,
    /// Compact UTC stamp, `YYYYMMDDThhmmssZ`.
    pub started_stamp: String,
    /// Date partition components, kept explicit so key building cannot drift
    /// from the stamp.
    pub year: i32,
    pub month: u8,
    pub day: u8,
}

impl RecordingPrefix {
    pub fn new(id: Ulid, started: time::OffsetDateTime) -> Self {
        let started = started.to_offset(time::UtcOffset::UTC);
        let started_stamp = format!(
            "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
            started.year(),
            u8::from(started.month()),
            started.day(),
            started.hour(),
            started.minute(),
            started.second(),
        );
        Self {
            id,
            started_stamp,
            year: started.year(),
            month: u8::from(started.month()),
            day: started.day(),
        }
    }

    /// The common prefix for everything belonging to this recording.
    pub fn root(&self) -> BlobKey {
        BlobKey::new(format!(
            "recordings/{:04}/{:02}/{:02}/{}_{}",
            self.year, self.month, self.day, self.started_stamp, self.id
        ))
        .expect("recording prefix components are known-safe")
    }

    pub fn manifest(&self) -> BlobKey {
        self.root()
            .join("manifest.json")
            .expect("literal segment is safe")
    }

    /// Recording-level metadata, written when capture starts rather than when
    /// it ends, so a crash does not take the recording's identity with it.
    pub fn header(&self) -> BlobKey {
        self.root()
            .join("recording.json")
            .expect("literal segment is safe")
    }

    /// Immutable track identity, written once.
    pub fn track_header(&self, track_id: &TrackId) -> BlobKey {
        self.root()
            .join("tracks")
            .and_then(|k| k.join(track_id.as_str()))
            .and_then(|k| k.join("track.json"))
            .expect("track id is validated on construction")
    }

    /// The format governing chunks from `from_seq` onwards.
    ///
    /// Zero-padded so epochs sort in capture order alongside the chunks they
    /// describe, and named distinctly enough not to collide with chunk keys.
    pub fn format_epoch(&self, track_id: &TrackId, from_seq: u32) -> BlobKey {
        self.root()
            .join("tracks")
            .and_then(|k| k.join(track_id.as_str()))
            .and_then(|k| k.join(&format!("format-{from_seq:06}.json")))
            .expect("track id is validated on construction")
    }

    /// Key for one chunk of one track. `seq` is zero-padded so chunks sort
    /// lexicographically, which is what makes stitching a plain listing.
    pub fn chunk(&self, track_id: &TrackId, seq: u32, extension: &str) -> BlobKey {
        self.root()
            .join("tracks")
            .and_then(|k| k.join(track_id.as_str()))
            .and_then(|k| k.join(&format!("{seq:06}.{extension}")))
            .expect("track id and extension are validated on construction")
    }

    pub fn export(&self, filename: &str) -> Result<BlobKey, BlobKeyError> {
        self.root()
            .join("exports")
            .and_then(|k| k.join(filename))
    }
}

/// A track identifier, e.g. `a_local-mic_01` or `v_screen_01`.
///
/// The `a_`/`v_` prefix keeps audio and video tracks visually grouped in a
/// bucket listing and makes the media type readable without opening the
/// manifest.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct TrackId(String);

impl TrackId {
    pub fn new(raw: impl Into<String>) -> Result<Self, BlobKeyError> {
        let raw = raw.into();
        // A track id is a single key segment, so validate it as one.
        BlobKey::new(&raw)?;
        if raw.contains('/') {
            return Err(BlobKeyError::UnsafeChar(raw, '/'));
        }
        Ok(Self(raw))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TrackId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for TrackId {
    type Error = BlobKeyError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<TrackId> for String {
    fn from(value: TrackId) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn rejects_traversal_and_unsafe_characters() {
        assert_eq!(
            BlobKey::new("a/../b").unwrap_err(),
            BlobKeyError::Traversal("a/../b".into())
        );
        assert!(matches!(
            BlobKey::new("a/b c").unwrap_err(),
            BlobKeyError::UnsafeChar(_, ' ')
        ));
        assert!(matches!(
            BlobKey::new("a//b").unwrap_err(),
            BlobKeyError::EmptySegment(_)
        ));
        assert_eq!(BlobKey::new("").unwrap_err(), BlobKeyError::Empty);
        // A leading slash produces an empty first segment.
        assert!(BlobKey::new("/a").is_err());
    }

    #[test]
    fn builds_a_sortable_time_partitioned_layout() {
        let id = Ulid::from_string("01J9S8Q4N4M7X2K6Y8A1B2C3D4").unwrap();
        let prefix = RecordingPrefix::new(id, datetime!(2026-07-25 14:12:03 UTC));

        assert_eq!(
            prefix.manifest().as_str(),
            "recordings/2026/07/25/20260725T141203Z_01J9S8Q4N4M7X2K6Y8A1B2C3D4/manifest.json"
        );

        let track = TrackId::new("a_local-mic_01").unwrap();
        assert_eq!(
            prefix.chunk(&track, 7, "flac").as_str(),
            "recordings/2026/07/25/20260725T141203Z_01J9S8Q4N4M7X2K6Y8A1B2C3D4/tracks/a_local-mic_01/000007.flac"
        );
    }

    #[test]
    fn chunk_keys_sort_in_capture_order_past_the_padding_boundary() {
        let prefix = RecordingPrefix::new(Ulid::nil(), datetime!(2026-07-25 14:12:03 UTC));
        let track = TrackId::new("a_local-mic_01").unwrap();

        let mut keys: Vec<String> = [9u32, 10, 100, 1_000]
            .iter()
            .map(|seq| prefix.chunk(&track, *seq, "flac").to_string())
            .collect();
        let expected = keys.clone();
        keys.sort();

        assert_eq!(keys, expected, "lexicographic order must match capture order");
    }

    #[test]
    fn normalises_utc_before_stamping() {
        // A non-UTC input must not shift the date partition.
        let offset = datetime!(2026-07-25 01:12:03 +03:00);
        let prefix = RecordingPrefix::new(Ulid::nil(), offset);
        assert_eq!(prefix.started_stamp, "20260724T221203Z");
        assert_eq!((prefix.year, prefix.month, prefix.day), (2026, 7, 24));
    }

    #[test]
    fn parent_returns_the_enclosing_prefix() {
        let key = BlobKey::new("recordings/2026/07/25/x/manifest.json").unwrap();
        assert_eq!(key.parent().unwrap().as_str(), "recordings/2026/07/25/x");
    }
}
