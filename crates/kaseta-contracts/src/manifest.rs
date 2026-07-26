//! The recording manifest — the durable description of what was captured.
//!
//! The manifest is assembled when a session ends. Durability across a crash
//! does not depend on it: [`RecordingHeader`] is written when capture starts,
//! [`TrackHeader`] and a [`FormatEpoch`] when a track's format is negotiated,
//! and a [`Chunk`] sidecar beside every audio blob.
//!
//! Recovery from those artifacts is faithful but not byte-identical. `ended_at`
//! is a wall-clock value and the durable timing is boottime-based, so a
//! recovered recording's end is inferred from its last chunk rather than known.
//! A root holding a header but no chunks was interrupted before capture
//! produced anything, and carries no audio to recover.
//!
//! It models `N` tracks of arbitrary source from the outset; adding
//! per-application audio or video introduces new [`TrackSource`] variants, not
//! a schema migration.
//!
//! # Timing
//!
//! Two audio devices run on independent hardware clocks. A microphone and a
//! sink monitor will drift apart over a long meeting, and aligning them by
//! sample count silently corrupts the result. Every chunk therefore carries the
//! daemon's canonical monotonic clock (`CLOCK_BOOTTIME`) alongside the sample
//! count and, when PipeWire reports it, the stream's own presentation
//! timestamp. Alignment is computed from those stamps after the fact, so drift
//! stays *correctable* rather than baked into the audio.

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::blob::{BlobKey, TrackId};

pub const MANIFEST_VERSION: &str = "recording-manifest/v1";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordingManifest {
    /// Contract version. Readers must reject unknown major versions rather than
    /// guess.
    pub manifest_version: String,
    pub recording_id: Ulid,
    #[serde(with = "time::serde::rfc3339")]
    pub started_at: time::OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option", default)]
    pub ended_at: Option<time::OffsetDateTime>,
    pub canonical_clock: CanonicalClock,
    pub timeline: Timeline,
    pub tracks: Vec<Track>,
    #[serde(default)]
    pub notes: RecordingNotes,
}

impl RecordingManifest {
    pub fn track(&self, id: &TrackId) -> Option<&Track> {
        self.tracks.iter().find(|t| &t.track_id == id)
    }
}

/// The clock all timestamps in this manifest are expressed against.
///
/// `CLOCK_BOOTTIME` is used rather than the wall clock because it is monotonic
/// and, unlike `CLOCK_MONOTONIC`, continues to advance across system suspend —
/// which matters when a laptop lid closes mid-meeting.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CanonicalClock {
    pub kind: ClockKind,
    /// Reading of the canonical clock at the instant capture started.
    pub started_at_ns: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClockKind {
    BoottimeNs,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Timeline {
    /// The track whose clock defines the reference timeline. Other tracks are
    /// mapped onto it at stitch time.
    pub master_track_id: TrackId,
    pub nominal_sample_rate_hz: u32,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RecordingNotes {
    /// Whether the operator confirmed headphones. Without them the far end
    /// bleeds from the speakers into the microphone track and the
    /// local-versus-remote attribution degrades.
    #[serde(default)]
    pub headphones_expected: bool,
    #[serde(default)]
    pub echo_risk: EchoRisk,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EchoRisk {
    Low,
    #[default]
    Unknown,
    High,
}

/// Recording-level metadata, written when capture starts.
///
/// The manifest is only assembled when a session ends, so without this a crash
/// would lose the recording's identity and clock origin even though every chunk
/// survived. Together with the per-track headers and chunk sidecars it is
/// enough to rebuild a [`RecordingManifest`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordingHeader {
    pub manifest_version: String,
    pub recording_id: Ulid,
    #[serde(with = "time::serde::rfc3339")]
    pub started_at: time::OffsetDateTime,
    pub canonical_clock: CanonicalClock,
    /// The reference track, chosen when the session was configured. Not
    /// re-derivable afterwards: the choice falls back to the first track when no
    /// microphone is present, and track ordering is not preserved by storage.
    pub master_track_id: TrackId,
    /// Operator-supplied context. Lost entirely if not persisted here, since
    /// nothing about it is derivable from the audio.
    #[serde(default)]
    pub notes: RecordingNotes,
}

/// Immutable track identity, written once.
///
/// Carries what a track *is* — which device it came from, what it represents,
/// which hardware clock it runs on. Deliberately excludes the sample format,
/// which is not immutable: a Bluetooth headset switching profile renegotiates
/// mid-recording. Formats live in [`FormatEpoch`] instead, so a header can
/// never describe a format that some of the track's chunks were not captured
/// at.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrackHeader {
    pub track_id: TrackId,
    pub media_type: MediaType,
    pub role: TrackRole,
    pub source: TrackSource,
    pub clock_domain: ClockDomain,
}

/// The format a run of chunks was captured at.
///
/// One is written when capture starts and another after every renegotiation,
/// keyed by the first chunk sequence it governs. Sorting epochs by `from_seq`
/// partitions a track's chunks by the format each was actually recorded at.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FormatEpoch {
    /// First chunk sequence captured under this format.
    pub from_seq: u32,
    pub format: TrackFormat,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Track {
    pub track_id: TrackId,
    pub media_type: MediaType,
    pub role: TrackRole,
    pub source: TrackSource,
    pub clock_domain: ClockDomain,
    pub format: TrackFormat,
    pub chunks: Vec<Chunk>,
}

impl Track {
    /// Total samples committed to this track across all chunks.
    pub fn sample_count(&self) -> u64 {
        self.chunks.iter().filter_map(|c| c.sample_count).sum()
    }

    /// Measured sample rate implied by the canonical clock, compared against the
    /// rate the device claims. A meaningful gap between the two is drift.
    ///
    /// Returns `None` for tracks with no timed samples, e.g. a video track or a
    /// track that captured nothing.
    pub fn observed_sample_rate_hz(&self) -> Option<f64> {
        let first = self.chunks.first()?;
        let last = self.chunks.last()?;
        let elapsed_ns = last.boottime_end_ns.checked_sub(first.boottime_start_ns)?;
        if elapsed_ns == 0 {
            return None;
        }
        let samples = self.sample_count();
        if samples == 0 {
            return None;
        }
        Some(samples as f64 / (elapsed_ns as f64 / 1e9))
    }

    /// Accumulated drift against the nominal rate over the whole track.
    pub fn drift(&self) -> Option<Drift> {
        let observed = self.observed_sample_rate_hz()?;
        let nominal = self.format.sample_rate_hz? as f64;
        if nominal == 0.0 {
            return None;
        }
        let duration_s = self.sample_count() as f64 / nominal;
        // Time the samples actually represent, versus the time they claim to.
        let observed_duration_s = self.sample_count() as f64 / observed;
        Some(Drift {
            observed_sample_rate_hz: observed,
            nominal_sample_rate_hz: nominal,
            offset_ms: (observed_duration_s - duration_s) * 1000.0,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Drift {
    pub observed_sample_rate_hz: f64,
    pub nominal_sample_rate_hz: f64,
    /// Signed accumulated offset. Positive means the track ran slow relative to
    /// its nominal rate.
    pub offset_ms: f64,
}

impl Drift {
    /// Threshold past which a merged *audio* artifact needs resampling.
    /// Transcript timestamps never need it — they are mapped per chunk.
    pub const RESAMPLE_THRESHOLD_MS: f64 = 20.0;

    pub fn requires_resample(&self) -> bool {
        self.offset_ms.abs() > Self::RESAMPLE_THRESHOLD_MS
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaType {
    Audio,
    Video,
}

/// What this track represents in the conversation, independent of where it came
/// from. This is what lets a transcript be attributed without any ML: everything
/// on `LocalMic` was said by the operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrackRole {
    /// The operator's own microphone.
    LocalMic,
    /// Everything the machine played back — the far end of the call, mixed.
    RemoteMix,
    /// Audio from one specific application.
    Application,
    /// Screen, window, or camera video.
    Visual,
}

/// Where the bytes came from. New variants extend capture without touching the
/// manifest version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TrackSource {
    Microphone {
        /// PipeWire node name. Recorded for provenance and reconnection, never
        /// as a durable identity — node IDs are reused across restarts.
        node_name: String,
        display_name: String,
    },
    SinkMonitor {
        node_name: String,
        display_name: String,
    },
    ApplicationNode {
        node_name: String,
        display_name: String,
        /// Best-effort binary name, for reattaching after an app restart.
        application_binary: Option<String>,
    },
    ScreenVideo {
        display_name: String,
    },
    WindowVideo {
        display_name: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClockDomain {
    pub source_clock: String,
    /// Identifies the underlying hardware clock. Two tracks sharing this value
    /// cannot drift relative to one another.
    pub device_clock_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrackFormat {
    pub container: String,
    pub codec: String,
    pub sample_rate_hz: Option<u32>,
    pub channels: Option<u16>,
    pub sample_format: Option<String>,
}

/// One committed, immutable unit of captured media.
///
/// Chunks are the unit of durability, upload, retry, and dedup. A crash loses at
/// most the chunk being written, never the recording.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Chunk {
    pub seq: u32,
    /// Storage-agnostic address. Never a filesystem path.
    pub blob: BlobKey,
    pub sha256: String,
    pub bytes: u64,
    /// Frames per channel. `None` for video.
    pub sample_count: Option<u64>,
    pub boottime_start_ns: u64,
    pub boottime_end_ns: u64,
    /// PipeWire's own presentation timestamps, when the stream reports them.
    /// Used to cross-check the canonical clock.
    pub source_pts_start_ns: Option<u64>,
    pub source_pts_end_ns: Option<u64>,
    /// Set when the capture stream broke and resumed, meaning this chunk does
    /// not continue seamlessly from the previous one.
    #[serde(default)]
    pub discontinuity: bool,
    /// Nanoseconds of audio known to be missing before this chunk.
    #[serde(default)]
    pub gap_before_ns: u64,
    #[serde(default)]
    pub drops_before_chunk: u64,
}

impl Chunk {
    pub fn duration_ns(&self) -> u64 {
        self.boottime_end_ns.saturating_sub(self.boottime_start_ns)
    }

    /// Maps a sample offset within this chunk onto the canonical timeline.
    ///
    /// Linear within the chunk, which is why chunks stay short: drift inside a
    /// single chunk is negligible, so no global resampling is needed to place a
    /// transcript word accurately.
    pub fn sample_to_boottime_ns(&self, sample_offset: u64) -> u64 {
        match self.sample_count {
            Some(total) if total > 0 => {
                let fraction = (sample_offset.min(total) as f64) / (total as f64);
                self.boottime_start_ns + (self.duration_ns() as f64 * fraction) as u64
            }
            _ => self.boottime_start_ns,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::{RecordingPrefix, TrackId};
    use time::macros::datetime;

    fn track_with(sample_rate: u32, chunks: Vec<Chunk>) -> Track {
        Track {
            track_id: TrackId::new("a_local-mic_01").unwrap(),
            media_type: MediaType::Audio,
            role: TrackRole::LocalMic,
            source: TrackSource::Microphone {
                node_name: "alsa_input.test".into(),
                display_name: "Test".into(),
            },
            clock_domain: ClockDomain {
                source_clock: "pipewire".into(),
                device_clock_id: None,
            },
            format: TrackFormat {
                container: "flac".into(),
                codec: "flac".into(),
                sample_rate_hz: Some(sample_rate),
                channels: Some(1),
                sample_format: Some("s16".into()),
            },
            chunks,
        }
    }

    fn chunk(seq: u32, samples: u64, start_ns: u64, end_ns: u64) -> Chunk {
        let prefix = RecordingPrefix::new(Ulid::nil(), datetime!(2026-07-25 14:12:03 UTC));
        Chunk {
            seq,
            blob: prefix.chunk(&TrackId::new("a_local-mic_01").unwrap(), seq, "flac"),
            sha256: "0".repeat(64),
            bytes: samples * 2,
            sample_count: Some(samples),
            boottime_start_ns: start_ns,
            boottime_end_ns: end_ns,
            source_pts_start_ns: None,
            source_pts_end_ns: None,
            discontinuity: false,
            gap_before_ns: 0,
            drops_before_chunk: 0,
        }
    }

    #[test]
    fn a_clock_running_true_reports_no_meaningful_drift() {
        // 48000 samples delivered in exactly one second.
        let t = track_with(48_000, vec![chunk(0, 48_000, 0, 1_000_000_000)]);
        let drift = t.drift().unwrap();
        assert!(drift.offset_ms.abs() < 0.001);
        assert!(!drift.requires_resample());
    }

    #[test]
    fn detects_a_slow_device_clock_over_a_long_track() {
        // Ten minutes of audio that actually took 10 minutes plus 500 ms:
        // the device produced fewer samples than its nominal rate promises.
        let samples = 48_000 * 600;
        let elapsed = 600_500_000_000u64;
        let t = track_with(48_000, vec![chunk(0, samples, 0, elapsed)]);
        let drift = t.drift().unwrap();

        assert!(
            drift.offset_ms > 400.0,
            "expected roughly 500 ms of drift, got {}",
            drift.offset_ms
        );
        assert!(drift.requires_resample());
    }

    #[test]
    fn drift_is_none_without_timed_samples() {
        assert!(track_with(48_000, vec![]).drift().is_none());
        assert!(track_with(48_000, vec![chunk(0, 0, 0, 0)]).drift().is_none());
    }

    #[test]
    fn maps_sample_offsets_onto_the_canonical_timeline() {
        let c = chunk(0, 48_000, 1_000_000_000, 2_000_000_000);
        assert_eq!(c.sample_to_boottime_ns(0), 1_000_000_000);
        assert_eq!(c.sample_to_boottime_ns(24_000), 1_500_000_000);
        assert_eq!(c.sample_to_boottime_ns(48_000), 2_000_000_000);
        // Offsets past the end clamp rather than running off the timeline.
        assert_eq!(c.sample_to_boottime_ns(99_999), 2_000_000_000);
    }

    #[test]
    fn manifest_round_trips_through_json() {
        let prefix = RecordingPrefix::new(Ulid::nil(), datetime!(2026-07-25 14:12:03 UTC));
        let manifest = RecordingManifest {
            manifest_version: MANIFEST_VERSION.into(),
            recording_id: Ulid::nil(),
            started_at: datetime!(2026-07-25 14:12:03 UTC),
            ended_at: None,
            canonical_clock: CanonicalClock {
                kind: ClockKind::BoottimeNs,
                started_at_ns: 58_122_344_199_123,
            },
            timeline: Timeline {
                master_track_id: TrackId::new("a_local-mic_01").unwrap(),
                nominal_sample_rate_hz: 48_000,
            },
            tracks: vec![track_with(48_000, vec![chunk(0, 48_000, 0, 1_000_000_000)])],
            notes: RecordingNotes::default(),
        };

        let json = serde_json::to_string(&manifest).unwrap();
        let back: RecordingManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(back.manifest_version, MANIFEST_VERSION);
        assert_eq!(back.tracks[0].chunks[0].blob, prefix.chunk(&TrackId::new("a_local-mic_01").unwrap(), 0, "flac"));
        assert_eq!(back.tracks[0].sample_count(), 48_000);
    }
}
