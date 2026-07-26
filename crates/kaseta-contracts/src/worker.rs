//! The daemon ↔ ASR worker protocol.
//!
//! The worker is a short-lived child process: it is spawned with a job spec on
//! stdin, writes a result to stdout, and exits. Steady-state memory cost of
//! transcription is therefore zero.
//!
//! The protocol is deliberately narrow and versioned so the engine behind it
//! stays swappable — Parakeet, Whisper, or a future model — without the daemon
//! knowing which one ran. The daemon reads only `contract_version`, `status`,
//! `tracks[].segments[]` and `warnings[]`; everything else is engine metadata
//! carried for provenance.
//!
//! Inputs and outputs are [`BlobKey`]s, never paths, so moving the worker off
//! this machine is a store swap rather than a protocol change.

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::blob::{BlobKey, TrackId};

pub const TRANSCRIBE_SPEC_VERSION: &str = "transcribe-tracks/v1";
pub const WORKER_RESULT_VERSION: &str = "worker-result/v1";

/// How the worker resolves [`BlobKey`]s to bytes.
///
/// The contract stays storage-agnostic — it names keys, never paths — but the
/// worker is a separate process and must be told where to look. Adding a remote
/// store is a new variant here, not a change to the protocol.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StorageRef {
    LocalFs { root: String },
}

/// One track's audio to transcribe.
///
/// Points at the *merged* export rather than at chunks. Merging already
/// concatenated the chunks and padded every dropped-audio gap with silence of
/// exactly the missing duration, which makes the file a linear timeline: an
/// offset within it maps to the canonical clock by simple addition. Handing the
/// worker chunks instead would make it responsible for reassembly and for gap
/// arithmetic it has no reason to know about.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrackAudio {
    pub track_id: TrackId,
    /// Who this track carries, so the worker can label segments without
    /// inferring anything.
    pub speaker_hint: SpeakerHint,
    pub audio: BlobKey,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TranscribeSpec {
    pub contract_version: String,
    pub job_id: Ulid,
    pub recording_id: Ulid,
    pub revision: u32,
    pub storage: StorageRef,
    pub tracks: Vec<TrackAudio>,
    pub params: TranscribeParams,
}

impl TranscribeSpec {
    pub fn new(
        job_id: Ulid,
        recording_id: Ulid,
        revision: u32,
        storage: StorageRef,
        tracks: Vec<TrackAudio>,
        params: TranscribeParams,
    ) -> Self {
        Self {
            contract_version: TRANSCRIBE_SPEC_VERSION.into(),
            job_id,
            recording_id,
            revision,
            storage,
            tracks,
            params,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TranscribeParams {
    /// `auto` lets the worker choose based on what it can load. Naming a
    /// specific engine pins it, which is what reprocessing uses.
    pub engine: String,
    pub model: String,
    /// BCP-47 tag, or `auto` for per-segment detection.
    pub language: String,
    pub word_timestamps: bool,
    /// Voice activity detection, to skip silence rather than transcribe it.
    pub vad: bool,
}

impl Default for TranscribeParams {
    fn default() -> Self {
        Self {
            engine: "auto".into(),
            model: "parakeet-tdt-0.6b-v3-int8".into(),
            language: "en".into(),
            word_timestamps: true,
            vad: true,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerResult {
    pub contract_version: String,
    pub job_id: Ulid,
    pub status: WorkerStatus,
    #[serde(default)]
    pub engine: Option<EngineInfo>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub error: Option<WorkerError>,
    #[serde(default)]
    pub tracks: Vec<TrackTranscript>,
}

impl WorkerResult {
    /// Rejects results this build does not understand rather than silently
    /// misreading them.
    pub fn validate(&self) -> Result<(), WorkerProtocolError> {
        if self.contract_version != WORKER_RESULT_VERSION {
            return Err(WorkerProtocolError::UnsupportedVersion(
                self.contract_version.clone(),
            ));
        }
        if self.status == WorkerStatus::Ok && self.tracks.is_empty() {
            return Err(WorkerProtocolError::EmptySuccess);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerStatus {
    Ok,
    /// Some tracks transcribed, others failed. The recording is usable but
    /// incomplete.
    Partial,
    Error,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerError {
    pub code: String,
    pub message: String,
    /// Whether the daemon should retry. The worker knows better than the daemon
    /// whether a failure is transient.
    #[serde(default)]
    pub retryable: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EngineInfo {
    pub name: String,
    pub version: String,
    pub model: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrackTranscript {
    pub track_id: TrackId,
    pub language: Option<String>,
    pub segments: Vec<Segment>,
}

/// A stretch of recognised speech.
///
/// Offsets are relative to the start of the track's audio, in nanoseconds. The
/// worker does not know about the canonical clock and must not: keeping the
/// mapping in the daemon means a change to how time is tracked never requires a
/// matching change in a separate language.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Segment {
    pub start_ns: u64,
    pub end_ns: u64,
    pub text: String,
    /// Attribution derived from which track this came from, requiring no
    /// speaker model. Audio captured from the microphone was spoken by the
    /// operator; audio captured from the sink monitor was not.
    pub speaker_hint: Option<SpeakerHint>,
    #[serde(default)]
    pub words: Vec<Word>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpeakerHint {
    /// The operator.
    Local,
    /// Someone on the far end — one or more people, not yet separated.
    Remote,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Word {
    pub start_ns: u64,
    pub end_ns: u64,
    pub text: String,
}

#[derive(Debug, thiserror::Error)]
pub enum WorkerProtocolError {
    #[error("unsupported worker result version: {0}")]
    UnsupportedVersion(String),
    #[error("worker reported success but returned no tracks")]
    EmptySuccess,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok_result() -> WorkerResult {
        WorkerResult {
            contract_version: WORKER_RESULT_VERSION.into(),
            job_id: Ulid::nil(),
            status: WorkerStatus::Ok,
            engine: Some(EngineInfo {
                name: "onnx-asr".into(),
                version: "0.6.0".into(),
                model: "parakeet-tdt-0.6b-v3-int8".into(),
            }),
            warnings: vec![],
            error: None,
            tracks: vec![TrackTranscript {
                track_id: TrackId::new("a_local-mic_01").unwrap(),
                language: Some("en".into()),
                segments: vec![Segment {
                    start_ns: 1_000_000_000,
                    end_ns: 2_000_000_000,
                    text: "Can you hear me?".into(),
                    speaker_hint: Some(SpeakerHint::Local),
                    words: vec![],
                }],
            }],
        }
    }

    #[test]
    fn accepts_a_well_formed_result() {
        assert!(ok_result().validate().is_ok());
    }

    #[test]
    fn rejects_a_future_contract_version() {
        let mut r = ok_result();
        r.contract_version = "worker-result/v2".into();
        assert!(matches!(
            r.validate(),
            Err(WorkerProtocolError::UnsupportedVersion(_))
        ));
    }

    #[test]
    fn rejects_success_with_nothing_transcribed() {
        let mut r = ok_result();
        r.tracks.clear();
        assert!(matches!(r.validate(), Err(WorkerProtocolError::EmptySuccess)));
    }

    #[test]
    fn an_error_result_needs_no_tracks() {
        let mut r = ok_result();
        r.status = WorkerStatus::Error;
        r.tracks.clear();
        r.error = Some(WorkerError {
            code: "model_missing".into(),
            message: "model not downloaded".into(),
            retryable: false,
        });
        assert!(r.validate().is_ok());
    }

    #[test]
    fn spec_round_trips_and_carries_no_filesystem_paths() {
        let spec = TranscribeSpec::new(
            Ulid::nil(),
            Ulid::nil(),
            1,
            StorageRef::LocalFs {
                root: "/var/lib/kaseta".into(),
            },
            vec![TrackAudio {
                track_id: TrackId::new("a_local-mic_01").unwrap(),
                speaker_hint: SpeakerHint::Local,
                audio: BlobKey::new("recordings/2026/07/25/x/exports/a_local-mic_01.flac").unwrap(),
            }],
            TranscribeParams::default(),
        );
        let json = serde_json::to_string(&spec).unwrap();
        // Audio is named by key. Where those keys resolve is a separate,
        // explicit field, so moving the worker elsewhere is a new storage
        // variant rather than a protocol change.
        assert!(!json.contains("file://"));
        assert!(json.contains("\"audio\":\"recordings/"));
        assert!(json.contains("\"kind\":\"local_fs\""));

        let back: TranscribeSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back.contract_version, TRANSCRIBE_SPEC_VERSION);
        assert_eq!(back.params.model, "parakeet-tdt-0.6b-v3-int8");
    }
}
