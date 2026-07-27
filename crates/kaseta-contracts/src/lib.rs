//! Shared contracts between the Kaseta daemon, its workers, and its UI.
//!
//! This crate holds every type that crosses a process or storage boundary. It
//! has no I/O and no knowledge of where anything is stored, which is what keeps
//! a single-machine deployment and a hosted one on the same schema.
//!
//! The rule the whole design rests on: **act local, model remote.** Local
//! implementations are fine; local *assumptions* baked into these contracts are
//! not. Concretely, nothing here names a filesystem path, a hostname, or a
//! single-user assumption.

pub mod blob;
pub mod derived;
pub mod jobs;
pub mod manifest;
pub mod worker;

pub use blob::{BlobKey, BlobKeyError, RecordingPrefix, TrackId};
pub use derived::{
    ActionItem, LibraryMetadata, SummaryBody, SummaryDocument, TranscriptDocument, TranscriptLine,
    DERIVED_VERSION,
};
pub use jobs::{IllegalTransition, Job, JobState, JobType};
pub use manifest::{
    CanonicalClock, Chunk, ClockKind, Drift, FormatEpoch, MediaType, RecordingHeader,
    RecordingManifest, Track, TrackFormat, TrackHeader, TrackRole, TrackSource, MANIFEST_VERSION,
};
pub use worker::{
    Segment, SpeakerHint, TranscribeParams, TranscribeSpec, WorkerResult, WorkerStatus,
    TRANSCRIBE_SPEC_VERSION, WORKER_RESULT_VERSION,
};
