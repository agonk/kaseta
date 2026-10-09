//! Writing a recording's objects, shared by capture and import.
//!
//! A recording is the same set of objects however its audio arrived: a header
//! naming it and its clock origin, a header per track, a format epoch, chunks
//! with their timing sidecars, and finally the manifest. Capture produces them
//! from a live stream and an import from a decoded file. Writing them through
//! one module is what keeps the two indistinguishable to everything
//! downstream, which reads the objects and never asks where they came from.

use anyhow::{Context, Result};
use kaseta_contracts::manifest::{
    CanonicalClock, Chunk, ClockKind, FormatEpoch, ImportSource, RecordingHeader,
    RecordingManifest, RecordingNotes, Timeline, Track, TrackFormat, TrackHeader,
    MANIFEST_VERSION,
};
use kaseta_contracts::{BlobKey, RecordingPrefix, TrackId};
use ulid::Ulid;

use super::chunk::SealedChunk;
use crate::blobstore::BlobStore;

/// Writes the recording header, which names the recording and fixes its clock
/// origin before any audio exists.
///
/// Idempotent: writing the same header again is a no-op, and writing a
/// different one under the same key is refused, because every chunk timestamp
/// already written was measured against the first.
pub fn write_header(store: &dyn BlobStore, prefix: &RecordingPrefix, header: &RecordingHeader) -> Result<()> {
    store
        .put_idempotent(
            &prefix.header(),
            &serde_json::to_vec(header).context("serialising recording header")?,
        )
        .context("writing recording header")?;
    Ok(())
}

/// Writes a track's immutable identity.
pub fn write_track_header(
    store: &dyn BlobStore,
    prefix: &RecordingPrefix,
    header: &TrackHeader,
) -> Result<()> {
    let encoded = serde_json::to_vec(header).context("serialising track header")?;
    store
        .put_idempotent(&prefix.track_header(&header.track_id), &encoded)
        .context("writing track header")?;
    Ok(())
}

/// Writes the format that governs a track's chunks from `epoch.from_seq` on.
pub fn write_format_epoch(
    store: &dyn BlobStore,
    prefix: &RecordingPrefix,
    track_id: &TrackId,
    epoch: &FormatEpoch,
) -> Result<()> {
    let encoded = serde_json::to_vec(epoch).context("serialising format epoch")?;
    store
        .put_idempotent(&prefix.format_epoch(track_id, epoch.from_seq), &encoded)
        .context("writing format epoch")?;
    Ok(())
}

/// The format every chunk is stored in: FLAC of 16-bit samples.
pub fn flac_format(sample_rate_hz: u32, channels: u16) -> TrackFormat {
    TrackFormat {
        container: "flac".into(),
        codec: "flac".into(),
        sample_rate_hz: Some(sample_rate_hz),
        channels: Some(channels),
        sample_format: Some("s16".into()),
    }
}

/// Writes a sealed chunk to storage and returns its manifest record.
///
/// Two objects are written per chunk: the audio, then a sidecar holding its
/// timing metadata. The manifest is only assembled at the end, so without the
/// sidecar a crash would leave audio on disk whose timestamps, discontinuity
/// flags and digests died with the in-memory state, unusable for alignment or
/// transcription.
///
/// Audio is written first. A sidecar therefore implies its audio is present,
/// and recovery can treat any chunk lacking one as incomplete.
pub fn persist_chunk(
    store: &dyn BlobStore,
    prefix: &RecordingPrefix,
    track_id: &TrackId,
    chunk: &SealedChunk,
) -> Result<Chunk> {
    let key: BlobKey = prefix.chunk(track_id, chunk.seq, "flac");
    store
        .put_idempotent(&key, &chunk.encoded)
        .with_context(|| format!("persisting chunk {} of {track_id}", chunk.seq))?;

    let record = Chunk {
        seq: chunk.seq,
        blob: key,
        sha256: chunk.sha256.clone(),
        bytes: chunk.encoded.len() as u64,
        sample_count: Some(chunk.sample_count),
        boottime_start_ns: chunk.boottime_start_ns,
        boottime_end_ns: chunk.boottime_end_ns,
        source_pts_start_ns: chunk.source_pts_start_ns,
        source_pts_end_ns: chunk.source_pts_end_ns,
        discontinuity: chunk.discontinuity,
        gap_before_ns: chunk.gap_before_ns,
        drops_before_chunk: chunk.drops_before_chunk,
    };

    let sidecar = prefix.chunk(track_id, record.seq, "json");
    let encoded = serde_json::to_vec(&record).context("serialising chunk metadata")?;
    store
        .put_idempotent(&sidecar, &encoded)
        .with_context(|| format!("persisting metadata for chunk {} of {track_id}", record.seq))?;

    Ok(record)
}

/// Everything a manifest is assembled from.
pub struct ManifestParts {
    pub recording_id: Ulid,
    pub started_at: time::OffsetDateTime,
    pub ended_at: Option<time::OffsetDateTime>,
    /// The canonical clock reading every chunk timestamp is measured from.
    pub clock_started_ns: u64,
    pub master_track_id: TrackId,
    pub tracks: Vec<Track>,
    pub notes: RecordingNotes,
    pub source: Option<ImportSource>,
}

/// Assembles a manifest.
///
/// The nominal rate is the master track's, falling back to 48 kHz for a
/// master that captured nothing, so a recording that is mostly silence still
/// has a timeline to read.
pub fn build_manifest(parts: ManifestParts) -> RecordingManifest {
    let nominal_rate = parts
        .tracks
        .iter()
        .find(|t| t.track_id == parts.master_track_id)
        .and_then(|t| t.format.sample_rate_hz)
        .unwrap_or(48_000);

    RecordingManifest {
        manifest_version: MANIFEST_VERSION.into(),
        recording_id: parts.recording_id,
        started_at: parts.started_at,
        ended_at: parts.ended_at,
        canonical_clock: CanonicalClock {
            kind: ClockKind::BoottimeNs,
            started_at_ns: parts.clock_started_ns,
        },
        timeline: Timeline {
            master_track_id: parts.master_track_id,
            nominal_sample_rate_hz: nominal_rate,
        },
        tracks: parts.tracks,
        notes: parts.notes,
        source: parts.source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blobstore::LocalFsStore;
    use crate::capture::chunk::{CapturedBuffer, ChunkConfig, ChunkWriter};
    use kaseta_contracts::manifest::{ClockDomain, MediaType, TrackRole, TrackSource};

    fn sealed() -> SealedChunk {
        let mut writer = ChunkWriter::new(ChunkConfig {
            sample_rate_hz: 48_000,
            channels: 1,
            chunk_duration_s: 1,
        });
        let samples = vec![100i16; 48_000];
        writer
            .push(CapturedBuffer {
                samples: &samples,
                arrived_at_ns: 2_000_000_000,
                source_pts_ns: None,
            })
            .unwrap()
            .remove(0)
    }

    /// The sidecar is what survives a crash in place of the manifest, so it
    /// must say exactly what the returned record says.
    #[test]
    fn a_chunk_is_stored_with_a_sidecar_matching_its_record() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();
        let prefix = RecordingPrefix::new(Ulid::new(), time::OffsetDateTime::UNIX_EPOCH);
        let track = TrackId::new("a_imported_01").unwrap();
        let chunk = sealed();

        let record = persist_chunk(&store, &prefix, &track, &chunk).unwrap();

        assert_eq!(store.get(&record.blob).unwrap(), chunk.encoded);
        assert_eq!(record.sample_count, Some(48_000));
        assert_eq!(record.boottime_start_ns, 1_000_000_000);
        let sidecar: Chunk =
            serde_json::from_slice(&store.get(&prefix.chunk(&track, 0, "json")).unwrap()).unwrap();
        assert_eq!(sidecar.sha256, record.sha256);
        assert_eq!(sidecar.boottime_end_ns, record.boottime_end_ns);

        // Persisting the same chunk again, as a retried write would, is free.
        persist_chunk(&store, &prefix, &track, &chunk).unwrap();
    }

    #[test]
    fn a_header_cannot_be_replaced_by_a_different_one() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();
        let id = Ulid::new();
        let prefix = RecordingPrefix::new(id, time::OffsetDateTime::UNIX_EPOCH);
        let header = |origin| RecordingHeader {
            manifest_version: MANIFEST_VERSION.into(),
            recording_id: id,
            started_at: time::OffsetDateTime::UNIX_EPOCH,
            canonical_clock: CanonicalClock {
                kind: ClockKind::BoottimeNs,
                started_at_ns: origin,
            },
            master_track_id: TrackId::new("a_imported_01").unwrap(),
            notes: RecordingNotes::default(),
        };

        write_header(&store, &prefix, &header(5)).unwrap();
        write_header(&store, &prefix, &header(5)).unwrap();
        assert!(
            write_header(&store, &prefix, &header(6)).is_err(),
            "chunks already written were timed against the first origin"
        );
    }

    #[test]
    fn the_manifest_takes_its_nominal_rate_from_the_master_track() {
        let master = TrackId::new("a_imported_01").unwrap();
        let track = Track {
            track_id: master.clone(),
            media_type: MediaType::Audio,
            role: TrackRole::Unattributed,
            source: TrackSource::ImportedFile { stream_index: 1 },
            clock_domain: ClockDomain {
                source_clock: "file".into(),
                device_clock_id: None,
            },
            format: flac_format(44_100, 1),
            chunks: Vec::new(),
        };
        let manifest = build_manifest(ManifestParts {
            recording_id: Ulid::new(),
            started_at: time::OffsetDateTime::UNIX_EPOCH,
            ended_at: None,
            clock_started_ns: 9,
            master_track_id: master,
            tracks: vec![track],
            notes: RecordingNotes::default(),
            source: None,
        });
        assert_eq!(manifest.timeline.nominal_sample_rate_hz, 44_100);
        assert_eq!(manifest.canonical_clock.started_at_ns, 9);
        assert_eq!(manifest.manifest_version, MANIFEST_VERSION);
    }
}
