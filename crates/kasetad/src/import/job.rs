//! The `ImportMedia` stage: an uploaded file in, a recording out.
//!
//! # Order, and why
//!
//! 1. The recording must still be wanted. One deleted before or during the
//!    decode is cleared away instead, staging included.
//! 2. The recording header is written once and then reused. It fixes the
//!    clock origin every chunk is timed against, so a retry after a crash
//!    produces exactly the timings the first attempt did.
//! 3. Whatever a previous attempt left (chunks, exports, a manifest) is
//!    removed, so there is never a second, different set of objects beside
//!    the first.
//! 4. The file is probed and its audio decoded straight into chunks, under a
//!    duration limit and a disk budget.
//! 5. A kept original is copied out of staging (copied, never moved: a retry
//!    still needs it until the import is final).
//! 6. The manifest is written once, then the exports.
//! 7. One transaction marks the recording ready, the job succeeded and queues
//!    transcription and backup, but only if the recording is still wanted
//!    and this job is still the one running it. If not, everything written is
//!    removed and nothing is indexed.
//! 8. Staging is removed, best effort; the startup sweep is the backstop.
//!
//! Every field of the manifest comes from durable inputs (the intent, the
//! stored header, the unchanged upload) or from the decoded audio itself.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use kaseta_contracts::manifest::{
    CanonicalClock, ClockDomain, ClockKind, FormatEpoch, ImportSource, MediaType, RecordingHeader,
    RecordingManifest, RecordingNotes, Track, TrackHeader, TrackRole, TrackSource,
    MANIFEST_VERSION,
};
use kaseta_contracts::{BlobKey, RecordingPrefix, TrackId, IMPORTED_TRACK_ID};
use rusqlite::{params, OptionalExtension};
use ulid::Ulid;

use super::intent::{Intent, IntentState};
use super::media::{self, DecodeLimits, DECODE_SAMPLE_RATE_HZ};
use super::{ImportRuntime, DECODE_BYTES_PER_SECOND, DECODE_HEADROOM_BYTES};
use crate::blobstore::BlobStore;
use crate::capture::chunk::{CapturedBuffer, ChunkConfig, ChunkWriter, DEFAULT_CHUNK_DURATION_S};
use crate::capture::persist::{self, ManifestParts};
use crate::clock::samples_to_ns;
use crate::db::Db;
use crate::scheduler::{Permanent, StageOutcome};

/// The upload this import needs is no longer in staging. Nothing can bring it
/// back, so the only way forward is a new import.
pub const UPLOAD_GONE: &str = "the uploaded file is gone; delete this item and import it again";
pub const UNSUPPORTED: &str = "unsupported file type";
pub const NO_AUDIO: &str = "This file has no audio track";
pub const EMPTY_AUDIO: &str = "the file's audio track is empty";
pub const DELETED: &str = "the recording was deleted while it was being imported";

/// Free space is checked again after this many chunks, ten minutes of audio:
/// the budget was an estimate, and the disk is shared with everything else.
const DISK_CHECK_EVERY_CHUNKS: usize = 40;

/// The shortest time a decode is allowed, however short the file says it is.
const MIN_DECODE_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// How much an import may ask of the machine.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// The longest recording an import may produce.
    pub max_duration: Duration,
    /// Reads free space on the filesystem holding a path. A parameter so the
    /// budget's refusals can be tested without filling a disk.
    pub free_space: fn(&Path) -> Result<u64>,
}

impl Limits {
    pub fn from_settings(settings: &crate::config::ImportSettings) -> Self {
        Self {
            max_duration: Duration::from_secs(u64::from(settings.max_duration_hours()) * 3600),
            free_space: super::free_space,
        }
    }

    /// The decoded-frame cap: the duration limit, enforced on the audio
    /// itself rather than on what the file claims about it.
    fn max_frames(&self) -> u64 {
        self.max_duration.as_secs() * u64::from(DECODE_SAMPLE_RATE_HZ)
    }
}

/// Runs the stage for one recording.
///
/// A failure is the stage's failure, except when the recording was deleted
/// meanwhile: then whatever the attempt left is cleared away and the stage
/// ends quietly, because a deletion is not something to report as an error.
pub fn run(
    store: &dyn BlobStore,
    db: &Arc<Mutex<Db>>,
    runtime: &ImportRuntime,
    limits: &Limits,
    recording_id: Ulid,
    job_id: Ulid,
) -> Result<StageOutcome> {
    match attempt(store, db, runtime, limits, recording_id, job_id) {
        Ok(outcome) => Ok(outcome),
        Err(e) => match wanted(db, recording_id) {
            Ok(Wanted::No { started_at }) => {
                tracing::info!(%recording_id, error = %format!("{e:#}"), "import abandoned after deletion");
                abandon(store, db, recording_id, started_at)?;
                Ok(StageOutcome::Skipped(DELETED.into()))
            }
            _ => Err(e),
        },
    }
}

fn attempt(
    store: &dyn BlobStore,
    db: &Arc<Mutex<Db>>,
    runtime: &ImportRuntime,
    limits: &Limits,
    recording_id: Ulid,
    job_id: Ulid,
) -> Result<StageOutcome> {
    let started_at = match wanted(db, recording_id)? {
        Wanted::Yes { started_at } => started_at,
        Wanted::No { started_at } => {
            abandon(store, db, recording_id, started_at)?;
            return Ok(StageOutcome::Skipped(DELETED.into()));
        }
    };

    let decoded = decode_into_recording(store, runtime, limits, recording_id, started_at)?;
    finalise(store, db, recording_id, job_id, &decoded)
}

/// What a decode produced, ready to be finalised.
pub(crate) struct Decoded {
    prefix: RecordingPrefix,
    manifest: RecordingManifest,
}

/// Steps 2 to 6: everything up to, and not including, the transaction that
/// makes the recording ready.
pub(crate) fn decode_into_recording(
    store: &dyn BlobStore,
    runtime: &ImportRuntime,
    limits: &Limits,
    recording_id: Ulid,
    started_at: time::OffsetDateTime,
) -> Result<Decoded> {
    let intent = read_intent(store, recording_id)?;
    let (bytes, sha256) = match (intent.bytes, intent.sha256.clone()) {
        (Some(bytes), Some(sha256)) => (bytes, sha256),
        _ => return Err(Permanent(UPLOAD_GONE.into()).into()),
    };
    let data_root = store
        .local_root()
        .context("imports need a store on the local filesystem")?
        .to_path_buf();
    let upload = data_root.join(intent.upload_key()?.as_str());
    if !upload.is_file() {
        return Err(Permanent(UPLOAD_GONE.into()).into());
    }
    // Both name the moment the upload began. Disagreeing would put the
    // objects under a prefix the index does not point at, and no retry
    // changes either.
    if intent.started_at() != started_at {
        tracing::warn!(
            %recording_id,
            upload = %intent.started_at(),
            recording = %started_at,
            "an import's upload and recording disagree on when it began"
        );
        return Err(Permanent(
            "the upload does not match this recording; delete this item and import it again"
                .into(),
        )
        .into());
    }
    let tools = runtime
        .toolchain()
        .map_err(|reason| Permanent(reason.to_string()))?;

    let prefix = RecordingPrefix::new(recording_id, started_at);
    let track_id = TrackId::new(IMPORTED_TRACK_ID).expect("a literal track id is valid");
    let notes = RecordingNotes {
        title: Some(intent.title.clone()),
        ..RecordingNotes::default()
    };
    let original_key = intent
        .keep_original
        .then(|| prefix.original(&intent.ext))
        .transpose()
        .context("building the original's key")?;

    let origin = clock_origin(store, &prefix, recording_id, started_at, &track_id, &notes)?;
    clear_previous_attempt(store, &prefix, original_key.as_ref())?;

    let probe = media::probe(tools, &upload)?;
    let demuxer = media::demuxer_for(&probe.format_name)
        .ok_or_else(|| Permanent(UNSUPPORTED.into()))?;
    let stream = media::select_stream(&probe)
        .ok_or_else(|| Permanent(NO_AUDIO.into()))?
        .clone();
    tracing::info!(
        %recording_id,
        format = %probe.format_name,
        stream = stream.index,
        codec = %stream.codec_name,
        "decoding an import"
    );

    // The stated duration is admitted against the limit; an absent one is
    // budgeted as the longest allowed. Either way the frame cap below is
    // what actually holds the line.
    let max_s = limits.max_duration.as_secs_f64();
    let stated = probe.duration_of(&stream);
    if stated.is_some_and(|d| d > max_s) {
        return Err(Permanent(too_long(limits)).into());
    }
    let budget_s = stated.unwrap_or(max_s);

    let needed = (budget_s.ceil() as u64)
        .saturating_mul(DECODE_BYTES_PER_SECOND)
        .saturating_add(DECODE_HEADROOM_BYTES)
        .saturating_add(if intent.keep_original { bytes } else { 0 });
    let free = (limits.free_space)(&data_root)?;
    if free < needed {
        bail!("not enough disk space: needs {}", gigabytes(needed));
    }

    persist::write_track_header(
        store,
        &prefix,
        &TrackHeader {
            track_id: track_id.clone(),
            media_type: MediaType::Audio,
            role: TrackRole::Unattributed,
            source: TrackSource::ImportedFile {
                stream_index: stream.index,
            },
            clock_domain: clock_domain(),
        },
    )?;
    persist::write_format_epoch(
        store,
        &prefix,
        &track_id,
        &FormatEpoch {
            from_seq: 0,
            format: persist::flac_format(DECODE_SAMPLE_RATE_HZ, 1),
        },
    )?;

    let mut writer = ChunkWriter::new(ChunkConfig {
        sample_rate_hz: DECODE_SAMPLE_RATE_HZ,
        channels: 1,
        chunk_duration_s: DEFAULT_CHUNK_DURATION_S,
    });
    let mut chunks = Vec::new();
    let mut decoded: u64 = 0;
    let timeout = MIN_DECODE_TIMEOUT.max(Duration::from_secs_f64(budget_s * 2.0));

    let frames = media::decode(
        tools,
        &upload,
        demuxer,
        stream.index,
        DecodeLimits {
            timeout,
            max_frames: limits.max_frames(),
        },
        |samples| {
            if samples.is_empty() {
                return Ok(());
            }
            decoded += samples.len() as u64;
            // A buffer "arrives" when its last sample does, which is how the
            // chunk writer reads arrival times: the first chunk then starts
            // exactly at the origin, and each later one where the previous
            // ended.
            let sealed = writer.push(CapturedBuffer {
                samples,
                arrived_at_ns: origin + samples_to_ns(decoded, DECODE_SAMPLE_RATE_HZ),
                source_pts_ns: None,
            })?;
            for chunk in sealed {
                chunks.push(persist::persist_chunk(store, &prefix, &track_id, &chunk)?);
                if chunks.len() % DISK_CHECK_EVERY_CHUNKS == 0
                    && (limits.free_space)(&data_root)? < DECODE_HEADROOM_BYTES
                {
                    bail!(
                        "not enough disk space: fewer than {} left while decoding",
                        gigabytes(DECODE_HEADROOM_BYTES)
                    );
                }
            }
            Ok(())
        },
    )?;
    if let Some(tail) = writer.flush()? {
        chunks.push(persist::persist_chunk(store, &prefix, &track_id, &tail)?);
    }
    if frames == 0 {
        return Err(Permanent(EMPTY_AUDIO.into()).into());
    }

    if let Some(key) = &original_key {
        store
            .copy_file(&upload, key, &sha256)
            .context("keeping the original file")?;
    }

    let duration_ns = samples_to_ns(frames, DECODE_SAMPLE_RATE_HZ);
    let manifest = persist::build_manifest(ManifestParts {
        recording_id,
        started_at,
        ended_at: Some(started_at + time::Duration::nanoseconds(duration_ns as i64)),
        clock_started_ns: origin,
        master_track_id: track_id.clone(),
        tracks: vec![Track {
            track_id,
            media_type: MediaType::Audio,
            role: TrackRole::Unattributed,
            source: TrackSource::ImportedFile {
                stream_index: stream.index,
            },
            clock_domain: clock_domain(),
            format: persist::flac_format(DECODE_SAMPLE_RATE_HZ, 1),
            chunks,
        }],
        notes,
        source: Some(ImportSource {
            original_filename: intent.original_filename.clone(),
            original_key,
            original_bytes: bytes,
            original_sha256: sha256,
            container: demuxer.to_string(),
            codec: stream.codec_name.clone(),
            content_type: media::content_type(&probe, demuxer),
            media_kind: probe.media_kind(),
            media_created_at: probe.created_at,
            imported_at: intent.created_at,
            duration_s: duration_ns as f64 / 1e9,
        }),
    });

    store
        .put(
            &prefix.manifest(),
            &serde_json::to_vec_pretty(&manifest).context("serialising the manifest")?,
        )
        .context("writing the manifest")?;
    crate::export::merge_recording(store, &manifest, &prefix).context("exporting the track")?;
    crate::export::mix_recording(store, &manifest, &prefix).context("exporting the mix")?;

    Ok(Decoded { prefix, manifest })
}

/// Step 7 onwards: makes the recording ready, or clears away what was written
/// for a recording that is no longer wanted.
pub(crate) fn finalise(
    store: &dyn BlobStore,
    db: &Arc<Mutex<Db>>,
    recording_id: Ulid,
    job_id: Ulid,
    decoded: &Decoded,
) -> Result<StageOutcome> {
    let keys = store.list_prefix(decoded.prefix.root().as_str())?;
    let fields = crate::library::index_fields(&decoded.manifest, &keys);

    let finalised = lock(db)?.finalize_import_success(recording_id, job_id, &fields)?;
    if finalised {
        if let Err(e) = super::remove_staging(store, recording_id) {
            tracing::warn!(
                %recording_id,
                error = %format!("{e:#}"),
                "imported, but the upload could not be removed from staging"
            );
        }
        tracing::info!(%recording_id, "imported");
        return Ok(StageOutcome::Finalized);
    }

    // Overtaken. Indexing now would bring back a deleted recording, or
    // publish objects some other attempt owns.
    match wanted(db, recording_id)? {
        Wanted::No { started_at } => {
            abandon(store, db, recording_id, started_at)?;
            Ok(StageOutcome::Skipped(DELETED.into()))
        }
        Wanted::Yes { .. } => {
            for key in store.list_prefix(decoded.prefix.root().as_str())? {
                if written_by_decode(&decoded.prefix, &key) {
                    if let Err(e) = store.delete(&key) {
                        tracing::warn!(%key, error = %format!("{e:#}"), "could not remove object");
                    }
                }
            }
            Ok(StageOutcome::Skipped(
                "this import was overtaken by another attempt".into(),
            ))
        }
    }
}

/// Whether the recording is still wanted, and when it started.
enum Wanted {
    Yes { started_at: time::OffsetDateTime },
    /// Deleted, or tombstoned while its objects are cleared. `started_at` is
    /// known while the tombstone stands.
    No { started_at: Option<time::OffsetDateTime> },
}

fn wanted(db: &Arc<Mutex<Db>>, recording_id: Ulid) -> Result<Wanted> {
    let row: Option<(i64, bool)> = lock(db)?
        .conn()
        .query_row(
            "SELECT started_at, deleted_at IS NOT NULL FROM recordings WHERE id = ?1",
            params![recording_id.to_string()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let started_at = |s: i64| time::OffsetDateTime::from_unix_timestamp(s).ok();
    Ok(match row {
        Some((s, false)) => Wanted::Yes {
            started_at: started_at(s).context("the recording has an invalid start time")?,
        },
        Some((s, true)) => Wanted::No {
            started_at: started_at(s),
        },
        None => Wanted::No { started_at: None },
    })
}

/// Clears away an import whose recording was deleted: its objects, its
/// staging, and then its tombstone, so the deletion completes.
///
/// The tombstone goes only once every object is gone. One that survived would
/// otherwise be found by the reconciler and indexed as a recording again;
/// with the tombstone standing, the startup purge finishes the job instead.
fn abandon(
    store: &dyn BlobStore,
    db: &Arc<Mutex<Db>>,
    recording_id: Ulid,
    started_at: Option<time::OffsetDateTime>,
) -> Result<()> {
    // Without a row the prefix is read from the intent, which names the
    // same moment.
    let started_at = started_at.or_else(|| {
        Intent::read(store, recording_id)
            .ok()
            .flatten()
            .map(|i| i.started_at())
    });
    let cleared = started_at
        .map(|s| purge(store, &RecordingPrefix::new(recording_id, s)))
        .unwrap_or(true);
    if let Err(e) = super::remove_staging(store, recording_id) {
        tracing::warn!(%recording_id, error = %format!("{e:#}"), "could not remove staging");
    }
    if cleared {
        lock(db)?.conn().execute(
            "DELETE FROM recordings WHERE id = ?1 AND deleted_at IS NOT NULL",
            params![recording_id.to_string()],
        )?;
    }
    Ok(())
}

/// Removes every object under `prefix`. Returns whether all of them went.
fn purge(store: &dyn BlobStore, prefix: &RecordingPrefix) -> bool {
    let Ok(keys) = store.list_prefix(prefix.root().as_str()) else {
        return false;
    };
    let mut cleared = true;
    for key in keys {
        if let Err(e) = store.delete(&key) {
            tracing::warn!(%key, error = %format!("{e:#}"), "could not remove object");
            cleared = false;
        }
    }
    cleared
}

/// The intent, which must describe a complete upload of this recording.
fn read_intent(store: &dyn BlobStore, recording_id: Ulid) -> Result<Intent> {
    let intent = match Intent::read(store, recording_id) {
        Ok(Some(intent)) => intent,
        Ok(None) => return Err(Permanent(UPLOAD_GONE.into()).into()),
        Err(e) => {
            tracing::warn!(%recording_id, error = %format!("{e:#}"), "unreadable import intent");
            return Err(Permanent(UPLOAD_GONE.into()).into());
        }
    };
    if intent.state != IntentState::Uploaded || intent.recording_id != recording_id {
        return Err(Permanent(UPLOAD_GONE.into()).into());
    }
    Ok(intent)
}

/// The clock origin every chunk is timed from: the stored header's, or a
/// fresh one written now.
///
/// Reused rather than taken again on a retry. A new origin would move every
/// chunk on the canonical clock, and anything already measured against the
/// first (nothing yet, but the transcript will be) would no longer line up.
fn clock_origin(
    store: &dyn BlobStore,
    prefix: &RecordingPrefix,
    recording_id: Ulid,
    started_at: time::OffsetDateTime,
    track_id: &TrackId,
    notes: &RecordingNotes,
) -> Result<u64> {
    let key = prefix.header();
    if store.exists(&key)? {
        let header: RecordingHeader = serde_json::from_slice(&store.get(&key)?)
            .context("reading the recording header")?;
        anyhow::ensure!(
            header.recording_id == recording_id,
            "the recording header names a different recording"
        );
        return Ok(header.canonical_clock.started_at_ns);
    }

    // Zero reads as "no origin" wherever the index is consulted.
    let origin = crate::clock::boottime_ns().max(1);
    persist::write_header(
        store,
        prefix,
        &RecordingHeader {
            manifest_version: MANIFEST_VERSION.into(),
            recording_id,
            started_at,
            canonical_clock: CanonicalClock {
                kind: ClockKind::BoottimeNs,
                started_at_ns: origin,
            },
            master_track_id: track_id.clone(),
            notes: notes.clone(),
        },
    )?;
    Ok(origin)
}

/// Removes what an interrupted attempt wrote, keeping the header (whose origin
/// is being reused) and a kept original (which a retry would copy again
/// identically).
fn clear_previous_attempt(
    store: &dyn BlobStore,
    prefix: &RecordingPrefix,
    original: Option<&BlobKey>,
) -> Result<()> {
    for key in store.list_prefix(prefix.root().as_str())? {
        if !written_by_decode(prefix, &key) || Some(&key) == original {
            continue;
        }
        store
            .delete(&key)
            .with_context(|| format!("removing {key}, left by an earlier attempt"))?;
    }
    Ok(())
}

/// Whether `key` is something a decode writes, other than the header: the
/// tracks, the exports, the manifest and the kept original.
///
/// Anything else under the prefix was put there by someone else, a rename
/// made while the file was still decoding for instance, and is not the
/// decode's to remove.
fn written_by_decode(prefix: &RecordingPrefix, key: &BlobKey) -> bool {
    let root = prefix.root();
    let Some(relative) = key
        .as_str()
        .strip_prefix(root.as_str())
        .and_then(|r| r.strip_prefix('/'))
    else {
        return false;
    };
    relative == "manifest.json"
        || ["tracks/", "exports/", "source/"]
            .iter()
            .any(|dir| relative.starts_with(dir))
}

fn clock_domain() -> ClockDomain {
    ClockDomain {
        source_clock: "file".into(),
        device_clock_id: None,
    }
}

fn too_long(limits: &Limits) -> String {
    let hours = limits.max_duration.as_secs_f64() / 3600.0;
    if hours >= 1.0 && hours.fract() == 0.0 {
        format!("the file is longer than the import limit of {hours} hours")
    } else {
        format!(
            "the file is longer than the import limit of {} seconds",
            limits.max_duration.as_secs()
        )
    }
}

fn gigabytes(bytes: u64) -> String {
    format!("{:.1} GB", bytes as f64 / 1e9)
}

fn lock(db: &Arc<Mutex<Db>>) -> Result<std::sync::MutexGuard<'_, Db>> {
    db.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))
}

/// The staged upload's path, for tests that need to touch it.
#[cfg(test)]
pub(crate) fn staged_upload(store: &dyn BlobStore, id: Ulid) -> std::path::PathBuf {
    let intent = Intent::read(store, id).unwrap().unwrap();
    store
        .local_root()
        .unwrap()
        .join(intent.upload_key().unwrap().as_str())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn only_what_a_decode_writes_is_cleared_on_a_retry() {
        let prefix = RecordingPrefix::new(Ulid::new(), time::OffsetDateTime::UNIX_EPOCH);
        let key = |name: &str| prefix.root().join(name).unwrap();
        for ours in [
            "manifest.json",
            "tracks/a_imported_01/000000.flac",
            "tracks/a_imported_01/track.json",
            "exports/mixed.flac",
            "source/original.mp4",
        ] {
            assert!(written_by_decode(&prefix, &key(ours)), "{ours}");
        }
        for theirs in ["recording.json", "library.json", "transcripts/v1.json"] {
            assert!(!written_by_decode(&prefix, &key(theirs)), "{theirs}");
        }
        let elsewhere = RecordingPrefix::new(Ulid::new(), time::OffsetDateTime::UNIX_EPOCH);
        assert!(!written_by_decode(&prefix, &elsewhere.manifest()));
    }

    #[test]
    fn the_limit_is_stated_the_way_it_was_set() {
        let limits = |secs| Limits {
            max_duration: Duration::from_secs(secs),
            free_space: super::super::free_space,
        };
        assert_eq!(too_long(&limits(6 * 3600)), "the file is longer than the import limit of 6 hours");
        assert_eq!(too_long(&limits(90)), "the file is longer than the import limit of 90 seconds");
        assert_eq!(limits(2).max_frames(), 96_000);
        assert_eq!(gigabytes(1_500_000_000), "1.5 GB");
    }
}
