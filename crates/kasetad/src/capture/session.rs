//! A recording session: several tracks captured together and sealed into one
//! manifest.
//!
//! The session owns one [`CaptureStream`] per track and a collector thread that
//! drains all of them. Streams are never synchronised live — each runs on its
//! own device clock — so the collector's only timing job is to stamp chunks with
//! the canonical clock and record what it observed. Alignment is computed later
//! from those stamps.
//!
//! Chunks are persisted the moment they are sealed rather than at the end, so a
//! session killed mid-recording leaves everything up to the last sealed chunk
//! recoverable.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use anyhow::{Context, Result};
use kaseta_contracts::manifest::{
    CanonicalClock, Chunk, ClockDomain, ClockKind, MediaType, RecordingHeader, RecordingManifest,
    RecordingNotes, Timeline, Track, TrackFormat, TrackHeader, TrackRole, TrackSource,
    MANIFEST_VERSION,
};
use kaseta_contracts::{BlobKey, RecordingPrefix, TrackId};
use ulid::Ulid;

use super::chunk::{CapturedBuffer, ChunkConfig, ChunkWriter, SealedChunk};
use super::devices::{capture_target, AudioDevice, DeviceKind};
use super::stream::{CaptureEvent, CaptureStream};
use crate::blobstore::BlobStore;
use crate::clock::boottime_ns;

/// One track to capture.
#[derive(Clone, Debug)]
pub struct TrackSpec {
    pub track_id: TrackId,
    pub role: TrackRole,
    pub device: AudioDevice,
}

impl TrackSpec {
    /// Builds the two tracks a meeting needs: the operator's microphone and
    /// whatever the machine plays back.
    pub fn meeting(mic: AudioDevice, playback: AudioDevice) -> Result<Vec<Self>> {
        Ok(vec![
            Self {
                track_id: TrackId::new("a_local-mic_01")?,
                role: TrackRole::LocalMic,
                device: mic,
            },
            Self {
                track_id: TrackId::new("a_remote-mix_01")?,
                role: TrackRole::RemoteMix,
                device: playback,
            },
        ])
    }
}

/// Progress reported while a session runs, for level meters and health display.
#[derive(Clone, Debug, Default)]
pub struct SessionStats {
    pub chunks_sealed: u64,
    pub bytes_written: u64,
    pub frames_captured: u64,
    pub discontinuities: u64,
    /// Buffers the server had ready that were never collected.
    pub drops: u64,
}

pub struct RecordingSession {
    recording_id: Ulid,
    prefix: RecordingPrefix,
    started_at: time::OffsetDateTime,
    clock_started_ns: u64,
    streams: Vec<CaptureStream>,
    collector: Option<JoinHandle<Result<CollectedTracks>>>,
    stop_flag: Arc<AtomicBool>,
    /// First collector error, published as soon as it happens.
    ///
    /// Once the collector exits, its channel closes and every capture callback
    /// silently discards buffers. Without this the caller would keep recording
    /// into nothing and only discover the failure at `stop`, by which point the
    /// rest of the meeting is gone.
    failure: Arc<Mutex<Option<String>>>,
    specs: Vec<TrackSpec>,
}

/// What the collector thread produces once every stream has ended.
type CollectedTracks = BTreeMap<usize, TrackAccumulator>;

/// The result of a finished session.
#[derive(Debug)]
pub struct SessionOutcome {
    pub manifest: RecordingManifest,
    /// Per-track capture health, keyed by track id. Reported separately from
    /// the manifest because it describes the capture run rather than the audio.
    pub tracks: Vec<TrackOutcome>,
}

#[derive(Debug)]
pub struct TrackOutcome {
    pub track_id: TrackId,
    pub drops: u64,
    /// Loudest sample as a fraction of full scale. A track that captured
    /// correctly but heard nothing is indistinguishable from a broken one
    /// without this.
    pub peak_level: f32,
    pub discontinuities: u64,
}

impl RecordingSession {
    /// Opens every track and begins capturing.
    pub fn start(
        specs: Vec<TrackSpec>,
        store: Arc<dyn BlobStore>,
        started_at: time::OffsetDateTime,
    ) -> Result<Self> {
        anyhow::ensure!(!specs.is_empty(), "a recording needs at least one track");

        let recording_id = Ulid::new();
        let prefix = RecordingPrefix::new(recording_id, started_at);
        let clock_started_ns = boottime_ns();

        // Written before capture begins: a recording that dies in its first
        // seconds must still be identifiable, and the clock origin is not
        // derivable from the chunks.
        let header = RecordingHeader {
            manifest_version: MANIFEST_VERSION.into(),
            recording_id,
            started_at,
            canonical_clock: CanonicalClock {
                kind: ClockKind::BoottimeNs,
                started_at_ns: clock_started_ns,
            },
        };
        store
            .put_idempotent(
                &prefix.header(),
                &serde_json::to_vec(&header).context("serialising recording header")?,
            )
            .context("writing recording header")?;

        let (tx, rx) = mpsc::channel::<(usize, CaptureEvent)>();
        let stop_flag = Arc::new(AtomicBool::new(false));

        let mut streams = Vec::with_capacity(specs.len());
        for (index, spec) in specs.iter().enumerate() {
            let target = capture_target(&spec.device);
            let stream = CaptureStream::start(target, tagged_sender(tx.clone(), index))
                .with_context(|| format!("starting capture for {}", spec.device.display_name))?;
            streams.push(stream);
        }
        // Dropping the original sender lets the collector see the channel close
        // once every stream thread has exited.
        drop(tx);

        let failure = Arc::new(Mutex::new(None));
        let collector = spawn_collector(
            rx,
            specs.clone(),
            prefix.clone(),
            store,
            Arc::clone(&failure),
        );

        Ok(Self {
            recording_id,
            prefix,
            started_at,
            clock_started_ns,
            streams,
            collector: Some(collector),
            stop_flag,
            failure,
            specs,
        })
    }

    #[allow(dead_code)]
    pub fn recording_id(&self) -> Ulid {
        self.recording_id
    }

    pub fn prefix(&self) -> &RecordingPrefix {
        &self.prefix
    }

    /// The first collector failure, if the session has stopped persisting.
    ///
    /// A session that reports a failure is no longer recording anything and
    /// should be stopped; continuing only discards audio.
    pub fn failure(&self) -> Option<String> {
        self.failure.lock().ok().and_then(|f| f.clone())
    }

    /// Stops every track, flushes trailing audio, and returns the manifest.
    pub fn stop(mut self) -> Result<SessionOutcome> {
        self.stop_flag.store(true, Ordering::SeqCst);

        // Stopping each stream joins its thread, so once this returns no further
        // buffers can arrive and the collector's channel will close.
        for stream in self.streams.drain(..) {
            stream.stop();
        }

        let collected = self
            .collector
            .take()
            .context("session already stopped")?
            .join()
            .map_err(|_| anyhow::anyhow!("collector thread panicked"))??;

        let ended_at = time::OffsetDateTime::now_utc();

        let tracks = self
            .specs
            .iter()
            .enumerate()
            .map(|(index, spec)| {
                let acc = collected.get(&index);
                TrackOutcome {
                    track_id: spec.track_id.clone(),
                    peak_level: acc.map(|a| a.peak_level).unwrap_or(0.0),
                    drops: acc.map(|a| a.stats.drops).unwrap_or(0),
                    discontinuities: acc.map(|a| a.stats.discontinuities).unwrap_or(0),
                }
            })
            .collect();

        let manifest = self.build_manifest(collected, ended_at)?;
        Ok(SessionOutcome { manifest, tracks })
    }

    fn build_manifest(
        &self,
        mut collected: CollectedTracks,
        ended_at: time::OffsetDateTime,
    ) -> Result<RecordingManifest> {
        let mut tracks = Vec::with_capacity(self.specs.len());

        for (index, spec) in self.specs.iter().enumerate() {
            let acc = collected.remove(&index).unwrap_or_default();
            tracks.push(Track {
                track_id: spec.track_id.clone(),
                media_type: MediaType::Audio,
                role: spec.role,
                source: track_source(&spec.device),
                clock_domain: ClockDomain {
                    source_clock: "pipewire".into(),
                    device_clock_id: Some(spec.device.node_name.clone()),
                },
                format: TrackFormat {
                    container: "flac".into(),
                    codec: "flac".into(),
                    sample_rate_hz: acc.sample_rate_hz,
                    channels: acc.channels,
                    sample_format: Some("s16".into()),
                },
                chunks: acc.chunks,
            });
        }

        // The master timeline is the microphone when present: it is the track
        // whose timing the operator experiences directly.
        let master = self
            .specs
            .iter()
            .find(|s| s.role == TrackRole::LocalMic)
            .or_else(|| self.specs.first())
            .map(|s| s.track_id.clone())
            .context("no tracks in session")?;

        let nominal_rate = tracks
            .iter()
            .find(|t| t.track_id == master)
            .and_then(|t| t.format.sample_rate_hz)
            .unwrap_or(48_000);

        Ok(RecordingManifest {
            manifest_version: MANIFEST_VERSION.into(),
            recording_id: self.recording_id,
            started_at: self.started_at,
            ended_at: Some(ended_at),
            canonical_clock: CanonicalClock {
                kind: ClockKind::BoottimeNs,
                started_at_ns: self.clock_started_ns,
            },
            timeline: Timeline {
                master_track_id: master,
                nominal_sample_rate_hz: nominal_rate,
            },
            tracks,
            notes: RecordingNotes::default(),
        })
    }
}

fn track_source(device: &AudioDevice) -> TrackSource {
    match device.kind {
        DeviceKind::Microphone => TrackSource::Microphone {
            node_name: device.node_name.clone(),
            display_name: device.display_name.clone(),
        },
        DeviceKind::SinkMonitor => TrackSource::SinkMonitor {
            node_name: device.node_name.clone(),
            display_name: device.display_name.clone(),
        },
    }
}

/// Wraps a shared sender so each stream tags its events with its track index.
fn tagged_sender(tx: Sender<(usize, CaptureEvent)>, index: usize) -> Sender<CaptureEvent> {
    let (inner_tx, inner_rx) = mpsc::channel::<CaptureEvent>();
    std::thread::Builder::new()
        .name(format!("kaseta-tag-{index}"))
        .spawn(move || {
            for event in inner_rx {
                if tx.send((index, event)).is_err() {
                    break;
                }
            }
        })
        .expect("spawning tag thread");
    inner_tx
}

/// Per-track state accumulated while recording.
#[derive(Default)]
struct TrackAccumulator {
    writer: Option<ChunkWriter>,
    chunks: Vec<Chunk>,
    sample_rate_hz: Option<u32>,
    channels: Option<u16>,
    stats: SessionStats,
    /// Loudest sample seen, carried out of the writer before it is dropped.
    peak_level: f32,
}

fn spawn_collector(
    rx: Receiver<(usize, CaptureEvent)>,
    specs: Vec<TrackSpec>,
    prefix: RecordingPrefix,
    store: Arc<dyn BlobStore>,
    failure: Arc<Mutex<Option<String>>>,
) -> JoinHandle<Result<CollectedTracks>> {
    std::thread::Builder::new()
        .name("kaseta-collector".into())
        .spawn(move || -> Result<CollectedTracks> {
            // Publishes the first error before unwinding. Once this thread
            // exits, the channel closes and every capture callback silently
            // discards its buffers, so the owner must be able to see the
            // failure while it is happening rather than at `stop`.
            let publish = |e: anyhow::Error| -> anyhow::Error {
                if let Ok(mut slot) = failure.lock() {
                    slot.get_or_insert_with(|| format!("{e:#}"));
                }
                tracing::error!(error = %format!("{e:#}"), "capture collector failed");
                e
            };

            let mut tracks: CollectedTracks = BTreeMap::new();

            for (index, event) in rx {
                let Some(spec) = specs.get(index) else { continue };
                let acc = tracks.entry(index).or_default();

                match event {
                    CaptureEvent::Negotiated {
                        sample_rate_hz,
                        channels,
                    } => {
                        tracing::info!(
                            track = %spec.track_id,
                            device = %spec.device.display_name,
                            sample_rate_hz,
                            channels,
                            "capture format negotiated"
                        );
                        acc.sample_rate_hz = Some(sample_rate_hz);
                        acc.channels = Some(channels);

                        let config = ChunkConfig {
                            sample_rate_hz,
                            channels,
                            ..ChunkConfig::default()
                        };

                        // The format is only known now, and nothing about the
                        // device or its clock domain is derivable from the
                        // chunks, so this is the point at which a track becomes
                        // reconstructable.
                        let track_header = TrackHeader {
                            track_id: spec.track_id.clone(),
                            media_type: MediaType::Audio,
                            role: spec.role,
                            source: track_source(&spec.device),
                            clock_domain: ClockDomain {
                                source_clock: "pipewire".into(),
                                device_clock_id: Some(spec.device.node_name.clone()),
                            },
                            format: TrackFormat {
                                container: "flac".into(),
                                codec: "flac".into(),
                                sample_rate_hz: Some(sample_rate_hz),
                                channels: Some(channels),
                                sample_format: Some("s16".into()),
                            },
                        };
                        let encoded = serde_json::to_vec(&track_header)
                            .context("serialising track header")
                            .map_err(&publish)?;
                        store
                            .put(&prefix.track_header(&spec.track_id), &encoded)
                            .context("writing track header")
                            .map_err(&publish)?;

                        match acc.writer.as_mut() {
                            // A renegotiation mid-recording, e.g. a Bluetooth
                            // headset switching profile. The writer is adapted
                            // rather than replaced so the sequence counter
                            // survives; a fresh one would restart at zero and
                            // collide with chunks already stored.
                            Some(writer) => {
                                let tail = writer.reconfigure(config).map_err(&publish)?;
                                if let Some(tail) = tail {
                                    persist_chunk(&*store, &prefix, &spec.track_id, tail, acc)
                                        .map_err(&publish)?;
                                }
                            }
                            None => acc.writer = Some(ChunkWriter::new(config)),
                        }
                    }
                    CaptureEvent::Buffer {
                        samples,
                        arrived_at_ns,
                        source_pts_ns,
                    } => {
                        let Some(writer) = acc.writer.as_mut() else {
                            // Audio before format negotiation cannot be sized or
                            // timestamped correctly, so it is dropped rather
                            // than guessed at.
                            continue;
                        };
                        acc.stats.frames_captured +=
                            (samples.len() / writer.config().channels.max(1) as usize) as u64;

                        let sealed = writer
                            .push(CapturedBuffer {
                                samples: &samples,
                                arrived_at_ns,
                                source_pts_ns,
                            })
                            .map_err(&publish)?;
                        for chunk in sealed {
                            persist_chunk(&*store, &prefix, &spec.track_id, chunk, acc)
                                .map_err(&publish)?;
                        }
                    }
                    CaptureEvent::Dropped => {
                        acc.stats.drops += 1;
                        if let Some(writer) = acc.writer.as_mut() {
                            writer.note_drop();
                        }
                    }
                    CaptureEvent::Ended { reason } => {
                        tracing::info!(track = %spec.track_id, %reason, "capture stream ended");
                        if let Some(writer) = acc.writer.as_mut() {
                            let tail = writer.flush().map_err(&publish)?;
                            acc.peak_level = writer.peak_level();
                            if let Some(chunk) = tail {
                                persist_chunk(&*store, &prefix, &spec.track_id, chunk, acc)
                                    .map_err(&publish)?;
                            }
                        }
                    }
                }
            }

            Ok(tracks)
        })
        .expect("spawning collector thread")
}

/// Writes a sealed chunk to storage and records it in the manifest.
///
/// Two objects are written per chunk: the audio, then a sidecar holding its
/// timing metadata. The manifest is only assembled when the session ends, so
/// without the sidecar a crash would leave audio on disk whose timestamps,
/// discontinuity flags and digests died with the in-memory accumulator —
/// unusable for alignment or transcription.
///
/// Audio is written first. A sidecar therefore implies its audio is present,
/// and recovery can treat any chunk lacking one as incomplete.
fn persist_chunk(
    store: &dyn BlobStore,
    prefix: &RecordingPrefix,
    track_id: &TrackId,
    chunk: SealedChunk,
    acc: &mut TrackAccumulator,
) -> Result<()> {
    let key: BlobKey = prefix.chunk(track_id, chunk.seq, "flac");
    store
        .put_idempotent(&key, &chunk.encoded)
        .with_context(|| format!("persisting chunk {} of {track_id}", chunk.seq))?;

    acc.stats.chunks_sealed += 1;
    acc.stats.bytes_written += chunk.encoded.len() as u64;
    if chunk.discontinuity {
        acc.stats.discontinuities += 1;
    }

    let record = Chunk {
        seq: chunk.seq,
        blob: key,
        sha256: chunk.sha256,
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

    acc.chunks.push(record);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capture::devices::DeviceKind;

    fn device(name: &str, kind: DeviceKind) -> AudioDevice {
        AudioDevice {
            node_name: name.into(),
            display_name: name.into(),
            kind,
            channels: Some(2),
            is_default: false,
        }
    }

    #[test]
    fn a_meeting_records_the_operator_and_the_far_end_separately() {
        let specs = TrackSpec::meeting(
            device("mic", DeviceKind::Microphone),
            device("speakers", DeviceKind::SinkMonitor),
        )
        .unwrap();

        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].role, TrackRole::LocalMic);
        assert_eq!(specs[1].role, TrackRole::RemoteMix);
        assert_ne!(
            specs[0].track_id, specs[1].track_id,
            "tracks must not share a key prefix or they would overwrite each other"
        );
    }

    #[test]
    fn track_sources_record_which_kind_of_device_they_came_from() {
        let mic = track_source(&device("alsa_input.usb", DeviceKind::Microphone));
        assert!(matches!(mic, TrackSource::Microphone { .. }));

        let monitor = track_source(&device("alsa_output.pci", DeviceKind::SinkMonitor));
        assert!(
            matches!(monitor, TrackSource::SinkMonitor { .. }),
            "playback must be recorded as a monitor so attribution stays correct"
        );
    }
}
