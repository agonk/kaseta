//! Running the transcription worker and placing its output on the timeline.
//!
//! The worker is a short-lived child process holding the model, so nothing is
//! resident between jobs. It is handed merged per-track audio and returns
//! offsets from the start of each file; this module maps those onto the
//! canonical clock and stores the result.
//!
//! # Attribution without a speaker model
//!
//! Each track is transcribed separately and carries the role it was captured
//! under. Everything from the microphone was spoken by the operator, everything
//! from the playback monitor by someone else. That gives every line a speaker
//! with no diarization at all — the payoff for having recorded the two sides
//! apart in the first place.

use std::io::Write;
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use kaseta_contracts::manifest::{RecordingManifest, TrackRole};
use kaseta_contracts::worker::{
    SpeakerHint, StorageRef, TrackAudio, TranscribeParams, TranscribeSpec, WorkerResult,
    WorkerStatus,
};
use kaseta_contracts::TrackId;
use rusqlite::params;
use ulid::Ulid;

use crate::blobstore::BlobStore;
use crate::db::Db;

/// The least time a worker is given before it is killed.
///
/// Transcription is far faster than real time on the intended model, so
/// exceeding the budget means the worker is stuck rather than slow. Without a
/// bound, a hung child would occupy the pipeline indefinitely.
const WORKER_TIMEOUT: Duration = Duration::from_secs(3 * 3600);

/// How long a worker may run on `manifest`: three hours, or one and a half
/// times the recording, whichever is longer.
///
/// A flat budget was enough while recordings were meetings. An imported
/// lecture can run for hours, and killing it at a fixed mark would fail it
/// identically on every retry, each time after hours of work.
fn worker_timeout(manifest: &RecordingManifest) -> Duration {
    let longest_s = manifest
        .tracks
        .iter()
        .filter_map(|t| {
            let rate = t.format.sample_rate_hz.filter(|r| *r > 0)?;
            Some(t.sample_count() as f64 / f64::from(rate))
        })
        .fold(0.0, f64::max);
    WORKER_TIMEOUT.max(Duration::from_secs_f64(longest_s * 1.5))
}

/// A segment placed on the canonical timeline.
#[derive(Debug, Clone)]
pub struct PlacedSegment {
    pub track_id: TrackId,
    pub seq: usize,
    pub start_boottime_ns: u64,
    pub end_boottime_ns: u64,
    pub speaker_hint: SpeakerHint,
    pub text: String,
}

/// Transcribes a recording, touching no database.
///
/// Split from storing so a caller can run this — which spawns a child process
/// and can take as long as the meeting itself — without holding a lock that
/// every request and every other job needs.
pub fn transcribe(
    store: &dyn BlobStore,
    storage_root: &str,
    manifest: &RecordingManifest,
) -> Result<Transcription> {
    let spec = build_spec(store, storage_root, manifest)?;
    if spec.tracks.is_empty() {
        return Ok(Transcription {
            segments: Vec::new(),
            engine: None,
            language: None,
        });
    }

    let result = run_worker(&spec, worker_timeout(manifest))?;
    let segments = place_on_timeline(manifest, &result)?;

    Ok(Transcription {
        segments,
        engine: result.engine,
        language: result.tracks.first().and_then(|t| t.language.clone()),
    })
}

/// What the worker is asked to do for `manifest`: one entry per track with
/// audio, each prepared as the mono WAV the recogniser reads and labelled
/// with who its role says is speaking.
///
/// Separate from running the worker, so what a recording is turned into can
/// be checked without a model installed. The parameters are the same for
/// every recording, captured or imported; nothing about where the audio came
/// from reaches the engine.
pub fn build_spec(
    store: &dyn BlobStore,
    storage_root: &str,
    manifest: &RecordingManifest,
) -> Result<TranscribeSpec> {
    let prefix = kaseta_contracts::RecordingPrefix::new(manifest.recording_id, manifest.started_at);

    let mut tracks = Vec::new();
    for track in &manifest.tracks {
        if track.chunks.is_empty() {
            continue;
        }
        let merged = prefix
            .export(&format!("{}.flac", track.track_id))
            .context("building export key")?;
        // Transcribing a track whose export never landed would silently drop a
        // side of the conversation.
        if !store.exists(&merged)? {
            bail!(
                "{} has no exported audio; run `kasetad export` first",
                track.track_id
            );
        }

        // The recogniser reads mono WAV only, while archives are stereo FLAC.
        // Converting here keeps the worker free of audio-decoding dependencies.
        let audio = crate::export::write_asr_audio(store, track, &prefix)
            .with_context(|| format!("preparing {} for transcription", track.track_id))?;
        tracks.push(TrackAudio {
            track_id: track.track_id.clone(),
            speaker_hint: hint_for(track.role),
            audio,
        });
    }

    Ok(TranscribeSpec::new(
        Ulid::new(),
        manifest.recording_id,
        1,
        StorageRef::LocalFs {
            root: storage_root.to_string(),
        },
        tracks,
        TranscribeParams::default(),
    ))
}

/// A finished transcription, before it is stored.
#[derive(Debug)]
pub struct Transcription {
    pub segments: Vec<PlacedSegment>,
    pub engine: Option<kaseta_contracts::worker::EngineInfo>,
    pub language: Option<String>,
}

/// Stores a transcript. Fast, and the only part needing the database.
pub fn store_transcript(
    store: &dyn crate::blobstore::BlobStore,
    prefix: &kaseta_contracts::RecordingPrefix,
    db: &Db,
    recording_id: Ulid,
    transcription: &Transcription,
) -> Result<usize> {
    write_transcript(db, recording_id, transcription)?;

    // The document is assembled from what was just indexed rather than from
    // the segments in hand, because rebasing onto the recording's own timeline
    // needs the clock origin, which lives in the index. Deriving it twice from
    // two sources is how the copy and the original start to disagree.
    if let Some(document) = crate::derived::transcript_document(db, recording_id)? {
        // A transcript that reached the index but not the store is complete
        // for someone using this machine and absent from their backup. Not
        // worth failing the job and re-running the model over: it is marked
        // instead, and the sweep at startup writes what is missing.
        if let Err(e) = crate::derived::publish_transcript(store, prefix, &document) {
            tracing::warn!(
                %recording_id,
                error = %format!("{e:#}"),
                "the transcript is indexed but not yet stored; it will be written again later"
            );
        }
        crate::derived::mark_dirty(db, recording_id)?;
    }

    Ok(transcription.segments.len())
}

/// What a track's role implies about who is speaking on it.
fn hint_for(role: TrackRole) -> SpeakerHint {
    match role {
        TrackRole::LocalMic => SpeakerHint::Local,
        TrackRole::RemoteMix => SpeakerHint::Remote,
        // An imported file is one track carrying every voice in it.
        TrackRole::Unattributed => SpeakerHint::Unknown,
        _ => SpeakerHint::Unknown,
    }
}

/// Locates the interpreter that has the worker installed.
pub fn worker_python() -> String {
    let candidate = std::env::var("KASETA_WORKER_PYTHON")
        .map(std::path::PathBuf::from)
        // The layout the worker's README sets up, for running from the
        // repository. An installed daemon is told where it is instead, since
        // its working directory is the home, not the checkout.
        .unwrap_or_else(|_| std::path::PathBuf::from("worker/.venv/bin/python"));

    if !candidate.is_file() {
        // Falls back to whatever `python3` resolves to, so the failure names
        // what was tried rather than being a bare file-not-found.
        return "python3".to_string();
    }

    // Made absolute without resolving symlinks. A virtual environment works by
    // its interpreter being *at* that path — Python derives the environment
    // from how it was invoked — so following the symlink to the system binary
    // silently discards the environment and every package in it.
    std::path::absolute(&candidate)
        .unwrap_or(candidate)
        .display()
        .to_string()
}

/// Whether the transcription worker can actually be run.
///
/// Checked by `doctor`: a worker that cannot start fails every transcription
/// job in the background, where the only sign is a recording that never gains
/// a transcript.
pub fn worker_status() -> Result<String> {
    let python = worker_python();
    let output = Command::new(&python)
        .args(["-c", "import kaseta_worker, onnx_asr; print(onnx_asr.__name__)"])
        .output()
        .with_context(|| format!("running {python}"))?;

    if output.status.success() {
        Ok(python)
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let reason: String = stderr.lines().last().unwrap_or("unknown error").into();
        bail!("{python}: {reason}")
    }
}

#[cfg(test)]
mod worker_path_tests {
    use super::*;

    #[test]
    fn the_interpreter_path_is_not_resolved_through_symlinks() {
        // A virtual environment's `python` is a symlink to the system binary.
        // Following it yields an interpreter that cannot see any of the
        // environment's packages, and the worker fails to import.
        let dir = tempfile::TempDir::new().unwrap();
        let real = dir.path().join("python3-real");
        std::fs::write(&real, "").unwrap();

        let venv = dir.path().join("venv-python");
        std::os::unix::fs::symlink(&real, &venv).unwrap();

        std::env::set_var("KASETA_WORKER_PYTHON", &venv);
        let resolved = worker_python();
        std::env::remove_var("KASETA_WORKER_PYTHON");

        assert_eq!(
            resolved,
            venv.display().to_string(),
            "the environment's own path must be preserved"
        );
        assert!(!resolved.ends_with("python3-real"));
    }

    #[test]
    fn a_relative_interpreter_path_is_made_absolute() {
        // The daemon's working directory is the home when run as a service, so
        // a relative path would resolve against the wrong place.
        std::env::set_var("KASETA_WORKER_PYTHON", "definitely/not/here/python");
        let resolved = worker_python();
        std::env::remove_var("KASETA_WORKER_PYTHON");
        // Absent, so it falls back rather than returning something unusable.
        assert_eq!(resolved, "python3");
    }
}

/// Spawns the worker, feeds it the spec, and reads back its result.
fn run_worker(spec: &TranscribeSpec, budget: Duration) -> Result<WorkerResult> {
    let python = worker_python();
    let spec_json = serde_json::to_vec(spec).context("serialising the job spec")?;

    // No working directory is set. It was `worker/`, relative — which resolved
    // against the repository when run by hand and against the home directory
    // when run as a service, where it does not exist, so the spawn failed
    // before Python started. The worker is installed into its interpreter's
    // environment, so `-m kaseta_worker` resolves from anywhere.
    let mut child = Command::new(&python)
        .args(["-m", "kaseta_worker"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| {
            format!("starting the transcription worker with {python} (is it installed?)")
        })?;

    child
        .stdin
        .take()
        .context("worker has no stdin")?
        .write_all(&spec_json)
        .context("sending the job spec")?;

    let output = wait_with_timeout(child, budget)?;

    let result: WorkerResult = serde_json::from_slice(&output).map_err(|e| {
        // A worker that printed something other than a result is a bug in the
        // worker; showing what it said makes that diagnosable.
        let preview: String = String::from_utf8_lossy(&output).chars().take(400).collect();
        anyhow!("worker returned unreadable output: {e}\n{preview}")
    })?;

    result.validate().context("worker result failed validation")?;

    if result.status == WorkerStatus::Error {
        let error = result
            .error
            .as_ref()
            .map(|e| format!("{}: {}", e.code, e.message))
            .unwrap_or_else(|| "worker reported an error".into());
        bail!("{error}");
    }
    for warning in &result.warnings {
        tracing::warn!(%warning, "transcription warning");
    }

    Ok(result)
}

/// Waits for the worker, killing it if it exceeds the budget.
fn wait_with_timeout(mut child: std::process::Child, budget: Duration) -> Result<Vec<u8>> {
    use std::sync::mpsc;

    let mut stdout = child.stdout.take().context("worker has no stdout")?;
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut buf = Vec::new();
        let read = std::io::Read::read_to_end(&mut stdout, &mut buf);
        let _ = tx.send(read.map(|_| buf));
    });

    match rx.recv_timeout(budget) {
        Ok(Ok(buf)) => {
            let _ = child.wait();
            Ok(buf)
        }
        Ok(Err(e)) => {
            let _ = child.kill();
            Err(e).context("reading worker output")
        }
        Err(_) => {
            // A stuck worker holds the pipeline; ending it turns an indefinite
            // hang into a retryable failure.
            let _ = child.kill();
            let _ = child.wait();
            bail!("the transcription worker exceeded its time budget and was stopped")
        }
    }
}

/// Maps worker offsets onto the canonical clock.
///
/// Each track's merged audio begins at its first chunk, and merging padded every
/// dropped-audio gap with silence of exactly the missing duration. The file is
/// therefore a linear timeline, and placement is addition — no per-chunk search,
/// and no accumulated error across a long recording.
pub fn place_on_timeline(
    manifest: &RecordingManifest,
    result: &WorkerResult,
) -> Result<Vec<PlacedSegment>> {
    let mut placed = Vec::new();

    for transcript in &result.tracks {
        let track = manifest
            .track(&transcript.track_id)
            .with_context(|| format!("worker returned unknown track {}", transcript.track_id))?;

        let base = track
            .chunks
            .first()
            .map(|c| c.boottime_start_ns)
            .context("track has no chunks to anchor its timeline")?;

        for (seq, segment) in transcript.segments.iter().enumerate() {
            if segment.text.is_empty() {
                continue;
            }
            placed.push(PlacedSegment {
                track_id: transcript.track_id.clone(),
                seq,
                start_boottime_ns: base + segment.start_ns,
                end_boottime_ns: base + segment.end_ns.max(segment.start_ns),
                speaker_hint: segment.speaker_hint.unwrap_or(SpeakerHint::Unknown),
                text: segment.text.clone(),
            });
        }
    }

    // Interleaving the two sides by time is what turns two parallel transcripts
    // into a readable conversation.
    placed.sort_by_key(|s| (s.start_boottime_ns, s.track_id.to_string()));
    Ok(placed)
}

fn write_transcript(
    db: &Db,
    recording_id: Ulid,
    transcription: &Transcription,
) -> Result<()> {
    let transcript_id = Ulid::new();
    let engine = transcription.engine.as_ref();
    let placed = &transcription.segments;

    let tx = db.conn().unchecked_transaction()?;

    // Reprocessing replaces the previous transcript rather than accumulating
    // duplicates; segments cascade.
    tx.execute(
        "DELETE FROM transcripts WHERE recording_id = ?1 AND revision = 1",
        params![recording_id.to_string()],
    )?;

    tx.execute(
        "INSERT INTO transcripts (id, recording_id, revision, engine_name, engine_model, language)
         VALUES (?1, ?2, 1, ?3, ?4, ?5)",
        params![
            transcript_id.to_string(),
            recording_id.to_string(),
            engine.map(|e| e.name.as_str()),
            engine.map(|e| e.model.as_str()),
            transcription.language.as_deref(),
        ],
    )?;

    for segment in placed {
        tx.execute(
            "INSERT INTO transcript_segments
                 (id, transcript_id, track_id, seq, start_boottime_ns, end_boottime_ns,
                  speaker_hint, text)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                Ulid::new().to_string(),
                transcript_id.to_string(),
                segment.track_id.as_str(),
                segment.seq as i64,
                segment.start_boottime_ns as i64,
                segment.end_boottime_ns as i64,
                speaker_label(segment.speaker_hint),
                segment.text,
            ],
        )?;
    }

    tx.commit()?;
    Ok(())
}

fn speaker_label(hint: SpeakerHint) -> &'static str {
    match hint {
        SpeakerHint::Local => "local",
        SpeakerHint::Remote => "remote",
        SpeakerHint::Unknown => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaseta_contracts::manifest::{
        CanonicalClock, Chunk, ClockDomain, ClockKind, MediaType, RecordingNotes, Timeline, Track,
        TrackFormat, TrackSource, MANIFEST_VERSION,
    };
    use kaseta_contracts::worker::{Segment, TrackTranscript, WORKER_RESULT_VERSION};

    fn track(id: &str, role: TrackRole, first_chunk_start_ns: u64) -> Track {
        let track_id = TrackId::new(id).unwrap();
        Track {
            track_id: track_id.clone(),
            media_type: MediaType::Audio,
            role,
            source: TrackSource::Microphone {
                node_name: "test".into(),
                display_name: "Test".into(),
            },
            clock_domain: ClockDomain {
                source_clock: "pipewire".into(),
                device_clock_id: None,
            },
            format: TrackFormat {
                container: "flac".into(),
                codec: "flac".into(),
                sample_rate_hz: Some(48_000),
                channels: Some(1),
                sample_format: Some("s16".into()),
            },
            chunks: vec![Chunk {
                seq: 0,
                blob: kaseta_contracts::BlobKey::new("a/b.flac").unwrap(),
                sha256: "0".repeat(64),
                bytes: 1,
                sample_count: Some(48_000),
                boottime_start_ns: first_chunk_start_ns,
                boottime_end_ns: first_chunk_start_ns + 1_000_000_000,
                source_pts_start_ns: None,
                source_pts_end_ns: None,
                discontinuity: false,
                gap_before_ns: 0,
                drops_before_chunk: 0,
            }],
        }
    }

    fn manifest(tracks: Vec<Track>) -> RecordingManifest {
        RecordingManifest {
            manifest_version: MANIFEST_VERSION.into(),
            recording_id: Ulid::nil(),
            started_at: time::OffsetDateTime::UNIX_EPOCH,
            ended_at: None,
            canonical_clock: CanonicalClock {
                kind: ClockKind::BoottimeNs,
                started_at_ns: 0,
            },
            timeline: Timeline {
                master_track_id: TrackId::new("a_local-mic_01").unwrap(),
                nominal_sample_rate_hz: 48_000,
            },
            tracks,
            notes: RecordingNotes::default(),
            source: None,
        }
    }

    fn transcript(id: &str, segments: Vec<(u64, u64, &str, SpeakerHint)>) -> TrackTranscript {
        TrackTranscript {
            track_id: TrackId::new(id).unwrap(),
            language: Some("en".into()),
            segments: segments
                .into_iter()
                .map(|(start, end, text, hint)| Segment {
                    start_ns: start,
                    end_ns: end,
                    text: text.into(),
                    speaker_hint: Some(hint),
                    words: vec![],
                })
                .collect(),
        }
    }

    fn result(tracks: Vec<TrackTranscript>) -> WorkerResult {
        WorkerResult {
            contract_version: WORKER_RESULT_VERSION.into(),
            job_id: Ulid::nil(),
            status: WorkerStatus::Ok,
            engine: None,
            warnings: vec![],
            error: None,
            tracks,
        }
    }

    #[test]
    fn offsets_are_anchored_to_the_tracks_own_start() {
        // The track's audio begins one second into the recording, so a segment
        // half a second into the file belongs at 1.5s on the canonical clock.
        let m = manifest(vec![track("a_local-mic_01", TrackRole::LocalMic, 1_000_000_000)]);
        let r = result(vec![transcript(
            "a_local-mic_01",
            vec![(500_000_000, 900_000_000, "hello", SpeakerHint::Local)],
        )]);

        let placed = place_on_timeline(&m, &r).unwrap();
        assert_eq!(placed.len(), 1);
        assert_eq!(placed[0].start_boottime_ns, 1_500_000_000);
        assert_eq!(placed[0].end_boottime_ns, 1_900_000_000);
    }

    #[test]
    fn both_sides_interleave_into_one_conversation() {
        // Tracks start at different times, which is what makes anchoring each to
        // its own base necessary before they can be ordered against each other.
        let m = manifest(vec![
            track("a_local-mic_01", TrackRole::LocalMic, 1_000_000_000),
            track("a_remote-mix_01", TrackRole::RemoteMix, 0),
        ]);
        let r = result(vec![
            transcript(
                "a_local-mic_01",
                vec![(0, 100_000_000, "my turn", SpeakerHint::Local)],
            ),
            transcript(
                "a_remote-mix_01",
                vec![
                    (0, 100_000_000, "their turn", SpeakerHint::Remote),
                    (2_000_000_000, 2_500_000_000, "their reply", SpeakerHint::Remote),
                ],
            ),
        ]);

        let placed = place_on_timeline(&m, &r).unwrap();
        let text: Vec<&str> = placed.iter().map(|s| s.text.as_str()).collect();
        assert_eq!(
            text,
            vec!["their turn", "my turn", "their reply"],
            "segments must read in the order they were spoken"
        );
    }

    #[test]
    fn every_line_carries_a_speaker_without_diarization() {
        let m = manifest(vec![
            track("a_local-mic_01", TrackRole::LocalMic, 0),
            track("a_remote-mix_01", TrackRole::RemoteMix, 0),
        ]);
        let r = result(vec![
            transcript("a_local-mic_01", vec![(0, 1, "me", SpeakerHint::Local)]),
            transcript("a_remote-mix_01", vec![(0, 1, "them", SpeakerHint::Remote)]),
        ]);

        let placed = place_on_timeline(&m, &r).unwrap();
        let me = placed.iter().find(|s| s.text == "me").unwrap();
        let them = placed.iter().find(|s| s.text == "them").unwrap();
        assert_eq!(me.speaker_hint, SpeakerHint::Local);
        assert_eq!(them.speaker_hint, SpeakerHint::Remote);
    }

    #[test]
    fn track_roles_determine_who_is_speaking() {
        assert_eq!(hint_for(TrackRole::LocalMic), SpeakerHint::Local);
        assert_eq!(hint_for(TrackRole::RemoteMix), SpeakerHint::Remote);
        assert_eq!(hint_for(TrackRole::Visual), SpeakerHint::Unknown);
    }

    #[test]
    fn an_imported_track_is_attributed_to_nobody() {
        assert_eq!(hint_for(TrackRole::Unattributed), SpeakerHint::Unknown);
    }

    fn with_samples(mut t: Track, frames: u64) -> Track {
        t.chunks[0].sample_count = Some(frames);
        t
    }

    /// Three hours is plenty for a meeting. A six-hour lecture gets half as
    /// long again as it lasts, or every attempt would be killed at the same
    /// point.
    #[test]
    fn the_worker_budget_grows_with_the_recording() {
        let hour = 48_000 * 3600;
        let short = manifest(vec![with_samples(track("a_local-mic_01", TrackRole::LocalMic, 0), hour)]);
        assert_eq!(worker_timeout(&short), WORKER_TIMEOUT);

        let long = manifest(vec![
            with_samples(track("a_local-mic_01", TrackRole::LocalMic, 0), hour),
            with_samples(track("a_imported_01", TrackRole::Unattributed, 0), 6 * hour),
        ]);
        assert_eq!(worker_timeout(&long), Duration::from_secs(9 * 3600));

        assert_eq!(worker_timeout(&manifest(vec![])), WORKER_TIMEOUT);
    }

    /// The spec is buildable, and checkable, without the worker: one entry
    /// per track with audio, each pointing at the prepared WAV.
    #[test]
    fn the_spec_names_each_track_and_who_speaks_on_it() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = crate::blobstore::LocalFsStore::new(dir.path()).unwrap();
        let mut m = manifest(vec![track("a_imported_01", TrackRole::Unattributed, 0)]);
        m.recording_id = Ulid::new();
        let prefix = kaseta_contracts::RecordingPrefix::new(m.recording_id, m.started_at);

        // Without the merged export there is nothing to prepare from.
        assert!(build_spec(&store, "/data", &m).is_err());

        // A prepared WAV already in place is used as it is.
        store.put(&prefix.export("a_imported_01.flac").unwrap(), b"flac").unwrap();
        store.put(&prefix.export("a_imported_01.asr.wav").unwrap(), b"wav").unwrap();
        let spec = build_spec(&store, "/data", &m).unwrap();

        assert_eq!(spec.recording_id, m.recording_id);
        assert_eq!(spec.tracks.len(), 1);
        assert_eq!(spec.tracks[0].speaker_hint, SpeakerHint::Unknown);
        assert_eq!(spec.tracks[0].audio, prefix.export("a_imported_01.asr.wav").unwrap());
        assert!(matches!(spec.storage, StorageRef::LocalFs { ref root } if root == "/data"));
    }

    #[test]
    fn empty_segments_are_dropped_rather_than_stored() {
        let m = manifest(vec![track("a_local-mic_01", TrackRole::LocalMic, 0)]);
        let r = result(vec![transcript(
            "a_local-mic_01",
            vec![(0, 1, "", SpeakerHint::Local), (2, 3, "real", SpeakerHint::Local)],
        )]);

        let placed = place_on_timeline(&m, &r).unwrap();
        assert_eq!(placed.len(), 1);
        assert_eq!(placed[0].text, "real");
    }

    #[test]
    fn a_track_the_manifest_does_not_know_is_rejected() {
        // Accepting it would place segments against an unknown timeline.
        let m = manifest(vec![track("a_local-mic_01", TrackRole::LocalMic, 0)]);
        let r = result(vec![transcript(
            "a_remote-mix_01",
            vec![(0, 1, "orphan", SpeakerHint::Remote)],
        )]);

        assert!(place_on_timeline(&m, &r).is_err());
    }

    #[test]
    fn a_reversed_segment_does_not_produce_a_negative_span() {
        let m = manifest(vec![track("a_local-mic_01", TrackRole::LocalMic, 0)]);
        let r = result(vec![transcript(
            "a_local-mic_01",
            vec![(500, 100, "backwards", SpeakerHint::Local)],
        )]);

        let placed = place_on_timeline(&m, &r).unwrap();
        assert!(placed[0].end_boottime_ns >= placed[0].start_boottime_ns);
    }
}
