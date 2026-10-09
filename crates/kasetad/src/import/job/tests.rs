//! The decode stage against real files, run through the real sandbox.
//!
//! Each test returns early, saying why, on a machine that cannot import.

use std::time::Duration;

use kaseta_contracts::manifest::{MediaType, RecordingManifest, TrackRole, TrackSource};
use kaseta_contracts::worker::SpeakerHint;
use kaseta_contracts::{JobState, JobType, RecordingPrefix};

use super::*;
use crate::import::fixtures::{Fixture, Harness};

/// What a fixture is expected to become.
struct Expect {
    demuxer: &'static str,
    content_type: &'static str,
    media_kind: MediaType,
    stream_index: u32,
}

fn manifest_of(h: &Harness, id: Ulid) -> RecordingManifest {
    let started_at = started_at_of(h, id);
    let key = RecordingPrefix::new(id, started_at).manifest();
    serde_json::from_slice(&h.store.get(&key).unwrap()).unwrap()
}

fn started_at_of(h: &Harness, id: Ulid) -> time::OffsetDateTime {
    let seconds: i64 = h
        .db
        .lock()
        .unwrap()
        .conn()
        .query_row(
            "SELECT started_at FROM recordings WHERE id = ?1",
            rusqlite::params![id.to_string()],
            |r| r.get(0),
        )
        .unwrap();
    time::OffsetDateTime::from_unix_timestamp(seconds).unwrap()
}

fn job_states(h: &Harness, id: Ulid) -> Vec<(String, String)> {
    let db = h.db.lock().unwrap();
    let mut stmt = db
        .conn()
        .prepare("SELECT job_type, state FROM jobs WHERE recording_id = ?1 ORDER BY enqueue_seq")
        .unwrap();
    stmt.query_map(rusqlite::params![id.to_string()], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

/// The duration ffprobe gives the stream the decode will choose.
fn stated_duration(h: &Harness, file: &Path) -> f64 {
    let tools = h.runtime.toolchain().unwrap();
    let probe = media::probe(tools, file).unwrap();
    let stream = media::select_stream(&probe).unwrap();
    probe.duration_of(stream).unwrap()
}

fn run_job(h: &Harness, id: Ulid, job: &kaseta_contracts::Job) -> Result<StageOutcome> {
    run(&h.store, &h.db, &h.runtime, &h.limits(), id, job.id)
}

fn permanent_reason(result: Result<StageOutcome>) -> String {
    let err = result.expect_err("the import must fail");
    err.downcast_ref::<Permanent>()
        .unwrap_or_else(|| panic!("expected a permanent failure, got: {err:#}"))
        .0
        .clone()
}

/// Imports `fixture` and checks everything a finished import must be.
fn assert_imports(fixture: Fixture, expect: Expect) {
    let Some(h) = Harness::new() else { return };
    let Some(file) = h.fixture(fixture) else { return };
    let expected_s = stated_duration(&h, &file);
    let (id, job) = h.stage(&file, false);

    let outcome = run_job(&h, id, &job).unwrap();
    assert!(matches!(outcome, StageOutcome::Finalized), "{outcome:?}");

    let manifest = manifest_of(&h, id);
    assert_eq!(manifest.tracks.len(), 1);
    let track = &manifest.tracks[0];
    assert_eq!(track.track_id.as_str(), IMPORTED_TRACK_ID);
    assert_eq!(track.role, TrackRole::Unattributed);
    assert_eq!(
        track.source,
        TrackSource::ImportedFile {
            stream_index: expect.stream_index
        }
    );
    assert_eq!(track.format.sample_rate_hz, Some(48_000));
    assert_eq!(track.format.channels, Some(1));

    // The decoded length is the stream's, to within 50 ms.
    let frames = track.sample_count();
    let expected_frames = expected_s * 48_000.0;
    assert!(
        (frames as f64 - expected_frames).abs() <= 0.05 * 48_000.0,
        "{}: decoded {frames} frames, expected about {expected_frames}",
        fixture.file_name()
    );

    // The clock origin is the first chunk's start, and chunks follow one
    // another without a gap.
    assert_eq!(
        manifest.canonical_clock.started_at_ns,
        track.chunks[0].boottime_start_ns
    );
    for pair in track.chunks.windows(2) {
        assert_eq!(pair[0].boottime_end_ns, pair[1].boottime_start_ns);
    }

    let source = manifest.source.as_ref().expect("an import names its source");
    assert_eq!(source.container, expect.demuxer, "{}", fixture.file_name());
    assert_eq!(source.content_type, expect.content_type, "{}", fixture.file_name());
    assert_eq!(source.media_kind, expect.media_kind);
    assert_eq!(source.original_filename, fixture.file_name());
    assert_eq!(source.original_key, None);
    assert!((source.duration_s - frames as f64 / 48_000.0).abs() < 1e-6);
    assert_eq!(
        manifest.ended_at,
        Some(manifest.started_at + time::Duration::nanoseconds((source.duration_s * 1e9).round() as i64))
    );
    let stem = fixture.file_name().rsplit_once('.').unwrap().0;
    assert_eq!(manifest.notes.title.as_deref(), Some(stem));

    let objects = h.objects(id, manifest.started_at);
    for expected in ["exports/a_imported_01.flac", "exports/mixed.flac", "recording.json", "manifest.json"] {
        assert!(objects.iter().any(|o| o == expected), "{expected} missing from {objects:?}");
    }

    let (status, origin, source_json, original_local) = h.row(id).unwrap();
    assert_eq!((status.as_str(), origin.as_str()), ("ready", "imported"));
    assert!(source_json.unwrap().contains(expect.content_type));
    assert_eq!(original_local, 0);

    // Succeeded by finalisation, with transcription and backup queued once.
    assert_eq!(
        job_states(&h, id),
        [
            ("import_media".to_string(), "succeeded".to_string()),
            ("transcribe".into(), "queued".into()),
            ("upload_remote".into(), "queued".into()),
        ]
    );
    assert!(!h.staging_exists(id), "staging goes once the import is final");

    // Transcription is handed one track, attributed to nobody.
    let spec = crate::transcribe::build_spec(&h.store, &h.store.root().display().to_string(), &manifest)
        .unwrap();
    assert_eq!(spec.tracks.len(), 1);
    assert_eq!(spec.tracks[0].track_id.as_str(), IMPORTED_TRACK_ID);
    assert_eq!(spec.tracks[0].speaker_hint, SpeakerHint::Unknown);
    assert!(h.store.exists(&spec.tracks[0].audio).unwrap());
}

/// The commonest thing anyone will import: picture first, sound second.
#[test]
fn a_phone_video_imports_its_sound() {
    assert_imports(
        Fixture::VideoFirstMp4,
        Expect {
            demuxer: "mov",
            content_type: "video/mp4",
            media_kind: MediaType::Video,
            stream_index: 1,
        },
    );
}

/// The default stream is the second one, and it is the longer one, so the
/// decoded length shows which was taken.
#[test]
fn the_default_of_several_audio_streams_is_imported() {
    assert_imports(
        Fixture::MultiAudioMkv,
        Expect {
            demuxer: "matroska",
            content_type: "audio/webm",
            media_kind: MediaType::Audio,
            stream_index: 1,
        },
    );
}

#[test]
fn a_webm_clip_imports() {
    assert_imports(
        Fixture::ClipWebm,
        Expect {
            demuxer: "matroska",
            content_type: "video/webm",
            media_kind: MediaType::Video,
            stream_index: 1,
        },
    );
}

#[test]
fn an_mp3_imports() {
    assert_imports(
        Fixture::Mp3,
        Expect {
            demuxer: "mp3",
            content_type: "audio/mpeg",
            media_kind: MediaType::Audio,
            stream_index: 0,
        },
    );
}

#[test]
fn surround_sound_is_folded_to_one_channel() {
    assert_imports(
        Fixture::SurroundWav,
        Expect {
            demuxer: "wav",
            content_type: "audio/wav",
            media_kind: MediaType::Audio,
            stream_index: 0,
        },
    );
}

#[test]
fn a_stereo_flac_imports() {
    assert_imports(
        Fixture::StereoFlac,
        Expect {
            demuxer: "flac",
            content_type: "audio/flac",
            media_kind: MediaType::Audio,
            stream_index: 0,
        },
    );
}

#[test]
fn an_ogg_opus_file_imports() {
    assert_imports(
        Fixture::OpusOgg,
        Expect {
            demuxer: "ogg",
            content_type: "audio/ogg",
            media_kind: MediaType::Audio,
            stream_index: 0,
        },
    );
}

/// What the file is decides how it is read, not what it is called.
#[test]
fn an_mp3_named_like_a_video_imports_as_an_mp3() {
    assert_imports(
        Fixture::Mp3NamedMp4,
        Expect {
            demuxer: "mp3",
            content_type: "audio/mpeg",
            media_kind: MediaType::Audio,
            stream_index: 0,
        },
    );
}

#[test]
fn a_video_without_sound_fails_for_good() {
    let Some(h) = Harness::new() else { return };
    let Some(file) = h.fixture(Fixture::NoAudioMp4) else { return };
    let (id, job) = h.stage(&file, false);

    assert_eq!(permanent_reason(run_job(&h, id, &job)), NO_AUDIO);
    assert!(h.staging_exists(id), "a failed import keeps its upload for a retry");
}

#[test]
fn a_text_file_named_like_a_video_is_unsupported() {
    let Some(h) = Harness::new() else { return };
    let Some(file) = h.fixture(Fixture::TextNamedMp4) else { return };
    let (id, job) = h.stage(&file, false);

    assert_eq!(permanent_reason(run_job(&h, id, &job)), UNSUPPORTED);
}

#[test]
fn an_audio_stream_with_nothing_in_it_fails_for_good() {
    let Some(h) = Harness::new() else { return };
    let Some(file) = h.fixture(Fixture::EmptyWav) else { return };
    let (id, job) = h.stage(&file, false);

    assert_eq!(permanent_reason(run_job(&h, id, &job)), EMPTY_AUDIO);
}

#[test]
fn a_kept_original_is_copied_beside_the_recording() {
    let Some(h) = Harness::new() else { return };
    let Some(file) = h.fixture(Fixture::VideoFirstMp4) else { return };
    let (id, job) = h.stage(&file, true);
    let intent = Intent::read(&h.store, id).unwrap().unwrap();

    assert!(matches!(run_job(&h, id, &job).unwrap(), StageOutcome::Finalized));

    let manifest = manifest_of(&h, id);
    let source = manifest.source.unwrap();
    let key = source.original_key.expect("kept");
    assert!(key.as_str().ends_with("/source/original.mp4"), "{key}");
    assert_eq!(h.store.get(&key).unwrap(), std::fs::read(&file).unwrap());
    assert_eq!(Some(source.original_sha256), intent.sha256);
    assert_eq!(Some(source.original_bytes), intent.bytes);
    assert_eq!(h.row(id).unwrap().3, 1, "the original is local");
    assert!(!h.staging_exists(id));
}

/// The daemon killed mid-import: the orphaned job is requeued, finds the
/// header and the upload where it left them, reuses the clock origin and
/// writes chunks timed exactly as before, clearing what the first attempt
/// left behind.
#[test]
fn a_retry_after_a_crash_reproduces_the_same_recording() {
    let Some(h) = Harness::new() else { return };
    let Some(file) = h.fixture(Fixture::LongWav) else { return };
    let (id, job) = h.stage(&file, false);

    let started_at = started_at_of(&h, id);
    let decoded = decode_into_recording(&h.store, &h.runtime, &h.limits(), id, started_at).unwrap();
    let first = decoded.manifest.clone();
    assert!(first.tracks[0].chunks.len() >= 4, "a minute is four chunks or more");

    // The crash: the stored manifest is gone, one chunk is torn away, and an
    // export from an attempt that went further is still there.
    let prefix = RecordingPrefix::new(id, started_at);
    h.store.delete(&prefix.manifest()).unwrap();
    h.store.delete(&first.tracks[0].chunks[2].blob).unwrap();
    let stray = prefix.export("stray.flac").unwrap();
    h.store.put(&stray, b"left by a later step").unwrap();

    // The restart: orphaned jobs go back to the queue.
    {
        let db = h.db.lock().unwrap();
        assert_eq!(db.recover_orphaned_jobs().unwrap(), vec![job.id]);
    }
    let again = h
        .db
        .lock()
        .unwrap()
        .claim_next_job(std::process::id())
        .unwrap()
        .unwrap();
    assert_eq!(again.id, job.id);
    assert!(h.staging_exists(id), "the upload is still there to decode");

    assert!(matches!(run_job(&h, id, &again).unwrap(), StageOutcome::Finalized));

    let second = manifest_of(&h, id);
    assert_eq!(second.canonical_clock.started_at_ns, first.canonical_clock.started_at_ns);
    let timings = |m: &RecordingManifest| -> Vec<(u64, u64, String)> {
        m.tracks[0]
            .chunks
            .iter()
            .map(|c| (c.boottime_start_ns, c.boottime_end_ns, c.sha256.clone()))
            .collect()
    };
    assert_eq!(timings(&second), timings(&first));
    assert!(!h.store.exists(&stray).unwrap(), "nothing from the earlier attempt remains");
    assert_eq!(h.row(id).unwrap().0, "ready");
}

/// Deleted before the decode began: the stage clears everything and the
/// deletion completes.
#[test]
fn an_import_deleted_while_queued_leaves_nothing() {
    let Some(h) = Harness::new() else { return };
    let Some(file) = h.fixture(Fixture::Mp3) else { return };
    let (id, job) = h.stage(&file, true);

    {
        let db = h.db.lock().unwrap();
        assert!(crate::library::delete(&h.store, &db, id).unwrap());
    }
    let outcome = run_job(&h, id, &job).unwrap();
    assert!(matches!(outcome, StageOutcome::Skipped(ref r) if r == DELETED), "{outcome:?}");

    assert_eq!(h.row(id), None);
    assert!(!h.staging_exists(id));
    let db = h.db.lock().unwrap();
    crate::library::reconcile(&h.store, &db).unwrap();
    drop(db);
    assert_eq!(h.row(id), None, "nothing to resurrect");
}

/// Deleted while the file was being decoded: finalisation finds the
/// recording gone, indexes nothing, and removes what the decode wrote.
#[test]
fn an_import_deleted_mid_decode_is_never_resurrected() {
    let Some(h) = Harness::new() else { return };
    let Some(file) = h.fixture(Fixture::Mp3) else { return };
    let (id, job) = h.stage(&file, true);
    let started_at = started_at_of(&h, id);

    let decoded = decode_into_recording(&h.store, &h.runtime, &h.limits(), id, started_at).unwrap();
    {
        let db = h.db.lock().unwrap();
        assert!(crate::library::delete(&h.store, &db, id).unwrap());
        // The decode is still running, so the deletion leaves its tombstone
        // for the decode to finish.
        let (deleted, pending): (bool, i64) = db
            .conn()
            .query_row(
                "SELECT deleted_at IS NOT NULL, purge_pending FROM recordings WHERE id = ?1",
                rusqlite::params![id.to_string()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert!(deleted && pending == 1);
    }

    let outcome = finalise(&h.store, &h.db, id, job.id, &decoded).unwrap();
    assert!(matches!(outcome, StageOutcome::Skipped(ref r) if r == DELETED), "{outcome:?}");

    assert_eq!(h.row(id), None, "the deletion completed");
    assert!(h.objects(id, started_at).is_empty(), "{:?}", h.objects(id, started_at));
    assert!(!h.staging_exists(id));

    let db = h.db.lock().unwrap();
    crate::library::reconcile(&h.store, &db).unwrap();
    drop(db);
    assert_eq!(h.row(id), None, "no manifest survived to be indexed again");
}

/// A finalisation for a job that is no longer the running one indexes
/// nothing, and leaves no objects of its own behind.
#[test]
fn an_overtaken_import_publishes_nothing() {
    let Some(h) = Harness::new() else { return };
    let Some(file) = h.fixture(Fixture::Mp3) else { return };
    let (id, _job) = h.stage(&file, false);
    let started_at = started_at_of(&h, id);

    let decoded = decode_into_recording(&h.store, &h.runtime, &h.limits(), id, started_at).unwrap();
    let outcome = finalise(&h.store, &h.db, id, Ulid::new(), &decoded).unwrap();

    assert!(matches!(outcome, StageOutcome::Skipped(_)), "{outcome:?}");
    assert_eq!(h.row(id).unwrap().0, "processing");
    // The header stays: it fixes the clock origin any later attempt reuses.
    assert_eq!(h.objects(id, started_at), ["recording.json"]);
    assert!(h.staging_exists(id), "the running import still needs its upload");
}

#[test]
fn a_file_longer_than_the_limit_fails_for_good() {
    let Some(h) = Harness::new() else { return };
    let Some(file) = h.fixture(Fixture::Mp3) else { return };
    let (id, job) = h.stage(&file, false);
    let limits = Limits {
        max_duration: Duration::from_secs(1),
        ..h.limits()
    };

    let reason = permanent_reason(run(&h.store, &h.db, &h.runtime, &limits, id, job.id));
    assert!(reason.contains("longer than the import limit"), "{reason}");
}

/// A file that states no duration is budgeted as the longest import
/// allowed, since nothing else is known about it.
#[test]
fn a_file_of_unknown_length_is_budgeted_at_the_limit() {
    let Some(h) = Harness::new() else { return };
    let Some(file) = h.fixture(Fixture::EmptyWav) else { return };
    let (id, job) = h.stage(&file, false);
    let limits = Limits {
        max_duration: Duration::from_secs(3600),
        free_space: |_| Ok(1 << 30),
    };

    let err = run(&h.store, &h.db, &h.runtime, &limits, id, job.id).unwrap_err();
    // An hour at 512 KiB a second, plus the headroom.
    assert!(format!("{err:#}").contains("needs 2.2 GB"), "{err:#}");
}

/// Running out partway is caught too: free space is looked at again every
/// ten minutes of audio, and the decode stops there.
#[test]
fn the_disk_is_watched_while_decoding() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    // One test owns the counter.
    static CALLS: AtomicUsize = AtomicUsize::new(0);

    let Some(h) = Harness::new() else { return };
    let Some(file) = h.fixture(Fixture::ElevenMinutesMp3) else { return };
    // Plenty at admission, nothing at the first look during the decode.
    let limits = Limits {
        free_space: |_| Ok(if CALLS.fetch_add(1, Ordering::SeqCst) == 0 { 1 << 40 } else { 0 }),
        ..h.limits()
    };
    let (id, job) = h.stage(&file, false);

    let err = run(&h.store, &h.db, &h.runtime, &limits, id, job.id).unwrap_err();
    assert!(err.downcast_ref::<Permanent>().is_none(), "worth retrying: {err:#}");
    assert!(format!("{err:#}").contains("not enough disk space"), "{err:#}");
    assert_eq!(CALLS.load(Ordering::SeqCst), 2);

    let chunks = h
        .objects(id, started_at_of(&h, id))
        .into_iter()
        .filter(|o| o.ends_with(".flac") && o.starts_with("tracks/"))
        .count();
    assert_eq!(chunks, DISK_CHECK_EVERY_CHUNKS, "stopped at the check");
}

/// A disk too full to hold the decode is a reason to wait, not to give up.
#[test]
fn not_enough_disk_space_is_worth_retrying() {
    let Some(h) = Harness::new() else { return };
    let Some(file) = h.fixture(Fixture::Mp3) else { return };
    let (id, job) = h.stage(&file, true);
    let limits = Limits {
        free_space: |_| Ok(10 * 1024 * 1024),
        ..h.limits()
    };

    let err = run(&h.store, &h.db, &h.runtime, &limits, id, job.id).unwrap_err();
    assert!(err.downcast_ref::<Permanent>().is_none());
    assert!(format!("{err:#}").contains("not enough disk space: needs"), "{err:#}");
}

/// The duration a file states is only its claim. The frames decoded are
/// counted, and the decode is stopped the moment they pass the limit.
#[test]
fn decoding_stops_at_the_frame_limit() {
    let Some(h) = Harness::new() else { return };
    let Some(file) = h.fixture(Fixture::Mp3) else { return };
    let tools = h.runtime.toolchain().unwrap();

    let mut seen = 0u64;
    let err = media::decode(
        tools,
        &file,
        "mp3",
        0,
        media::DecodeLimits {
            timeout: Duration::from_secs(60),
            max_frames: 48_000,
        },
        |samples| {
            seen += samples.len() as u64;
            Ok(())
        },
    )
    .unwrap_err();

    assert!(err.downcast_ref::<Permanent>().unwrap().0.contains("longer than the import limit"));
    assert!(seen <= 48_000, "nothing past the limit reaches the recording");
}

/// A decode that runs out of time is killed with everything it started:
/// nothing of it survives inside or outside the sandbox.
#[test]
fn a_timed_out_decode_leaves_no_process_behind() {
    let Some(h) = Harness::new() else { return };
    let Some(file) = h.fixture(Fixture::LongWav) else { return };
    let tools = h.runtime.toolchain().unwrap();
    // A directory of its own, so the processes it started can be told apart
    // from any other test's.
    let dir = h.fixtures.path().join(format!("timeout-{}", Ulid::new()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = {
        let moved = dir.join("long.wav");
        std::fs::copy(&file, &moved).unwrap();
        moved
    };

    let started = std::time::Instant::now();
    let err = media::decode(
        tools,
        &file,
        "wav",
        0,
        media::DecodeLimits {
            timeout: Duration::from_millis(500),
            max_frames: u64::MAX,
        },
        |_| {
            // A consumer slower than the decoder, so it is still running when
            // the budget ends.
            std::thread::sleep(Duration::from_millis(200));
            Ok(())
        },
    )
    .unwrap_err();
    assert!(format!("{err:#}").contains("longer than"), "{err:#}");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "stopped at the budget, not when the file ran out: {:?}",
        started.elapsed()
    );

    let marker = dir.to_string_lossy().into_owned();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let survivors = processes_mentioning(&marker);
        if survivors.is_empty() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "still running after the timeout: {survivors:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Processes whose command line contains `needle`.
fn processes_mentioning(needle: &str) -> Vec<String> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir("/proc").unwrap().flatten() {
        if !entry.file_name().to_string_lossy().chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        let Ok(cmdline) = std::fs::read(entry.path().join("cmdline")) else { continue };
        let cmdline = String::from_utf8_lossy(&cmdline).replace('\0', " ");
        if cmdline.contains(needle) {
            found.push(cmdline);
        }
    }
    found
}

/// Without the sandbox nothing is decoded, and the reason names the fix.
#[test]
fn without_a_sandbox_the_import_fails_with_the_reason() {
    let Some(h) = Harness::new() else { return };
    let Some(file) = h.fixture(Fixture::Mp3) else { return };
    let (id, job) = h.stage(&file, false);
    let runtime = ImportRuntime::unavailable("Importing needs bubblewrap: sudo pacman -S --needed bubblewrap");

    let reason = permanent_reason(run(&h.store, &h.db, &runtime, &h.limits(), id, job.id));
    assert_eq!(reason, "Importing needs bubblewrap: sudo pacman -S --needed bubblewrap");
}

#[test]
fn a_missing_upload_cannot_be_imported() {
    let Some(h) = Harness::new() else { return };
    let Some(file) = h.fixture(Fixture::Mp3) else { return };
    let (id, job) = h.stage(&file, false);
    std::fs::remove_file(staged_upload(&h.store, id)).unwrap();

    assert_eq!(permanent_reason(run_job(&h, id, &job)), UPLOAD_GONE);
    assert!(!crate::import::upload_present(&h.store, id).unwrap());
}

/// An upload whose intent never reached "uploaded" may be partial.
#[test]
fn an_upload_that_never_finished_cannot_be_imported() {
    let Some(h) = Harness::new() else { return };
    let Some(file) = h.fixture(Fixture::Mp3) else { return };
    let (id, job) = h.stage(&file, false);
    let mut intent = Intent::read(&h.store, id).unwrap().unwrap();
    intent.state = IntentState::Uploading;
    h.store
        .put(&kaseta_contracts::ImportStaging::new(id).intent(), &serde_json::to_vec(&intent).unwrap())
        .unwrap();

    assert_eq!(permanent_reason(run_job(&h, id, &job)), UPLOAD_GONE);
}

#[test]
fn the_job_state_is_left_to_the_scheduler_on_failure() {
    let Some(h) = Harness::new() else { return };
    let Some(file) = h.fixture(Fixture::TextNamedMp4) else { return };
    let (id, job) = h.stage(&file, false);

    let _ = run_job(&h, id, &job);
    let state = h.db.lock().unwrap().job(job.id).unwrap().unwrap().state;
    assert_eq!(state, JobState::Running, "the stage reports; the scheduler records");
    assert_eq!(job.job_type, JobType::ImportMedia);
}
