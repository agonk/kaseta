//! Exporting a long recording costs no more memory than exporting a short one.
//!
//! Merging, mixing and preparing transcription audio used to decode a whole
//! track into one buffer, so an imported three-hour video would have needed
//! gigabytes. They now stream. This test holds that line by measuring it: a
//! counting allocator tracks the bytes live on the test's thread, and each
//! export runs over a 2-minute and a 6-minute recording. The peak must stay
//! under a fixed bound smaller than the audio itself, and must not grow when
//! the recording triples.
//!
//! The lengths are short because tests run unoptimised, where an hour of audio
//! takes minutes per export. They are long enough to be decisive: holding the
//! longer recording whole, in any of the forms the exports used to hold it,
//! costs well over the bound, while streaming costs the same at either length.
//!
//! An allocator is process-wide, which is why this is its own test binary
//! rather than a unit test among hundreds of others. The counters are also
//! per-thread, so nothing the test harness does on another thread can disturb
//! a measurement.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::io::Cursor;

use kaseta_contracts::manifest::{
    CanonicalClock, Chunk, ClockDomain, ClockKind, MediaType, RecordingManifest, RecordingNotes,
    Timeline, Track, TrackFormat, TrackRole, TrackSource,
};
use kaseta_contracts::{RecordingPrefix, TrackId, IMPORTED_TRACK_ID, MANIFEST_VERSION};
use kasetad::blobstore::{sha256_hex, BlobStore, LocalFsStore};
use kasetad::export::{merge_recording, mix_recording, write_asr_audio};
use kasetad::flac::FlacFileWriter;
use time::macros::datetime;
use ulid::Ulid;

struct Counting;

thread_local! {
    static LIVE: Cell<i64> = const { Cell::new(0) };
    static PEAK: Cell<i64> = const { Cell::new(0) };
}

fn note(delta: i64) {
    // `try_with`: the allocator also runs while a thread's locals are being
    // torn down, when they can no longer be touched.
    let _ = LIVE.try_with(|live| {
        let now = live.get() + delta;
        live.set(now);
        let _ = PEAK.try_with(|peak| {
            if now > peak.get() {
                peak.set(now);
            }
        });
    });
}

// SAFETY: every call is forwarded unchanged to the system allocator; the
// wrapper only counts sizes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            note(layout.size() as i64);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);
        if !ptr.is_null() {
            note(layout.size() as i64);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        note(-(layout.size() as i64));
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let moved = System.realloc(ptr, layout, new_size);
        if !moved.is_null() {
            note(new_size as i64 - layout.size() as i64);
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Runs `f` and returns the most memory it had live at once, beyond what was
/// live when it started.
fn peak_during<T>(f: impl FnOnce() -> T) -> (T, i64) {
    let baseline = LIVE.with(Cell::get);
    PEAK.with(|p| p.set(baseline));
    let result = f();
    let peak = PEAK.with(Cell::get) - baseline;
    (result, peak)
}

const RATE: u32 = 48_000;
const CHUNK_SECONDS: u64 = 15;
const MIB: i64 = 1024 * 1024;

/// The bound every export must stay under, whatever the recording's length.
///
/// Streaming holds one decoded chunk, one encoded frame and the write buffers,
/// about 5 MiB. Six minutes of the merge's encoded frames, the mix's stereo
/// samples or the 16 kHz transcription audio each need more than this.
const BOUND: i64 = 8 * MIB;

/// An imported recording of `minutes` of speech-like audio, one mono track,
/// with every chunk stored.
///
/// All chunks share one encoded blob. The reader verifies and decodes each
/// chunk independently, so this exercises exactly the work a real recording
/// does while sparing the test from encoding an hour of audio first.
fn imported_recording(store: &LocalFsStore, minutes: u64) -> (RecordingManifest, RecordingPrefix) {
    let started_at = datetime!(2026-10-09 12:00:00 UTC);
    let id = Ulid::from_parts(minutes, 1);
    let prefix = RecordingPrefix::new(id, started_at);
    let track_id = TrackId::new(IMPORTED_TRACK_ID).unwrap();

    let frames = (RATE as u64 * CHUNK_SECONDS) as usize;
    let samples: Vec<i16> = (0..frames)
        .map(|i| {
            let t = i as f64 / RATE as f64;
            ((t * 220.0 * std::f64::consts::TAU).sin() * (t * 0.7).sin() * 12_000.0) as i16
        })
        .collect();
    let mut flac = FlacFileWriter::new(Cursor::new(Vec::new()), RATE, 1).unwrap();
    flac.write_samples(&samples).unwrap();
    let encoded = flac.finish().unwrap().0.into_inner();
    let blob = prefix.chunk(&track_id, 0, "flac");
    store.put(&blob, &encoded).unwrap();
    let sha256 = sha256_hex(&encoded);

    let chunk_ns = CHUNK_SECONDS * 1_000_000_000;
    let count = minutes * 60 / CHUNK_SECONDS;
    let origin = 1_000_000_000u64;
    let chunks = (0..count)
        .map(|seq| Chunk {
            seq: seq as u32,
            blob: blob.clone(),
            sha256: sha256.clone(),
            bytes: encoded.len() as u64,
            sample_count: Some(frames as u64),
            boottime_start_ns: origin + seq * chunk_ns,
            boottime_end_ns: origin + (seq + 1) * chunk_ns,
            source_pts_start_ns: None,
            source_pts_end_ns: None,
            discontinuity: false,
            gap_before_ns: 0,
            drops_before_chunk: 0,
        })
        .collect();

    let manifest = RecordingManifest {
        manifest_version: MANIFEST_VERSION.into(),
        recording_id: id,
        started_at,
        ended_at: None,
        canonical_clock: CanonicalClock {
            kind: ClockKind::BoottimeNs,
            started_at_ns: origin,
        },
        timeline: Timeline {
            master_track_id: track_id.clone(),
            nominal_sample_rate_hz: RATE,
        },
        tracks: vec![Track {
            track_id,
            media_type: MediaType::Audio,
            role: TrackRole::Unattributed,
            source: TrackSource::ImportedFile { stream_index: 1 },
            clock_domain: ClockDomain {
                source_clock: "import".into(),
                device_clock_id: None,
            },
            format: TrackFormat {
                container: "flac".into(),
                codec: "flac".into(),
                sample_rate_hz: Some(RATE),
                channels: Some(1),
                sample_format: Some("s16".into()),
            },
            chunks,
        }],
        notes: RecordingNotes::default(),
        source: None,
    };
    (manifest, prefix)
}

struct Peaks {
    merge: i64,
    mix: i64,
    asr: i64,
}

fn export_and_measure(store: &LocalFsStore, minutes: u64) -> Peaks {
    let (manifest, prefix) = imported_recording(store, minutes);
    let frames = minutes * 60 * RATE as u64;

    let (merged, merge) = peak_during(|| merge_recording(store, &manifest, &prefix).unwrap());
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].frames, frames, "{minutes} min merge length");

    let (mixed, mix) = peak_during(|| mix_recording(store, &manifest, &prefix).unwrap().unwrap());
    assert_eq!(mixed.frames, frames, "{minutes} min mix length");
    assert_mix_decodes(store, &mixed.key, frames);

    let (asr, asr_peak) =
        peak_during(|| write_asr_audio(store, &manifest.tracks[0], &prefix).unwrap());
    let asr_bytes = store.size(&asr).unwrap();
    assert_eq!(
        asr_bytes,
        44 + frames / 3 * 2,
        "{minutes} min of 16 kHz mono 16-bit, plus the header"
    );

    Peaks {
        merge,
        mix,
        asr: asr_peak,
    }
}

/// Decodes the whole mix, counting frames, to prove the streamed file is
/// complete and readable rather than merely the right size.
fn assert_mix_decodes(store: &LocalFsStore, key: &kaseta_contracts::BlobKey, frames: u64) {
    let file = std::io::BufReader::new(store.open_file(key).unwrap());
    let mut reader = claxon::FlacReader::new(file).unwrap();
    assert_eq!(reader.streaminfo().channels, 2);
    assert_eq!(reader.streaminfo().samples, Some(frames));

    let mut blocks = reader.blocks();
    let mut buffer = Vec::new();
    let mut decoded = 0u64;
    while let Some(block) = blocks.read_next_or_eof(buffer).unwrap() {
        decoded += block.duration() as u64;
        buffer = block.into_buffer();
    }
    assert_eq!(decoded, frames, "every frame of the mix decodes");
}

#[test]
fn exports_hold_no_buffer_that_grows_with_the_recording() {
    let dir = tempfile::TempDir::new().unwrap();
    let store = LocalFsStore::new(dir.path()).unwrap();

    let short = export_and_measure(&store, 2);
    let long = export_and_measure(&store, 6);

    for (name, short, long) in [
        ("merge", short.merge, long.merge),
        ("mix", short.mix, long.mix),
        ("transcription audio", short.asr, long.asr),
    ] {
        eprintln!(
            "{name}: peak {:.1} MiB at 2 min, {:.1} MiB at 6 min",
            short as f64 / MIB as f64,
            long as f64 / MIB as f64
        );
        assert!(
            long < BOUND,
            "{name} peaked at {long} bytes for six minutes of audio; the bound is {BOUND}"
        );
        assert!(
            long * 4 < short * 5,
            "{name} grew from {short} to {long} bytes when the recording tripled: \
             something holds audio in proportion to its length"
        );
    }
}
