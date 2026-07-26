//! Merges a recording's chunks into one continuous file per track.
//!
//! Chunks are the durable write format: sealing every few seconds bounds crash
//! loss and keeps memory flat regardless of recording length. They are not a
//! convenient listening format, so a merge produces one file per track.
//!
//! # Why this is not concatenation
//!
//! FLAC frames cannot simply be joined — each stream carries its own header and
//! block structure. More importantly, chunks are not always contiguous in time:
//! where the audio server dropped buffers, a chunk records how much audio went
//! missing. Joining chunks end-to-end would silently close those holes and pull
//! everything after them earlier, desynchronising the track from its own
//! timestamps and from the other track. Gaps are therefore padded with silence
//! of exactly the missing duration.
//!
//! Decoding and re-encoding is lossless in both directions, so a merged file is
//! sample-identical to what was captured.

use anyhow::{bail, Context, Result};
use kaseta_contracts::manifest::{RecordingManifest, Track};
use kaseta_contracts::BlobKey;

use crate::blobstore::BlobStore;
use crate::clock::samples_to_ns;

/// What a merge produced for one track.
#[derive(Debug)]
pub struct MergedTrack {
    pub key: BlobKey,
    pub bytes: u64,
    pub frames: u64,
    /// Silence inserted to preserve timing across dropped audio.
    pub padded_frames: u64,
}

/// Merges every track in `manifest`, writing one file per track under the
/// recording's `exports/` prefix.
pub fn merge_recording(
    store: &dyn BlobStore,
    manifest: &RecordingManifest,
    prefix: &kaseta_contracts::RecordingPrefix,
) -> Result<Vec<MergedTrack>> {
    let mut merged = Vec::with_capacity(manifest.tracks.len());
    for track in &manifest.tracks {
        if track.chunks.is_empty() {
            continue;
        }
        merged.push(merge_track(store, track, prefix)?);
    }
    Ok(merged)
}

fn merge_track(
    store: &dyn BlobStore,
    track: &Track,
    prefix: &kaseta_contracts::RecordingPrefix,
) -> Result<MergedTrack> {
    let sample_rate = track
        .format
        .sample_rate_hz
        .context("track has no sample rate; cannot merge")?;
    let channels = track.format.channels.unwrap_or(1);

    let mut samples: Vec<i16> = Vec::new();
    let mut padded_frames: u64 = 0;

    // Chunks are stored in capture order, but sort defensively: correctness
    // here must not depend on the manifest's ordering.
    let mut chunks: Vec<_> = track.chunks.iter().collect();
    chunks.sort_by_key(|c| c.seq);

    for chunk in chunks {
        // Silence stands in for audio that never reached us, so everything
        // after a dropout keeps its true position on the timeline.
        if chunk.gap_before_ns > 0 {
            let missing_frames = ns_to_frames(chunk.gap_before_ns, sample_rate);
            samples.extend(std::iter::repeat_n(0i16, missing_frames as usize * channels as usize));
            padded_frames += missing_frames;
        }

        let encoded = store
            .get_verified(&chunk.blob, &chunk.sha256)
            .with_context(|| format!("reading chunk {} of {}", chunk.seq, track.track_id))?;

        let decoded = decode_flac(&encoded, channels)
            .with_context(|| format!("decoding chunk {} of {}", chunk.seq, track.track_id))?;
        samples.extend(decoded);
    }

    let frames = (samples.len() / channels.max(1) as usize) as u64;
    let encoded = crate::capture::chunk::encode_flac_samples(&samples, sample_rate, channels)
        .context("encoding merged track")?;

    let key = prefix
        .export(&format!("{}.flac", track.track_id))
        .context("building export key")?;
    store
        .put(&key, &encoded)
        .with_context(|| format!("writing merged track {}", track.track_id))?;

    Ok(MergedTrack {
        key,
        bytes: encoded.len() as u64,
        frames,
        padded_frames,
    })
}

/// Converts a duration to whole frames, rounding to nearest.
///
/// Rounding rather than truncating keeps repeated small gaps from accumulating
/// a systematic backwards drift over a long recording.
fn ns_to_frames(ns: u64, sample_rate_hz: u32) -> u64 {
    ((ns as u128 * sample_rate_hz as u128 + 500_000_000) / 1_000_000_000) as u64
}

/// Decodes a FLAC stream to interleaved 16-bit samples.
fn decode_flac(bytes: &[u8], expected_channels: u16) -> Result<Vec<i16>> {
    let mut reader = claxon::FlacReader::new(std::io::Cursor::new(bytes))
        .map_err(|e| anyhow::anyhow!("not a readable FLAC stream: {e}"))?;

    let info = reader.streaminfo();
    if info.channels != expected_channels as u32 {
        bail!(
            "chunk has {} channels but the track declares {expected_channels}",
            info.channels
        );
    }
    if info.bits_per_sample != 16 {
        bail!("chunk is {}-bit; only 16-bit is supported", info.bits_per_sample);
    }

    let mut out = Vec::new();
    for sample in reader.samples() {
        let sample = sample.map_err(|e| anyhow::anyhow!("decoding FLAC samples: {e}"))?;
        // The stream declares 16-bit, so every value fits; a value that does not
        // means the stream is inconsistent with its own header.
        out.push(
            i16::try_from(sample)
                .map_err(|_| anyhow::anyhow!("sample {sample} exceeds 16-bit range"))?,
        );
    }
    Ok(out)
}

/// Duration a merged track represents, for reporting.
pub fn frames_to_seconds(frames: u64, sample_rate_hz: u32) -> f64 {
    samples_to_ns(frames, sample_rate_hz) as f64 / 1e9
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blobstore::LocalFsStore;
    use crate::capture::chunk::encode_flac_samples;
    use kaseta_contracts::manifest::{
        Chunk, ClockDomain, MediaType, TrackFormat, TrackRole, TrackSource,
    };
    use kaseta_contracts::{RecordingPrefix, TrackId};
    use tempfile::TempDir;
    use time::macros::datetime;
    use ulid::Ulid;

    fn tone(frames: usize, channels: u16) -> Vec<i16> {
        (0..frames * channels as usize)
            .map(|i| ((i as f64 * 0.05).sin() * 8000.0) as i16)
            .collect()
    }

    /// Builds a track whose chunks are already written to `store`.
    fn track_with_chunks(
        store: &LocalFsStore,
        prefix: &RecordingPrefix,
        channels: u16,
        chunks: &[(Vec<i16>, u64)],
    ) -> Track {
        let track_id = TrackId::new("a_local-mic_01").unwrap();
        let mut records = Vec::new();

        for (seq, (samples, gap_before_ns)) in chunks.iter().enumerate() {
            let encoded = encode_flac_samples(samples, 48_000, channels).unwrap();
            let key = prefix.chunk(&track_id, seq as u32, "flac");
            store.put(&key, &encoded).unwrap();
            records.push(Chunk {
                seq: seq as u32,
                blob: key,
                sha256: crate::blobstore::sha256_hex(&encoded),
                bytes: encoded.len() as u64,
                sample_count: Some((samples.len() / channels as usize) as u64),
                boottime_start_ns: 0,
                boottime_end_ns: 1_000_000_000,
                source_pts_start_ns: None,
                source_pts_end_ns: None,
                discontinuity: *gap_before_ns > 0,
                gap_before_ns: *gap_before_ns,
                drops_before_chunk: 0,
            });
        }

        Track {
            track_id,
            media_type: MediaType::Audio,
            role: TrackRole::LocalMic,
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
                channels: Some(channels),
                sample_format: Some("s16".into()),
            },
            chunks: records,
        }
    }

    fn setup() -> (TempDir, LocalFsStore, RecordingPrefix) {
        let dir = TempDir::new().unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();
        let prefix = RecordingPrefix::new(Ulid::nil(), datetime!(2026-07-26 19:00:00 UTC));
        (dir, store, prefix)
    }

    #[test]
    fn merges_chunks_into_one_continuous_track() {
        let (_dir, store, prefix) = setup();
        let a = tone(48_000, 1);
        let b = tone(48_000, 1);
        let track = track_with_chunks(&store, &prefix, 1, &[(a, 0), (b, 0)]);

        let merged = merge_track(&store, &track, &prefix).unwrap();

        assert_eq!(merged.frames, 96_000, "both chunks must appear in full");
        assert_eq!(merged.padded_frames, 0);
        assert!(store.exists(&merged.key).unwrap());
    }

    #[test]
    fn a_dropout_is_padded_so_later_audio_keeps_its_position() {
        let (_dir, store, prefix) = setup();
        // Half a second of audio went missing between the two chunks.
        let track = track_with_chunks(
            &store,
            &prefix,
            1,
            &[(tone(48_000, 1), 0), (tone(48_000, 1), 500_000_000)],
        );

        let merged = merge_track(&store, &track, &prefix).unwrap();

        assert_eq!(merged.padded_frames, 24_000);
        assert_eq!(
            merged.frames, 120_000,
            "the hole must occupy its true duration, not be closed up"
        );
    }

    #[test]
    fn merged_audio_is_sample_identical_to_what_was_captured() {
        let (_dir, store, prefix) = setup();
        let first = tone(1_000, 1);
        let second: Vec<i16> = tone(1_000, 1).iter().map(|s| s / 2).collect();
        let track =
            track_with_chunks(&store, &prefix, 1, &[(first.clone(), 0), (second.clone(), 0)]);

        let merged = merge_track(&store, &track, &prefix).unwrap();
        let decoded = decode_flac(&store.get(&merged.key).unwrap(), 1).unwrap();

        let expected: Vec<i16> = first.iter().chain(second.iter()).copied().collect();
        assert_eq!(decoded, expected, "merging must be lossless");
    }

    #[test]
    fn preserves_stereo_interleaving() {
        let (_dir, store, prefix) = setup();
        let samples = tone(1_000, 2);
        let track = track_with_chunks(&store, &prefix, 2, &[(samples.clone(), 0)]);

        let merged = merge_track(&store, &track, &prefix).unwrap();
        assert_eq!(merged.frames, 1_000, "frames are per channel");

        let decoded = decode_flac(&store.get(&merged.key).unwrap(), 2).unwrap();
        assert_eq!(decoded, samples);
    }

    #[test]
    fn chunks_are_merged_in_sequence_order_regardless_of_manifest_order() {
        let (_dir, store, prefix) = setup();
        let first = tone(1_000, 1);
        let second: Vec<i16> = vec![1_234; 1_000];
        let mut track =
            track_with_chunks(&store, &prefix, 1, &[(first.clone(), 0), (second.clone(), 0)]);
        track.chunks.reverse();

        let merged = merge_track(&store, &track, &prefix).unwrap();
        let decoded = decode_flac(&store.get(&merged.key).unwrap(), 1).unwrap();

        let expected: Vec<i16> = first.iter().chain(second.iter()).copied().collect();
        assert_eq!(decoded, expected, "capture order must win over manifest order");
    }

    #[test]
    fn a_corrupted_chunk_fails_the_merge_rather_than_producing_wrong_audio() {
        let (_dir, store, prefix) = setup();
        let track = track_with_chunks(&store, &prefix, 1, &[(tone(1_000, 1), 0)]);

        // Overwrite the chunk with different audio; its recorded digest no
        // longer matches.
        store
            .put(
                &track.chunks[0].blob,
                &encode_flac_samples(&vec![42i16; 1_000], 48_000, 1).unwrap(),
            )
            .unwrap();

        let err = merge_track(&store, &track, &prefix).unwrap_err();
        assert!(
            format!("{err:#}").contains("integrity check"),
            "expected an integrity failure, got: {err:#}"
        );
    }

    #[test]
    fn gap_padding_rounds_rather_than_truncating() {
        // A gap that is not a whole number of frames must not lose a sample
        // each time, which would accumulate over a long recording.
        assert_eq!(ns_to_frames(500_000_000, 48_000), 24_000);
        assert_eq!(ns_to_frames(20_833, 48_000), 1);
        assert_eq!(ns_to_frames(0, 48_000), 0);
    }
}
