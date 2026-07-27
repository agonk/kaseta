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
use kaseta_contracts::manifest::{Chunk, RecordingManifest, Track};
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

/// Yields a track's audio chunk by chunk, in order, with gaps filled.
///
/// Nothing accumulates: each chunk is decoded, handed out, and dropped. A
/// three-hour recording costs the same memory as a three-minute one, which is
/// the same property capture already has and which the export path previously
/// threw away by decoding whole tracks into one buffer.
struct TrackReader<'a> {
    store: &'a dyn BlobStore,
    track: &'a Track,
    chunks: Vec<&'a Chunk>,
    next: usize,
    sample_rate: u32,
    channels: u16,
    /// Decoded samples not yet consumed by the caller.
    buffer: std::collections::VecDeque<i16>,
    padded_frames: u64,
}

impl<'a> TrackReader<'a> {
    fn new(store: &'a dyn BlobStore, track: &'a Track) -> Result<Self> {
        let sample_rate = track
            .format
            .sample_rate_hz
            .context("track has no sample rate")?;
        // Chunks are stored in capture order, but sort defensively: correctness
        // must not depend on the manifest's ordering.
        let mut chunks: Vec<&Chunk> = track.chunks.iter().collect();
        chunks.sort_by_key(|c| c.seq);

        Ok(Self {
            store,
            track,
            chunks,
            next: 0,
            sample_rate,
            channels: track.format.channels.unwrap_or(1).max(1),
            buffer: std::collections::VecDeque::new(),
            padded_frames: 0,
        })
    }

    /// Total interleaved samples this reader will produce.
    fn total_samples(&self) -> u64 {
        let audio: u64 = self
            .chunks
            .iter()
            .filter_map(|c| c.sample_count)
            .sum::<u64>();
        let padding: u64 = self
            .chunks
            .iter()
            .map(|c| ns_to_frames(c.gap_before_ns, self.sample_rate))
            .sum();
        (audio + padding) * self.channels as u64
    }

    /// Decodes the next chunk into the buffer. Returns false at end of track.
    fn pull_chunk(&mut self) -> Result<bool> {
        let Some(chunk) = self.chunks.get(self.next) else {
            return Ok(false);
        };
        self.next += 1;

        // Silence stands in for audio that never reached us, so everything
        // after a dropout keeps its true position on the timeline.
        if chunk.gap_before_ns > 0 {
            let missing = ns_to_frames(chunk.gap_before_ns, self.sample_rate);
            self.buffer
                .extend(std::iter::repeat_n(0i16, missing as usize * self.channels as usize));
            self.padded_frames += missing;
        }

        let encoded = self
            .store
            .get_verified(&chunk.blob, &chunk.sha256)
            .with_context(|| format!("reading chunk {} of {}", chunk.seq, self.track.track_id))?;
        let decoded = decode_flac(&encoded, self.channels)
            .with_context(|| format!("decoding chunk {} of {}", chunk.seq, self.track.track_id))?;
        self.buffer.extend(decoded);
        Ok(true)
    }

    /// Takes up to `want` interleaved samples, decoding more as needed.
    fn take(&mut self, want: usize) -> Result<Vec<i16>> {
        while self.buffer.len() < want {
            if !self.pull_chunk()? {
                break;
            }
        }
        let n = want.min(self.buffer.len());
        Ok(self.buffer.drain(..n).collect())
    }
}

/// Feeds a [`TrackReader`] to the FLAC encoder block by block.
///
/// The encoder pulls fixed-size blocks, so the whole track never has to exist
/// in memory at once.
struct StreamingSource<'a, 'b> {
    reader: &'b mut TrackReader<'a>,
    total: usize,
    /// The encoder's error type cannot carry a cause, so a read failure is kept
    /// here and re-raised afterwards. Without this a corrupt chunk surfaces as
    /// an opaque "invalid format" instead of naming the integrity failure.
    failure: Option<anyhow::Error>,
}

impl flacenc::source::Source for StreamingSource<'_, '_> {
    fn channels(&self) -> usize {
        self.reader.channels as usize
    }

    fn bits_per_sample(&self) -> usize {
        16
    }

    fn sample_rate(&self) -> usize {
        self.reader.sample_rate as usize
    }

    fn read_samples<F: flacenc::source::Fill>(
        &mut self,
        block_size: usize,
        dest: &mut F,
    ) -> std::result::Result<usize, flacenc::error::SourceError> {
        let channels = self.reader.channels as usize;
        let want = block_size * channels;

        let samples = match self.reader.take(want) {
            Ok(samples) => samples,
            Err(e) => {
                self.failure = Some(e);
                return Err(flacenc::error::SourceError::by_reason(
                    flacenc::error::SourceErrorReason::InvalidFormat,
                ));
            }
        };
        if samples.is_empty() {
            return Ok(0);
        }

        let widened: Vec<i32> = samples.iter().map(|s| *s as i32).collect();
        dest.fill_interleaved(&widened)?;
        Ok(samples.len() / channels)
    }

    fn len_hint(&self) -> Option<usize> {
        Some(self.total)
    }
}

fn merge_track(
    store: &dyn BlobStore,
    track: &Track,
    prefix: &kaseta_contracts::RecordingPrefix,
) -> Result<MergedTrack> {
    let mut reader = TrackReader::new(store, track)?;
    let channels = reader.channels;
    let sample_rate = reader.sample_rate;
    let total = (reader.total_samples() / channels.max(1) as u64) as usize;

    let encoded = {
        let mut source = StreamingSource {
            reader: &mut reader,
            total,
            failure: None,
        };
        let encoded = encode_flac_streaming(&mut source, sample_rate);
        match source.failure.take() {
            // Reading failed underneath the encoder; report why, not that the
            // encoder was unhappy.
            Some(cause) => return Err(cause),
            None => encoded?,
        }
    };

    let key = prefix
        .export(&format!("{}.flac", track.track_id))
        .context("building export key")?;
    store
        .put(&key, &encoded)
        .with_context(|| format!("writing merged track {}", track.track_id))?;

    Ok(MergedTrack {
        key,
        bytes: encoded.len() as u64,
        frames: total as u64,
        padded_frames: reader.padded_frames,
    })
}

/// Encodes from a pull-based source rather than a buffer of every sample.
fn encode_flac_streaming<S: flacenc::source::Source>(
    source: &mut S,
    sample_rate: u32,
) -> Result<Vec<u8>> {
    use flacenc::component::BitRepr;
    use flacenc::error::Verify;

    let config = flacenc::config::Encoder::default()
        .into_verified()
        .map_err(|e| anyhow::anyhow!("invalid FLAC encoder config: {e:?}"))?;

    let stream = flacenc::encode_with_fixed_block_size(&config, source, config.block_size)
        .map_err(|e| anyhow::anyhow!("FLAC encoding failed: {e:?}"))?;

    let mut sink = flacenc::bitsink::ByteSink::new();
    stream
        .write(&mut sink)
        .map_err(|e| anyhow::anyhow!("writing FLAC stream failed: {e:?}"))?;

    debug_assert!(sample_rate > 0);
    Ok(sink.into_inner())
}

/// A single stereo file combining every track in a recording.
#[derive(Debug)]
pub struct MixedRecording {
    pub key: BlobKey,
    pub bytes: u64,
    pub frames: u64,
    pub sample_rate_hz: u32,
    /// Attenuation applied to avoid clipping, as a fraction. `1.0` means none.
    pub gain: f32,
}

/// Mixes every track into one stereo file, aligned on the canonical clock.
///
/// # Why alignment matters
///
/// Tracks do not start together. Each stream negotiates its format and begins
/// delivering independently, typically tens to hundreds of milliseconds apart.
/// Summing both from sample zero would offset one speaker against the other by
/// that amount for the entire recording. Each track is therefore placed at its
/// true offset from the earliest track's first sample, measured on the
/// canonical clock.
///
/// Output is stereo: a mono microphone is placed in both channels rather than
/// left only.
pub fn mix_recording(
    store: &dyn BlobStore,
    manifest: &RecordingManifest,
    prefix: &kaseta_contracts::RecordingPrefix,
) -> Result<Option<MixedRecording>> {
    let tracks: Vec<&Track> = manifest
        .tracks
        .iter()
        .filter(|t| !t.chunks.is_empty() && t.media_type == kaseta_contracts::MediaType::Audio)
        .collect();
    if tracks.is_empty() {
        return Ok(None);
    }

    // Resampling is not attempted, so a rate mismatch is reported rather than
    // producing audio that slowly slides out of sync.
    let sample_rate = tracks[0]
        .format
        .sample_rate_hz
        .context("track has no sample rate; cannot mix")?;
    if let Some(odd) = tracks
        .iter()
        .find(|t| t.format.sample_rate_hz != Some(sample_rate))
    {
        bail!(
            "cannot mix tracks at different sample rates: {} is {:?}, expected {sample_rate}",
            odd.track_id,
            odd.format.sample_rate_hz
        );
    }

    let earliest = tracks
        .iter()
        .filter_map(|t| t.chunks.first().map(|c| c.boottime_start_ns))
        .min()
        .context("no chunk carries a start time")?;

    // Summed in i16 with saturation rather than i32: an i32 accumulator would
    // double what a long recording needs, and clipping is rare enough that
    // detecting it and redoing the pass costs less than always paying for the
    // wider type. Each track is streamed rather than decoded whole.
    let mut mixed: Vec<i16> = Vec::new();
    let mut needed_attenuation = false;

    for pass in 0..2 {
        // The second pass runs only if the first clipped, applying the
        // attenuation that pass proved necessary.
        let gain = if pass == 0 { 1.0f32 } else { 0.5f32 };
        mixed.clear();
        let mut clipped = false;

        for track in &tracks {
            let mut reader = TrackReader::new(store, track)?;
            let channels = reader.channels;

            let offset_ns = track
                .chunks
                .first()
                .map(|c| c.boottime_start_ns.saturating_sub(earliest))
                .unwrap_or(0);
            let mut out_frame = ns_to_frames(offset_ns, sample_rate) as usize;

            let window = sample_rate as usize * channels as usize;
            loop {
                let block = reader.take(window)?;
                if block.is_empty() {
                    break;
                }
                let frames = block.len() / channels as usize;
                let needed = (out_frame + frames) * 2;
                if mixed.len() < needed {
                    mixed.resize(needed, 0);
                }

                for frame in 0..frames {
                    let base = frame * channels as usize;
                    // A mono source belongs in the middle, not hard left.
                    let (left, right) = if channels == 1 {
                        (block[base], block[base])
                    } else {
                        (block[base], block[base + 1])
                    };
                    let out = (out_frame + frame) * 2;

                    for (i, sample) in [left, right].into_iter().enumerate() {
                        let scaled = (sample as f32 * gain) as i32;
                        let sum = mixed[out + i] as i32 + scaled;
                        if sum > i16::MAX as i32 || sum < i16::MIN as i32 {
                            clipped = true;
                        }
                        mixed[out + i] = sum.clamp(i16::MIN as i32, i16::MAX as i32) as i16;
                    }
                }
                out_frame += frames;
            }
        }

        if !clipped {
            break;
        }
        // Whether attenuation was required is a property of the first pass. The
        // second pass exists to apply it, so its own result must not overwrite
        // the answer.
        needed_attenuation = true;
    }

    let gain = if needed_attenuation { 0.5f32 } else { 1.0f32 };
    let samples = mixed;

    let frames = (samples.len() / 2) as u64;
    let encoded = crate::capture::chunk::encode_flac_samples(&samples, sample_rate, 2)
        .context("encoding mixed recording")?;

    let key = prefix.export("mixed.flac").context("building export key")?;
    store.put(&key, &encoded).context("writing mixed recording")?;

    Ok(Some(MixedRecording {
        key,
        bytes: encoded.len() as u64,
        frames,
        sample_rate_hz: sample_rate,
        gain,
    }))
}

/// Sample rate speech models expect. Higher rates carry no speech information
/// they can use and cost proportionally more to process.
pub const ASR_SAMPLE_RATE_HZ: u32 = 16_000;

/// Writes a track as mono 16 kHz WAV, ready for transcription.
///
/// The recogniser reads WAV through Python's standard library and accepts mono
/// only, while archives are stereo FLAC — so the conversion has to happen
/// somewhere. Doing it here keeps the worker free of audio-decoding
/// dependencies and produces exactly the format the model wants, roughly a
/// sixth the size of the archive.
pub fn write_asr_audio(
    store: &dyn BlobStore,
    track: &Track,
    prefix: &kaseta_contracts::RecordingPrefix,
) -> Result<BlobKey> {
    let mut reader = TrackReader::new(store, track)?;
    let source_rate = reader.sample_rate;
    let channels = reader.channels;

    // One second of input at a time. Converting in windows keeps memory flat,
    // and a whole second is an exact multiple of any sensible rate ratio, so
    // resampling each window independently introduces no boundary artefact.
    let window = source_rate as usize * channels as usize;
    let mut resampled: Vec<i16> = Vec::new();
    loop {
        let block = reader.take(window)?;
        if block.is_empty() {
            break;
        }
        let mono = downmix_to_mono(&block, channels);
        resampled.extend(resample(&mono, source_rate, ASR_SAMPLE_RATE_HZ));
    }

    let wav = encode_wav(&resampled, ASR_SAMPLE_RATE_HZ);

    let key = prefix
        .export(&format!("{}.asr.wav", track.track_id))
        .context("building transcription audio key")?;
    store
        .put(&key, &wav)
        .with_context(|| format!("writing transcription audio for {}", track.track_id))?;
    Ok(key)
}

/// Averages channels rather than discarding all but one.
///
/// Taking a single channel would silence anything panned to the other, which on
/// a stereo playback capture can mean losing a speaker entirely.
fn downmix_to_mono(samples: &[i16], channels: u16) -> Vec<i16> {
    if channels <= 1 {
        return samples.to_vec();
    }
    samples
        .chunks_exact(channels as usize)
        .map(|frame| {
            let sum: i32 = frame.iter().map(|s| *s as i32).sum();
            (sum / frame.len() as i32) as i16
        })
        .collect()
}

/// Resamples by averaging groups of input samples.
///
/// Averaging rather than picking every Nth sample matters: plain decimation
/// folds everything above the new Nyquist limit back down into the audible
/// band as aliasing, which speech models hear as noise. A box average is a
/// crude low-pass, but the ratios here are small integers and speech content
/// sits well below the limit, so it is enough.
fn resample(samples: &[i16], from_hz: u32, to_hz: u32) -> Vec<i16> {
    if from_hz == to_hz || from_hz == 0 || to_hz == 0 || samples.is_empty() {
        return samples.to_vec();
    }

    let out_len = (samples.len() as u64 * to_hz as u64 / from_hz as u64) as usize;
    let mut out = Vec::with_capacity(out_len);

    for i in 0..out_len {
        let start = (i as u64 * from_hz as u64 / to_hz as u64) as usize;
        let end = (((i + 1) as u64 * from_hz as u64 / to_hz as u64) as usize).min(samples.len());
        let window = &samples[start..end.max(start + 1).min(samples.len())];
        if window.is_empty() {
            break;
        }
        let sum: i32 = window.iter().map(|s| *s as i32).sum();
        out.push((sum / window.len() as i32) as i16);
    }
    out
}

/// Encodes mono 16-bit samples as a WAV file.
fn encode_wav(samples: &[i16], sample_rate_hz: u32) -> Vec<u8> {
    let data_bytes = (samples.len() * 2) as u32;
    let mut wav = Vec::with_capacity(44 + data_bytes as usize);

    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&(36 + data_bytes).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16u32.to_le_bytes()); // PCM header size
    wav.extend_from_slice(&1u16.to_le_bytes()); // uncompressed
    wav.extend_from_slice(&1u16.to_le_bytes()); // mono
    wav.extend_from_slice(&sample_rate_hz.to_le_bytes());
    wav.extend_from_slice(&(sample_rate_hz * 2).to_le_bytes()); // bytes per second
    wav.extend_from_slice(&2u16.to_le_bytes()); // block align
    wav.extend_from_slice(&16u16.to_le_bytes()); // bits per sample
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&data_bytes.to_le_bytes());
    for s in samples {
        wav.extend_from_slice(&s.to_le_bytes());
    }
    wav
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

    /// Builds a manifest with two tracks whose chunks are already stored.
    fn two_track_manifest(
        store: &LocalFsStore,
        prefix: &RecordingPrefix,
        mic: (Vec<i16>, u16, u64),
        remote: (Vec<i16>, u16, u64),
    ) -> RecordingManifest {
        use kaseta_contracts::manifest::{CanonicalClock, ClockKind, RecordingNotes, Timeline};

        let build = |id: &str, role: TrackRole, (samples, channels, start_ns): (Vec<i16>, u16, u64)| {
            let track_id = TrackId::new(id).unwrap();
            let encoded = encode_flac_samples(&samples, 48_000, channels).unwrap();
            let key = prefix.chunk(&track_id, 0, "flac");
            store.put(&key, &encoded).unwrap();
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
                    channels: Some(channels),
                    sample_format: Some("s16".into()),
                },
                chunks: vec![Chunk {
                    seq: 0,
                    blob: key,
                    sha256: crate::blobstore::sha256_hex(&encoded),
                    bytes: encoded.len() as u64,
                    sample_count: Some((samples.len() / channels as usize) as u64),
                    boottime_start_ns: start_ns,
                    boottime_end_ns: start_ns + 1_000_000_000,
                    source_pts_start_ns: None,
                    source_pts_end_ns: None,
                    discontinuity: false,
                    gap_before_ns: 0,
                    drops_before_chunk: 0,
                }],
            }
        };

        RecordingManifest {
            manifest_version: kaseta_contracts::MANIFEST_VERSION.into(),
            recording_id: Ulid::nil(),
            started_at: datetime!(2026-07-26 19:00:00 UTC),
            ended_at: None,
            canonical_clock: CanonicalClock {
                kind: ClockKind::BoottimeNs,
                started_at_ns: 0,
            },
            timeline: Timeline {
                master_track_id: TrackId::new("a_local-mic_01").unwrap(),
                nominal_sample_rate_hz: 48_000,
            },
            tracks: vec![
                build("a_local-mic_01", TrackRole::LocalMic, mic),
                build("a_remote-mix_01", TrackRole::RemoteMix, remote),
            ],
            notes: RecordingNotes::default(),
        }
    }

    #[test]
    fn mixes_both_sides_into_one_stereo_file() {
        let (_dir, store, prefix) = setup();
        let manifest = two_track_manifest(
            &store,
            &prefix,
            (tone(48_000, 1), 1, 0),
            (tone(48_000, 2), 2, 0),
        );

        let mixed = mix_recording(&store, &manifest, &prefix).unwrap().unwrap();

        assert_eq!(mixed.frames, 48_000);
        assert_eq!(mixed.sample_rate_hz, 48_000);
        let decoded = decode_flac(&store.get(&mixed.key).unwrap(), 2).unwrap();
        assert_eq!(decoded.len(), 96_000, "output must be stereo");
    }

    #[test]
    fn tracks_are_aligned_on_the_canonical_clock_not_sample_zero() {
        let (_dir, store, prefix) = setup();
        // The remote track starts half a second after the mic. Summing from
        // sample zero would offset the two speakers for the whole recording.
        let manifest = two_track_manifest(
            &store,
            &prefix,
            (vec![1_000i16; 48_000], 1, 0),
            (vec![2_000i16; 96_000], 2, 500_000_000),
        );

        let mixed = mix_recording(&store, &manifest, &prefix).unwrap().unwrap();
        let decoded = decode_flac(&store.get(&mixed.key).unwrap(), 2).unwrap();

        // First half second: microphone only.
        assert_eq!(decoded[0], 1_000);
        // After the offset: both tracks summed.
        let at_offset = 24_000 * 2;
        assert_eq!(
            decoded[at_offset], 3_000,
            "the later track must begin at its true offset"
        );
        assert_eq!(
            mixed.frames, 72_000,
            "output spans from the earliest start to the latest end"
        );
    }

    #[test]
    fn a_mono_microphone_is_centred_rather_than_hard_left() {
        let (_dir, store, prefix) = setup();
        let manifest = two_track_manifest(
            &store,
            &prefix,
            (vec![900i16; 1_000], 1, 0),
            (vec![0i16; 2_000], 2, 0),
        );

        let mixed = mix_recording(&store, &manifest, &prefix).unwrap().unwrap();
        let decoded = decode_flac(&store.get(&mixed.key).unwrap(), 2).unwrap();

        assert_eq!(decoded[0], 900, "left");
        assert_eq!(decoded[1], 900, "right");
    }

    #[test]
    fn attenuates_only_when_the_sum_would_clip() {
        let (_dir, store, prefix) = setup();

        // Quiet enough to sum without clipping: no attenuation.
        let quiet = two_track_manifest(
            &store,
            &prefix,
            (vec![1_000i16; 1_000], 1, 0),
            (vec![1_000i16; 2_000], 2, 0),
        );
        assert_eq!(mix_recording(&store, &quiet, &prefix).unwrap().unwrap().gain, 1.0);

        // Two near-full-scale tracks would overflow i16.
        let (_dir2, store2, prefix2) = setup();
        let loud = two_track_manifest(
            &store2,
            &prefix2,
            (vec![30_000i16; 1_000], 1, 0),
            (vec![30_000i16; 2_000], 2, 0),
        );
        let mixed = mix_recording(&store2, &loud, &prefix2).unwrap().unwrap();
        assert!(mixed.gain < 1.0, "a clipping sum must be attenuated");

        let decoded = decode_flac(&store2.get(&mixed.key).unwrap(), 2).unwrap();
        assert!(
            decoded.iter().all(|s| *s > 0),
            "attenuation must not wrap samples to negative"
        );
    }

    #[test]
    fn refuses_to_mix_mismatched_sample_rates() {
        let (_dir, store, prefix) = setup();
        let mut manifest = two_track_manifest(
            &store,
            &prefix,
            (tone(1_000, 1), 1, 0),
            (tone(2_000, 2), 2, 0),
        );
        manifest.tracks[1].format.sample_rate_hz = Some(16_000);

        let err = mix_recording(&store, &manifest, &prefix).unwrap_err();
        assert!(
            err.to_string().contains("different sample rates"),
            "expected a rate mismatch error, got: {err}"
        );
    }

    #[test]
    fn downmixing_keeps_audio_that_sits_in_one_channel() {
        // Discarding a channel would silence anything panned to it, which on a
        // stereo playback capture can mean losing a speaker entirely.
        let stereo = vec![0i16, 1000, 0, 1000];
        assert_eq!(downmix_to_mono(&stereo, 2), vec![500, 500]);
        // Mono passes through untouched.
        assert_eq!(downmix_to_mono(&[1, 2, 3], 1), vec![1, 2, 3]);
    }

    #[test]
    fn resampling_lands_on_the_expected_length() {
        let input: Vec<i16> = (0..48_000).map(|i| (i % 100) as i16).collect();
        let out = resample(&input, 48_000, 16_000);
        assert_eq!(out.len(), 16_000, "one second in, one second out");
    }

    #[test]
    fn resampling_averages_rather_than_dropping_samples() {
        // Plain decimation would return the first of each group and fold
        // everything above the new Nyquist limit back into the audible band.
        let input = vec![0i16, 300, 600, 0, 300, 600];
        let out = resample(&input, 48_000, 16_000);
        assert_eq!(out, vec![300, 300], "each output is the mean of its window");
    }

    #[test]
    fn resampling_is_a_no_op_at_the_same_rate() {
        let input = vec![1i16, 2, 3];
        assert_eq!(resample(&input, 16_000, 16_000), input);
        assert!(resample(&[], 48_000, 16_000).is_empty());
    }

    #[test]
    fn the_wav_header_describes_what_follows() {
        let wav = encode_wav(&[0i16, 1, -1], 16_000);

        assert_eq!(&wav[0..4], b"RIFF");
        assert_eq!(&wav[8..12], b"WAVE");
        assert_eq!(&wav[36..40], b"data");
        assert_eq!(wav.len(), 44 + 6, "header plus three 16-bit samples");

        let channels = u16::from_le_bytes([wav[22], wav[23]]);
        let rate = u32::from_le_bytes([wav[24], wav[25], wav[26], wav[27]]);
        let bits = u16::from_le_bytes([wav[34], wav[35]]);
        // The recogniser reads mono 16-bit WAV and rejects anything else.
        assert_eq!((channels, rate, bits), (1, 16_000, 16));

        let declared = u32::from_le_bytes([wav[40], wav[41], wav[42], wav[43]]);
        assert_eq!(declared as usize, wav.len() - 44);
    }

    #[test]
    fn the_wav_python_reads_matches_what_we_write() {
        // Python's `wave` module is what the worker uses, so the header must
        // satisfy it rather than merely look plausible.
        let wav = encode_wav(&[100i16, -100, 200], 16_000);
        let riff_size = u32::from_le_bytes([wav[4], wav[5], wav[6], wav[7]]);
        assert_eq!(riff_size as usize, wav.len() - 8);

        let byte_rate = u32::from_le_bytes([wav[28], wav[29], wav[30], wav[31]]);
        let block_align = u16::from_le_bytes([wav[32], wav[33]]);
        assert_eq!(byte_rate, 16_000 * 2);
        assert_eq!(block_align, 2);
    }

    #[test]
    fn memory_does_not_grow_with_the_length_of_a_recording() {
        // The reader is the whole point of the streaming rewrite: decoding a
        // track used to allocate every sample at once, so a long meeting cost
        // gigabytes. Buffered samples must stay bounded by the window asked
        // for, not by how much audio remains.
        let (_dir, store, prefix) = setup();
        let chunks: Vec<(Vec<i16>, u64)> =
            (0..20).map(|_| (tone(48_000, 1), 0)).collect();
        let track = track_with_chunks(&store, &prefix, 1, &chunks);

        let mut reader = TrackReader::new(&store, &track).unwrap();
        let mut total = 0usize;
        loop {
            let block = reader.take(4_800).unwrap();
            if block.is_empty() {
                break;
            }
            total += block.len();
            assert!(
                reader.buffer.len() < 48_000 * 2,
                "buffered {} samples; the reader is accumulating rather than streaming",
                reader.buffer.len()
            );
        }
        assert_eq!(total, 20 * 48_000, "every sample must still be delivered");
    }

    #[test]
    fn the_reader_reports_what_it_will_produce_before_reading_it() {
        // The encoder asks for a length hint up front, so it must be derivable
        // from the manifest without decoding anything.
        let (_dir, store, prefix) = setup();
        let track = track_with_chunks(
            &store,
            &prefix,
            1,
            &[(tone(48_000, 1), 0), (tone(48_000, 1), 500_000_000)],
        );

        let reader = TrackReader::new(&store, &track).unwrap();
        // Two seconds of audio plus half a second of padded gap.
        assert_eq!(reader.total_samples(), 48_000 * 2 + 24_000);
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
