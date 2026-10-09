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
//!
//! # Memory
//!
//! Every export here streams: audio is decoded a chunk at a time, processed a
//! window at a time, and encoded output goes to a file beside its key as it is
//! produced ([`PendingBlob`]), committed whole at the end. Nothing holds a
//! recording-length buffer, so exporting a three-hour import costs what a
//! three-minute meeting does. `tests/export_memory.rs` holds the line.

use std::io::{Seek, SeekFrom, Write};

use anyhow::{bail, Context, Result};
use kaseta_contracts::manifest::{Chunk, RecordingManifest, Track};
use kaseta_contracts::BlobKey;

use crate::blobstore::{BlobStore, PendingBlob};
use crate::clock::samples_to_ns;
use crate::flac::FlacFileWriter;

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
    /// Silence still owed before the next chunk's audio.
    ///
    /// Held as a count rather than expanded into the buffer: a long dropout
    /// would otherwise allocate minutes of zeroes at once, which is exactly the
    /// unbounded allocation streaming exists to avoid.
    pending_silence: usize,
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
            pending_silence: 0,
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
        // after a dropout keeps its true position on the timeline. Recorded as
        // a count and emitted as the caller asks for it.
        if chunk.gap_before_ns > 0 {
            let missing = ns_to_frames(chunk.gap_before_ns, self.sample_rate);
            self.pending_silence += missing as usize * self.channels as usize;
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
    ///
    /// Never returns more than asked for, and never holds more than one chunk
    /// plus the remainder of the current request.
    fn take(&mut self, want: usize) -> Result<Vec<i16>> {
        let mut out = Vec::with_capacity(want);

        while out.len() < want {
            // Owed silence is produced a request at a time, so a long dropout
            // costs no memory beyond what was asked for.
            if self.pending_silence > 0 {
                let n = (want - out.len()).min(self.pending_silence);
                out.extend(std::iter::repeat_n(0i16, n));
                self.pending_silence -= n;
                continue;
            }

            if !self.buffer.is_empty() {
                let n = (want - out.len()).min(self.buffer.len());
                out.extend(self.buffer.drain(..n));
                continue;
            }

            if !self.pull_chunk()? {
                break;
            }
        }
        Ok(out)
    }

    /// One second of interleaved samples: the step every export reads in.
    ///
    /// A whole second is an exact multiple of any sensible rate ratio, which
    /// the ASR resampler relies on, and small enough not to matter.
    fn window(&self) -> usize {
        self.sample_rate as usize * self.channels as usize
    }
}

fn merge_track(
    store: &dyn BlobStore,
    track: &Track,
    prefix: &kaseta_contracts::RecordingPrefix,
) -> Result<MergedTrack> {
    let mut reader = TrackReader::new(store, track)?;
    let key = prefix
        .export(&format!("{}.flac", track.track_id))
        .context("building export key")?;

    let mut out = PendingBlob::create(store, &key)
        .with_context(|| format!("writing merged track {}", track.track_id))?;
    let mut flac = FlacFileWriter::new(out.writer(), reader.sample_rate, reader.channels)?;
    let window = reader.window();
    loop {
        let block = reader.take(window)?;
        if block.is_empty() {
            break;
        }
        flac.write_samples(&block)
            .with_context(|| format!("encoding merged track {}", track.track_id))?;
    }
    let (_, written) = flac
        .finish()
        .with_context(|| format!("encoding merged track {}", track.track_id))?;
    let stored = out
        .commit()
        .with_context(|| format!("writing merged track {}", track.track_id))?;

    Ok(MergedTrack {
        key,
        bytes: stored.bytes,
        frames: written.frames,
        padded_frames: reader.padded_frames,
    })
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
///
/// # Streaming
///
/// All tracks are read side by side, a second at a time, and each mixed second
/// is encoded and reduced into the waveform as soon as it exists. Whether the
/// sum clips is only known by looking at it, so the first pass runs at full
/// gain and stops at the first clipped sample; only then is the recording mixed
/// again, attenuated. Most recordings never clip and are read once.
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

    let key = prefix.export("mixed.flac").context("building export key")?;

    let mut needed_attenuation = false;
    for gain in [1.0f32, 0.5f32] {
        let mut inputs = Vec::with_capacity(tracks.len());
        for track in &tracks {
            let offset_ns = track
                .chunks
                .first()
                .map(|c| c.boottime_start_ns.saturating_sub(earliest))
                .unwrap_or(0);
            inputs.push(MixInput {
                reader: TrackReader::new(store, track)?,
                start: ns_to_frames(offset_ns, sample_rate),
                done: false,
            });
        }
        // Known from the manifest before anything is decoded, which is what
        // lets the waveform be reduced on the fly into a fixed number of points.
        let expected_frames = inputs
            .iter()
            .map(|i| i.start + i.reader.total_samples() / i.reader.channels as u64)
            .max()
            .unwrap_or(0);

        let mut out = PendingBlob::create(store, &key).context("writing mixed recording")?;
        let mut flac = FlacFileWriter::new(out.writer(), sample_rate, 2)?;
        let mut peaks = PeakMeter::new(expected_frames, WAVEFORM_POINTS);

        // Only the full-gain pass can be abandoned: the attenuated one is the
        // answer whether or not it still clips, exactly as a single clamped
        // pass would be.
        let stop_on_clip = gain == 1.0;
        let outcome = mix_tracks(&mut inputs, sample_rate, gain, stop_on_clip, |block| {
            peaks.push(block, 2);
            flac.write_samples(block).context("encoding mixed recording")
        })?;

        if outcome.clipped && stop_on_clip {
            // Dropping the pending blob removes its partial file.
            needed_attenuation = true;
            continue;
        }

        flac.finish().context("encoding mixed recording")?;
        let stored = out.commit().context("writing mixed recording")?;

        // Computed here rather than on demand: the samples were already in
        // hand, and drawing a waveform from the file would mean decoding the
        // whole recording again.
        if let Ok(peaks_key) = prefix.export("mixed.peaks.json") {
            let encoded = serde_json::to_vec(&peaks.finish()).unwrap_or_else(|_| b"[]".to_vec());
            if let Err(e) = store.put(&peaks_key, &encoded) {
                // A missing waveform costs a nicer scrubber, not the recording.
                tracing::warn!(error = %format!("{e:#}"), "could not write the waveform");
            }
        }

        return Ok(Some(MixedRecording {
            key,
            bytes: stored.bytes,
            frames: outcome.frames,
            sample_rate_hz: sample_rate,
            gain: if needed_attenuation { 0.5 } else { 1.0 },
        }));
    }
    unreachable!("the attenuated pass always returns")
}

/// One track's place in a mix.
struct MixInput<'a> {
    reader: TrackReader<'a>,
    /// Output frame at which this track's first sample belongs.
    start: u64,
    /// The reader has delivered everything it has.
    done: bool,
}

struct MixOutcome {
    frames: u64,
    clipped: bool,
}

/// Sums `inputs` into stereo a second at a time, handing each mixed second to
/// `emit`.
///
/// The output runs from the earliest track's start to the latest track's end;
/// stretches where no track has audio, before a late track begins, are silence.
/// The length comes from what the readers deliver, not from the manifest, so a
/// chunk whose recorded sample count is wrong cannot truncate or pad the mix.
fn mix_tracks(
    inputs: &mut [MixInput<'_>],
    sample_rate: u32,
    gain: f32,
    stop_on_clip: bool,
    mut emit: impl FnMut(&[i16]) -> Result<()>,
) -> Result<MixOutcome> {
    let window = sample_rate.max(1) as u64;
    let mut sums = vec![0i32; window as usize * 2];
    let mut mixed: Vec<i16> = Vec::with_capacity(window as usize * 2);
    let mut position = 0u64;
    let mut frames = 0u64;
    let mut clipped = false;

    while inputs.iter().any(|i| !i.done) {
        sums.fill(0);
        let window_end = position + window;
        // The furthest any track reached inside this window, and whether any
        // track still has audio to come after it.
        let mut reached = position;
        let mut more_to_come = false;

        for input in inputs.iter_mut().filter(|i| !i.done) {
            let begin = input.start.max(position);
            if begin >= window_end {
                // Not started yet; its silence is already in `sums`.
                more_to_come = true;
                continue;
            }
            let want = (window_end - begin) as usize;
            let channels = input.reader.channels as usize;
            let block = input.reader.take(want * channels)?;
            let got = block.len() / channels;
            if got < want {
                input.done = true;
            } else {
                more_to_come = true;
            }
            if got > 0 {
                reached = reached.max(begin + got as u64);
            }

            let base = (begin - position) as usize;
            for (frame, samples) in block.chunks_exact(channels).enumerate() {
                // A mono source belongs in the middle, not hard left.
                let (left, right) = if channels == 1 {
                    (samples[0], samples[0])
                } else {
                    (samples[0], samples[1])
                };
                let out = (base + frame) * 2;
                sums[out] += (left as f32 * gain) as i32;
                sums[out + 1] += (right as f32 * gain) as i32;
            }
        }

        let valid = if more_to_come {
            window
        } else {
            reached - position
        };
        if valid == 0 {
            break;
        }

        mixed.clear();
        for sum in &sums[..valid as usize * 2] {
            if *sum > i16::MAX as i32 || *sum < i16::MIN as i32 {
                clipped = true;
            }
            mixed.push((*sum).clamp(i16::MIN as i32, i16::MAX as i32) as i16);
        }
        if clipped && stop_on_clip {
            return Ok(MixOutcome { frames, clipped });
        }
        emit(&mixed)?;
        frames += valid;
        position = window_end;
    }

    Ok(MixOutcome { frames, clipped })
}

/// How many points a waveform is reduced to.
///
/// Enough to look like the audio at any window width, small enough to send as
/// part of a page load. The player scales it to whatever space it has.
pub const WAVEFORM_POINTS: usize = 800;

/// Reduces audio to a peak per bucket, for drawing, as it streams past.
///
/// Peaks rather than averages: averaging washes speech down to a flat band,
/// because a waveform's mean over a bucket is near zero regardless of how loud
/// it was. The peak is what makes speech look like speech.
///
/// The bucket size comes from the expected length, so a correct estimate gives
/// exactly the buckets a whole-buffer reduction would. Should the audio run
/// longer than expected, adjacent buckets are merged pairwise and the bucket
/// size doubles: the point count stays bounded whatever the estimate said.
struct PeakMeter {
    points: usize,
    frames_per_point: u64,
    peaks: Vec<f32>,
    current: f32,
    filled: u64,
}

impl PeakMeter {
    fn new(expected_frames: u64, points: usize) -> Self {
        Self {
            points,
            frames_per_point: expected_frames.div_ceil(points.max(1) as u64).max(1),
            peaks: Vec::with_capacity(points + 1),
            current: 0.0,
            filled: 0,
        }
    }

    fn push(&mut self, samples: &[i16], channels: u16) {
        if self.points == 0 {
            return;
        }
        for frame in samples.chunks_exact(channels.max(1) as usize) {
            let peak = frame
                .iter()
                .map(|s| s.saturating_abs() as f32)
                .fold(0.0f32, f32::max);
            self.current = self.current.max(peak / i16::MAX as f32);
            self.filled += 1;
            if self.filled == self.frames_per_point {
                self.close_bucket();
            }
        }
    }

    fn close_bucket(&mut self) {
        self.peaks.push(self.current);
        self.current = 0.0;
        self.filled = 0;
        if self.peaks.len() > self.points {
            let leftover = (self.peaks.len() % 2 == 1).then(|| self.peaks[self.peaks.len() - 1]);
            self.peaks = self
                .peaks
                .chunks_exact(2)
                .map(|pair| pair[0].max(pair[1]))
                .collect();
            // An unpaired last bucket becomes the start of the next, larger one.
            if let Some(last) = leftover {
                self.current = last;
                self.filled = self.frames_per_point;
            }
            self.frames_per_point *= 2;
        }
    }

    fn finish(mut self) -> Vec<f32> {
        if self.filled > 0 {
            self.close_bucket();
            if self.filled > 0 {
                // Merging left a partial bucket behind; it is the last point.
                self.peaks.push(self.current);
            }
        }
        self.peaks
    }
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
    let key = prefix
        .export(&format!("{}.asr.wav", track.track_id))
        .context("building transcription audio key")?;

    // Rebuilding this decodes and resamples the whole track, and a retry would
    // repeat that work for no gain: the chunks it derives from are immutable.
    if store.exists(&key).unwrap_or(false) {
        return Ok(key);
    }

    let mut reader = TrackReader::new(store, track)?;
    let source_rate = reader.sample_rate;
    let channels = reader.channels;

    let mut out = PendingBlob::create(store, &key)
        .with_context(|| format!("writing transcription audio for {}", track.track_id))?;
    let mut wav = WavWriter::new(out.writer(), ASR_SAMPLE_RATE_HZ)?;

    // Converting in one-second windows keeps memory flat, and a whole second
    // is an exact multiple of any sensible rate ratio, so resampling each
    // window independently introduces no boundary artefact.
    let window = reader.window();
    loop {
        let block = reader.take(window)?;
        if block.is_empty() {
            break;
        }
        let mono = downmix_to_mono(&block, channels);
        wav.write_samples(&resample(&mono, source_rate, ASR_SAMPLE_RATE_HZ))?;
    }
    wav.finish()
        .with_context(|| format!("writing transcription audio for {}", track.track_id))?;
    out.commit()
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

/// Size of a canonical PCM WAV header.
const WAV_HEADER_BYTES: u64 = 44;

/// The header of a mono 16-bit PCM WAV file holding `data_bytes` of samples.
fn wav_header(sample_rate_hz: u32, data_bytes: u32) -> [u8; WAV_HEADER_BYTES as usize] {
    let mut header = [0u8; WAV_HEADER_BYTES as usize];
    let fields: [&[u8]; 12] = [
        b"RIFF",
        &(36 + data_bytes).to_le_bytes(),
        b"WAVEfmt ",
        &16u32.to_le_bytes(), // PCM header size
        &1u16.to_le_bytes(),  // uncompressed
        &1u16.to_le_bytes(),  // mono
        &sample_rate_hz.to_le_bytes(),
        &(sample_rate_hz * 2).to_le_bytes(), // bytes per second
        &2u16.to_le_bytes(),                 // block align
        &16u16.to_le_bytes(),                // bits per sample
        b"data",
        &data_bytes.to_le_bytes(),
    ];
    let mut at = 0;
    for field in fields {
        header[at..at + field.len()].copy_from_slice(field);
        at += field.len();
    }
    header
}

/// Writes mono 16-bit WAV as samples arrive.
///
/// The header states the data size up front. Rather than trusting a size
/// computed from the manifest, which can disagree with what the chunks
/// actually decode to, a placeholder goes first and the real size is written
/// back once the samples are out.
struct WavWriter<W: Write + Seek> {
    out: W,
    sample_rate_hz: u32,
    start: u64,
    data_bytes: u64,
    bytes: Vec<u8>,
}

impl<W: Write + Seek> WavWriter<W> {
    fn new(mut out: W, sample_rate_hz: u32) -> Result<Self> {
        let start = out.stream_position().context("locating the WAV start")?;
        out.write_all(&wav_header(sample_rate_hz, 0))
            .context("writing the WAV header")?;
        Ok(Self {
            out,
            sample_rate_hz,
            start,
            data_bytes: 0,
            bytes: Vec::new(),
        })
    }

    fn write_samples(&mut self, samples: &[i16]) -> Result<()> {
        self.bytes.clear();
        self.bytes.extend(samples.iter().flat_map(|s| s.to_le_bytes()));
        self.out
            .write_all(&self.bytes)
            .context("writing WAV samples")?;
        self.data_bytes += self.bytes.len() as u64;
        Ok(())
    }

    fn finish(mut self) -> Result<W> {
        // RIFF sizes are 32-bit. At 16 kHz mono that is over 37 hours, far past
        // any recording, but a wrapped size would make the file unreadable
        // rather than merely long, so it is refused outright.
        let data_bytes = u32::try_from(self.data_bytes)
            .ok()
            .filter(|b| *b <= u32::MAX - 36)
            .context("the transcription audio is too long for a WAV file")?;
        let end = self.out.stream_position().context("locating the WAV end")?;
        self.out
            .seek(SeekFrom::Start(self.start))
            .context("seeking back to the WAV header")?;
        self.out
            .write_all(&wav_header(self.sample_rate_hz, data_bytes))
            .context("patching the WAV header")?;
        self.out
            .seek(SeekFrom::Start(end))
            .context("returning to the WAV end")?;
        self.out.flush().context("flushing the WAV file")?;
        Ok(self.out)
    }
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
    use kaseta_contracts::manifest::{
        Chunk, ClockDomain, MediaType, TrackFormat, TrackRole, TrackSource,
    };
    use kaseta_contracts::{RecordingPrefix, TrackId};
    use tempfile::TempDir;
    use time::macros::datetime;
    use ulid::Ulid;

    fn encode_flac_samples(samples: &[i16], sample_rate_hz: u32, channels: u16) -> Result<Vec<u8>> {
        let mut writer =
            FlacFileWriter::new(std::io::Cursor::new(Vec::new()), sample_rate_hz, channels)?;
        writer.write_samples(samples)?;
        Ok(writer.finish()?.0.into_inner())
    }

    fn encode_wav(samples: &[i16], sample_rate_hz: u32) -> Vec<u8> {
        let mut wav = WavWriter::new(std::io::Cursor::new(Vec::new()), sample_rate_hz).unwrap();
        wav.write_samples(samples).unwrap();
        wav.finish().unwrap().into_inner()
    }

    fn peaks_from(samples: &[i16], channels: u16, points: usize) -> Vec<f32> {
        let frames = samples.len() / channels.max(1) as usize;
        let mut meter = PeakMeter::new(frames as u64, points);
        meter.push(samples, channels);
        meter.finish()
    }

    /// Files under the store that are not objects: anything an export left
    /// behind.
    fn temporaries(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut out = Vec::new();
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            for entry in std::fs::read_dir(&d).unwrap().flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.to_string_lossy().ends_with(".tmp") {
                    out.push(path);
                }
            }
        }
        out
    }

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
            source: None,
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
    fn a_waveform_reflects_where_the_audio_is_loud() {
        // Quiet first half, loud second half. Averaging would flatten both to
        // nearly nothing, because a waveform's mean is near zero either way.
        let mut samples = vec![100i16; 1_000];
        samples.extend(vec![20_000i16; 1_000]);

        let peaks = peaks_from(&samples, 1, 4);
        assert_eq!(peaks.len(), 4);
        assert!(peaks[0] < 0.05, "the quiet part should read quiet");
        assert!(peaks[3] > 0.5, "the loud part should read loud");
    }

    #[test]
    fn waveform_values_stay_within_range() {
        let peaks = peaks_from(&[i16::MIN, i16::MAX, 0], 1, 3);
        for p in peaks {
            assert!((0.0..=1.0).contains(&p), "{p} is outside 0..1");
        }
    }

    #[test]
    fn a_waveform_of_nothing_is_empty_rather_than_a_panic() {
        assert!(peaks_from(&[], 1, 100).is_empty());
        assert!(peaks_from(&[1, 2, 3], 1, 0).is_empty());
    }

    #[test]
    fn short_audio_still_produces_a_waveform() {
        // Fewer frames than requested points must not divide by zero or loop.
        let peaks = peaks_from(&[1_000i16; 3], 1, 800);
        assert!(!peaks.is_empty());
        assert!(peaks.len() <= 800);
    }

    #[test]
    fn gap_padding_rounds_rather_than_truncating() {
        // A gap that is not a whole number of frames must not lose a sample
        // each time, which would accumulate over a long recording.
        assert_eq!(ns_to_frames(500_000_000, 48_000), 24_000);
        assert_eq!(ns_to_frames(20_833, 48_000), 1);
        assert_eq!(ns_to_frames(0, 48_000), 0);
    }

    #[test]
    fn a_track_that_starts_after_another_ends_keeps_the_silence_between() {
        // One second of mic, then nothing, then the remote side two seconds
        // in. The mix must hold the second of silence, not close it up.
        let (_dir, store, prefix) = setup();
        let manifest = two_track_manifest(
            &store,
            &prefix,
            (vec![1_000i16; 48_000], 1, 0),
            (vec![2_000i16; 96_000], 2, 2_000_000_000),
        );

        let mixed = mix_recording(&store, &manifest, &prefix).unwrap().unwrap();
        let decoded = decode_flac(&store.get(&mixed.key).unwrap(), 2).unwrap();

        assert_eq!(mixed.frames, 3 * 48_000);
        assert_eq!(decoded[0], 1_000);
        assert_eq!(decoded[(48_000 + 10) * 2], 0, "the gap is silence");
        assert_eq!(decoded[(2 * 48_000 + 10) * 2], 2_000);
    }

    #[test]
    fn a_streamed_mix_equals_summing_whole_tracks() {
        // Several windows, an offset that is not a whole window, and stereo
        // against mono: compared with the obvious whole-buffer computation.
        let (_dir, store, prefix) = setup();
        let mic: Vec<i16> = tone(150_000, 1).iter().map(|s| s / 2).collect();
        let remote: Vec<i16> = tone(100_000, 2).iter().map(|s| s / 3).collect();
        let offset_frames = 30_001usize;
        let offset_ns = offset_frames as u64 * 1_000_000_000 / 48_000 + 1;
        let manifest = two_track_manifest(
            &store,
            &prefix,
            (mic.clone(), 1, 0),
            (remote.clone(), 2, offset_ns),
        );

        let mixed = mix_recording(&store, &manifest, &prefix).unwrap().unwrap();
        let decoded = decode_flac(&store.get(&mixed.key).unwrap(), 2).unwrap();

        let start = ns_to_frames(offset_ns, 48_000) as usize;
        let frames = (start + 100_000).max(150_000);
        let mut expected = vec![0i32; frames * 2];
        for (i, s) in mic.iter().enumerate() {
            expected[i * 2] += *s as i32;
            expected[i * 2 + 1] += *s as i32;
        }
        for (i, s) in remote.iter().enumerate() {
            expected[start * 2 + i] += *s as i32;
        }
        let expected: Vec<i16> = expected.iter().map(|s| *s as i16).collect();
        assert_eq!(mixed.frames as usize, frames);
        assert_eq!(decoded, expected);
    }

    #[test]
    fn exports_leave_no_temporary_files() {
        let (dir, store, prefix) = setup();
        // Loud enough to clip, so the abandoned full-gain pass is exercised.
        let manifest = two_track_manifest(
            &store,
            &prefix,
            (vec![30_000i16; 60_000], 1, 0),
            (vec![30_000i16; 120_000], 2, 0),
        );

        merge_recording(&store, &manifest, &prefix).unwrap();
        let mixed = mix_recording(&store, &manifest, &prefix).unwrap().unwrap();
        write_asr_audio(&store, &manifest.tracks[0], &prefix).unwrap();

        assert!(mixed.gain < 1.0);
        assert!(
            temporaries(dir.path()).is_empty(),
            "left behind: {:?}",
            temporaries(dir.path())
        );
    }

    #[test]
    fn a_failed_export_leaves_neither_an_object_nor_a_temporary() {
        let (dir, store, prefix) = setup();
        let track = track_with_chunks(&store, &prefix, 1, &[(tone(1_000, 1), 0)]);
        store
            .put(
                &track.chunks[0].blob,
                &encode_flac_samples(&vec![42i16; 1_000], 48_000, 1).unwrap(),
            )
            .unwrap();

        assert!(merge_track(&store, &track, &prefix).is_err());
        let key = prefix.export("a_local-mic_01.flac").unwrap();
        assert!(!store.exists(&key).unwrap());
        assert!(temporaries(dir.path()).is_empty());
    }

    #[test]
    fn transcription_audio_declares_what_it_actually_holds() {
        // The manifest's sample counts are wrong here. The WAV header must
        // describe the samples written, not the ones promised.
        let (_dir, store, prefix) = setup();
        let mut track =
            track_with_chunks(&store, &prefix, 2, &[(tone(48_000, 2), 0), (tone(24_000, 2), 0)]);
        track.chunks[0].sample_count = None;
        track.chunks[1].sample_count = Some(1);

        let key = write_asr_audio(&store, &track, &prefix).unwrap();
        let wav = store.get(&key).unwrap();

        let declared = u32::from_le_bytes([wav[40], wav[41], wav[42], wav[43]]) as usize;
        assert_eq!(declared, wav.len() - 44);
        assert_eq!(declared, 24_000 * 2, "1.5 s at 16 kHz mono, 16-bit");
        let riff = u32::from_le_bytes([wav[4], wav[5], wav[6], wav[7]]) as usize;
        assert_eq!(riff, wav.len() - 8);
    }

    #[test]
    fn a_waveform_stays_bounded_when_the_audio_outlasts_its_estimate() {
        // The estimate comes from the manifest and could be wrong. However
        // long the audio turns out to be, the point count must not grow with it.
        let mut meter = PeakMeter::new(1_000, 100);
        let mut loud_at_end = vec![0i16; 99_000];
        loud_at_end.extend(vec![30_000i16; 1_000]);
        meter.push(&loud_at_end, 1);
        let peaks = meter.finish();

        assert!(peaks.len() <= 100, "{} points", peaks.len());
        assert!(peaks.len() >= 50, "{} points: resolution was lost", peaks.len());
        assert!(peaks[0] < 0.01);
        assert!(*peaks.last().unwrap() > 0.9, "the loud end survives merging");
    }

    #[test]
    fn a_waveform_matches_a_whole_buffer_reduction_when_the_estimate_is_right() {
        let samples: Vec<i16> = (0..10_007).map(|i| ((i * 37) % 20_000) as i16).collect();
        let peaks = peaks_from(&samples, 1, 800);

        let per_point = 10_007usize.div_ceil(800);
        let expected: Vec<f32> = samples
            .chunks(per_point)
            .map(|b| b.iter().map(|s| s.saturating_abs() as f32).fold(0.0, f32::max) / i16::MAX as f32)
            .collect();
        assert_eq!(peaks, expected);
    }
}
