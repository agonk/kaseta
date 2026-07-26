//! Turns a stream of captured audio buffers into sealed, timestamped chunks.
//!
//! This is the durability and timing core of capture, kept free of any PipeWire
//! types so it can be tested without an audio server. The PipeWire thread does
//! nothing but hand buffers to a [`ChunkWriter`] and persist whatever it seals.
//!
//! # Why chunks
//!
//! A chunk is the unit of durability, upload, retry, and dedup. Sealing every
//! few seconds means a crash costs at most one chunk instead of the meeting,
//! and it bounds resident memory to one chunk's worth of samples regardless of
//! how long the recording runs.
//!
//! # Why timestamps rather than sample counts
//!
//! A microphone and a sink monitor run on independent hardware clocks and will
//! drift apart over a long meeting. Every chunk therefore records the canonical
//! clock at its first and last sample, so alignment is computed from measured
//! time rather than assumed from sample counts. Drift stays *correctable* rather
//! than silently baked into the audio.

use anyhow::Result;

use crate::blobstore::sha256_hex;
use crate::clock::samples_to_ns;

/// How much audio accumulates before a chunk is sealed.
///
/// Trades crash exposure against per-chunk overhead. Fifteen seconds keeps
/// worst-case loss small and resident buffers around 1.4 MB per mono 48 kHz
/// track, while producing few enough objects that a long meeting does not
/// generate thousands of uploads.
pub const DEFAULT_CHUNK_DURATION_S: u32 = 15;

/// A buffer whose arrival gap exceeds its own duration by more than this factor
/// is treated as a discontinuity rather than jitter.
const GAP_TOLERANCE: f64 = 1.5;

#[derive(Clone, Debug)]
pub struct ChunkConfig {
    pub sample_rate_hz: u32,
    pub channels: u16,
    pub chunk_duration_s: u32,
}

impl Default for ChunkConfig {
    fn default() -> Self {
        Self {
            sample_rate_hz: 48_000,
            channels: 1,
            chunk_duration_s: DEFAULT_CHUNK_DURATION_S,
        }
    }
}

impl ChunkConfig {
    /// Interleaved samples that fill one chunk.
    fn samples_per_chunk(&self) -> usize {
        self.sample_rate_hz as usize * self.chunk_duration_s as usize * self.channels as usize
    }
}

/// A chunk ready to be persisted.
#[derive(Clone, Debug)]
pub struct SealedChunk {
    pub seq: u32,
    pub encoded: Vec<u8>,
    pub sha256: String,
    /// Frames per channel, not interleaved samples.
    pub sample_count: u64,
    pub boottime_start_ns: u64,
    pub boottime_end_ns: u64,
    pub source_pts_start_ns: Option<u64>,
    pub source_pts_end_ns: Option<u64>,
    /// The capture stream broke before this chunk; it does not continue
    /// seamlessly from the previous one.
    pub discontinuity: bool,
    pub gap_before_ns: u64,
    /// Buffers the audio server had ready but that were never collected.
    pub drops_before_chunk: u64,
}

/// One captured buffer as delivered by the audio server.
#[derive(Clone, Debug)]
pub struct CapturedBuffer<'a> {
    /// Interleaved samples.
    pub samples: &'a [i16],
    /// Canonical clock reading at the moment the buffer was received.
    pub arrived_at_ns: u64,
    /// The stream's own presentation timestamp, when reported. Retained to
    /// cross-check the canonical clock rather than to drive alignment.
    pub source_pts_ns: Option<u64>,
}

pub struct ChunkWriter {
    config: ChunkConfig,
    /// Interleaved samples awaiting the next seal.
    pending: Vec<i16>,
    next_seq: u32,
    /// Canonical clock at the first sample of the pending chunk.
    pending_start_ns: Option<u64>,
    /// Canonical clock at the last sample received.
    pending_end_ns: u64,
    pending_pts_start_ns: Option<u64>,
    pending_pts_end_ns: Option<u64>,
    /// Set when a gap was detected before the pending chunk began.
    pending_discontinuity: bool,
    pending_gap_ns: u64,
    /// Arrival time of the previous buffer, for gap detection.
    last_arrival_ns: Option<u64>,
    /// Loudest absolute sample observed across the whole track.
    peak: i16,
    /// Buffers lost before the pending chunk was sealed.
    pending_drops: u64,
}

impl ChunkWriter {
    pub fn new(config: ChunkConfig) -> Self {
        let capacity = config.samples_per_chunk();
        Self {
            config,
            pending: Vec::with_capacity(capacity),
            next_seq: 0,
            pending_start_ns: None,
            pending_end_ns: 0,
            pending_pts_start_ns: None,
            pending_pts_end_ns: None,
            pending_discontinuity: false,
            pending_gap_ns: 0,
            last_arrival_ns: None,
            peak: 0,
            pending_drops: 0,
        }
    }

    pub fn config(&self) -> &ChunkConfig {
        &self.config
    }

    /// Frames currently buffered and not yet sealed. Drives the level meter.
    #[allow(dead_code)]
    pub fn pending_frames(&self) -> usize {
        self.pending.len() / self.config.channels.max(1) as usize
    }

    /// Records that the audio server had a buffer ready that was never taken.
    ///
    /// This loss is invisible to gap detection: the following buffer can still
    /// arrive on schedule, so elapsed time looks normal even though audio is
    /// missing. Counting it explicitly is the only way it surfaces.
    pub fn note_drop(&mut self) {
        self.pending_drops = self.pending_drops.saturating_add(1);
    }

    /// Loudest sample seen so far, as a fraction of full scale.
    ///
    /// Distinguishes "recorded silence" from "recorded nothing", which are
    /// otherwise indistinguishable: a silent track and a correctly captured one
    /// both produce chunks, and FLAC compresses silence to almost nothing.
    pub fn peak_level(&self) -> f32 {
        self.peak as f32 / i16::MAX as f32
    }

    /// Adopts a new format mid-stream, sealing whatever is buffered.
    ///
    /// A Bluetooth headset switching between its playback and headset profiles
    /// renegotiates the stream format while recording. The sequence counter must
    /// survive that: restarting it at zero would target keys that already hold
    /// different audio, and the storage layer refuses to overwrite them.
    ///
    /// The buffered remainder is sealed under the *old* format before switching,
    /// because its samples were captured at the old rate. The next chunk is
    /// marked discontinuous, since the two formats do not join seamlessly.
    pub fn reconfigure(&mut self, config: ChunkConfig) -> Result<Option<SealedChunk>> {
        let tail = self.flush()?;

        self.config = config;
        self.pending = Vec::with_capacity(self.config.samples_per_chunk());
        self.pending_start_ns = None;
        self.pending_end_ns = 0;
        self.pending_pts_start_ns = None;
        self.pending_pts_end_ns = None;
        self.last_arrival_ns = None;
        // `next_seq` is deliberately preserved.
        self.pending_discontinuity = true;
        self.pending_gap_ns = 0;

        Ok(tail)
    }

    /// Accepts a buffer, sealing and returning chunks as they fill.
    ///
    /// A single oversized buffer can complete more than one chunk, so this
    /// returns a vector rather than an option.
    pub fn push(&mut self, buffer: CapturedBuffer<'_>) -> Result<Vec<SealedChunk>> {
        let frames = buffer.samples.len() / self.config.channels.max(1) as usize;
        let buffer_duration_ns = samples_to_ns(frames as u64, self.config.sample_rate_hz);

        let mut sealed = Vec::new();

        // A hole in the audio must become a chunk boundary, not a flag on a
        // chunk that already holds pre-gap audio. Appending across a gap would
        // let `finish` interpolate timestamps as though the missing time never
        // elapsed, placing every later sample in the chunk too early.
        if let Some(missing_ns) = self.detect_gap(buffer.arrived_at_ns, buffer_duration_ns) {
            if let Some(chunk) = self.flush()? {
                sealed.push(chunk);
            }
            // Applies to the chunk that starts *after* the hole.
            self.pending_discontinuity = true;
            self.pending_gap_ns = missing_ns;
        }

        if self.pending_start_ns.is_none() {
            // A buffer arrives once its audio has been captured, so the first
            // sample precedes arrival by the buffer's own duration.
            self.pending_start_ns = Some(buffer.arrived_at_ns.saturating_sub(buffer_duration_ns));
            self.pending_pts_start_ns = buffer
                .source_pts_ns
                .map(|pts| pts.saturating_sub(buffer_duration_ns));
        }
        self.pending_end_ns = buffer.arrived_at_ns;
        self.pending_pts_end_ns = buffer.source_pts_ns.or(self.pending_pts_end_ns);
        self.last_arrival_ns = Some(buffer.arrived_at_ns);

        for sample in buffer.samples {
            // `saturating_abs` keeps i16::MIN from wrapping to a negative peak.
            self.peak = self.peak.max(sample.saturating_abs());
        }
        self.pending.extend_from_slice(buffer.samples);

        while self.pending.len() >= self.config.samples_per_chunk() {
            sealed.push(self.seal_full_chunk()?);
        }
        Ok(sealed)
    }

    /// Reports how much audio went missing before this buffer, if any.
    ///
    /// Under-runs and device changes leave a hole. Measuring it here, and
    /// letting the caller turn it into a chunk boundary, is what stops every
    /// timestamp after the glitch from being wrong.
    fn detect_gap(&self, arrived_at_ns: u64, buffer_duration_ns: u64) -> Option<u64> {
        let last = self.last_arrival_ns?;
        if buffer_duration_ns == 0 {
            return None;
        }
        let elapsed = arrived_at_ns.saturating_sub(last);
        let tolerated = (buffer_duration_ns as f64 * GAP_TOLERANCE) as u64;
        if elapsed > tolerated {
            Some(elapsed.saturating_sub(buffer_duration_ns))
        } else {
            None
        }
    }

    /// Seals exactly one chunk's worth of samples, retaining any excess.
    fn seal_full_chunk(&mut self) -> Result<SealedChunk> {
        let take = self.config.samples_per_chunk();
        let remainder = self.pending.split_off(take);
        let samples = std::mem::replace(&mut self.pending, remainder);

        // The sealed chunk covers only part of the buffered span; the rest
        // belongs to the next chunk. Interpolate the boundary from the sample
        // counts so no time is lost or double-counted between chunks.
        let sealed_frames = (samples.len() / self.config.channels.max(1) as usize) as u64;
        let start_ns = self.pending_start_ns.unwrap_or(self.pending_end_ns);
        let boundary_ns = start_ns + samples_to_ns(sealed_frames, self.config.sample_rate_hz);

        // Both clocks are interpolated to the same boundary. Reading the last
        // buffer's PTS instead would be wrong for every chunk except the one
        // the buffer actually ended in.
        let pts_start = self.pending_pts_start_ns;
        let pts_boundary =
            pts_start.map(|pts| pts + samples_to_ns(sealed_frames, self.config.sample_rate_hz));

        let chunk = self.finish(samples, start_ns, boundary_ns, pts_start, pts_boundary)?;

        // The retained remainder begins exactly where the sealed chunk ended,
        // on both clocks. The stream's own timestamp advances with the samples,
        // so it is interpolated the same way rather than snapped to the last
        // buffer's value — which would be wrong whenever a boundary falls
        // inside a buffer.
        self.pending_start_ns = Some(boundary_ns);
        self.pending_pts_start_ns = pts_boundary;
        self.pending_discontinuity = false;
        self.pending_gap_ns = 0;

        Ok(chunk)
    }

    /// Seals whatever remains, ending the track.
    ///
    /// Returns `None` when nothing is buffered, so stopping a recording that has
    /// just sealed a chunk does not emit an empty one.
    pub fn flush(&mut self) -> Result<Option<SealedChunk>> {
        if self.pending.is_empty() {
            return Ok(None);
        }
        let samples = std::mem::take(&mut self.pending);
        let start_ns = self.pending_start_ns.unwrap_or(self.pending_end_ns);
        let end_ns = self.pending_end_ns.max(start_ns);
        let frames = (samples.len() / self.config.channels.max(1) as usize) as u64;
        let pts_start = self.pending_pts_start_ns;
        let pts_end =
            pts_start.map(|pts| pts + samples_to_ns(frames, self.config.sample_rate_hz));

        let chunk = self.finish(samples, start_ns, end_ns, pts_start, pts_end)?;
        self.pending_start_ns = None;
        self.pending_discontinuity = false;
        self.pending_gap_ns = 0;
        Ok(Some(chunk))
    }

    fn finish(
        &mut self,
        samples: Vec<i16>,
        start_ns: u64,
        end_ns: u64,
        pts_start: Option<u64>,
        pts_end: Option<u64>,
    ) -> Result<SealedChunk> {
        let frames = (samples.len() / self.config.channels.max(1) as usize) as u64;
        let encoded = encode_flac(&samples, &self.config)?;
        let sha256 = sha256_hex(&encoded);

        let chunk = SealedChunk {
            seq: self.next_seq,
            encoded,
            sha256,
            sample_count: frames,
            boottime_start_ns: start_ns,
            boottime_end_ns: end_ns,
            source_pts_start_ns: pts_start,
            source_pts_end_ns: pts_end,
            discontinuity: self.pending_discontinuity,
            gap_before_ns: self.pending_gap_ns,
            drops_before_chunk: self.pending_drops,
        };

        self.pending_drops = 0;
        self.next_seq += 1;
        Ok(chunk)
    }
}

/// Encodes interleaved 16-bit samples as FLAC.
///
/// FLAC is lossless, so re-encoding for a different ASR model never compounds
/// quality loss, and it halves the storage a WAV archive would need.
fn encode_flac(samples: &[i16], config: &ChunkConfig) -> Result<Vec<u8>> {
    use flacenc::component::BitRepr;
    use flacenc::error::Verify;

    let channels = config.channels.max(1) as usize;
    // flacenc works in i32 regardless of the source bit depth.
    let widened: Vec<i32> = samples.iter().map(|s| *s as i32).collect();

    let encoder_config = flacenc::config::Encoder::default()
        .into_verified()
        .map_err(|e| anyhow::anyhow!("invalid FLAC encoder config: {e:?}"))?;

    let source = flacenc::source::MemSource::from_samples(
        &widened,
        channels,
        16,
        config.sample_rate_hz as usize,
    );

    let stream = flacenc::encode_with_fixed_block_size(
        &encoder_config,
        source,
        encoder_config.block_size,
    )
    .map_err(|e| anyhow::anyhow!("FLAC encoding failed: {e:?}"))?;

    let mut sink = flacenc::bitsink::ByteSink::new();
    stream
        .write(&mut sink)
        .map_err(|e| anyhow::anyhow!("writing FLAC stream failed: {e:?}"))?;

    Ok(sink.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A short chunk duration keeps tests fast while exercising the same paths.
    fn config() -> ChunkConfig {
        ChunkConfig {
            sample_rate_hz: 48_000,
            channels: 1,
            chunk_duration_s: 1,
        }
    }

    /// One second of a recognisable tone, so decoded output can be compared.
    fn tone(frames: usize) -> Vec<i16> {
        (0..frames)
            .map(|i| ((i as f64 * 0.05).sin() * 8000.0) as i16)
            .collect()
    }

    fn buffer(samples: &[i16], arrived_at_ns: u64) -> CapturedBuffer<'_> {
        CapturedBuffer {
            samples,
            arrived_at_ns,
            source_pts_ns: None,
        }
    }

    #[test]
    fn buffers_below_the_threshold_seal_nothing() {
        let mut w = ChunkWriter::new(config());
        let half = tone(24_000);
        assert!(w.push(buffer(&half, 500_000_000)).unwrap().is_empty());
        assert_eq!(w.pending_frames(), 24_000);
    }

    #[test]
    fn seals_a_chunk_once_a_full_second_accumulates() {
        let mut w = ChunkWriter::new(config());
        let half = tone(24_000);

        w.push(buffer(&half, 500_000_000)).unwrap();
        let sealed = w.push(buffer(&half, 1_000_000_000)).unwrap();

        assert_eq!(sealed.len(), 1);
        let c = &sealed[0];
        assert_eq!(c.seq, 0);
        assert_eq!(c.sample_count, 48_000);
        assert!(!c.encoded.is_empty());
        assert_eq!(c.sha256.len(), 64);
        assert_eq!(w.pending_frames(), 0);
    }

    #[test]
    fn chunk_start_precedes_arrival_by_the_buffer_duration() {
        let mut w = ChunkWriter::new(config());
        let full = tone(48_000);

        // The buffer arrives at t=1s carrying the second of audio before it.
        let sealed = w.push(buffer(&full, 1_000_000_000)).unwrap();
        assert_eq!(sealed[0].boottime_start_ns, 0);
    }

    #[test]
    fn an_oversized_buffer_seals_several_chunks_at_once() {
        let mut w = ChunkWriter::new(config());
        let three_seconds = tone(144_000);

        let sealed = w.push(buffer(&three_seconds, 3_000_000_000)).unwrap();

        assert_eq!(sealed.len(), 3);
        assert_eq!(sealed.iter().map(|c| c.seq).collect::<Vec<_>>(), vec![0, 1, 2]);
        for c in &sealed {
            assert_eq!(c.sample_count, 48_000);
        }
    }

    #[test]
    fn chunk_boundaries_are_contiguous_and_lose_no_time() {
        let mut w = ChunkWriter::new(config());
        let three_seconds = tone(144_000);
        let sealed = w.push(buffer(&three_seconds, 3_000_000_000)).unwrap();

        for pair in sealed.windows(2) {
            assert_eq!(
                pair[0].boottime_end_ns, pair[1].boottime_start_ns,
                "a gap or overlap between chunks would misplace every later word"
            );
        }
        assert_eq!(sealed[0].boottime_start_ns, 0);
        assert_eq!(sealed[2].boottime_end_ns, 3_000_000_000);
    }

    #[test]
    fn sequence_numbers_never_repeat_across_seals_and_flush() {
        let mut w = ChunkWriter::new(config());
        let full = tone(48_000);
        let partial = tone(1_000);

        w.push(buffer(&full, 1_000_000_000)).unwrap();
        w.push(buffer(&full, 2_000_000_000)).unwrap();
        w.push(buffer(&partial, 2_020_000_000)).unwrap();
        let tail = w.flush().unwrap().unwrap();

        assert_eq!(tail.seq, 2);
        assert_eq!(tail.sample_count, 1_000);
    }

    #[test]
    fn flushing_an_empty_writer_emits_nothing() {
        let mut w = ChunkWriter::new(config());
        assert!(w.flush().unwrap().is_none());

        // Sealing exactly on the boundary must not leave a zero-length chunk.
        let full = tone(48_000);
        w.push(buffer(&full, 1_000_000_000)).unwrap();
        assert!(w.flush().unwrap().is_none());
    }

    #[test]
    fn a_dropout_forces_a_chunk_boundary() {
        let mut w = ChunkWriter::new(config());
        let quarter = tone(12_000); // 250 ms per buffer

        assert!(w.push(buffer(&quarter, 250_000_000)).unwrap().is_empty());

        // The next buffer should arrive at 500 ms; it arrives at 2 s instead,
        // so 1.5 s of audio never reached us.
        let sealed = w.push(buffer(&quarter, 2_000_000_000)).unwrap();

        assert_eq!(
            sealed.len(),
            1,
            "audio captured before the hole must be sealed at the hole"
        );
        assert!(
            !sealed[0].discontinuity,
            "the gap follows this chunk; it does not precede it"
        );
        assert_eq!(sealed[0].sample_count, 12_000);

        let after = w.flush().unwrap().unwrap();
        assert!(after.discontinuity, "the chunk after the hole carries the flag");
        assert!(
            after.gap_before_ns >= 1_400_000_000,
            "expected ~1.5s missing, recorded {}ns",
            after.gap_before_ns
        );
    }

    #[test]
    fn audio_after_a_gap_is_not_timestamped_as_though_the_gap_never_happened() {
        // Concatenating across a hole makes `finish` interpolate as if the
        // missing time never elapsed, placing every later sample too early.
        let mut w = ChunkWriter::new(config());
        let quarter = tone(12_000);

        w.push(buffer(&quarter, 250_000_000)).unwrap();
        let before = w.push(buffer(&quarter, 2_000_000_000)).unwrap();
        assert_eq!(before[0].boottime_end_ns, 250_000_000);

        let after = w.flush().unwrap().unwrap();
        assert_eq!(
            after.boottime_start_ns, 1_750_000_000,
            "post-gap audio must start after the hole, not where the pre-gap audio ended"
        );
    }

    #[test]
    fn a_chunk_split_inside_a_buffer_interpolates_the_source_timestamp() {
        let mut w = ChunkWriter::new(config());
        // Three seconds delivered at once: boundaries fall inside the buffer.
        let three = tone(144_000);
        let sealed = w
            .push(CapturedBuffer {
                samples: &three,
                arrived_at_ns: 3_000_000_000,
                source_pts_ns: Some(3_000_000_000),
            })
            .unwrap();

        assert_eq!(sealed.len(), 3);
        for (i, c) in sealed.iter().enumerate() {
            assert_eq!(
                c.source_pts_start_ns,
                Some(i as u64 * 1_000_000_000),
                "chunk {i} start PTS must advance with the samples"
            );
            assert_eq!(
                c.source_pts_end_ns,
                Some((i as u64 + 1) * 1_000_000_000),
                "chunk {i} end PTS must be interpolated, not the last buffer's value"
            );
        }
        // Adjacent chunks must meet exactly, or merged audio would gap or overlap.
        for pair in sealed.windows(2) {
            assert_eq!(pair[0].source_pts_end_ns, pair[1].source_pts_start_ns);
        }
    }

    #[test]
    fn ordinary_jitter_is_not_mistaken_for_a_dropout() {
        let mut w = ChunkWriter::new(config());
        let quarter = tone(12_000);

        // Buffers arriving slightly late, well within tolerance.
        w.push(buffer(&quarter, 250_000_000)).unwrap();
        w.push(buffer(&quarter, 505_000_000)).unwrap();
        w.push(buffer(&quarter, 760_000_000)).unwrap();
        let sealed = w.push(buffer(&quarter, 1_010_000_000)).unwrap();

        assert!(!sealed[0].discontinuity, "jitter must not flag a discontinuity");
        assert_eq!(sealed[0].gap_before_ns, 0);
    }

    #[test]
    fn a_discontinuity_does_not_leak_into_the_following_chunk() {
        let mut w = ChunkWriter::new(config());
        let full = tone(48_000);

        w.push(buffer(&full, 1_000_000_000)).unwrap();
        // A large gap, then two clean seconds.
        let first = w.push(buffer(&full, 5_000_000_000)).unwrap();
        assert!(first[0].discontinuity);

        let second = w.push(buffer(&full, 6_000_000_000)).unwrap();
        assert!(
            !second[0].discontinuity,
            "the flag belongs to the chunk after the gap, not to every later chunk"
        );
        assert_eq!(second[0].gap_before_ns, 0);
    }

    #[test]
    fn stereo_frames_are_counted_per_channel() {
        let mut w = ChunkWriter::new(ChunkConfig {
            channels: 2,
            ..config()
        });
        // 48000 frames of stereo is 96000 interleaved samples.
        let stereo = tone(96_000);
        let sealed = w.push(buffer(&stereo, 1_000_000_000)).unwrap();

        assert_eq!(sealed.len(), 1);
        assert_eq!(
            sealed[0].sample_count, 48_000,
            "sample_count is frames, not interleaved samples"
        );
    }

    #[test]
    fn a_format_change_does_not_restart_the_sequence_counter() {
        // A Bluetooth headset switching profile renegotiates mid-recording.
        // Restarting seq at zero would target keys already holding different
        // audio, and the store refuses to overwrite them — breaking the
        // recording outright.
        let mut w = ChunkWriter::new(config());
        let full = tone(48_000);

        w.push(buffer(&full, 1_000_000_000)).unwrap();
        w.push(buffer(&full, 2_000_000_000)).unwrap();

        // Headset drops to 16 kHz mono.
        let tail = w
            .reconfigure(ChunkConfig {
                sample_rate_hz: 16_000,
                channels: 1,
                chunk_duration_s: 1,
            })
            .unwrap();
        assert!(tail.is_none(), "nothing was buffered, so nothing to seal");

        let after = w.push(buffer(&tone(16_000), 3_000_000_000)).unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(
            after[0].seq, 2,
            "sequence must continue across a format change, not restart"
        );
    }

    #[test]
    fn a_format_change_seals_buffered_audio_under_the_old_format() {
        let mut w = ChunkWriter::new(config());
        // Half a second buffered at 48 kHz.
        w.push(buffer(&tone(24_000), 500_000_000)).unwrap();

        let tail = w
            .reconfigure(ChunkConfig {
                sample_rate_hz: 16_000,
                channels: 1,
                chunk_duration_s: 1,
            })
            .unwrap()
            .expect("buffered audio must be sealed, not discarded");

        assert_eq!(tail.seq, 0);
        assert_eq!(
            tail.sample_count, 24_000,
            "samples captured at the old rate must be sealed at the old rate"
        );
    }

    #[test]
    fn the_chunk_after_a_format_change_is_marked_discontinuous() {
        let mut w = ChunkWriter::new(config());
        w.push(buffer(&tone(48_000), 1_000_000_000)).unwrap();

        w.reconfigure(ChunkConfig {
            sample_rate_hz: 16_000,
            channels: 1,
            chunk_duration_s: 1,
        })
        .unwrap();

        let after = w.push(buffer(&tone(16_000), 2_000_000_000)).unwrap();
        assert!(
            after[0].discontinuity,
            "two formats do not join seamlessly; the seam must be recorded"
        );
    }

    #[test]
    fn peak_level_separates_silence_from_signal() {
        let mut w = ChunkWriter::new(config());
        assert_eq!(w.peak_level(), 0.0);

        w.push(buffer(&vec![0i16; 24_000], 500_000_000)).unwrap();
        assert_eq!(
            w.peak_level(),
            0.0,
            "digital silence must report no signal"
        );

        w.push(buffer(&tone(24_000), 1_000_000_000)).unwrap();
        assert!(
            w.peak_level() > 0.2,
            "an audible tone must register, got {}",
            w.peak_level()
        );
    }

    #[test]
    fn peak_level_does_not_wrap_on_the_most_negative_sample() {
        let mut w = ChunkWriter::new(config());
        w.push(buffer(&[i16::MIN, 0], 100_000_000)).unwrap();
        assert!(
            w.peak_level() > 0.99,
            "i16::MIN must saturate to full scale, got {}",
            w.peak_level()
        );
    }

    #[test]
    fn encodes_a_decodable_flac_stream() {
        let samples = tone(48_000);
        let encoded = encode_flac(&samples, &config()).unwrap();

        assert_eq!(&encoded[..4], b"fLaC", "must carry the FLAC stream marker");
        assert!(
            encoded.len() < samples.len() * 2,
            "lossless coding of a tone should beat raw PCM: {} vs {}",
            encoded.len(),
            samples.len() * 2
        );
    }

    #[test]
    fn identical_audio_encodes_to_an_identical_digest() {
        let samples = tone(48_000);
        let a = encode_flac(&samples, &config()).unwrap();
        let b = encode_flac(&samples, &config()).unwrap();
        assert_eq!(
            sha256_hex(&a),
            sha256_hex(&b),
            "encoding must be deterministic or idempotent re-upload breaks"
        );
    }
}
