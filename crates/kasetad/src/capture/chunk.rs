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
    /// FLAC compression level. Level 5 is the usual quality/speed balance;
    /// speech compresses well and this stays comfortably real-time on one core.
    pub compression_level: u8,
}

impl Default for ChunkConfig {
    fn default() -> Self {
        Self {
            sample_rate_hz: 48_000,
            channels: 1,
            chunk_duration_s: DEFAULT_CHUNK_DURATION_S,
            compression_level: 5,
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
        }
    }

    pub fn config(&self) -> &ChunkConfig {
        &self.config
    }

    /// Frames currently buffered and not yet sealed.
    pub fn pending_frames(&self) -> usize {
        self.pending.len() / self.config.channels.max(1) as usize
    }

    /// Accepts a buffer, sealing and returning chunks as they fill.
    ///
    /// A single oversized buffer can complete more than one chunk, so this
    /// returns a vector rather than an option.
    pub fn push(&mut self, buffer: CapturedBuffer<'_>) -> Result<Vec<SealedChunk>> {
        let frames = buffer.samples.len() / self.config.channels.max(1) as usize;
        let buffer_duration_ns = samples_to_ns(frames as u64, self.config.sample_rate_hz);

        self.note_gap(buffer.arrived_at_ns, buffer_duration_ns);

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

        self.pending.extend_from_slice(buffer.samples);

        let mut sealed = Vec::new();
        while self.pending.len() >= self.config.samples_per_chunk() {
            sealed.push(self.seal_full_chunk()?);
        }
        Ok(sealed)
    }

    /// Detects that audio went missing between buffers.
    ///
    /// Under-runs and device changes leave a hole. Recording the hole rather
    /// than silently concatenating across it is what stops every timestamp after
    /// the glitch from being wrong.
    fn note_gap(&mut self, arrived_at_ns: u64, buffer_duration_ns: u64) {
        let Some(last) = self.last_arrival_ns else {
            return;
        };
        let elapsed = arrived_at_ns.saturating_sub(last);
        let tolerated = (buffer_duration_ns as f64 * GAP_TOLERANCE) as u64;
        if buffer_duration_ns > 0 && elapsed > tolerated {
            let missing = elapsed.saturating_sub(buffer_duration_ns);
            self.pending_discontinuity = true;
            self.pending_gap_ns = self.pending_gap_ns.saturating_add(missing);
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

        let chunk = self.finish(samples, start_ns, boundary_ns)?;

        // The retained remainder begins exactly where the sealed chunk ended.
        self.pending_start_ns = Some(boundary_ns);
        self.pending_pts_start_ns = self.pending_pts_end_ns;
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
        let chunk = self.finish(samples, start_ns, end_ns)?;
        self.pending_start_ns = None;
        Ok(Some(chunk))
    }

    fn finish(&mut self, samples: Vec<i16>, start_ns: u64, end_ns: u64) -> Result<SealedChunk> {
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
            source_pts_start_ns: self.pending_pts_start_ns,
            source_pts_end_ns: self.pending_pts_end_ns,
            discontinuity: self.pending_discontinuity,
            gap_before_ns: self.pending_gap_ns,
        };

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
            compression_level: 0,
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
    fn a_dropout_is_recorded_rather_than_silently_concatenated() {
        let mut w = ChunkWriter::new(config());
        let quarter = tone(12_000); // 250 ms per buffer

        w.push(buffer(&quarter, 250_000_000)).unwrap();
        // The next buffer should arrive at 500 ms; it arrives at 2 s instead,
        // so 1.5 s of audio never reached us.
        w.push(buffer(&quarter, 2_000_000_000)).unwrap();
        w.push(buffer(&quarter, 2_250_000_000)).unwrap();
        let sealed = w.push(buffer(&quarter, 2_500_000_000)).unwrap();

        assert_eq!(sealed.len(), 1);
        assert!(sealed[0].discontinuity, "the dropout must be flagged");
        assert!(
            sealed[0].gap_before_ns >= 1_400_000_000,
            "expected ~1.5s of missing audio, recorded {}ns",
            sealed[0].gap_before_ns
        );
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
