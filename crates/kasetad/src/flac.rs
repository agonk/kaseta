//! FLAC encoding straight into a seekable sink, one block at a time.
//!
//! `flacenc`'s convenience entry point builds a whole `Stream` in memory, every
//! encoded frame included, before anything can be written out. For a chunk of
//! fifteen seconds that is harmless; for a merged three-hour track it is
//! hundreds of megabytes held at once. This writer encodes each block with the
//! library's per-frame encoder and writes it immediately, so what it holds is
//! one block of samples and one encoded frame, whatever the length of the audio.
//!
//! The one thing a stream cannot know up front is its own summary: the total
//! sample count, the smallest and largest frame and the MD5 of the audio all
//! live in STREAMINFO, at the very start of the file. A placeholder is written
//! first and patched by seeking back once the last block is out, which is why
//! the sink must be seekable.

use std::io::{Seek, SeekFrom, Write};

use anyhow::{bail, Context, Result};
use flacenc::bitsink::ByteSink;
use flacenc::component::{BitRepr, Stream, StreamInfo};
use flacenc::error::{Verified, Verify};
use flacenc::source::{Context as SampleContext, Fill, FrameBuf};

/// `fLaC` plus one metadata block header plus the 34-byte STREAMINFO body.
///
/// Fixed by the format, which is what makes patching it in place possible: the
/// rewritten header can never be longer or shorter than the placeholder.
const HEADER_BYTES: usize = 42;

/// Every stream Kaseta writes is 16-bit, the depth capture delivers.
const BITS_PER_SAMPLE: usize = 16;

/// What a finished stream contains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlacWritten {
    /// Samples per channel.
    pub frames: u64,
    /// Encoded size, header included.
    pub bytes: u64,
}

/// Encodes interleaved 16-bit samples into `out` as they arrive.
pub struct FlacFileWriter<W: Write + Seek> {
    out: W,
    config: Verified<flacenc::config::Encoder>,
    info: StreamInfo,
    block: FrameBuf,
    context: SampleContext,
    channels: usize,
    block_size: usize,
    /// Interleaved samples of the block being filled, never more than one
    /// block's worth.
    pending: Vec<i32>,
    /// Reused for every frame, so encoding allocates one frame's bytes at most.
    sink: ByteSink,
    frames: u64,
    /// Where the stream begins in `out`, so the header patch lands on it even
    /// when the sink already held something.
    start: u64,
    wrote_frame: bool,
}

impl<W: Write + Seek> FlacFileWriter<W> {
    /// Starts a stream, writing a placeholder header at the sink's position.
    pub fn new(mut out: W, sample_rate_hz: u32, channels: u16) -> Result<Self> {
        let channels = channels as usize;
        if !(1..=8).contains(&channels) {
            bail!("FLAC carries 1 to 8 channels, not {channels}");
        }

        let config = flacenc::config::Encoder::default()
            .into_verified()
            .map_err(|e| anyhow::anyhow!("invalid FLAC encoder config: {e:?}"))?;
        let block_size = config.block_size;

        let mut info = StreamInfo::new(sample_rate_hz as usize, channels, BITS_PER_SAMPLE)
            .map_err(|e| anyhow::anyhow!("invalid FLAC stream parameters: {e:?}"))?;
        // Matches what the library's own fixed-block encoder declares, so a
        // streamed file and a buffered one describe themselves the same way.
        info.set_block_sizes(block_size, block_size)
            .map_err(|e| anyhow::anyhow!("invalid FLAC block size: {e:?}"))?;

        let start = out.stream_position().context("locating the FLAC stream start")?;
        let mut writer = Self {
            out,
            config,
            block: FrameBuf::with_size(channels, block_size)
                .map_err(|e| anyhow::anyhow!("allocating a FLAC block: {e:?}"))?,
            context: SampleContext::new(BITS_PER_SAMPLE, channels),
            info,
            channels,
            block_size,
            pending: Vec::with_capacity(block_size * channels),
            sink: ByteSink::new(),
            frames: 0,
            start,
            wrote_frame: false,
        };
        let header = writer.header()?;
        writer
            .out
            .write_all(&header)
            .context("writing the FLAC header")?;
        Ok(writer)
    }

    /// Appends interleaved samples. A call need not end on a block or even a
    /// frame boundary; the remainder waits for the next call.
    pub fn write_samples(&mut self, mut samples: &[i16]) -> Result<()> {
        let capacity = self.block_size * self.channels;
        while !samples.is_empty() {
            let room = capacity - self.pending.len();
            let (now, rest) = samples.split_at(room.min(samples.len()));
            self.pending.extend(now.iter().map(|s| *s as i32));
            samples = rest;
            if self.pending.len() == capacity {
                self.encode_pending()?;
            }
        }
        Ok(())
    }

    /// Writes the final short block, patches the header and hands the sink
    /// back, positioned at the end of the stream.
    pub fn finish(mut self) -> Result<(W, FlacWritten)> {
        if !self.pending.len().is_multiple_of(self.channels) {
            bail!(
                "the stream ends partway through a frame: {} samples left over for {} channels",
                self.pending.len(),
                self.channels
            );
        }
        if !self.pending.is_empty() {
            self.encode_pending()?;
        }

        self.info.set_md5_digest(&self.context.md5_digest());
        if !self.wrote_frame {
            // No frame ever narrowed the range, so it still holds its "nothing
            // seen" sentinels. Zero is the format's "unknown".
            self.info
                .set_frame_sizes(0, 0)
                .map_err(|e| anyhow::anyhow!("resetting FLAC frame sizes: {e:?}"))?;
        }

        let end = self
            .out
            .stream_position()
            .context("locating the FLAC stream end")?;
        let header = self.header()?;
        self.out
            .seek(SeekFrom::Start(self.start))
            .context("seeking back to the FLAC header")?;
        self.out
            .write_all(&header)
            .context("patching the FLAC header")?;
        self.out
            .seek(SeekFrom::Start(end))
            .context("returning to the FLAC stream end")?;
        self.out.flush().context("flushing the FLAC stream")?;

        let written = FlacWritten {
            frames: self.frames,
            bytes: end - self.start,
        };
        Ok((self.out, written))
    }

    fn encode_pending(&mut self) -> Result<()> {
        let mut fill = (&mut self.block, &mut self.context);
        fill.fill_interleaved(&self.pending)
            .map_err(|e| anyhow::anyhow!("loading a FLAC block: {e:?}"))?;
        let number = self
            .context
            .current_frame_number()
            .context("a filled block has no frame number")?;

        let frame = flacenc::encode_fixed_size_frame(&self.config, &self.block, number, &self.info)
            .map_err(|e| anyhow::anyhow!("FLAC encoding failed: {e:?}"))?;
        self.info.update_frame_info(&frame);

        self.sink.clear();
        frame
            .write(&mut self.sink)
            .map_err(|e| anyhow::anyhow!("serialising a FLAC frame: {e:?}"))?;
        self.out
            .write_all(self.sink.as_slice())
            .context("writing a FLAC frame")?;

        self.frames += (self.pending.len() / self.channels) as u64;
        self.pending.clear();
        self.wrote_frame = true;
        Ok(())
    }

    /// The stream header as it stands: the marker and STREAMINFO, flagged as
    /// the last metadata block.
    fn header(&mut self) -> Result<Vec<u8>> {
        self.sink.clear();
        Stream::with_stream_info(self.info.clone())
            .write(&mut self.sink)
            .map_err(|e| anyhow::anyhow!("serialising the FLAC header: {e:?}"))?;
        let header = self.sink.as_slice().to_vec();
        if header.len() != HEADER_BYTES {
            bail!(
                "the FLAC header is {} bytes, not the {HEADER_BYTES} the format fixes",
                header.len()
            );
        }
        Ok(header)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn tone(frames: usize, channels: u16) -> Vec<i16> {
        (0..frames * channels as usize)
            .map(|i| ((i as f64 * 0.031).sin() * 9000.0) as i16)
            .collect()
    }

    fn encode(samples: &[i16], channels: u16, piece: usize) -> (Vec<u8>, FlacWritten) {
        let mut writer = FlacFileWriter::new(Cursor::new(Vec::new()), 48_000, channels).unwrap();
        for part in samples.chunks(piece.max(1)) {
            writer.write_samples(part).unwrap();
        }
        let (out, written) = writer.finish().unwrap();
        (out.into_inner(), written)
    }

    fn decode(bytes: &[u8]) -> (claxon::metadata::StreamInfo, Vec<i16>) {
        let mut reader = claxon::FlacReader::new(Cursor::new(bytes)).unwrap();
        let info = reader.streaminfo();
        let samples = reader
            .samples()
            .map(|s| s.unwrap() as i16)
            .collect::<Vec<_>>();
        (info, samples)
    }

    #[test]
    fn a_streamed_file_decodes_to_exactly_what_was_written() {
        // Fed in pieces that line up with nothing: not a block, not a frame.
        // The writer has to carry the remainder across calls.
        let samples = tone(10_000, 2);
        let (bytes, written) = encode(&samples, 2, 333);

        let (info, decoded) = decode(&bytes);
        assert_eq!(decoded, samples, "encoding must be lossless");
        assert_eq!(written.frames, 10_000);
        assert_eq!(written.bytes, bytes.len() as u64);
        assert_eq!(info.channels, 2);
        assert_eq!(info.sample_rate, 48_000);
        assert_eq!(info.bits_per_sample, 16);
    }

    #[test]
    fn the_header_is_patched_with_the_stream_summary() {
        let samples = tone(9_000, 1);
        let (bytes, _) = encode(&samples, 1, 4_096);

        let (info, _) = decode(&bytes);
        assert_eq!(
            info.samples,
            Some(9_000),
            "the sample count is only known at the end and must be written back"
        );
        let min = info.min_frame_size.expect("min frame size recorded");
        let max = info.max_frame_size.expect("max frame size recorded");
        assert!(min > 0 && min <= max, "frame sizes {min}..{max}");
        assert_ne!(info.md5sum, [0u8; 16], "the audio's digest is recorded");
    }

    #[test]
    fn audio_shorter_than_one_block_still_makes_a_stream() {
        let samples = tone(100, 1);
        let (bytes, written) = encode(&samples, 1, 7);
        assert_eq!(decode(&bytes).1, samples);
        assert_eq!(written.frames, 100);
    }

    #[test]
    fn an_empty_stream_is_a_valid_file_of_no_audio() {
        let (bytes, written) = encode(&[], 2, 1);
        assert_eq!(bytes.len(), HEADER_BYTES);
        assert_eq!(written.frames, 0);
        let (info, decoded) = decode(&bytes);
        assert!(decoded.is_empty());
        // Zero is also the format's "length unknown", which is how a decoder
        // may report it.
        assert!(matches!(info.samples, None | Some(0)), "{:?}", info.samples);
    }

    #[test]
    fn a_stream_ending_mid_frame_is_refused() {
        let mut writer = FlacFileWriter::new(Cursor::new(Vec::new()), 48_000, 2).unwrap();
        writer.write_samples(&[1, 2, 3]).unwrap();
        let err = writer.finish().unwrap_err();
        assert!(err.to_string().contains("partway through a frame"), "{err}");
    }

    #[test]
    fn the_stream_starts_wherever_the_sink_already_was() {
        // The patch must land on this stream's header, not on byte zero of a
        // sink that held something before it.
        let mut cursor = Cursor::new(Vec::new());
        cursor.write_all(b"preamble").unwrap();
        let samples = tone(5_000, 1);
        let mut writer = FlacFileWriter::new(cursor, 48_000, 1).unwrap();
        writer.write_samples(&samples).unwrap();
        let (out, written) = writer.finish().unwrap();
        let bytes = out.into_inner();

        assert_eq!(&bytes[..8], b"preamble");
        assert_eq!(written.bytes as usize, bytes.len() - 8);
        assert_eq!(decode(&bytes[8..]).1, samples);
    }

    /// Records every write the encoder makes.
    #[derive(Default)]
    struct Recorder {
        inner: Cursor<Vec<u8>>,
        largest_write: usize,
        writes: usize,
    }

    impl Write for Recorder {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.largest_write = self.largest_write.max(buf.len());
            self.writes += 1;
            self.inner.write(buf)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Seek for Recorder {
        fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
            self.inner.seek(pos)
        }
    }

    #[test]
    fn encoded_audio_reaches_the_sink_a_frame_at_a_time() {
        // The point of this writer: output leaves as it is produced, so no
        // write is ever larger than one encoded block, however long the audio.
        let samples = tone(48_000 * 20, 2);
        let mut writer = FlacFileWriter::new(Recorder::default(), 48_000, 2).unwrap();
        for second in samples.chunks(96_000) {
            writer.write_samples(second).unwrap();
        }
        let bytes_before_finish = writer.out.inner.get_ref().len();
        let (recorder, written) = writer.finish().unwrap();

        // One 4096-frame stereo block of 16-bit audio is 16 KiB raw; an encoded
        // frame can only be smaller or marginally larger.
        assert!(
            recorder.largest_write <= 20 * 1024,
            "a single write of {} bytes means output was buffered",
            recorder.largest_write
        );
        assert!(recorder.writes > 200, "expected one write per frame");
        assert!(
            bytes_before_finish as u64 + 4 * 4096 >= written.bytes,
            "all but the last block must be out before finish"
        );
    }
}
