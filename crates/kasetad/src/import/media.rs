//! What is inside an uploaded file, and turning its audio into samples.
//!
//! Everything here is decided by looking at the content, never at the name. A
//! file called `talk.mp4` may hold an mp3, a text file, or a playlist that
//! points somewhere else entirely, and ffmpeg will happily follow whatever it
//! finds unless told exactly what it may read. So the file is probed under a
//! closed list of formats, the format found is mapped through a closed table
//! to one demuxer, and the decode is then forced to use that demuxer and
//! nothing else.

use std::ffi::OsString;
use std::path::Path;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use kaseta_contracts::manifest::MediaType;
use serde::Deserialize;

use super::sandbox::{Tool, Toolchain};
use crate::scheduler::Permanent;

/// The formats ffprobe and ffmpeg may open at all, by demuxer name.
///
/// Passed as `-format_whitelist`, so a file that would need any other demuxer
/// (a playlist, a concatenation list, an image sequence, a device) is refused
/// by ffmpeg itself before any of it is interpreted.
pub const FORMAT_WHITELIST: &str =
    "mov,matroska,mp3,wav,w64,flac,ogg,aac,asf,avi,mpegts,caf,aiff,amr,ac3,eac3,au,mpeg";

/// What every import is decoded to: the rate and layout chunks are stored at.
pub const DECODE_SAMPLE_RATE_HZ: u32 = 48_000;

/// The most ffprobe may print. A file with an absurd number of streams must
/// not become an absurd allocation.
const MAX_PROBE_OUTPUT: usize = 4 * 1024 * 1024;

/// How long probing may take. It reads headers, not the whole file.
const PROBE_TIMEOUT: Duration = Duration::from_secs(120);

/// Maps the format ffprobe reports to the one demuxer the decode is forced to.
///
/// ffprobe names a demuxer by every alias it answers to, so the mp4 family
/// arrives as `mov,mp4,m4a,3gp,3g2,mj2`. The table is closed: a format missing
/// from it is unsupported, whatever ffmpeg could make of it.
pub fn demuxer_for(format_name: &str) -> Option<&'static str> {
    Some(match format_name {
        "mov,mp4,m4a,3gp,3g2,mj2" => "mov",
        "matroska,webm" => "matroska",
        "mp3" => "mp3",
        "wav" => "wav",
        "w64" => "w64",
        "flac" => "flac",
        "ogg" => "ogg",
        "aac" => "aac",
        "asf" => "asf",
        "avi" => "avi",
        "mpegts" => "mpegts",
        "caf" => "caf",
        "aiff" => "aiff",
        "amr" => "amr",
        "ac3" => "ac3",
        "eac3" => "eac3",
        "au" => "au",
        "mpeg" => "mpeg",
        _ => return None,
    })
}

/// What probing found.
#[derive(Clone, Debug, PartialEq)]
pub struct Probe {
    /// ffprobe's own name for the format, e.g. `mov,mp4,m4a,3gp,3g2,mj2`.
    pub format_name: String,
    /// The container's duration, when it states one.
    pub duration_s: Option<f64>,
    /// The mp4 family's brand, which separates QuickTime from MP4 from M4A.
    pub major_brand: Option<String>,
    /// When the file says it was made. Its own claim, unverified.
    pub created_at: Option<time::OffsetDateTime>,
    pub streams: Vec<StreamInfo>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StreamInfo {
    /// ffprobe's global stream index, which is what `-map 0:<index>` takes.
    pub index: u32,
    /// `audio`, `video`, `subtitle`, `data` or `attachment`.
    pub codec_type: String,
    pub codec_name: String,
    /// Marked as the stream to play by default.
    pub is_default: bool,
    /// A cover image rather than moving pictures. An mp3 with album art has a
    /// "video" stream that is one still frame.
    pub attached_pic: bool,
    pub duration_s: Option<f64>,
}

impl StreamInfo {
    fn is_audio(&self) -> bool {
        self.codec_type == "audio"
    }

    fn is_picture(&self) -> bool {
        self.codec_type == "video" && !self.attached_pic
    }
}

#[derive(Deserialize)]
struct RawProbe {
    #[serde(default)]
    streams: Vec<RawStream>,
    format: Option<RawFormat>,
}

#[derive(Deserialize)]
struct RawStream {
    index: u32,
    #[serde(default)]
    codec_type: Option<String>,
    #[serde(default)]
    codec_name: Option<String>,
    #[serde(default)]
    duration: Option<String>,
    #[serde(default)]
    disposition: Option<RawDisposition>,
}

#[derive(Deserialize, Default)]
struct RawDisposition {
    #[serde(default)]
    default: u8,
    #[serde(default)]
    attached_pic: u8,
}

#[derive(Deserialize)]
struct RawFormat {
    format_name: String,
    #[serde(default)]
    duration: Option<String>,
    #[serde(default)]
    tags: std::collections::HashMap<String, String>,
}

impl Probe {
    /// Reads ffprobe's JSON (`-show_format -show_streams`).
    pub fn parse(json: &[u8]) -> Result<Self> {
        let raw: RawProbe = serde_json::from_slice(json).context("reading ffprobe's report")?;
        let format = raw.format.context("ffprobe reported no format")?;
        let tag = |name: &str| {
            format
                .tags
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.clone())
        };

        Ok(Self {
            duration_s: parse_seconds(format.duration.as_deref()),
            major_brand: tag("major_brand"),
            created_at: tag("creation_time").and_then(|t| parse_created_at(&t)),
            format_name: format.format_name,
            streams: raw
                .streams
                .into_iter()
                .map(|s| {
                    let disposition = s.disposition.unwrap_or_default();
                    StreamInfo {
                        index: s.index,
                        codec_type: s.codec_type.unwrap_or_default(),
                        codec_name: s.codec_name.unwrap_or_else(|| "unknown".into()),
                        is_default: disposition.default != 0,
                        attached_pic: disposition.attached_pic != 0,
                        duration_s: parse_seconds(s.duration.as_deref()),
                    }
                })
                .collect(),
        })
    }

    /// Whether the file shows anything. Cover art does not count: an audio
    /// file with an album picture is still an audio file.
    pub fn media_kind(&self) -> MediaType {
        if self.streams.iter().any(StreamInfo::is_picture) {
            MediaType::Video
        } else {
            MediaType::Audio
        }
    }

    /// How long `stream` lasts: its own duration, else the container's.
    ///
    /// Only a finite, positive number counts. Anything else is "not known",
    /// and the caller budgets for the longest import allowed instead.
    pub fn duration_of(&self, stream: &StreamInfo) -> Option<f64> {
        stream
            .duration_s
            .or(self.duration_s)
            .filter(|d| d.is_finite() && *d > 0.0)
    }
}

fn parse_seconds(raw: Option<&str>) -> Option<f64> {
    raw.and_then(|s| s.trim().parse::<f64>().ok())
        .filter(|d| d.is_finite() && *d > 0.0)
}

/// A creation time the file states, if it is a believable one.
///
/// Cameras with an unset clock write 1904 or 1970, which is not a date anyone
/// recorded anything on; those read as "not stated".
fn parse_created_at(raw: &str) -> Option<time::OffsetDateTime> {
    let parsed =
        time::OffsetDateTime::parse(raw.trim(), &time::format_description::well_known::Rfc3339)
            .ok()?;
    (parsed.year() >= 1980).then_some(parsed)
}

/// The audio stream to decode: the one marked default, else the first.
pub fn select_stream(probe: &Probe) -> Option<&StreamInfo> {
    let mut audio = probe.streams.iter().filter(|s| s.is_audio());
    let first = audio.clone().next()?;
    Some(audio.find(|s| s.is_default).unwrap_or(first))
}

/// The media type to serve the original with, from what probing found.
///
/// The extension is never consulted. Within the mp4 family the brand tells
/// QuickTime, MP4, M4A and 3GPP apart; Matroska counts as WebM only when every
/// stream is a codec WebM allows; every other demuxer has one type, under
/// `video/` when the file shows pictures and `audio/` when it does not.
pub fn content_type(probe: &Probe, demuxer: &str) -> String {
    let video = probe.media_kind() == MediaType::Video;
    let family = if video { "video" } else { "audio" };

    match demuxer {
        "mov" => {
            let brand = probe
                .major_brand
                .as_deref()
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();
            if brand == "qt" {
                "video/quicktime".into()
            } else if brand == "m4a" || brand == "m4b" {
                "audio/mp4".into()
            } else if brand.starts_with("3gp") {
                "video/3gpp".into()
            } else {
                format!("{family}/mp4")
            }
        }
        "matroska" => {
            const WEBM: [&str; 5] = ["vp8", "vp9", "av1", "vorbis", "opus"];
            let webm = probe
                .streams
                .iter()
                .filter(|s| s.is_audio() || s.is_picture())
                .all(|s| WEBM.contains(&s.codec_name.as_str()));
            if webm {
                format!("{family}/webm")
            } else {
                format!("{family}/x-matroska")
            }
        }
        other => {
            let subtype = match other {
                "mp3" | "mpeg" => "mpeg",
                "wav" => "wav",
                "w64" => "x-w64",
                "flac" => "flac",
                "ogg" => "ogg",
                "aac" => "aac",
                "asf" => "x-ms-asf",
                "avi" => "x-msvideo",
                "mpegts" => "mp2t",
                "caf" => "x-caf",
                "aiff" => "aiff",
                "amr" => "amr",
                "ac3" => "ac3",
                "eac3" => "eac3",
                "au" => "basic",
                _ => return "application/octet-stream".into(),
            };
            format!("{family}/{subtype}")
        }
    }
}

/// Runs ffprobe on the staged file.
///
/// ffprobe failing is a statement about the file: under the format whitelist
/// it fails for anything that is not one of the supported formats, and that
/// will not change on a second attempt.
pub fn probe(tools: &Toolchain, path: &Path) -> Result<Probe> {
    let dir = path.parent().context("a staged file has a directory")?;
    let args: Vec<OsString> = [
        "-hide_banner",
        "-v",
        "error",
        "-protocol_whitelist",
        "file",
        "-format_whitelist",
        FORMAT_WHITELIST,
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
    ]
    .iter()
    .map(OsString::from)
    .chain(std::iter::once(file_url(path)))
    .collect();

    let mut out = Vec::new();
    let finished = tools.run(Some(dir), Tool::Ffprobe, &args, PROBE_TIMEOUT, |bytes| {
        if out.len() + bytes.len() > MAX_PROBE_OUTPUT {
            return Err(Permanent("unsupported file type: its description is too large".into()).into());
        }
        out.extend_from_slice(bytes);
        Ok(true)
    })?;

    if finished.timed_out {
        anyhow::bail!("ffprobe did not finish reading the file within {PROBE_TIMEOUT:?}");
    }
    if !finished.status.success() {
        tracing::info!(stderr = %finished.stderr_tail, "ffprobe refused the file");
        return Err(Permanent("unsupported file type".into()).into());
    }
    Probe::parse(&out).map_err(|e| {
        tracing::info!(error = %format!("{e:#}"), "ffprobe's report was unreadable");
        Permanent("unsupported file type".into()).into()
    })
}

/// How far a decode may go.
#[derive(Clone, Copy, Debug)]
pub struct DecodeLimits {
    /// Killed after this long, whatever it is doing.
    pub timeout: Duration,
    /// Killed once it has produced more frames than this. A file's stated
    /// duration is the file's claim; the decoded frames are the fact.
    pub max_frames: u64,
}

/// Decodes one stream to 48 kHz mono 16-bit samples, handing them to `sink`
/// in bounded pieces. Returns the number of frames decoded.
///
/// Memory is bounded by one read of the pipe, whatever the file's length.
///
/// A sink error stops the decode and is returned as it is, so a sink can stop
/// it with a failure of its own kind (retryable or not).
pub fn decode(
    tools: &Toolchain,
    path: &Path,
    demuxer: &str,
    stream_index: u32,
    limits: DecodeLimits,
    mut sink: impl FnMut(&[i16]) -> Result<()>,
) -> Result<u64> {
    let dir = path.parent().context("a staged file has a directory")?;

    let mut args: Vec<OsString> = [
        "-nostdin",
        "-hide_banner",
        "-v",
        "error",
        "-threads",
        "2",
        "-protocol_whitelist",
        "file",
        "-format_whitelist",
        FORMAT_WHITELIST,
        "-f",
        demuxer,
    ]
    .iter()
    .map(OsString::from)
    .collect();
    if demuxer == "mov" {
        // An mp4 can reference media in other files ("data references").
        // Off by default; stated anyway, because it is the one way an
        // allowed format could still reach outside the file.
        args.extend(["-enable_drefs", "0"].map(OsString::from));
    }
    args.push("-i".into());
    args.push(file_url(path));
    args.extend(
        [
            "-map".to_string(),
            format!("0:{stream_index}"),
            "-vn".into(),
            "-sn".into(),
            "-dn".into(),
            "-ac".into(),
            "1".into(),
            "-ar".into(),
            DECODE_SAMPLE_RATE_HZ.to_string(),
            "-f".into(),
            "s16le".into(),
            "pipe:1".into(),
        ]
        .map(OsString::from),
    );

    let mut frames: u64 = 0;
    let mut reader = S16Reader::default();
    let mut samples: Vec<i16> = Vec::new();
    let mut failure: Option<anyhow::Error> = None;
    let mut over_limit = false;

    let finished = tools.run(Some(dir), Tool::Ffmpeg, &args, limits.timeout, |bytes| {
        reader.read(bytes, &mut samples);
        frames += samples.len() as u64;
        if frames > limits.max_frames {
            over_limit = true;
            return Ok(false);
        }
        if let Err(e) = sink(&samples) {
            failure = Some(e);
            return Ok(false);
        }
        Ok(true)
    })?;

    if over_limit {
        return Err(Permanent("the file is longer than the import limit".into()).into());
    }
    if let Some(e) = failure {
        return Err(e);
    }
    if finished.timed_out {
        anyhow::bail!(
            "decoding took longer than {} minutes and was stopped",
            limits.timeout.as_secs().div_ceil(60)
        );
    }
    if !finished.status.success() {
        let last = finished
            .stderr_tail
            .lines()
            .rev()
            .find(|l| !l.trim().is_empty())
            .unwrap_or("no reason given")
            .trim()
            .to_string();
        return Err(anyhow!("ffmpeg could not decode the file: {last}"));
    }
    Ok(frames)
}

/// Turns a byte stream of little-endian 16-bit samples into samples.
///
/// A pipe read can end halfway through a sample. The odd byte is carried into
/// the next read rather than dropped, which would shift every later sample by
/// a byte and turn the rest of the recording into noise.
#[derive(Default)]
struct S16Reader {
    carry: Option<u8>,
}

impl S16Reader {
    /// Replaces `out` with every whole sample `bytes` completes.
    fn read(&mut self, bytes: &[u8], out: &mut Vec<i16>) {
        out.clear();
        let mut rest = bytes;
        if let Some(low) = self.carry.take() {
            match rest.split_first() {
                Some((&high, tail)) => {
                    out.push(i16::from_le_bytes([low, high]));
                    rest = tail;
                }
                None => {
                    self.carry = Some(low);
                    return;
                }
            }
        }
        let mut pairs = rest.chunks_exact(2);
        out.extend(pairs.by_ref().map(|p| i16::from_le_bytes([p[0], p[1]])));
        self.carry = pairs.remainder().first().copied();
    }
}

/// The input argument for a local file. The `file:` scheme makes a name that
/// happens to look like a URL, or starts with a dash, mean the file.
fn file_url(path: &Path) -> OsString {
    let mut url = OsString::from("file:");
    url.push(path.as_os_str());
    url
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream(index: u32, codec_type: &str, codec: &str, default: bool) -> StreamInfo {
        StreamInfo {
            index,
            codec_type: codec_type.into(),
            codec_name: codec.into(),
            is_default: default,
            attached_pic: false,
            duration_s: Some(3.0),
        }
    }

    fn probe_of(format_name: &str, streams: Vec<StreamInfo>) -> Probe {
        Probe {
            format_name: format_name.into(),
            duration_s: Some(3.0),
            major_brand: None,
            created_at: None,
            streams,
        }
    }

    #[test]
    fn every_supported_format_maps_to_one_demuxer() {
        let table = [
            ("mov,mp4,m4a,3gp,3g2,mj2", "mov"),
            ("matroska,webm", "matroska"),
            ("mp3", "mp3"),
            ("wav", "wav"),
            ("w64", "w64"),
            ("flac", "flac"),
            ("ogg", "ogg"),
            ("aac", "aac"),
            ("asf", "asf"),
            ("avi", "avi"),
            ("mpegts", "mpegts"),
            ("caf", "caf"),
            ("aiff", "aiff"),
            ("amr", "amr"),
            ("ac3", "ac3"),
            ("eac3", "eac3"),
            ("au", "au"),
            ("mpeg", "mpeg"),
        ];
        for (format, demuxer) in table {
            assert_eq!(demuxer_for(format), Some(demuxer), "{format}");
            assert!(
                FORMAT_WHITELIST.split(',').any(|w| w == demuxer),
                "{demuxer} must be allowed through the whitelist too"
            );
        }
    }

    /// Whatever ffmpeg could read through these, none of them is a file of
    /// audio: they point at other files, devices or generated input.
    #[test]
    fn formats_that_reach_beyond_the_file_are_unsupported() {
        for format in [
            "hls", "concat", "image2", "lavfi", "tee", "sdp", "rtp", "data", "mov", "mp4",
            "matroska", "webm", "", "MP3",
        ] {
            assert_eq!(demuxer_for(format), None, "{format:?}");
        }
    }

    #[test]
    fn the_default_audio_stream_is_chosen_over_the_first() {
        let probe = probe_of(
            "matroska,webm",
            vec![
                stream(0, "video", "vp9", true),
                stream(1, "audio", "opus", false),
                stream(2, "audio", "opus", true),
            ],
        );
        assert_eq!(select_stream(&probe).unwrap().index, 2);
    }

    /// A phone video: the picture is stream 0 and the sound stream 1.
    #[test]
    fn without_a_default_the_first_audio_stream_is_chosen() {
        let probe = probe_of(
            "mov,mp4,m4a,3gp,3g2,mj2",
            vec![
                stream(0, "video", "h264", true),
                stream(1, "audio", "aac", false),
                stream(2, "audio", "aac", false),
            ],
        );
        assert_eq!(select_stream(&probe).unwrap().index, 1);
    }

    #[test]
    fn a_file_without_audio_has_no_stream_to_choose() {
        let probe = probe_of("mov,mp4,m4a,3gp,3g2,mj2", vec![stream(0, "video", "h264", true)]);
        assert!(select_stream(&probe).is_none());
    }

    #[test]
    fn cover_art_does_not_make_a_file_a_video() {
        let mut cover = stream(1, "video", "mjpeg", false);
        cover.attached_pic = true;
        let probe = probe_of("mp3", vec![stream(0, "audio", "mp3", true), cover]);
        assert_eq!(probe.media_kind(), MediaType::Audio);
        assert_eq!(content_type(&probe, "mp3"), "audio/mpeg");
    }

    #[test]
    fn the_content_type_comes_from_the_content() {
        let video = |format: &str, v: &str, a: &str| {
            probe_of(format, vec![stream(0, "video", v, true), stream(1, "audio", a, true)])
        };
        let audio = |format: &str, a: &str| probe_of(format, vec![stream(0, "audio", a, true)]);
        let branded = |brand: &str, p: Probe| Probe {
            major_brand: Some(brand.into()),
            ..p
        };
        let mov = "mov,mp4,m4a,3gp,3g2,mj2";

        let cases = [
            (branded("qt  ", video(mov, "h264", "aac")), "mov", "video/quicktime"),
            (branded("isom", video(mov, "h264", "aac")), "mov", "video/mp4"),
            (branded("M4A ", audio(mov, "aac")), "mov", "audio/mp4"),
            (branded("M4B ", audio(mov, "aac")), "mov", "audio/mp4"),
            (branded("3gp4", video(mov, "h263", "amr_nb")), "mov", "video/3gpp"),
            (branded("isom", audio(mov, "aac")), "mov", "audio/mp4"),
            (audio(mov, "aac"), "mov", "audio/mp4"),
            (video("matroska,webm", "vp9", "opus"), "matroska", "video/webm"),
            (video("matroska,webm", "av1", "vorbis"), "matroska", "video/webm"),
            (video("matroska,webm", "h264", "aac"), "matroska", "video/x-matroska"),
            (video("matroska,webm", "vp9", "aac"), "matroska", "video/x-matroska"),
            (audio("matroska,webm", "opus"), "matroska", "audio/webm"),
            (audio("matroska,webm", "flac"), "matroska", "audio/x-matroska"),
            (audio("wav", "pcm_s16le"), "wav", "audio/wav"),
            (audio("flac", "flac"), "flac", "audio/flac"),
            (audio("ogg", "opus"), "ogg", "audio/ogg"),
            (video("avi", "mpeg4", "mp3"), "avi", "video/x-msvideo"),
            (video("mpegts", "h264", "aac"), "mpegts", "video/mp2t"),
            (video("asf", "wmv2", "wmav2"), "asf", "video/x-ms-asf"),
            (audio("asf", "wmav2"), "asf", "audio/x-ms-asf"),
        ];
        for (probe, demuxer, expected) in cases {
            assert_eq!(content_type(&probe, demuxer), expected, "{probe:?}");
        }
    }

    #[test]
    fn ffprobes_report_is_read() {
        let json = br#"{
            "streams": [
                {"index": 0, "codec_name": "h264", "codec_type": "video",
                 "duration": "3.000000", "disposition": {"default": 1, "attached_pic": 0}},
                {"index": 1, "codec_name": "aac", "codec_type": "audio",
                 "duration": "2.990000", "disposition": {"default": 1, "attached_pic": 0}},
                {"index": 2, "codec_type": "data"}
            ],
            "format": {
                "format_name": "mov,mp4,m4a,3gp,3g2,mj2",
                "duration": "3.010000",
                "tags": {"major_brand": "isom", "creation_time": "2026-10-01T17:30:00.000000Z"}
            }
        }"#;
        let probe = Probe::parse(json).unwrap();

        assert_eq!(probe.format_name, "mov,mp4,m4a,3gp,3g2,mj2");
        assert_eq!(probe.major_brand.as_deref(), Some("isom"));
        assert_eq!(
            probe.created_at,
            Some(time::macros::datetime!(2026-10-01 17:30:00 UTC))
        );
        assert_eq!(probe.streams.len(), 3);
        assert_eq!(probe.streams[2].codec_name, "unknown");
        assert_eq!(probe.media_kind(), MediaType::Video);
        let audio = select_stream(&probe).unwrap();
        assert_eq!(probe.duration_of(audio), Some(2.99));
    }

    /// A Matroska stream often states no duration of its own; the
    /// container's stands in. Neither stated means "unknown", not zero.
    #[test]
    fn a_missing_or_absurd_duration_reads_as_unknown() {
        let mut probe = probe_of("matroska,webm", vec![stream(0, "audio", "opus", true)]);
        probe.streams[0].duration_s = None;
        assert_eq!(probe.duration_of(&probe.streams[0].clone()), Some(3.0));

        probe.duration_s = None;
        assert_eq!(probe.duration_of(&probe.streams[0].clone()), None);

        assert_eq!(parse_seconds(Some("N/A")), None);
        assert_eq!(parse_seconds(Some("inf")), None);
        assert_eq!(parse_seconds(Some("-1")), None);
        assert_eq!(parse_seconds(Some("0")), None);
    }

    #[test]
    fn an_unset_camera_clock_is_not_a_creation_date() {
        assert_eq!(parse_created_at("1970-01-01T00:00:00.000000Z"), None);
        assert_eq!(parse_created_at("1904-01-01T00:00:00Z"), None);
        assert_eq!(parse_created_at("yesterday"), None);
        assert!(parse_created_at("2019-05-04T10:00:00Z").is_some());
    }

    /// Every way of splitting a stream across reads yields the same samples.
    #[test]
    fn a_sample_split_across_reads_is_reassembled() {
        let expected: Vec<i16> = vec![1, -2, 300, i16::MIN, i16::MAX, -1, 0x1234];
        let bytes: Vec<u8> = expected.iter().flat_map(|s| s.to_le_bytes()).collect();

        for sizes in [vec![1usize], vec![3], vec![1, 2, 5, 1], vec![13, 1], vec![14]] {
            let mut reader = S16Reader::default();
            let mut out = Vec::new();
            let mut got = Vec::new();
            let mut at = 0;
            let mut i = 0;
            while at < bytes.len() {
                let n = sizes[i % sizes.len()].min(bytes.len() - at);
                reader.read(&bytes[at..at + n], &mut out);
                got.extend_from_slice(&out);
                at += n;
                i += 1;
            }
            assert_eq!(got, expected, "reads of {sizes:?}");
        }
    }

    #[test]
    fn a_path_is_passed_as_a_file_url() {
        assert_eq!(file_url(Path::new("/data/imports/x/upload.mp4")), "file:/data/imports/x/upload.mp4");
    }
}
