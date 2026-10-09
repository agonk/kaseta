//! Media files and a staged import, for tests that run the real tools.
//!
//! Fixtures are generated with ffmpeg from its synthetic sources rather than
//! checked in, so the repository carries no binary media and every fixture is
//! exactly what its command line says. Tests that need them skip, with a
//! message, on a machine without ffmpeg, ffprobe or a working bubblewrap.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use kaseta_contracts::{ImportStaging, Job};
use ulid::Ulid;

use super::intent::{Intent, IntentState};
use super::job::Limits;
use super::sandbox::Toolchain;
use super::ImportRuntime;
use crate::blobstore::{sha256_reader, BlobStore, LocalFsStore};
use crate::db::{Db, NewImport};

/// The tools, when this machine can run them in the sandbox.
pub fn toolchain() -> Option<Toolchain> {
    static TOOLS: OnceLock<Option<Toolchain>> = OnceLock::new();
    TOOLS
        .get_or_init(|| match Toolchain::detect() {
            Ok(tools) => Some(tools),
            Err(reason) => {
                eprintln!("skipping: {reason}");
                None
            }
        })
        .clone()
}

/// The fixtures, each a few seconds of a sine tone.
#[derive(Clone, Copy, Debug)]
pub enum Fixture {
    /// A phone video: H.264 picture as stream 0, AAC sound as stream 1.
    VideoFirstMp4,
    /// Two Opus streams, 2 s and 4 s long, the second marked default.
    MultiAudioMkv,
    /// VP9 and Opus.
    ClipWebm,
    Mp3,
    /// Six channels, to be folded down to one.
    SurroundWav,
    StereoFlac,
    OpusOgg,
    /// Pictures and no sound.
    NoAudioMp4,
    /// An mp3 under a video's name.
    Mp3NamedMp4,
    /// Plain text under a video's name.
    TextNamedMp4,
    /// A WAV header announcing no samples.
    EmptyWav,
    /// A minute of sound, for anything that needs a decode to take a while.
    LongWav,
    /// Eleven minutes, small: past the point where free space is checked
    /// again during a decode.
    ElevenMinutesMp3,
}

impl Fixture {
    /// The name the file is uploaded under.
    pub fn file_name(self) -> &'static str {
        match self {
            Fixture::VideoFirstMp4 => "video_first.mp4",
            Fixture::MultiAudioMkv => "multi_audio.mkv",
            Fixture::ClipWebm => "clip.webm",
            Fixture::Mp3 => "voice.mp3",
            Fixture::SurroundWav => "surround.wav",
            Fixture::StereoFlac => "stereo.flac",
            Fixture::OpusOgg => "voice.ogg",
            Fixture::NoAudioMp4 => "no_audio.mp4",
            Fixture::Mp3NamedMp4 => "liar.mp4",
            Fixture::TextNamedMp4 => "text.mp4",
            Fixture::EmptyWav => "empty.wav",
            Fixture::LongWav => "long.wav",
            Fixture::ElevenMinutesMp3 => "eleven.mp3",
        }
    }

    /// Writes the fixture into `dir`, or `None` if this ffmpeg lacks an
    /// encoder it needs.
    pub fn generate(self, dir: &Path) -> Option<PathBuf> {
        let out = dir.join(self.file_name());
        let sine = |seconds: u32, hz: u32| format!("sine=frequency={hz}:duration={seconds}");
        let picture = "testsrc=size=64x64:rate=10:duration=3";
        let args: Vec<String> = match self {
            Fixture::VideoFirstMp4 => lavfi(&[picture, &sine(3, 440)], &["-map", "0:v", "-map", "1:a", "-c:v", "libx264", "-c:a", "aac"]),
            Fixture::MultiAudioMkv => lavfi(
                &[&sine(2, 300), &sine(4, 600)],
                &["-map", "0:a", "-map", "1:a", "-c:a", "libopus", "-disposition:a:0", "0", "-disposition:a:1", "default"],
            ),
            Fixture::ClipWebm => lavfi(
                &[picture, &sine(3, 440)],
                &["-map", "0:v", "-map", "1:a", "-c:v", "libvpx-vp9", "-deadline", "realtime", "-cpu-used", "8", "-c:a", "libopus"],
            ),
            Fixture::Mp3 | Fixture::Mp3NamedMp4 => lavfi(&[&sine(3, 440)], &["-c:a", "libmp3lame", "-f", "mp3"]),
            Fixture::SurroundWav => lavfi(&[&sine(3, 440)], &["-ac", "6", "-c:a", "pcm_s16le"]),
            Fixture::StereoFlac => lavfi(&[&sine(3, 440)], &["-ac", "2", "-c:a", "flac"]),
            Fixture::OpusOgg => lavfi(&[&sine(3, 440)], &["-c:a", "libopus", "-f", "ogg"]),
            Fixture::NoAudioMp4 => lavfi(&[picture], &["-c:v", "libx264"]),
            Fixture::LongWav => lavfi(&[&sine(60, 440)], &["-c:a", "pcm_s16le"]),
            Fixture::ElevenMinutesMp3 => lavfi(
                &[&sine(660, 440)],
                &["-ar", "16000", "-c:a", "libmp3lame", "-b:a", "16k", "-f", "mp3"],
            ),
            Fixture::TextNamedMp4 => {
                std::fs::write(&out, b"This is a shopping list, not a video.\n").unwrap();
                return Some(out);
            }
            Fixture::EmptyWav => {
                std::fs::write(&out, empty_wav()).unwrap();
                return Some(out);
            }
        };

        let status = Command::new("ffmpeg")
            .args(["-nostdin", "-hide_banner", "-v", "error", "-y"])
            .args(&args)
            .arg(&out)
            .status()
            .ok()?;
        if !status.success() {
            eprintln!("skipping {}: this ffmpeg could not generate it", self.file_name());
            return None;
        }
        Some(out)
    }
}

fn lavfi(sources: &[&str], rest: &[&str]) -> Vec<String> {
    sources
        .iter()
        .flat_map(|s| ["-f".to_string(), "lavfi".into(), "-i".into(), s.to_string()])
        .chain(rest.iter().map(|s| s.to_string()))
        .collect()
}

/// A valid 48 kHz mono WAV holding no samples.
fn empty_wav() -> Vec<u8> {
    let mut wav = Vec::new();
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&36u32.to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16u32.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&48_000u32.to_le_bytes());
    wav.extend_from_slice(&96_000u32.to_le_bytes());
    wav.extend_from_slice(&2u16.to_le_bytes());
    wav.extend_from_slice(&16u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&0u32.to_le_bytes());
    wav
}

/// A store, a database and a runtime able to import.
pub struct Harness {
    /// Held so the store's directory outlives the test.
    _dir: tempfile::TempDir,
    pub store: LocalFsStore,
    pub db: Arc<Mutex<Db>>,
    pub runtime: ImportRuntime,
    pub fixtures: tempfile::TempDir,
}

impl Harness {
    /// `None`, with a message, when this machine cannot import.
    pub fn new() -> Option<Self> {
        let tools = toolchain()?;
        let dir = tempfile::TempDir::new().unwrap();
        Some(Self {
            store: LocalFsStore::new(dir.path()).unwrap(),
            _dir: dir,
            db: Arc::new(Mutex::new(Db::open_in_memory().unwrap())),
            runtime: ImportRuntime::with(tools),
            fixtures: tempfile::TempDir::new().unwrap(),
        })
    }

    /// The default limits, with the disk reported as roomy: whether this
    /// machine happens to have space is not what these tests are about, and
    /// the budget's refusals have tests of their own.
    pub fn limits(&self) -> Limits {
        Limits {
            max_duration: Duration::from_secs(6 * 3600),
            free_space: |_| Ok(1 << 40),
        }
    }

    pub fn fixture(&self, fixture: Fixture) -> Option<PathBuf> {
        fixture.generate(self.fixtures.path())
    }

    /// Stages `file` as an upload, exactly as the upload route leaves one,
    /// creates its recording and claims its decode. Returns the recording
    /// and the running job.
    pub fn stage(&self, file: &Path, keep_original: bool) -> (Ulid, Job) {
        let id = Ulid::new();
        let name = file.file_name().unwrap().to_string_lossy().into_owned();
        let (stem, ext) = name.rsplit_once('.').unwrap_or((&name, "bin"));

        let upload = ImportStaging::new(id).upload(ext).unwrap();
        let path = self.store.root().join(upload.as_str());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::copy(file, &path).unwrap();
        let (bytes, sha256) = sha256_reader(std::fs::File::open(&path).unwrap()).unwrap();

        let intent = Intent {
            state: IntentState::Uploaded,
            recording_id: id,
            created_at: time::OffsetDateTime::now_utc(),
            original_filename: name.clone(),
            ext: upload.as_str().rsplit_once('.').unwrap().1.to_string(),
            keep_original,
            title: stem.to_string(),
            bytes: Some(bytes),
            sha256: Some(sha256),
        };
        self.store
            .put(&ImportStaging::new(id).intent(), &serde_json::to_vec(&intent).unwrap())
            .unwrap();

        let db = self.db.lock().unwrap();
        db.create_import(&NewImport {
            recording_id: id,
            started_at: intent.started_at(),
            title: intent.title.clone(),
        })
        .unwrap();
        let job = db.claim_next_job(std::process::id()).unwrap().unwrap();
        assert_eq!(job.recording_id, id);
        (id, job)
    }

    /// The recording's status, origin and source cache, from the index.
    pub fn row(&self, id: Ulid) -> Option<(String, String, Option<String>, i64)> {
        use rusqlite::OptionalExtension;
        self.db
            .lock()
            .unwrap()
            .conn()
            .query_row(
                "SELECT status, origin, source_json, original_local FROM recordings WHERE id = ?1",
                rusqlite::params![id.to_string()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()
            .unwrap()
    }

    pub fn staging_exists(&self, id: Ulid) -> bool {
        super::staging_dir(&self.store, id).unwrap().exists()
    }

    /// Every object under the recording's prefix.
    pub fn objects(&self, id: Ulid, started_at: time::OffsetDateTime) -> Vec<String> {
        let prefix = kaseta_contracts::RecordingPrefix::new(id, started_at);
        self.store
            .list_prefix(prefix.root().as_str())
            .unwrap()
            .into_iter()
            .map(|k| k.as_str().rsplit_once(&format!("{id}/")).unwrap().1.to_string())
            .collect()
    }
}
