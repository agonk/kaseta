//! `kasetad import`: hand a file to the running daemon from a terminal.
//!
//! The command does not decode anything itself. It streams the file to the
//! daemon's own upload route, exactly as the page does, and then watches the
//! recording until the daemon has decided whether it is one. A second path
//! that wrote into storage directly would be a second implementation of every
//! check the route makes, and it would race the daemon that owns the index.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use ulid::Ulid;

use super::upload::{percent_encode, FILENAME_HEADER, KEEP_ORIGINAL_HEADER, TITLE_HEADER};

/// The formats an import accepts, as the help and refusals name them.
pub const SUPPORTED_FORMATS: &str = "MP4/MOV/M4A/3GP, MKV/WebM, MP3, WAV/W64, FLAC, Ogg/Opus, \
AAC, WMA/ASF, AVI, MPEG-TS, MPEG-PS, CAF, AIFF, AMR, AC-3/E-AC-3, AU";

/// How often the recording is looked at while it is being decoded.
const POLL_EVERY: Duration = Duration::from_secs(1);

/// What `kasetad import` was asked to do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImportArgs {
    pub path: PathBuf,
    pub title: Option<String>,
    pub keep_original: bool,
}

impl ImportArgs {
    /// Reads the arguments after `import`.
    pub fn parse(args: &[String]) -> Result<Self> {
        let mut path = None;
        let mut title = None;
        let mut keep_original = false;
        let mut rest = args.iter();
        while let Some(arg) = rest.next() {
            match arg.as_str() {
                "--keep-original" => keep_original = true,
                "--title" => {
                    let value = rest.next().context("--title needs a value")?;
                    title = Some(value.clone());
                }
                other if other.starts_with("--title=") => {
                    title = Some(other["--title=".len()..].to_string());
                }
                other if other.starts_with("--") => bail!("unknown option {other}"),
                other => {
                    if path.replace(PathBuf::from(other)).is_some() {
                        bail!("import one file at a time");
                    }
                }
            }
        }
        Ok(Self {
            path: path.context("name the file to import: kasetad import <PATH>")?,
            title: title.map(|t| t.trim().to_string()).filter(|t| !t.is_empty()),
            keep_original,
        })
    }
}

/// Uploads the file and reports what became of it.
pub fn run(args: &ImportArgs, port: u16) -> Result<()> {
    let daemon = Daemon::connect(port)?;
    let bytes = std::fs::metadata(&args.path)
        .with_context(|| format!("reading {}", args.path.display()))?
        .len();
    println!(
        "Uploading {} ({:.1} MB)",
        args.path.display(),
        bytes as f64 / 1e6
    );
    let id = daemon.upload(args)?;
    println!("Recording {id} created; decoding the audio.");

    match daemon.wait(id, POLL_EVERY)? {
        Outcome::Imported(item) => {
            println!(
                "Imported \"{}\" ({}).",
                item.title,
                duration(item.duration_ms)
            );
            println!("It is in the library; transcription follows on its own.");
            Ok(())
        }
        Outcome::Failed(reason) if reason == super::job::UNSUPPORTED => {
            bail!("the import failed: {reason}. Supported formats: {SUPPORTED_FORMATS}")
        }
        Outcome::Failed(reason) => bail!("the import failed: {reason}"),
        Outcome::Deleted => bail!("the recording was deleted before the import finished"),
    }
}

fn duration(ms: i64) -> String {
    let s = ms / 1000;
    if s >= 3600 {
        format!("{}:{:02}:{:02}", s / 3600, s / 60 % 60, s % 60)
    } else {
        format!("{}:{:02}", s / 60, s % 60)
    }
}

/// The running daemon, reached over its own API.
pub struct Daemon {
    base: String,
    http: reqwest::blocking::Client,
    token: String,
}

/// How an import ended.
#[derive(Debug)]
pub enum Outcome {
    Imported(Item),
    Failed(String),
    Deleted,
}

/// The parts of a library item the command reads.
#[derive(Debug, Deserialize)]
pub struct Item {
    pub title: String,
    pub status: String,
    pub duration_ms: i64,
    #[serde(default)]
    pub stages: Vec<Stage>,
}

#[derive(Debug, Deserialize)]
pub struct Stage {
    pub stage: String,
    pub state: String,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Deserialize)]
struct Status {
    import: ImportStatus,
}

#[derive(Deserialize)]
struct ImportStatus {
    available: bool,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Deserialize)]
struct Created {
    recording_id: Ulid,
}

impl Daemon {
    /// Finds the daemon on `port` and takes its token, the same one its page
    /// carries, which every change has to present.
    pub fn connect(port: u16) -> Result<Self> {
        let base = format!("http://127.0.0.1:{port}");
        // No overall timeout: an upload of a few gigabytes takes as long as
        // it takes. The daemon gives up on a client that stops sending, and
        // connecting is bounded on its own.
        let http = reqwest::blocking::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(None)
            .build()
            .context("building the HTTP client")?;

        let token = http
            .get(format!("{base}/api/v1/token"))
            .timeout(Duration::from_secs(10))
            .send()
            .map_err(|e| {
                anyhow::anyhow!(
                    "the Kaseta daemon is not reachable at {base} ({e}); start it with \
                     `kasetad serve`, or set KASETA_PORT to the port it runs on"
                )
            })?
            .error_for_status()
            .context("the daemon would not hand out its token")?
            .text()
            .context("reading the daemon's token")?;

        let daemon = Self {
            base,
            http,
            token: token.trim().to_string(),
        };
        let status: Status = daemon.get_json("/api/v1/status")?;
        if !status.import.available {
            bail!(
                "{}",
                status
                    .import
                    .reason
                    .unwrap_or_else(|| "this daemon cannot import files".into())
            );
        }
        Ok(daemon)
    }

    fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.http
            .get(format!("{}{path}", self.base))
            .timeout(Duration::from_secs(30))
            .send()
            .with_context(|| format!("asking the daemon for {path}"))?
            .error_for_status()
            .with_context(|| format!("asking the daemon for {path}"))?
            .json()
            .with_context(|| format!("reading the daemon's answer to {path}"))
    }

    /// Streams the file to the daemon and returns the recording it created.
    pub fn upload(&self, args: &ImportArgs) -> Result<Ulid> {
        let file = std::fs::File::open(&args.path)
            .with_context(|| format!("opening {}", args.path.display()))?;
        let bytes = file
            .metadata()
            .with_context(|| format!("reading {}", args.path.display()))?
            .len();
        let name = file_name(&args.path)?;

        let mut request = self
            .http
            .post(format!("{}/api/v1/imports", self.base))
            .header("x-kaseta-token", &self.token)
            .header(FILENAME_HEADER, percent_encode(&name))
            .header(KEEP_ORIGINAL_HEADER, if args.keep_original { "1" } else { "0" })
            .body(reqwest::blocking::Body::sized(file, bytes));
        if let Some(title) = &args.title {
            request = request.header(TITLE_HEADER, percent_encode(title));
        }

        let response = request.send().context("sending the file to the daemon")?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().unwrap_or_default();
            let message = serde_json::from_str::<serde_json::Value>(&body)
                .ok()
                .and_then(|v| v["error"].as_str().map(str::to_string))
                .unwrap_or(body);
            bail!("the daemon refused the file ({status}): {message}");
        }
        let created: Created = response.json().context("reading the daemon's answer")?;
        Ok(created.recording_id)
    }

    /// Watches the recording until its decode has finished, one way or the
    /// other.
    pub fn wait(&self, id: Ulid, every: Duration) -> Result<Outcome> {
        let mut retrying = false;
        loop {
            let response = self
                .http
                .get(format!("{}/api/v1/recordings/{id}", self.base))
                .timeout(Duration::from_secs(30))
                .send()
                .context("asking the daemon about the import")?;
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(Outcome::Deleted);
            }
            let item: Item = response
                .error_for_status()
                .context("asking the daemon about the import")?
                .json()
                .context("reading the daemon's answer")?;

            let decode = item.stages.iter().find(|s| s.stage == "import_media");
            match decode.map(|s| s.state.as_str()) {
                Some("succeeded") if item.status == "ready" => return Ok(Outcome::Imported(item)),
                Some("failed") => {
                    let reason = decode
                        .and_then(|s| s.error.clone())
                        .unwrap_or_else(|| "no reason was given".into());
                    return Ok(Outcome::Failed(reason));
                }
                Some("retrying") if !retrying => {
                    retrying = true;
                    println!("Decoding hit a problem and will be tried again.");
                }
                _ if item.status == "failed" => {
                    return Ok(Outcome::Failed("the recording could not be imported".into()))
                }
                _ => {}
            }
            std::thread::sleep(every);
        }
    }
}

/// The name the file is uploaded under: its own, as text.
fn file_name(path: &Path) -> Result<String> {
    let name = path
        .file_name()
        .with_context(|| format!("{} does not name a file", path.display()))?;
    // A name that is not UTF-8 is shown with replacement characters rather
    // than refused; the content decides the format, not the name.
    Ok(name.to_string_lossy().into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    fn args(list: &[&str]) -> Result<ImportArgs> {
        ImportArgs::parse(&list.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }

    #[test]
    fn the_arguments_are_read_in_any_order() {
        assert_eq!(
            args(&["talk.mp4"]).unwrap(),
            ImportArgs {
                path: "talk.mp4".into(),
                title: None,
                keep_original: false
            }
        );
        let expected = ImportArgs {
            path: "/tmp/talk.mp4".into(),
            title: Some("Keynote".into()),
            keep_original: true,
        };
        assert_eq!(args(&["--keep-original", "--title", "Keynote", "/tmp/talk.mp4"]).unwrap(), expected);
        assert_eq!(args(&["/tmp/talk.mp4", "--title=Keynote", "--keep-original"]).unwrap(), expected);
        // A blank title is no title; the file's name is used instead.
        assert_eq!(args(&["a.mp3", "--title", "  "]).unwrap().title, None);
    }

    #[test]
    fn unusable_arguments_are_refused() {
        for (list, expected) in [
            (&[][..], "name the file"),
            (&["--keep-original"][..], "name the file"),
            (&["a.mp4", "b.mp4"][..], "one file at a time"),
            (&["a.mp4", "--title"][..], "needs a value"),
            (&["a.mp4", "--keep"][..], "unknown option"),
        ] {
            let err = args(list).unwrap_err().to_string();
            assert!(err.contains(expected), "{list:?}: {err}");
        }
    }

    #[test]
    fn durations_read_like_a_clock() {
        assert_eq!(duration(3_000), "0:03");
        assert_eq!(duration(605_000), "10:05");
        assert_eq!(duration(3_723_000), "1:02:03");
    }

    /// A daemon serving the real router on a real port, with an import
    /// runtime that has tools (never run here) and room on the disk.
    struct Served {
        _dir: tempfile::TempDir,
        db: Arc<Mutex<crate::db::Db>>,
        store: Arc<dyn crate::blobstore::BlobStore>,
        port: u16,
        _runtime: tokio::runtime::Runtime,
    }

    fn serve(available: bool) -> Served {
        use crate::import::sandbox::{HostLayout, Toolchain};
        use crate::import::ImportRuntime;

        let dir = tempfile::TempDir::new().unwrap();
        let store: Arc<dyn crate::blobstore::BlobStore> =
            Arc::new(crate::blobstore::LocalFsStore::new(dir.path()).unwrap());
        let db = Arc::new(Mutex::new(crate::db::Db::open_in_memory().unwrap()));
        let imports = if available {
            ImportRuntime::with(Toolchain::at(
                "/usr/bin/bwrap".into(),
                "/usr/bin/ffmpeg".into(),
                "/usr/bin/ffprobe".into(),
                HostLayout::of_host(),
            ))
            .with_free_space(|_| Ok(1 << 40))
        } else {
            ImportRuntime::unavailable("Importing needs ffmpeg: sudo pacman -S --needed ffmpeg")
        };

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let supervisor = Arc::new(
            crate::supervisor::Supervisor::spawn(Arc::clone(&store), Arc::clone(&db)).unwrap(),
        );
        let router = crate::http::router(
            supervisor,
            Arc::clone(&store),
            Arc::clone(&db),
            Arc::new(imports),
            Arc::new("cli-test-token".into()),
            port,
        );
        runtime.spawn(async move { axum::serve(listener, router).await.unwrap() });

        Served {
            _dir: dir,
            db,
            store,
            port,
            _runtime: runtime,
        }
    }

    /// The command uploads through the same route the page uses, and then
    /// reports what the daemon decided.
    #[test]
    fn a_file_is_uploaded_to_the_running_daemon_and_followed_to_the_end() {
        let served = serve(true);
        let files = tempfile::TempDir::new().unwrap();
        let path = files.path().join("Vortrag \u{dc}ber Rust.mp3");
        std::fs::write(&path, b"not really an mp3, which is the decoder's business").unwrap();

        let daemon = Daemon::connect(served.port).unwrap();
        let id = daemon
            .upload(&ImportArgs {
                path: path.clone(),
                title: Some("Rust talk".into()),
                keep_original: true,
            })
            .unwrap();

        let intent = crate::import::intent::Intent::read(&*served.store, id).unwrap().unwrap();
        assert_eq!(intent.original_filename, "Vortrag \u{dc}ber Rust.mp3");
        assert_eq!(intent.title, "Rust talk");
        assert!(intent.keep_original);
        assert_eq!(intent.bytes, Some(std::fs::metadata(&path).unwrap().len()));

        // The decode is the scheduler's; here it is simply declared done.
        let job = {
            let db = served.db.lock().unwrap();
            let job = db.claim_next_job(1).unwrap().unwrap();
            db.conn()
                .execute(
                    "UPDATE recordings SET status = 'ready', duration_ms = 3000 WHERE id = ?1",
                    rusqlite::params![id.to_string()],
                )
                .unwrap();
            db.conn()
                .execute("UPDATE jobs SET state = 'succeeded' WHERE id = ?1", rusqlite::params![job.id.to_string()])
                .unwrap();
            job
        };
        assert_eq!(job.recording_id, id);
        match daemon.wait(id, Duration::from_millis(10)).unwrap() {
            Outcome::Imported(item) => {
                assert_eq!(item.title, "Rust talk");
                assert_eq!(item.duration_ms, 3000);
            }
            other => panic!("expected an import, got {other:?}"),
        }
    }

    #[test]
    fn a_failed_import_is_reported_with_its_reason() {
        let served = serve(true);
        let files = tempfile::TempDir::new().unwrap();
        let path = files.path().join("text.mp4");
        std::fs::write(&path, b"a shopping list").unwrap();

        let daemon = Daemon::connect(served.port).unwrap();
        let id = daemon
            .upload(&ImportArgs {
                path,
                title: None,
                keep_original: false,
            })
            .unwrap();
        {
            let db = served.db.lock().unwrap();
            let job = db.claim_next_job(1).unwrap().unwrap();
            db.fail_import_terminal(id, job.id, crate::import::job::UNSUPPORTED).unwrap();
        }
        match daemon.wait(id, Duration::from_millis(10)).unwrap() {
            Outcome::Failed(reason) => assert_eq!(reason, crate::import::job::UNSUPPORTED),
            other => panic!("expected a failure, got {other:?}"),
        }
    }

    #[test]
    fn a_refusal_is_reported_in_the_daemons_words() {
        let served = serve(true);
        let daemon = Daemon::connect(served.port).unwrap();
        let files = tempfile::TempDir::new().unwrap();
        let path = files.path().join("empty.wav");
        std::fs::write(&path, b"").unwrap();

        let err = daemon
            .upload(&ImportArgs {
                path,
                title: None,
                keep_original: false,
            })
            .unwrap_err()
            .to_string();
        assert!(err.contains("the file is empty"), "{err}");
    }

    #[test]
    fn a_daemon_that_cannot_import_says_so_before_anything_is_sent() {
        let served = serve(false);
        let err = Daemon::connect(served.port).err().unwrap().to_string();
        assert!(err.contains("Importing needs ffmpeg"), "{err}");
    }

    #[test]
    fn no_daemon_is_a_clear_message() {
        // Bound and released, so nothing is listening there.
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let err = Daemon::connect(port).err().unwrap().to_string();
        assert!(err.contains("not reachable") && err.contains("kasetad serve"), "{err}");
    }
}
