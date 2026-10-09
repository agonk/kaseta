//! Receiving a file to import.
//!
//! The upload is a raw request body, not a form: a form would have to be
//! parsed before its file could be written, and the file can be larger than
//! memory. What the person asked for travels in headers instead, percent
//! encoded so any filename survives the trip, and the body is streamed
//! straight into staging as it arrives.
//!
//! # Order, and why
//!
//! 1. The intent is written first, as `uploading`, before a byte of the file
//!    lands. Whatever happens next, the staging directory says what it is and
//!    when it began, which is what the startup sweep needs to clear it away.
//! 2. The body is streamed into `upload.<ext>.part`, hashed as it goes, and
//!    renamed only once exactly the declared number of bytes has arrived.
//! 3. The intent is rewritten as `uploaded`, with the size and digest.
//! 4. Only then is the recording created, together with its decode, in one
//!    transaction. Nothing in the index ever points at an upload that is not
//!    complete.
//!
//! Any failure, a disconnect included, removes the staging directory: the
//! writer takes its `.part` with it, and a guard takes the rest.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::body::{Body, HttpBody};
use axum::http::HeaderMap;
use kaseta_contracts::ImportStaging;
use ulid::Ulid;

use super::intent::{Intent, IntentState};
use crate::blobstore::BlobStore;
use crate::db::{Db, NewImport};
use crate::staging::AsyncStagingWriter;

/// The file's name on the person's machine, percent-encoded UTF-8.
pub const FILENAME_HEADER: &str = "x-kaseta-filename";
/// The title to give the recording, percent-encoded UTF-8. Optional; the
/// file's name without its extension otherwise.
pub const TITLE_HEADER: &str = "x-kaseta-title";
/// `1` to keep the original file beside the recording, `0` (or absent) not to.
pub const KEEP_ORIGINAL_HEADER: &str = "x-kaseta-keep-original";

/// Longest filename accepted, in bytes once decoded: what every common
/// filesystem allows for one name.
pub const MAX_FILENAME_BYTES: usize = 255;
/// Longest title accepted, in characters.
pub const MAX_TITLE_CHARS: usize = 200;

/// Free space that must remain beyond the upload itself before one is
/// accepted. The upload is only the start: decoding writes chunks and exports
/// beside it, and the disk is shared with everything else on the machine.
pub const UPLOAD_HEADROOM_BYTES: u64 = 1 << 30;

/// How long an upload may go without a single byte arriving before it is
/// abandoned. Long enough for a stalled network to recover, short enough that
/// a client that went away does not hold the one upload slot for long.
pub const BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// What the person asked for, read from the request's headers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadRequest {
    /// The file's name, without any directory it was given with.
    pub filename: String,
    /// The recording's title: the one given, or the file's name without its
    /// extension.
    pub title: String,
    pub keep_original: bool,
}

impl UploadRequest {
    /// Reads and checks the metadata headers. The error is a sentence for
    /// the person who sent them.
    pub fn from_headers(headers: &HeaderMap) -> std::result::Result<Self, String> {
        let raw = headers
            .get(FILENAME_HEADER)
            .ok_or_else(|| format!("the file's name is missing ({FILENAME_HEADER})"))?;
        let decoded = percent_decode(raw.as_bytes())
            .map_err(|e| format!("the file's name is not readable: {e}"))?;
        // A browser sends a bare name, but a client might send a path. Only
        // the last segment is the file's name; the rest says something about
        // the sender's machine that is nobody's business here.
        let filename = decoded
            .rsplit(['/', '\\'])
            .next()
            .unwrap_or_default()
            .trim()
            .to_string();
        if filename.is_empty() {
            return Err("the file has no name".into());
        }
        if filename.len() > MAX_FILENAME_BYTES {
            return Err(format!(
                "the file's name is longer than {MAX_FILENAME_BYTES} bytes"
            ));
        }
        if filename.chars().any(char::is_control) {
            return Err("the file's name contains control characters".into());
        }

        let title = match headers.get(TITLE_HEADER) {
            None => None,
            Some(raw) => {
                let title = percent_decode(raw.as_bytes())
                    .map_err(|e| format!("the title is not readable: {e}"))?;
                let title = title.trim().to_string();
                if title.chars().count() > MAX_TITLE_CHARS {
                    return Err(format!("the title is longer than {MAX_TITLE_CHARS} characters"));
                }
                if title.chars().any(char::is_control) {
                    return Err("the title contains control characters".into());
                }
                Some(title).filter(|t| !t.is_empty())
            }
        };

        let keep_original = match headers.get(KEEP_ORIGINAL_HEADER).map(|v| v.as_bytes()) {
            None | Some(b"0") => false,
            Some(b"1") => true,
            Some(_) => return Err(format!("{KEEP_ORIGINAL_HEADER} must be 1 or 0")),
        };

        Ok(Self {
            title: title.unwrap_or_else(|| stem(&filename).to_string()),
            filename,
            keep_original,
        })
    }

    /// The extension the file was uploaded with, as given. Reduced to a safe
    /// one when it becomes part of a key.
    fn extension(&self) -> &str {
        match self.filename.rsplit_once('.') {
            Some((stem, ext)) if !stem.is_empty() => ext,
            _ => "",
        }
    }
}

/// A file's name without its extension, or the whole name when removing the
/// extension would leave nothing (`.profile`).
fn stem(filename: &str) -> &str {
    match filename.rsplit_once('.') {
        Some((stem, _)) if !stem.trim().is_empty() => stem,
        _ => filename,
    }
}

/// Percent-encodes `text` for a metadata header: every byte that is not an
/// unreserved URL character becomes `%XX`, so any name is plain ASCII on the
/// wire.
pub fn percent_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Decodes a percent-encoded header value, strictly.
///
/// Printable ASCII passes through, so what a browser's `encodeURIComponent`
/// leaves alone is accepted as it is. A `%` must be followed by two hex
/// digits, raw bytes outside printable ASCII are refused (they mean the value
/// was not encoded at all), and the result must be UTF-8. A lenient decoder
/// would turn each of those into a guess about what the name was.
pub fn percent_decode(raw: &[u8]) -> std::result::Result<String, String> {
    let mut bytes = Vec::with_capacity(raw.len());
    let mut rest = raw;
    while let Some((&byte, tail)) = rest.split_first() {
        match byte {
            b'%' => {
                let hex = tail
                    .get(..2)
                    .and_then(|h| std::str::from_utf8(h).ok())
                    .and_then(|h| u8::from_str_radix(h, 16).ok())
                    .ok_or("a % is not followed by two hex digits")?;
                bytes.push(hex);
                rest = &tail[2..];
            }
            0x20..=0x7e => {
                bytes.push(byte);
                rest = tail;
            }
            _ => return Err("it contains bytes that were not percent-encoded".into()),
        }
    }
    String::from_utf8(bytes).map_err(|_| "it is not UTF-8".into())
}

/// One upload at a time.
///
/// Two large uploads at once would each pass the free-space check against a
/// disk the other is about to fill. One at a time keeps that check honest
/// without a ledger of reservations, and a person importing files does not
/// need them to arrive in parallel.
#[derive(Debug, Default)]
pub struct UploadGate {
    busy: AtomicBool,
}

impl UploadGate {
    /// The slot, or `None` while another upload holds it.
    pub fn try_enter(self: &Arc<Self>) -> Option<UploadPermit> {
        self.busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| UploadPermit(Arc::clone(self)))
    }
}

/// Holds the upload slot. Released when dropped, which covers every way an
/// upload can end: success, refusal, error and a client that went away.
#[derive(Debug)]
pub struct UploadPermit(Arc<UploadGate>);

impl Drop for UploadPermit {
    fn drop(&mut self) {
        self.0.busy.store(false, Ordering::Release);
    }
}

/// Why an upload did not become an import.
#[derive(Debug)]
pub enum UploadError {
    /// Something about the request: a body shorter or longer than declared,
    /// a client that stopped sending. The message is for the sender.
    Request(String),
    /// Something on this machine.
    Internal(anyhow::Error),
}

impl From<anyhow::Error> for UploadError {
    fn from(e: anyhow::Error) -> Self {
        Self::Internal(e)
    }
}

/// Streams `body` into staging and creates the recording that will decode it.
/// Returns the new recording's id.
///
/// `length` is the declared size, already admitted against the limits; the
/// body must be exactly that long.
pub async fn receive(
    store: Arc<dyn BlobStore>,
    db: Arc<Mutex<Db>>,
    request: UploadRequest,
    length: u64,
    mut body: Body,
    idle: Duration,
) -> std::result::Result<Ulid, UploadError> {
    let id = Ulid::new();
    let staging = ImportStaging::new(id);
    let upload_key = staging
        .upload(request.extension())
        .context("building the upload's key")?;
    let ext = upload_key
        .as_str()
        .rsplit_once('.')
        .map(|(_, ext)| ext.to_string())
        .context("the upload's key has no extension")?;
    let path = store
        .local_root()
        .context("imports need a store on the local filesystem")?
        .join(upload_key.as_str());

    // From here on, anything that goes wrong takes the staging directory
    // with it. Declared before the writer, so the writer (and its `.part`)
    // goes first.
    let mut cleanup = StagingCleanup {
        store: Arc::clone(&store),
        id,
        armed: true,
    };

    let mut intent = Intent {
        state: IntentState::Uploading,
        recording_id: id,
        created_at: time::OffsetDateTime::now_utc(),
        original_filename: request.filename.clone(),
        ext,
        keep_original: request.keep_original,
        title: request.title.clone(),
        bytes: None,
        sha256: None,
    };
    write_intent(&store, &intent).await?;

    let mut writer = AsyncStagingWriter::create(&path, length).await?;
    loop {
        let frame = match tokio::time::timeout(idle, std::future::poll_fn(|cx| {
            std::pin::Pin::new(&mut body).poll_frame(cx)
        }))
        .await
        {
            Err(_) => {
                return Err(UploadError::Request(format!(
                    "the upload stalled: nothing arrived for {} seconds",
                    idle.as_secs().max(1)
                )))
            }
            Ok(None) => break,
            Ok(Some(Err(e))) => {
                return Err(UploadError::Request(format!("the upload was interrupted: {e}")))
            }
            Ok(Some(Ok(frame))) => frame,
        };
        let Ok(data) = frame.into_data() else {
            continue;
        };
        if writer.written() + data.len() as u64 > length {
            return Err(UploadError::Request(format!(
                "the upload is longer than the {length} bytes it declared"
            )));
        }
        writer.write(&data).await?;
    }
    if writer.written() != length {
        return Err(UploadError::Request(format!(
            "the upload ended after {} of its {length} bytes",
            writer.written()
        )));
    }
    let put = writer.commit().await?;

    intent.state = IntentState::Uploaded;
    intent.bytes = Some(put.bytes);
    intent.sha256 = Some(put.sha256);
    write_intent(&store, &intent).await?;

    let new = NewImport {
        recording_id: id,
        started_at: intent.started_at(),
        title: intent.title.clone(),
    };
    tokio::task::spawn_blocking(move || -> Result<()> {
        let db = db.lock().map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        db.create_import(&new)?;
        Ok(())
    })
    .await
    .context("creating the recording")??;

    cleanup.armed = false;
    tracing::info!(recording_id = %id, bytes = length, "received a file to import");
    Ok(id)
}

/// Writes the intent atomically, off the async runtime.
async fn write_intent(store: &Arc<dyn BlobStore>, intent: &Intent) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(intent).context("serialising the import intent")?;
    let key = ImportStaging::new(intent.recording_id).intent();
    let store = Arc::clone(store);
    tokio::task::spawn_blocking(move || store.put(&key, &bytes))
        .await
        .context("writing the import intent")?
        .context("writing the import intent")
}

/// Removes an upload's staging directory unless the upload became an import.
struct StagingCleanup {
    store: Arc<dyn BlobStore>,
    id: Ulid,
    armed: bool,
}

impl Drop for StagingCleanup {
    fn drop(&mut self) {
        if self.armed {
            // Synchronous: drop cannot await, and this is a handful of
            // unlinks. Should it fail, the startup sweep finds an intent that
            // never reached `uploaded` and removes it once it is a day old.
            if let Err(e) = super::remove_staging(&*self.store, self.id) {
                tracing::warn!(id = %self.id, error = %format!("{e:#}"), "could not remove an abandoned upload");
            }
        }
    }
}

/// Whether `free` bytes leave room for an upload of `length`.
pub fn room_for(length: u64, free: u64) -> bool {
    free >= length.saturating_add(UPLOAD_HEADROOM_BYTES)
}

/// The free space an upload of `length` needs, for the refusal.
pub fn space_needed(length: u64) -> u64 {
    length.saturating_add(UPLOAD_HEADROOM_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers(pairs: &[(&str, &[u8])]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_bytes(value).unwrap(),
            );
        }
        map
    }

    fn parse(pairs: &[(&str, &[u8])]) -> std::result::Result<UploadRequest, String> {
        UploadRequest::from_headers(&headers(pairs))
    }

    #[test]
    fn percent_decoding_is_strict() {
        assert_eq!(percent_decode(b"Lecture%203.mp4").unwrap(), "Lecture 3.mp4");
        // What encodeURIComponent leaves alone is taken as it is.
        assert_eq!(percent_decode(b"it's (final)!.mp4").unwrap(), "it's (final)!.mp4");
        assert_eq!(percent_decode(b"%C3%9Cber%20%E2%80%93%20x").unwrap(), "\u{dc}ber \u{2013} x");
        assert_eq!(percent_decode(b"%c3%bc").unwrap(), "\u{fc}", "either case of hex");

        for (bad, why) in [
            (&b"100%"[..], "a lone %"),
            (b"%4", "one hex digit"),
            (b"%G1", "not hex"),
            (b"%FF%FE", "not UTF-8"),
            (b"%C3", "a truncated character"),
            (b"caf\xc3\xa9", "raw bytes that were never encoded"),
            (b"tab\there", "a raw control byte"),
        ] {
            assert!(percent_decode(bad).is_err(), "{why} must be refused");
        }
    }

    #[test]
    fn encoding_is_plain_ascii_and_round_trips() {
        for name in [
            "Lecture 3.mp4",
            "\u{dc}ber \u{2013} Interview (2025).mkv",
            "100% sure.mp3",
            "a/b\\c.wav",
            "\u{1f3a4}.m4a",
        ] {
            let encoded = percent_encode(name);
            assert!(encoded.bytes().all(|b| b.is_ascii_graphic()), "{encoded}");
            assert_eq!(percent_decode(encoded.as_bytes()).unwrap(), name);
        }
        assert_eq!(percent_encode("a b.mp4"), "a%20b.mp4");
    }

    #[test]
    fn the_metadata_headers_are_read_and_defaulted() {
        let request = parse(&[(FILENAME_HEADER, b"Lecture%203.mp4")]).unwrap();
        assert_eq!(
            request,
            UploadRequest {
                filename: "Lecture 3.mp4".into(),
                title: "Lecture 3".into(),
                keep_original: false,
            }
        );
        assert_eq!(request.extension(), "mp4");

        let request = parse(&[
            (FILENAME_HEADER, b"talk.mkv"),
            (TITLE_HEADER, b"%20Keynote%20"),
            (KEEP_ORIGINAL_HEADER, b"1"),
        ])
        .unwrap();
        assert_eq!(request.title, "Keynote");
        assert!(request.keep_original);

        // A blank title is no title.
        let request = parse(&[(FILENAME_HEADER, b"talk.mkv"), (TITLE_HEADER, b"%20")]).unwrap();
        assert_eq!(request.title, "talk");
    }

    #[test]
    fn a_name_without_an_extension_keeps_all_of_itself() {
        let request = parse(&[(FILENAME_HEADER, b".profile")]).unwrap();
        assert_eq!(request.title, ".profile");
        assert_eq!(request.extension(), "");
        let request = parse(&[(FILENAME_HEADER, b"recording")]).unwrap();
        assert_eq!(request.title, "recording");
        assert_eq!(request.extension(), "");
    }

    /// Only the last segment of a path is the file's name.
    #[test]
    fn a_path_is_reduced_to_its_name() {
        for sent in [&b"%2Fhome%2Fme%2Ftalk.mp4"[..], b"C%3A%5CUsers%5Cme%5Ctalk.mp4", b"..%2F..%2Ftalk.mp4"] {
            assert_eq!(parse(&[(FILENAME_HEADER, sent)]).unwrap().filename, "talk.mp4");
        }
    }

    #[test]
    fn unusable_metadata_is_refused_with_a_reason() {
        let long_name = format!("{}.mp4", "a".repeat(MAX_FILENAME_BYTES - 3));
        let long_title = "t".repeat(MAX_TITLE_CHARS + 1);
        // Headers to send, and a phrase the refusal must contain.
        type Case<'a> = (Vec<(&'a str, Vec<u8>)>, &'a str);
        let cases: Vec<Case> = vec![
            (vec![], "missing"),
            (vec![(FILENAME_HEADER, b"".to_vec())], "no name"),
            (vec![(FILENAME_HEADER, b"%2F".to_vec())], "no name"),
            (vec![(FILENAME_HEADER, b"%FF.mp4".to_vec())], "not readable"),
            (vec![(FILENAME_HEADER, b"a%0Ab.mp4".to_vec())], "control"),
            (vec![(FILENAME_HEADER, long_name.into_bytes())], "longer than 255"),
            (
                vec![(FILENAME_HEADER, b"a.mp4".to_vec()), (TITLE_HEADER, long_title.into_bytes())],
                "longer than 200",
            ),
            (
                vec![(FILENAME_HEADER, b"a.mp4".to_vec()), (TITLE_HEADER, b"%E2%82".to_vec())],
                "title is not readable",
            ),
            (
                vec![(FILENAME_HEADER, b"a.mp4".to_vec()), (TITLE_HEADER, b"x%0Dy".to_vec())],
                "control",
            ),
            (
                vec![(FILENAME_HEADER, b"a.mp4".to_vec()), (KEEP_ORIGINAL_HEADER, b"yes".to_vec())],
                "must be 1 or 0",
            ),
        ];
        for (pairs, expected) in cases {
            let pairs: Vec<(&str, &[u8])> = pairs.iter().map(|(n, v)| (*n, v.as_slice())).collect();
            let err = parse(&pairs).unwrap_err();
            assert!(err.contains(expected), "{pairs:?}: {err}");
        }

        // Exactly at the limits is fine, counted in bytes and characters.
        let at_limit = format!("{}.mp4", "a".repeat(MAX_FILENAME_BYTES - 4));
        assert!(parse(&[(FILENAME_HEADER, at_limit.as_bytes())]).is_ok());
        let wide_title = percent_encode(&"\u{e9}".repeat(MAX_TITLE_CHARS));
        assert!(parse(&[(FILENAME_HEADER, b"a.mp4"), (TITLE_HEADER, wide_title.as_bytes())]).is_ok());
    }

    #[test]
    fn one_upload_holds_the_slot_until_it_ends() {
        let gate = Arc::new(UploadGate::default());
        let first = gate.try_enter().expect("the slot is free");
        assert!(gate.try_enter().is_none(), "a second upload must wait");
        drop(first);
        assert!(gate.try_enter().is_some(), "released when the first ended");
    }

    #[test]
    fn admission_leaves_a_gigabyte_beyond_the_upload() {
        assert!(room_for(100, 100 + UPLOAD_HEADROOM_BYTES));
        assert!(!room_for(100, 99 + UPLOAD_HEADROOM_BYTES));
        assert!(!room_for(u64::MAX, u64::MAX - 1), "no overflow into acceptance");
        assert_eq!(space_needed(5), 5 + (1 << 30));
    }

    /// A body that sends `frames` and then, if `then_stall`, never another
    /// thing, like a client that went quiet.
    struct Scripted {
        frames: std::collections::VecDeque<std::result::Result<Vec<u8>, &'static str>>,
        then_stall: bool,
    }

    impl HttpBody for Scripted {
        type Data = axum::body::Bytes;
        type Error = std::io::Error;

        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<std::result::Result<http_body::Frame<Self::Data>, Self::Error>>>
        {
            match self.frames.pop_front() {
                Some(Ok(bytes)) => {
                    std::task::Poll::Ready(Some(Ok(http_body::Frame::data(bytes.into()))))
                }
                Some(Err(e)) => std::task::Poll::Ready(Some(Err(std::io::Error::other(e)))),
                None if self.then_stall => std::task::Poll::Pending,
                None => std::task::Poll::Ready(None),
            }
        }
    }

    fn body(frames: Vec<std::result::Result<Vec<u8>, &'static str>>, then_stall: bool) -> Body {
        Body::new(Scripted {
            frames: frames.into(),
            then_stall,
        })
    }

    struct Place {
        _dir: tempfile::TempDir,
        store: Arc<dyn BlobStore>,
        db: Arc<Mutex<Db>>,
    }

    fn place() -> Place {
        let dir = tempfile::TempDir::new().unwrap();
        Place {
            store: Arc::new(crate::blobstore::LocalFsStore::new(dir.path()).unwrap()),
            _dir: dir,
            db: Arc::new(Mutex::new(Db::open_in_memory().unwrap())),
        }
    }

    impl Place {
        /// Whatever is in staging, at any depth.
        fn staged(&self) -> Vec<std::path::PathBuf> {
            let root = self.store.local_root().unwrap().join("imports");
            let mut found = Vec::new();
            let mut pending = vec![root];
            while let Some(dir) = pending.pop() {
                let Ok(entries) = std::fs::read_dir(&dir) else { continue };
                for entry in entries.flatten() {
                    found.push(entry.path());
                    if entry.path().is_dir() {
                        pending.push(entry.path());
                    }
                }
            }
            found
        }

        fn recordings(&self) -> i64 {
            self.db
                .lock()
                .unwrap()
                .conn()
                .query_row("SELECT COUNT(*) FROM recordings", [], |r| r.get(0))
                .unwrap()
        }

        async fn receive(&self, length: u64, body: Body, idle: Duration) -> std::result::Result<Ulid, UploadError> {
            let request = UploadRequest {
                filename: "talk.mp4".into(),
                title: "talk".into(),
                keep_original: true,
            };
            receive(Arc::clone(&self.store), Arc::clone(&self.db), request, length, body, idle).await
        }
    }

    fn request_error(result: std::result::Result<Ulid, UploadError>) -> String {
        match result {
            Err(UploadError::Request(message)) => message,
            other => panic!("expected the request to be refused, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_complete_upload_becomes_an_import_waiting_to_be_decoded() {
        let place = place();
        let id = place
            .receive(6, body(vec![Ok(b"abc".to_vec()), Ok(b"def".to_vec())], false), BODY_IDLE_TIMEOUT)
            .await
            .unwrap();

        let intent = Intent::read(&*place.store, id).unwrap().unwrap();
        assert_eq!(intent.state, IntentState::Uploaded);
        assert_eq!(intent.recording_id, id);
        assert_eq!(intent.bytes, Some(6));
        assert_eq!(
            intent.sha256.as_deref(),
            Some(crate::blobstore::sha256_reader(&b"abcdef"[..]).unwrap().1.as_str())
        );
        assert_eq!((intent.ext.as_str(), intent.title.as_str()), ("mp4", "talk"));
        assert!(intent.keep_original);
        assert!(super::super::upload_present(&*place.store, id).unwrap());
        let uploaded = std::fs::read(
            place.store.local_root().unwrap().join(intent.upload_key().unwrap().as_str()),
        )
        .unwrap();
        assert_eq!(uploaded, b"abcdef");
        assert!(
            !place.staged().iter().any(|p| p.to_string_lossy().ends_with(".part")),
            "nothing half-written is left"
        );

        let db = place.db.lock().unwrap();
        let (status, origin, title, started_at): (String, String, String, i64) = db
            .conn()
            .query_row(
                "SELECT status, origin, title, started_at FROM recordings WHERE id = ?1",
                rusqlite::params![id.to_string()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!((status.as_str(), origin.as_str(), title.as_str()), ("processing", "imported", "talk"));
        assert_eq!(started_at, intent.started_at().unix_timestamp());
        let job = db.claim_next_job(1).unwrap().unwrap();
        assert_eq!((job.recording_id, job.job_type), (id, kaseta_contracts::JobType::ImportMedia));
    }

    #[tokio::test]
    async fn a_short_body_leaves_nothing_behind() {
        let place = place();
        let err = request_error(place.receive(10, body(vec![Ok(b"abc".to_vec())], false), BODY_IDLE_TIMEOUT).await);
        assert!(err.contains("ended after 3 of its 10 bytes"), "{err}");
        assert!(place.staged().is_empty(), "left {:?}", place.staged());
        assert_eq!(place.recordings(), 0);
    }

    #[tokio::test]
    async fn a_body_longer_than_declared_is_refused_at_the_first_extra_byte() {
        let place = place();
        let err = request_error(
            place
                .receive(4, body(vec![Ok(b"abc".to_vec()), Ok(b"de".to_vec())], false), BODY_IDLE_TIMEOUT)
                .await,
        );
        assert!(err.contains("longer than the 4 bytes"), "{err}");
        assert!(place.staged().is_empty());
        assert_eq!(place.recordings(), 0);
    }

    #[tokio::test]
    async fn an_interrupted_body_leaves_nothing_behind() {
        let place = place();
        let err = request_error(
            place
                .receive(10, body(vec![Ok(b"abc".to_vec()), Err("connection reset")], false), BODY_IDLE_TIMEOUT)
                .await,
        );
        assert!(err.contains("interrupted") && err.contains("connection reset"), "{err}");
        assert!(place.staged().is_empty());
        assert_eq!(place.recordings(), 0);
    }

    #[tokio::test]
    async fn a_client_that_goes_quiet_is_given_up_on() {
        let place = place();
        let err = request_error(
            place
                .receive(10, body(vec![Ok(b"abc".to_vec())], true), Duration::from_millis(50))
                .await,
        );
        assert!(err.contains("stalled"), "{err}");
        assert!(place.staged().is_empty());
    }

    /// A client that disconnects can take the handler with it mid-await. The
    /// cleanup must not depend on the handler running to the end.
    #[tokio::test]
    async fn an_upload_dropped_midway_cleans_up_after_itself() {
        let place = Arc::new(place());
        let receiving = {
            let place = Arc::clone(&place);
            tokio::spawn(async move {
                place
                    .receive(10, body(vec![Ok(b"abc".to_vec())], true), BODY_IDLE_TIMEOUT)
                    .await
            })
        };
        // Until the first bytes are on disk.
        for _ in 0..200 {
            if place.staged().iter().any(|p| p.to_string_lossy().ends_with(".part")) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!place.staged().is_empty(), "the upload should have started");

        receiving.abort();
        let _ = receiving.await;
        assert!(place.staged().is_empty(), "left {:?}", place.staged());
        assert_eq!(place.recordings(), 0);
    }
}
