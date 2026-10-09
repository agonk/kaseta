//! Copying recordings to S3-compatible object storage.
//!
//! Signing is implemented here rather than pulled in as a vendor SDK. The
//! request surface Kaseta needs is a `HEAD`, a `PUT`, and the four calls of a
//! multipart upload; an SDK for that would add a hundred crates to a binary
//! whose whole point is being small, and the signing algorithm is short, fixed,
//! and testable against the published reference vectors (see [`crate::sigv4`]).
//!
//! S3-compatible rather than S3-specific: Cloudflare R2, Backblaze, MinIO and
//! S3 itself all speak this, and only the endpoint distinguishes them.
//!
//! # Objects are streamed from disk
//!
//! A recording's objects range from a few hundred bytes of JSON to a kept
//! original of several gigabytes, so none is ever read into memory to be sent.
//! Every body is a file, hashed first and then read again as it is sent: S3
//! checks the declared hash against what arrives, and a second read of a file
//! costs nothing next to holding it. An object up to [`SINGLE_PUT_MAX`] goes in
//! one `PUT`. Anything larger goes as a multipart upload whose parts are each
//! hashed and sent the same way, and which is aborted on every path that does
//! not complete it, so a failure leaves no half-assembled upload accruing
//! storage charges in someone's bucket.

use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::os::unix::fs::FileExt;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use kaseta_contracts::{BlobKey, RecordingManifest};
use reqwest::blocking::{Body, Client, Response};
use reqwest::Method;
use sha2::{Digest, Sha256};

use crate::blobstore::BlobStore;
use crate::config::RemoteStorageSettings;
use crate::sigv4::{self, CanonicalRequest, Credentials, EMPTY_SHA256};

const MIB: u64 = 1024 * 1024;

/// The largest object sent in a single `PUT`.
///
/// S3 accepts up to 5 GiB that way, but a single request is all or nothing:
/// a connection dropped at 90% sends the whole object again. Above this size
/// the cost of a retry is bounded by a part instead.
pub const SINGLE_PUT_MAX: u64 = 64 * MIB;

/// The smallest part a multipart upload is cut into.
///
/// Well above S3's 5 MiB minimum, so a large original is a few hundred
/// requests rather than thousands, and small enough that a failed part is
/// cheap to send again.
const MIN_PART_BYTES: u64 = 32 * MIB;

/// How many parts an upload is planned to need at most.
///
/// S3 allows 10,000. Planning for fewer leaves headroom for a file that grows
/// between being measured and being sent rather than failing at the last part.
const PLANNED_MAX_PARTS: u64 = 9_000;

/// Time allowed for any request before its body is counted.
const REQUEST_BASE_TIMEOUT: Duration = Duration::from_secs(60);

/// The slowest upload rate a request is given time for.
///
/// A timeout here exists to notice a connection that has stopped, not to
/// police a slow one. A home uplink can sit at a few megabits, and a timeout
/// sized for a fast link would fail a large part on it identically on every
/// attempt.
const SLOWEST_UPLOAD_BYTES_PER_SEC: u64 = 256 * 1024;

/// What object storage needs in order to be written to.
#[derive(Clone, Debug)]
pub struct RemoteTarget {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl RemoteTarget {
    /// Builds a target from settings, or explains what is missing.
    ///
    /// Reported as one message naming every absent field, so configuring this
    /// is not a sequence of one-at-a-time corrections.
    pub fn from_settings(settings: &RemoteStorageSettings) -> Result<Self> {
        let mut missing = Vec::new();
        let field = |value: &Option<String>, name: &str, missing: &mut Vec<String>| {
            match value.as_deref().map(str::trim).filter(|v| !v.is_empty()) {
                Some(v) => v.to_string(),
                None => {
                    missing.push(name.to_string());
                    String::new()
                }
            }
        };

        let bucket = field(&settings.bucket, "bucket", &mut missing);
        let access_key_id = field(&settings.access_key_id, "access key ID", &mut missing);
        let secret_access_key =
            field(&settings.secret_access_key, "secret access key", &mut missing);

        if !missing.is_empty() {
            bail!("cloud backup is missing: {}", missing.join(", "));
        }

        // R2 ignores the region but still requires one in the signature; `auto`
        // is what it documents.
        let region = settings
            .region
            .clone()
            .filter(|r| !r.trim().is_empty())
            .unwrap_or_else(|| "auto".into());

        let endpoint = settings
            .endpoint
            .clone()
            .filter(|e| !e.trim().is_empty())
            .unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"))
            .trim_end_matches('/')
            .to_string();

        if !endpoint.starts_with("https://") {
            // Credentials and recordings would otherwise cross the network in
            // the clear.
            bail!("the storage endpoint must be https");
        }

        Ok(Self {
            endpoint,
            region,
            bucket,
            access_key_id,
            secret_access_key,
        })
    }

    /// The endpoint's scheme. Always `https` for a target built from settings;
    /// the tests' in-process server is the only plain-http one.
    fn scheme(&self) -> &str {
        if self.endpoint.starts_with("http://") {
            "http"
        } else {
            "https"
        }
    }

    fn without_scheme(&self) -> &str {
        self.endpoint
            .strip_prefix("https://")
            .or_else(|| self.endpoint.strip_prefix("http://"))
            .unwrap_or(&self.endpoint)
    }

    fn host(&self) -> &str {
        self.without_scheme().split('/').next().unwrap_or_default()
    }

    /// Any path the endpoint itself carries, e.g. `https://host/minio`.
    ///
    /// This has to appear in the signature as well as the URL. Signing
    /// `/bucket/key` while requesting `/minio/bucket/key` is rejected as a
    /// signature mismatch, with nothing in the error naming the cause.
    fn base_path(&self) -> &str {
        let rest = self.without_scheme();
        match rest.find('/') {
            Some(at) => &rest[at..],
            None => "",
        }
    }

    /// The path S3 signs and serves, identical in both.
    fn canonical_uri(&self, key: &str) -> String {
        format!("{}/{}/{}", self.base_path(), self.bucket, sigv4::encode_path(key))
    }

    /// The URL for `key`, with its query in exactly the form that is signed.
    fn url_for(&self, key: &str, query: &[(&str, &str)]) -> String {
        let mut url = format!("{}://{}{}", self.scheme(), self.host(), self.canonical_uri(key));
        if !query.is_empty() {
            url.push('?');
            url.push_str(&sigv4::canonical_query(query));
        }
        url
    }

    fn credentials(&self) -> Credentials<'_> {
        Credentials {
            access_key_id: &self.access_key_id,
            secret_access_key: &self.secret_access_key,
        }
    }
}

/// A signed request, ready to send.
pub struct SignedRequest {
    pub url: String,
    /// Every header to send with it, the signature included.
    pub headers: Vec<(String, String)>,
}

#[cfg(test)]
impl SignedRequest {
    fn header(&self, name: &str) -> &str {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
            .unwrap_or_else(|| panic!("no {name} header"))
    }
}

/// Signs a request for `key` with AWS Signature Version 4.
///
/// `headers` are signed in addition to `host`, `x-amz-date` and
/// `x-amz-content-sha256`. S3 requires every `x-amz-*` header that is sent to
/// be signed, so metadata goes through here rather than being added after.
pub fn sign(
    target: &RemoteTarget,
    method: &str,
    key: &str,
    query: &[(&str, &str)],
    headers: &[(&str, &str)],
    payload_sha256: &str,
    now: time::OffsetDateTime,
) -> SignedRequest {
    let uri = target.canonical_uri(key);
    let mut all = Vec::with_capacity(headers.len() + 1);
    all.push(("host", target.host()));
    all.extend_from_slice(headers);

    let signature = sigv4::sign(
        &CanonicalRequest {
            method,
            uri: &uri,
            query,
            headers: &all,
            payload_sha256,
        },
        target.credentials(),
        &target.region,
        "s3",
        now,
    );

    SignedRequest {
        url: target.url_for(key, query),
        headers: signature.headers,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutResult {
    Uploaded,
    AlreadyPresent,
}

/// Header carrying the digest of what was uploaded.
///
/// Length alone cannot decide this: a regenerated export can be a different
/// recording of the same size, and skipping it would leave stale audio under a
/// key that claims to be current.
const DIGEST_HEADER: &str = "x-amz-meta-kaseta-sha256";

/// How long one request may take, given how much it sends.
fn request_timeout(body_bytes: u64) -> Duration {
    REQUEST_BASE_TIMEOUT + Duration::from_secs(body_bytes / SLOWEST_UPLOAD_BYTES_PER_SEC)
}

/// The size every part but the last is cut to, for an object of `total` bytes.
///
/// At least [`MIN_PART_BYTES`], larger when that would need more than
/// [`PLANNED_MAX_PARTS`] parts, and a whole number of MiB either way.
fn part_size(total: u64) -> u64 {
    let needed = total.div_ceil(PLANNED_MAX_PARTS).max(MIN_PART_BYTES);
    needed.div_ceil(MIB) * MIB
}

/// Uploads one object from a file, skipping it if the same bytes are already
/// there.
///
/// `sha256` and `bytes` describe the file's whole content and are what the
/// caller already knows or has just computed; the file is not hashed again as
/// a whole. Uploads are retried after failures and re-run on restart, so this
/// must be idempotent and must not re-send a large object needlessly.
pub fn put_object_file(
    client: &Client,
    target: &RemoteTarget,
    key: &str,
    file: &File,
    sha256: &str,
    bytes: u64,
) -> Result<PutResult> {
    if object_matches(client, target, key, sha256) {
        return Ok(PutResult::AlreadyPresent);
    }
    if bytes <= SINGLE_PUT_MAX {
        put_single(client, target, key, file, sha256, bytes)?;
    } else {
        put_multipart(client, target, key, file, sha256, bytes)?;
    }
    Ok(PutResult::Uploaded)
}

/// One request against an object, before it is signed.
struct Call<'a> {
    method: Method,
    query: &'a [(&'a str, &'a str)],
    /// Headers signed and sent besides the ones every request has.
    headers: &'a [(&'a str, &'a str)],
    payload_sha256: &'a str,
}

/// Sends one signed request, with its body and the body's length when it has
/// one.
fn send(
    client: &Client,
    target: &RemoteTarget,
    key: &str,
    call: Call<'_>,
    body: Option<(Body, u64)>,
) -> reqwest::Result<Response> {
    let signed = sign(
        target,
        call.method.as_str(),
        key,
        call.query,
        call.headers,
        call.payload_sha256,
        time::OffsetDateTime::now_utc(),
    );
    let body_len = body.as_ref().map_or(0, |(_, len)| *len);
    let mut request = client
        .request(call.method, &signed.url)
        .timeout(request_timeout(body_len));
    for (name, value) in &signed.headers {
        request = request.header(name.as_str(), value.as_str());
    }
    if let Some((body, _)) = body {
        request = request.body(body);
    }
    request.send()
}

/// The error for a response that was not a success, with what the server said.
///
/// The body names the actual problem (a wrong region, a missing bucket, an
/// expired key) where the status alone makes them all look the same.
fn rejected(what: &str, key: &str, response: Response) -> anyhow::Error {
    let status = response.status();
    let body = response.text().unwrap_or_default();
    let preview: String = body.chars().take(400).collect();
    anyhow!("storage rejected {what} {key}: {status} {preview}")
}

fn put_single(
    client: &Client,
    target: &RemoteTarget,
    key: &str,
    file: &File,
    sha256: &str,
    bytes: u64,
) -> Result<()> {
    let reader = FileRange::new(file, 0, bytes)?;
    let call = Call {
        method: Method::PUT,
        query: &[],
        // Recorded so the next run can tell "already uploaded" from "same size
        // by coincidence".
        headers: &[(DIGEST_HEADER, sha256)],
        payload_sha256: sha256,
    };
    let response = send(client, target, key, call, Some((Body::sized(reader, bytes), bytes)))
    .with_context(|| format!("uploading {key}"))?;
    if !response.status().is_success() {
        return Err(rejected("the upload of", key, response));
    }
    Ok(())
}

fn put_multipart(
    client: &Client,
    target: &RemoteTarget,
    key: &str,
    file: &File,
    sha256: &str,
    bytes: u64,
) -> Result<()> {
    let upload_id = create_multipart(client, target, key, sha256)?;
    match send_parts(client, target, key, file, sha256, bytes, &upload_id) {
        Ok(()) => Ok(()),
        Err(e) => {
            // Left alone, an unfinished upload keeps its parts in the bucket,
            // billed and invisible, until a lifecycle rule nobody configured
            // removes it.
            if let Err(abort) = abort_multipart(client, target, key, &upload_id) {
                tracing::warn!(
                    %key,
                    error = %format!("{abort:#}"),
                    "could not abort an unfinished upload"
                );
            }
            Err(e)
        }
    }
}

/// Starts a multipart upload and returns its ID.
///
/// The digest goes on here, because this is where the finished object's
/// metadata is decided; the parts carry none.
fn create_multipart(
    client: &Client,
    target: &RemoteTarget,
    key: &str,
    sha256: &str,
) -> Result<String> {
    let call = Call {
        method: Method::POST,
        query: &[("uploads", "")],
        headers: &[(DIGEST_HEADER, sha256)],
        payload_sha256: EMPTY_SHA256,
    };
    let response = send(client, target, key, call, Some((Body::from(Vec::new()), 0)))
    .with_context(|| format!("starting the upload of {key}"))?;
    if !response.status().is_success() {
        return Err(rejected("the start of the upload of", key, response));
    }
    let body = response
        .text()
        .with_context(|| format!("reading the reply to starting {key}"))?;
    xml_text(&body, "UploadId")
        .filter(|id| !id.is_empty())
        .ok_or_else(|| anyhow!("storage started an upload of {key} without naming it"))
}

fn send_parts(
    client: &Client,
    target: &RemoteTarget,
    key: &str,
    file: &File,
    sha256: &str,
    bytes: u64,
    upload_id: &str,
) -> Result<()> {
    let size = part_size(bytes);
    let mut whole = Sha256::new();
    let mut etags = Vec::new();

    let mut offset = 0u64;
    let mut number = 1u32;
    while offset < bytes {
        let len = size.min(bytes - offset);
        let part_sha256 = hash_part(file, offset, len, &mut whole)
            .with_context(|| format!("reading part {number} of {key}"))?;

        let number_text = number.to_string();
        let call = Call {
            method: Method::PUT,
            query: &[("partNumber", &number_text), ("uploadId", upload_id)],
            headers: &[],
            payload_sha256: &part_sha256,
        };
        let body = Body::sized(FileRange::new(file, offset, len)?, len);
        let response = send(client, target, key, call, Some((body, len)))
            .with_context(|| format!("uploading part {number} of {key}"))?;
        if !response.status().is_success() {
            return Err(rejected(&format!("part {number} of"), key, response));
        }
        let etag = response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .ok_or_else(|| anyhow!("storage accepted part {number} of {key} without an ETag"))?;
        etags.push(etag);

        offset += len;
        number += 1;
    }

    // The parts were each verified by the server against their own hashes,
    // but nothing on its side checks the whole. The digest recorded on the
    // object is what subsequent runs trust to skip it, so it must describe exactly
    // what was sent; a file that changed under a known digest is not
    // completed.
    let sent = hex::encode(whole.finalize());
    if sent != sha256 {
        bail!("{key} changed while it was being uploaded (expected sha256 {sha256}, sent {sent})");
    }

    complete_multipart(client, target, key, upload_id, &etags)
}

fn complete_multipart(
    client: &Client,
    target: &RemoteTarget,
    key: &str,
    upload_id: &str,
    etags: &[String],
) -> Result<()> {
    let body = completion_xml(etags);
    let body_sha256 = hex::encode(Sha256::digest(body.as_bytes()));
    let len = body.len() as u64;
    let call = Call {
        method: Method::POST,
        query: &[("uploadId", upload_id)],
        headers: &[],
        payload_sha256: &body_sha256,
    };
    let response = send(client, target, key, call, Some((Body::from(body.into_bytes()), len)))
        .with_context(|| format!("completing the upload of {key}"))?;
    if !response.status().is_success() {
        return Err(rejected("the completion of", key, response));
    }
    // S3 can answer a completion with 200 and then report in the body that
    // it failed, because the status is sent before assembly ends.
    let reply = response
        .text()
        .with_context(|| format!("reading the reply to completing {key}"))?;
    if reply.contains("<Error>") {
        let preview: String = reply.chars().take(400).collect();
        bail!("storage could not complete the upload of {key}: {preview}");
    }
    Ok(())
}

fn abort_multipart(
    client: &Client,
    target: &RemoteTarget,
    key: &str,
    upload_id: &str,
) -> Result<()> {
    let call = Call {
        method: Method::DELETE,
        query: &[("uploadId", upload_id)],
        headers: &[],
        payload_sha256: EMPTY_SHA256,
    };
    let response = send(client, target, key, call, None)
        .with_context(|| format!("aborting the upload of {key}"))?;
    if !response.status().is_success() {
        return Err(rejected("aborting the upload of", key, response));
    }
    Ok(())
}

/// The `CompleteMultipartUpload` body, listing every part in order.
///
/// ETags arrive quoted, and are sent back exactly as received, escaped.
fn completion_xml(etags: &[String]) -> String {
    let mut xml = String::from("<CompleteMultipartUpload>");
    for (i, etag) in etags.iter().enumerate() {
        xml.push_str(&format!(
            "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
            i + 1,
            xml_escape(etag)
        ));
    }
    xml.push_str("</CompleteMultipartUpload>");
    xml
}

fn xml_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            other => out.push(other),
        }
    }
    out
}

/// The text of the first `<tag>` element in a reply, unescaped.
///
/// The replies read here are small, flat and fixed in shape, which is the only
/// reason a search is enough and an XML parser is not needed.
fn xml_text(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = start + xml[start..].find(&close)?;
    Some(
        xml[start..end]
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&apos;", "'")
            .replace("&amp;", "&"),
    )
}

/// Hashes one part, feeding the same bytes to the whole-object hash.
fn hash_part(file: &File, offset: u64, len: u64, whole: &mut Sha256) -> Result<String> {
    let mut reader = FileRange::new(file, offset, len)?;
    let mut part = Sha256::new();
    let mut buf = vec![0u8; 256 * 1024];
    let mut read = 0u64;
    loop {
        let n = match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        };
        part.update(&buf[..n]);
        whole.update(&buf[..n]);
        read += n as u64;
    }
    if read != len {
        bail!("the file ended {} bytes early", len - read);
    }
    Ok(hex::encode(part.finalize()))
}

/// A byte range of a file, read by position.
///
/// Positional reads rather than seeking: the handle is a clone, and clones
/// share one offset, so a seek through one would move every other reader of
/// the same file.
struct FileRange {
    file: File,
    pos: u64,
    end: u64,
}

impl FileRange {
    fn new(file: &File, offset: u64, len: u64) -> Result<Self> {
        Ok(Self {
            file: file.try_clone().context("reopening the file to send")?,
            pos: offset,
            end: offset + len,
        })
    }
}

impl Read for FileRange {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let left = self.end.saturating_sub(self.pos);
        if left == 0 {
            return Ok(0);
        }
        let want = buf.len().min(usize::try_from(left).unwrap_or(usize::MAX));
        let n = self.file.read_at(&mut buf[..want], self.pos)?;
        self.pos += n as u64;
        Ok(n)
    }
}

/// Whether the object already holds exactly these bytes.
///
/// Compared by a digest stored alongside the object rather than by downloading
/// it: the objects are audio, and re-reading them would cost as much as
/// uploading again.
fn object_matches(client: &Client, target: &RemoteTarget, key: &str, sha256: &str) -> bool {
    let call = Call {
        method: Method::HEAD,
        query: &[],
        headers: &[],
        payload_sha256: EMPTY_SHA256,
    };
    let response = send(client, target, key, call, None);

    let Ok(response) = response else {
        // A failed check is not a failed upload; proceed and let the PUT report.
        return false;
    };
    if !response.status().is_success() {
        return false;
    }

    // Without a digest the object predates this check, or was written by
    // something else. Re-uploading is the safe answer: it costs bandwidth,
    // where trusting it could leave the wrong audio in place.
    response
        .headers()
        .get(DIGEST_HEADER)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|remote| remote == sha256)
}

/// Digests the manifest already records, by key.
///
/// Every chunk's hash was taken as it was written, and an import's original
/// was hashed as it arrived. Hashing those again before each backup would
/// re-read the largest objects a recording has for an answer already on
/// record. If a file no longer matches, the upload is refused by storage (a
/// single `PUT`) or by the whole-object check (a multipart one), which is the
/// right outcome for a file that changed on disk.
pub fn known_digests(manifest: &RecordingManifest) -> HashMap<BlobKey, String> {
    let mut known: HashMap<BlobKey, String> = manifest
        .tracks
        .iter()
        .flat_map(|t| t.chunks.iter())
        .map(|c| (c.blob.clone(), c.sha256.clone()))
        .collect();
    if let Some(source) = &manifest.source {
        if let Some(key) = &source.original_key {
            known.insert(key.clone(), source.original_sha256.clone());
        }
    }
    known
}

/// What a backup pass did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Uploaded {
    pub uploaded: usize,
    pub skipped: usize,
}

/// Uploads every key, streaming each from the store's files.
///
/// Stops at the first failure: the stage is retried as a whole, and every
/// object already sent is skipped next time by its digest.
pub fn upload_keys(
    store: &dyn BlobStore,
    client: &Client,
    target: &RemoteTarget,
    keys: &[BlobKey],
    known: &HashMap<BlobKey, String>,
) -> Result<Uploaded> {
    let mut done = Uploaded::default();
    for key in keys {
        let file = store
            .open_file(key)
            .with_context(|| format!("opening {key} to upload"))?;
        let sha256 = match known.get(key) {
            Some(sha256) => sha256.clone(),
            None => {
                crate::blobstore::sha256_reader(store.open(key)?)
                    .with_context(|| format!("hashing {key}"))?
                    .1
            }
        };
        let bytes = file
            .metadata()
            .with_context(|| format!("reading the size of {key}"))?
            .len();
        match put_object_file(client, target, key.as_str(), &file, &sha256, bytes)? {
            PutResult::Uploaded => done.uploaded += 1,
            PutResult::AlreadyPresent => done.skipped += 1,
        }
    }
    Ok(done)
}

/// The client every backup request goes through.
///
/// No overall timeout on the client: each request sets its own, sized to what
/// it sends, because one fixed limit is either too short for a large part or
/// too long to notice a small request that stalled.
pub fn client() -> Result<Client> {
    Client::builder()
        .timeout(None::<Duration>)
        .connect_timeout(Duration::from_secs(30))
        .build()
        .context("building the storage client")
}

/// An in-process S3 stand-in, for tests.
///
/// It checks what a real service checks and a careless client gets wrong: it
/// recomputes every signature from the request as received, insists that
/// every `x-amz-*` header is signed, compares each body with its declared
/// hash, and refuses a completion whose parts are out of order, missing, too
/// small, or carry the wrong ETags.
#[cfg(test)]
pub(crate) mod fake_s3 {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::{Arc, Mutex};

    use axum::body::Bytes;
    use axum::extract::{DefaultBodyLimit, State};
    use axum::http::{HeaderMap, Method, StatusCode, Uri};
    use axum::response::{IntoResponse, Response};
    use sha2::{Digest, Sha256};

    use super::RemoteTarget;
    use crate::sigv4::{self, CanonicalRequest, Credentials};

    pub const ACCESS_KEY: &str = "AKIDEXAMPLE";
    pub const SECRET_KEY: &str = "fake-secret";
    pub const BUCKET: &str = "kaseta";
    pub const REGION: &str = "auto";

    #[derive(Clone, Debug)]
    pub struct Object {
        pub bytes: Vec<u8>,
        pub digest: Option<String>,
    }

    #[derive(Debug, Default)]
    pub struct Upload {
        pub key: String,
        pub digest: Option<String>,
        pub parts: BTreeMap<u32, (String, Vec<u8>)>,
    }

    #[derive(Debug, Default)]
    pub struct Fake {
        pub objects: HashMap<String, Object>,
        pub uploads: HashMap<String, Upload>,
        /// Each request, as `METHOD what key`.
        pub log: Vec<String>,
        pub aborted: Vec<String>,
        pub rejected_signatures: usize,
        /// A part number to refuse with a server error.
        pub fail_part: Option<u32>,
        next_upload: u32,
    }

    pub struct FakeS3 {
        pub port: u16,
        pub state: Arc<Mutex<Fake>>,
    }

    impl FakeS3 {
        pub fn start() -> Self {
            let state = Arc::new(Mutex::new(Fake::default()));
            let app = axum::Router::new()
                .fallback(handle)
                .with_state(Arc::clone(&state))
                .layer(DefaultBodyLimit::disable());

            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let port = listener.local_addr().unwrap().port();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                    axum::serve(listener, app).await.unwrap();
                });
            });
            Self { port, state }
        }

        pub fn target(&self) -> RemoteTarget {
            RemoteTarget {
                endpoint: format!("http://127.0.0.1:{}", self.port),
                region: REGION.into(),
                bucket: BUCKET.into(),
                access_key_id: ACCESS_KEY.into(),
                secret_access_key: SECRET_KEY.into(),
            }
        }

        pub fn fake(&self) -> std::sync::MutexGuard<'_, Fake> {
            self.state.lock().unwrap()
        }
    }

    fn reply(status: StatusCode, body: impl Into<String>) -> Response {
        (status, body.into()).into_response()
    }

    /// An S3-style error reply, whose code is what a client's message shows.
    fn error(status: StatusCode, code: &str) -> Response {
        reply(status, format!("<Error><Code>{code}</Code></Error>"))
    }

    fn decode(raw: &str) -> String {
        let bytes = raw.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' && i + 2 < bytes.len() {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap();
                out.push(u8::from_str_radix(hex, 16).unwrap());
                i += 3;
            } else {
                out.push(bytes[i]);
                i += 1;
            }
        }
        String::from_utf8(out).unwrap()
    }

    fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
        headers.get(name).and_then(|v| v.to_str().ok())
    }

    fn parse_amz_date(raw: &str) -> Option<time::OffsetDateTime> {
        let n = |range: std::ops::Range<usize>| raw.get(range)?.parse::<u32>().ok();
        let date = time::Date::from_calendar_date(
            n(0..4)? as i32,
            time::Month::try_from(n(4..6)? as u8).ok()?,
            n(6..8)? as u8,
        )
        .ok()?;
        let at = time::Time::from_hms(n(9..11)? as u8, n(11..13)? as u8, n(13..15)? as u8).ok()?;
        Some(date.with_time(at).assume_utc())
    }

    /// Recomputes the signature from the request as received.
    fn signature_holds(method: &Method, uri: &Uri, headers: &HeaderMap) -> bool {
        let Some(authorization) = header(headers, "authorization") else {
            return false;
        };
        let Some(signed) = authorization
            .split("SignedHeaders=")
            .nth(1)
            .and_then(|s| s.split(',').next())
        else {
            return false;
        };
        let signed: Vec<&str> = signed.split(';').collect();
        // S3's own rule: every x-amz-* header sent must be signed.
        for name in headers.keys() {
            if name.as_str().starts_with("x-amz-") && !signed.contains(&name.as_str()) {
                return false;
            }
        }
        for required in ["host", "x-amz-content-sha256", "x-amz-date"] {
            if !signed.contains(&required) {
                return false;
            }
        }

        let Some(now) = header(headers, "x-amz-date").and_then(parse_amz_date) else {
            return false;
        };
        let Some(payload) = header(headers, "x-amz-content-sha256") else {
            return false;
        };
        let mut values = Vec::new();
        for name in &signed {
            if *name == "x-amz-date" || *name == "x-amz-content-sha256" {
                continue;
            }
            let Some(value) = header(headers, name) else {
                return false;
            };
            values.push((*name, value));
        }
        let query: Vec<(String, String)> = uri
            .query()
            .unwrap_or("")
            .split('&')
            .filter(|p| !p.is_empty())
            .map(|p| match p.split_once('=') {
                Some((k, v)) => (decode(k), decode(v)),
                None => (decode(p), String::new()),
            })
            .collect();
        let query: Vec<(&str, &str)> =
            query.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

        let expected = sigv4::sign(
            &CanonicalRequest {
                method: method.as_str(),
                uri: uri.path(),
                query: &query,
                headers: &values,
                payload_sha256: payload,
            },
            Credentials {
                access_key_id: ACCESS_KEY,
                secret_access_key: SECRET_KEY,
            },
            REGION,
            "s3",
            now,
        );
        expected.authorization == authorization
    }

    fn query_value(uri: &Uri, name: &str) -> Option<String> {
        uri.query()?.split('&').find_map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (decode(k) == name).then(|| decode(v))
        })
    }

    async fn handle(
        State(state): State<Arc<Mutex<Fake>>>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        let mut fake = state.lock().unwrap();

        if !signature_holds(&method, &uri, &headers) {
            fake.rejected_signatures += 1;
            return error(StatusCode::FORBIDDEN, "SignatureDoesNotMatch");
        }
        let declared = header(&headers, "x-amz-content-sha256").unwrap_or_default();
        if hex::encode(Sha256::digest(&body)) != declared {
            return reply(
                StatusCode::BAD_REQUEST,
                "<Error><Code>XAmzContentSHA256Mismatch</Code></Error>",
            );
        }

        let Some(key) = uri.path().strip_prefix(&format!("/{BUCKET}/")) else {
            return error(StatusCode::NOT_FOUND, "NoSuchBucket");
        };
        let key = decode(key);
        let digest = header(&headers, super::DIGEST_HEADER).map(str::to_string);
        let upload_id = query_value(&uri, "uploadId");
        let part = query_value(&uri, "partNumber").and_then(|n| n.parse::<u32>().ok());
        let starts = query_value(&uri, "uploads").is_some();

        match (method.as_str(), upload_id, part, starts) {
            ("HEAD", None, None, false) => {
                fake.log.push(format!("HEAD {key}"));
                match fake.objects.get(&key) {
                    Some(object) => {
                        let mut response = reply(StatusCode::OK, "");
                        if let Some(d) = &object.digest {
                            response
                                .headers_mut()
                                .insert(super::DIGEST_HEADER, d.parse().unwrap());
                        }
                        response
                    }
                    None => reply(StatusCode::NOT_FOUND, ""),
                }
            }
            ("PUT", None, None, false) => {
                fake.log.push(format!("PUT {key}"));
                fake.objects.insert(
                    key,
                    Object {
                        bytes: body.to_vec(),
                        digest,
                    },
                );
                reply(StatusCode::OK, "")
            }
            ("POST", None, None, true) => {
                fake.log.push(format!("POST start {key}"));
                fake.next_upload += 1;
                // Characters a careless client would fail to encode in the
                // query string, escaped as S3 would escape them in XML.
                let id = format!("up/{}+&=x", fake.next_upload);
                fake.uploads.insert(
                    id.clone(),
                    Upload {
                        key,
                        digest,
                        parts: BTreeMap::new(),
                    },
                );
                reply(
                    StatusCode::OK,
                    format!(
                        "<?xml version=\"1.0\"?><InitiateMultipartUploadResult>\
                         <Bucket>{BUCKET}</Bucket><UploadId>{}</UploadId>\
                         </InitiateMultipartUploadResult>",
                        super::xml_escape(&id)
                    ),
                )
            }
            ("PUT", Some(id), Some(number), false) => {
                fake.log.push(format!("PUT part {number} {key} {}", body.len()));
                if fake.fail_part == Some(number) {
                    return error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError");
                }
                let Some(upload) = fake.uploads.get_mut(&id) else {
                    return error(StatusCode::NOT_FOUND, "NoSuchUpload");
                };
                let etag = format!("\"{}\"", &hex::encode(Sha256::digest(&body))[..32]);
                upload.parts.insert(number, (etag.clone(), body.to_vec()));
                let mut response = reply(StatusCode::OK, "");
                response.headers_mut().insert("etag", etag.parse().unwrap());
                response
            }
            ("POST", Some(id), None, false) => {
                fake.log.push(format!("POST complete {key}"));
                let Some(upload) = fake.uploads.remove(&id) else {
                    return error(StatusCode::NOT_FOUND, "NoSuchUpload");
                };
                let xml = String::from_utf8(body.to_vec()).unwrap();
                let listed: Vec<(u32, String)> = xml
                    .split("<Part>")
                    .skip(1)
                    .map(|p| {
                        (
                            super::xml_text(p, "PartNumber").unwrap().parse().unwrap(),
                            super::xml_text(p, "ETag").unwrap(),
                        )
                    })
                    .collect();
                let numbers: Vec<u32> = listed.iter().map(|(n, _)| *n).collect();
                let expected: Vec<u32> = (1..=upload.parts.len() as u32).collect();
                if numbers != expected {
                    return error(StatusCode::BAD_REQUEST, "InvalidPartOrder");
                }
                let mut assembled = Vec::new();
                for (i, (number, etag)) in listed.iter().enumerate() {
                    let Some((stored, bytes)) = upload.parts.get(number) else {
                        return error(StatusCode::BAD_REQUEST, "InvalidPart");
                    };
                    if stored != etag {
                        return error(StatusCode::BAD_REQUEST, "InvalidPart");
                    }
                    if i + 1 < listed.len() && bytes.len() < 5 * 1024 * 1024 {
                        return error(StatusCode::BAD_REQUEST, "EntityTooSmall");
                    }
                    assembled.extend_from_slice(bytes);
                }
                fake.objects.insert(
                    upload.key,
                    Object {
                        bytes: assembled,
                        digest: upload.digest,
                    },
                );
                reply(StatusCode::OK, "<CompleteMultipartUploadResult/>")
            }
            ("DELETE", Some(id), None, false) => {
                fake.log.push(format!("DELETE upload {key}"));
                fake.uploads.remove(&id);
                fake.aborted.push(key);
                reply(StatusCode::NO_CONTENT, "")
            }
            _ => error(StatusCode::BAD_REQUEST, "Unexpected"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    fn target() -> RemoteTarget {
        RemoteTarget {
            endpoint: "https://s3.us-east-1.amazonaws.com".into(),
            region: "us-east-1".into(),
            bucket: "examplebucket".into(),
            access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
        }
    }

    fn sign_put(
        target: &RemoteTarget,
        key: &str,
        payload: &[u8],
        at: time::OffsetDateTime,
    ) -> SignedRequest {
        let sha = hex::encode(Sha256::digest(payload));
        sign(target, "PUT", key, &[], &[], &sha, at)
    }

    #[test]
    fn signing_is_deterministic_for_a_given_moment() {
        let at = datetime!(2026-07-27 12:00:00 UTC);
        let a = sign_put(&target(), "recordings/a.flac", b"audio", at);
        let b = sign_put(&target(), "recordings/a.flac", b"audio", at);
        assert_eq!(a.header("authorization"), b.header("authorization"));
    }

    #[test]
    fn different_content_produces_a_different_signature() {
        // The payload hash is part of what is signed, so a body swapped in
        // transit cannot be presented under the same signature.
        let at = datetime!(2026-07-27 12:00:00 UTC);
        let a = sign_put(&target(), "k", b"one", at);
        let b = sign_put(&target(), "k", b"two", at);
        assert_ne!(a.header("authorization"), b.header("authorization"));
        assert_ne!(a.header("x-amz-content-sha256"), b.header("x-amz-content-sha256"));
    }

    #[test]
    fn the_signature_is_scoped_to_a_day_and_region() {
        let t = target();
        let signed = sign_put(&t, "k", b"x", datetime!(2026-07-27 12:00:00 UTC));
        assert!(signed.header("authorization").contains("20260727/us-east-1/s3/aws4_request"));
        assert!(signed.header("x-amz-date").starts_with("20260727T"));
    }

    /// Metadata is signed: S3 refuses an `x-amz-*` header that is sent but not
    /// covered by the signature.
    #[test]
    fn metadata_headers_are_signed_and_sent() {
        let signed = sign(
            &target(),
            "PUT",
            "k",
            &[],
            &[(DIGEST_HEADER, "abc")],
            EMPTY_SHA256,
            datetime!(2026-07-27 12:00:00 UTC),
        );
        assert!(signed
            .header("authorization")
            .contains(
                "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-meta-kaseta-sha256,"
            ));
        assert!(signed
            .headers
            .iter()
            .any(|(n, v)| n == DIGEST_HEADER && v == "abc"));
    }

    #[test]
    fn the_query_is_requested_exactly_as_it_is_signed() {
        let signed = sign(
            &target(),
            "PUT",
            "a b.flac",
            &[("uploadId", "x/y+z"), ("partNumber", "2")],
            &[],
            EMPTY_SHA256,
            datetime!(2026-07-27 12:00:00 UTC),
        );
        assert_eq!(
            signed.url,
            "https://s3.us-east-1.amazonaws.com/examplebucket/a%20b.flac\
             ?partNumber=2&uploadId=x%2Fy%2Bz"
        );
    }

    #[test]
    fn keys_keep_their_hierarchy_but_escape_everything_else() {
        // Slashes are the key's structure, not characters to escape; escaping
        // them would create a differently-named object.
        let t = target();
        assert_eq!(
            t.canonical_uri("recordings/2026/07/a.flac"),
            "/examplebucket/recordings/2026/07/a.flac"
        );
        assert_eq!(t.canonical_uri("a b.flac"), "/examplebucket/a%20b.flac");
        assert_eq!(t.canonical_uri("a+b"), "/examplebucket/a%2Bb");
    }

    #[test]
    fn an_amazon_endpoint_is_derived_when_none_is_given() {
        let settings = RemoteStorageSettings {
            bucket: Some("meetings".into()),
            access_key_id: Some("id".into()),
            secret_access_key: Some("secret".into()),
            region: Some("eu-west-1".into()),
            ..RemoteStorageSettings::default()
        };
        let target = RemoteTarget::from_settings(&settings).unwrap();
        assert_eq!(target.endpoint, "https://s3.eu-west-1.amazonaws.com");
    }

    #[test]
    fn a_custom_endpoint_is_used_verbatim_and_stripped_of_a_trailing_slash() {
        let settings = RemoteStorageSettings {
            endpoint: Some("https://acc.r2.cloudflarestorage.com/".into()),
            bucket: Some("meetings".into()),
            access_key_id: Some("id".into()),
            secret_access_key: Some("secret".into()),
            ..RemoteStorageSettings::default()
        };
        let target = RemoteTarget::from_settings(&settings).unwrap();
        assert_eq!(target.endpoint, "https://acc.r2.cloudflarestorage.com");
        // R2 ignores the region but the signature still requires one.
        assert_eq!(target.region, "auto");
    }

    #[test]
    fn every_missing_field_is_named_at_once() {
        // Configuring this should not be a sequence of one-at-a-time errors.
        let err = RemoteTarget::from_settings(&RemoteStorageSettings::default()).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("bucket"), "{message}");
        assert!(message.contains("access key ID"), "{message}");
        assert!(message.contains("secret access key"), "{message}");
    }

    #[test]
    fn plain_http_is_refused() {
        // Credentials and recordings would otherwise cross the network in the
        // clear.
        let settings = RemoteStorageSettings {
            endpoint: Some("http://insecure.example.com".into()),
            bucket: Some("b".into()),
            access_key_id: Some("id".into()),
            secret_access_key: Some("secret".into()),
            ..RemoteStorageSettings::default()
        };
        let err = RemoteTarget::from_settings(&settings).unwrap_err();
        assert!(err.to_string().contains("https"));
    }

    #[test]
    fn an_endpoint_path_prefix_is_signed_as_well_as_requested() {
        // Signing /bucket/key while requesting /minio/bucket/key is rejected
        // as a signature mismatch, and nothing in the error names the cause.
        let mut t = target();
        t.endpoint = "https://storage.example.com/minio".into();

        assert_eq!(t.base_path(), "/minio");
        assert_eq!(t.canonical_uri("a.flac"), "/minio/examplebucket/a.flac");
        assert_eq!(
            t.url_for("a.flac", &[]),
            "https://storage.example.com/minio/examplebucket/a.flac"
        );

        // What is signed must be exactly what is requested.
        let signed = sign_put(&t, "a.flac", b"x", datetime!(2026-07-27 12:00:00 UTC));
        assert!(signed.url.ends_with(&t.canonical_uri("a.flac")));
    }

    #[test]
    fn an_endpoint_without_a_path_signs_from_the_root() {
        let t = target();
        assert_eq!(t.base_path(), "");
        assert_eq!(t.canonical_uri("a.flac"), "/examplebucket/a.flac");
    }

    #[test]
    fn a_path_prefix_changes_the_signature() {
        let at = datetime!(2026-07-27 12:00:00 UTC);
        let plain = sign_put(&target(), "k", b"x", at);

        let mut prefixed = target();
        prefixed.endpoint = "https://s3.us-east-1.amazonaws.com/sub".into();
        let signed = sign_put(&prefixed, "k", b"x", at);

        assert_ne!(plain.header("authorization"), signed.header("authorization"));
    }

    #[test]
    fn the_host_is_extracted_without_scheme_or_path() {
        let mut t = target();
        t.endpoint = "https://acc.r2.cloudflarestorage.com".into();
        assert_eq!(t.host(), "acc.r2.cloudflarestorage.com");
        t.endpoint = "http://127.0.0.1:9000/sub".into();
        assert_eq!(t.host(), "127.0.0.1:9000");
        assert_eq!(t.base_path(), "/sub");
        assert_eq!(t.url_for("k", &[]), "http://127.0.0.1:9000/sub/examplebucket/k");
    }

    #[test]
    fn the_url_places_the_bucket_before_the_key() {
        let t = target();
        assert_eq!(
            t.url_for("recordings/a.flac", &[]),
            "https://s3.us-east-1.amazonaws.com/examplebucket/recordings/a.flac"
        );
    }

    /// Parts are a whole number of MiB, never below the floor, and never so
    /// small that an object would need more parts than S3 allows.
    #[test]
    fn parts_are_sized_to_the_object() {
        assert_eq!(part_size(70 * MIB), 32 * MIB);
        assert_eq!(part_size(64 * 1024 * MIB), 32 * MIB, "the largest upload allowed");
        // 1 TiB / 9000 is 116.5 MiB, rounded up to a whole MiB.
        assert_eq!(part_size(1024 * 1024 * MIB), 117 * MIB);

        let sizes = [
            SINGLE_PUT_MAX + 1,
            70 * MIB,
            5 * 1024 * MIB,
            300 * 1024 * MIB,
            5 * 1024 * 1024 * MIB,
        ];
        for total in sizes {
            let size = part_size(total);
            assert_eq!(size % MIB, 0, "{total}");
            assert!(size >= MIN_PART_BYTES, "{total}");
            assert!(total.div_ceil(size) <= PLANNED_MAX_PARTS, "{total}");
        }
    }

    #[test]
    fn a_request_is_given_time_in_proportion_to_its_body() {
        assert_eq!(request_timeout(0), Duration::from_secs(60));
        assert_eq!(request_timeout(32 * MIB), Duration::from_secs(60 + 128));
        assert!(request_timeout(SINGLE_PUT_MAX) > request_timeout(32 * MIB));
    }

    #[test]
    fn the_completion_lists_every_part_in_order_with_its_etag_escaped() {
        let xml = completion_xml(&["\"a\"".into(), "\"b&c\"".into()]);
        assert_eq!(
            xml,
            "<CompleteMultipartUpload>\
             <Part><PartNumber>1</PartNumber><ETag>&quot;a&quot;</ETag></Part>\
             <Part><PartNumber>2</PartNumber><ETag>&quot;b&amp;c&quot;</ETag></Part>\
             </CompleteMultipartUpload>"
        );
        assert_eq!(xml_text(&xml, "ETag").as_deref(), Some("\"a\""));
    }

    #[test]
    fn an_upload_id_is_read_unescaped() {
        let reply = "<InitiateMultipartUploadResult><UploadId>a&amp;b&lt;c</UploadId>\
                     </InitiateMultipartUploadResult>";
        assert_eq!(xml_text(reply, "UploadId").as_deref(), Some("a&b<c"));
        assert_eq!(xml_text(reply, "Missing"), None);
    }

    #[test]
    fn a_file_range_reads_only_its_bytes() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("f");
        std::fs::write(&path, b"0123456789").unwrap();
        let file = File::open(&path).unwrap();

        let mut out = String::new();
        FileRange::new(&file, 3, 4).unwrap().read_to_string(&mut out).unwrap();
        assert_eq!(out, "3456");

        // A second range over the same handle is unaffected by the first.
        let mut a = FileRange::new(&file, 0, 2).unwrap();
        let mut b = FileRange::new(&file, 8, 2).unwrap();
        let mut buf = [0u8; 2];
        a.read_exact(&mut buf[..1]).unwrap();
        b.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"89");
    }
}

#[cfg(test)]
mod upload_tests {
    use std::io::Write;

    use super::fake_s3::FakeS3;
    use super::*;
    use crate::blobstore::{sha256_reader, LocalFsStore};

    /// A file of `len` bytes that no two offsets of share a pattern with, so a
    /// part sent from the wrong offset cannot assemble into the right object.
    fn file_of(dir: &tempfile::TempDir, name: &str, len: u64) -> (File, String) {
        let path = dir.path().join(name);
        let mut out = std::io::BufWriter::new(File::create(&path).unwrap());
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut written = 0u64;
        let mut block = vec![0u8; 1024 * 1024];
        while written < len {
            for byte in block.iter_mut() {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                *byte = state as u8;
            }
            let n = (len - written).min(block.len() as u64) as usize;
            out.write_all(&block[..n]).unwrap();
            written += n as u64;
        }
        out.flush().unwrap();
        drop(out);
        let (_, sha) = sha256_reader(File::open(&path).unwrap()).unwrap();
        (File::open(&path).unwrap(), sha)
    }

    fn upload(s3: &FakeS3, key: &str, file: &File, sha: &str) -> Result<PutResult> {
        let len = file.metadata().unwrap().len();
        put_object_file(&client().unwrap(), &s3.target(), key, file, sha, len)
    }

    #[test]
    fn a_small_object_goes_in_one_put_with_its_digest() {
        let s3 = FakeS3::start();
        let dir = tempfile::TempDir::new().unwrap();
        let (file, sha) = file_of(&dir, "small", 100_000);

        let key = "recordings/2026/10/09/x/exports/mixed.flac";
        assert_eq!(upload(&s3, key, &file, &sha).unwrap(), PutResult::Uploaded);

        let fake = s3.fake();
        assert_eq!(fake.rejected_signatures, 0);
        assert_eq!(fake.log, vec![format!("HEAD {key}"), format!("PUT {key}")]);
        let object = &fake.objects[key];
        assert_eq!(object.bytes.len(), 100_000);
        assert_eq!(hex::encode(Sha256::digest(&object.bytes)), sha);
        assert_eq!(object.digest.as_deref(), Some(sha.as_str()));
    }

    /// 70 MiB is past the single-request limit: two full parts and a short
    /// last one, assembled in order into the same bytes.
    #[test]
    fn a_large_object_is_sent_in_parts_and_assembled_in_order() {
        let s3 = FakeS3::start();
        let dir = tempfile::TempDir::new().unwrap();
        let (file, sha) = file_of(&dir, "large", 70 * MIB);

        let key = "recordings/2026/10/09/x/source/original.mp4";
        assert_eq!(upload(&s3, key, &file, &sha).unwrap(), PutResult::Uploaded);

        let fake = s3.fake();
        assert_eq!(fake.rejected_signatures, 0);
        assert_eq!(
            fake.log,
            vec![
                format!("HEAD {key}"),
                format!("POST start {key}"),
                format!("PUT part 1 {key} {}", 32 * MIB),
                format!("PUT part 2 {key} {}", 32 * MIB),
                format!("PUT part 3 {key} {}", 6 * MIB),
                format!("POST complete {key}"),
            ]
        );
        let object = &fake.objects[key];
        assert_eq!(object.bytes.len() as u64, 70 * MIB);
        assert_eq!(hex::encode(Sha256::digest(&object.bytes)), sha);
        // Metadata given when the upload started ends up on the object, which
        // is what lets the next run skip it.
        assert_eq!(object.digest.as_deref(), Some(sha.as_str()));
        assert!(fake.uploads.is_empty());
        assert!(fake.aborted.is_empty());
    }

    #[test]
    fn an_object_already_there_with_the_same_digest_is_skipped() {
        let s3 = FakeS3::start();
        let dir = tempfile::TempDir::new().unwrap();
        let (small, small_sha) = file_of(&dir, "small", 1_000);
        let (large, large_sha) = file_of(&dir, "large", 70 * MIB);

        upload(&s3, "a", &small, &small_sha).unwrap();
        upload(&s3, "b", &large, &large_sha).unwrap();
        s3.fake().log.clear();

        assert_eq!(upload(&s3, "a", &small, &small_sha).unwrap(), PutResult::AlreadyPresent);
        assert_eq!(upload(&s3, "b", &large, &large_sha).unwrap(), PutResult::AlreadyPresent);
        assert_eq!(s3.fake().log, vec!["HEAD a".to_string(), "HEAD b".to_string()]);
    }

    /// Same key, different bytes: a regenerated export must replace the old.
    #[test]
    fn different_bytes_under_the_same_key_are_sent_again() {
        let s3 = FakeS3::start();
        let dir = tempfile::TempDir::new().unwrap();
        let (first, first_sha) = file_of(&dir, "first", 1_000);
        upload(&s3, "k", &first, &first_sha).unwrap();

        std::fs::write(dir.path().join("second"), b"other bytes").unwrap();
        let second = File::open(dir.path().join("second")).unwrap();
        let second_sha = hex::encode(Sha256::digest(b"other bytes"));
        assert_eq!(upload(&s3, "k", &second, &second_sha).unwrap(), PutResult::Uploaded);
        assert_eq!(s3.fake().objects["k"].bytes, b"other bytes");
    }

    /// A part that fails ends the upload: it is aborted, so nothing half
    /// assembled stays in the bucket, and no object appears.
    #[test]
    fn a_failed_part_aborts_the_upload() {
        let s3 = FakeS3::start();
        s3.fake().fail_part = Some(2);
        let dir = tempfile::TempDir::new().unwrap();
        let (file, sha) = file_of(&dir, "large", 70 * MIB);

        let err = upload(&s3, "big", &file, &sha).unwrap_err();
        assert!(format!("{err:#}").contains("part 2"), "{err:#}");

        let fake = s3.fake();
        assert_eq!(fake.aborted, vec!["big".to_string()]);
        assert!(fake.uploads.is_empty(), "the unfinished upload must be gone");
        assert!(!fake.objects.contains_key("big"));
        assert!(
            !fake.log.iter().any(|l| l.starts_with("PUT part 3")),
            "nothing is sent after a failed part: {:?}",
            fake.log
        );
    }

    /// The server checks each part, but only the client can check the whole.
    /// A digest that does not describe the bytes sent must not be recorded on
    /// an object, where subsequent runs would trust it.
    #[test]
    fn a_file_that_does_not_match_its_digest_is_not_completed() {
        let s3 = FakeS3::start();
        let dir = tempfile::TempDir::new().unwrap();
        let (file, _) = file_of(&dir, "large", 70 * MIB);
        let wrong = "0".repeat(64);

        let err = upload(&s3, "big", &file, &wrong).unwrap_err();
        assert!(format!("{err:#}").contains("changed"), "{err:#}");

        let fake = s3.fake();
        assert_eq!(fake.aborted, vec!["big".to_string()]);
        assert!(!fake.objects.contains_key("big"));
        assert!(!fake.log.iter().any(|l| l.starts_with("POST complete")));
    }

    /// For a single request the server is the check: the declared hash is
    /// compared with the body that arrived.
    #[test]
    fn a_small_file_that_does_not_match_its_digest_is_refused() {
        let s3 = FakeS3::start();
        let dir = tempfile::TempDir::new().unwrap();
        let (file, _) = file_of(&dir, "small", 1_000);

        let err = upload(&s3, "k", &file, &"0".repeat(64)).unwrap_err();
        assert!(format!("{err:#}").contains("XAmzContentSHA256Mismatch"), "{err:#}");
        assert!(!s3.fake().objects.contains_key("k"));
    }

    /// The fake is only worth trusting if it does reject a bad signature.
    #[test]
    fn the_stand_in_refuses_a_wrong_secret() {
        let s3 = FakeS3::start();
        let dir = tempfile::TempDir::new().unwrap();
        let (file, sha) = file_of(&dir, "small", 10);

        let mut target = s3.target();
        target.secret_access_key = "not-it".into();
        let err = put_object_file(&client().unwrap(), &target, "k", &file, &sha, 10).unwrap_err();
        assert!(format!("{err:#}").contains("SignatureDoesNotMatch"), "{err:#}");
        assert!(s3.fake().rejected_signatures > 0);
    }

    /// A recording's chunks are sent under the digests its manifest recorded,
    /// and everything else under a digest taken from the file.
    #[test]
    fn a_recording_is_uploaded_from_its_files_under_known_and_computed_digests() {
        let s3 = FakeS3::start();
        let dir = tempfile::TempDir::new().unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();

        let chunk = BlobKey::new("recordings/r/tracks/a_imported_01/000000.flac").unwrap();
        let manifest_key = BlobKey::new("recordings/r/manifest.json").unwrap();
        store.put(&chunk, b"chunk bytes").unwrap();
        store.put(&manifest_key, b"{}").unwrap();
        let mut known = HashMap::new();
        known.insert(chunk.clone(), hex::encode(Sha256::digest(b"chunk bytes")));

        let keys = vec![chunk.clone(), manifest_key.clone()];
        let done = upload_keys(&store, &client().unwrap(), &s3.target(), &keys, &known).unwrap();
        assert_eq!(done, Uploaded { uploaded: 2, skipped: 0 });
        {
            let fake = s3.fake();
            assert_eq!(fake.objects[chunk.as_str()].bytes, b"chunk bytes");
            assert_eq!(
                fake.objects[manifest_key.as_str()].digest.as_deref(),
                Some(hex::encode(Sha256::digest(b"{}")).as_str())
            );
        }

        let again = upload_keys(&store, &client().unwrap(), &s3.target(), &keys, &known).unwrap();
        assert_eq!(again, Uploaded { uploaded: 0, skipped: 2 });

        // A chunk that no longer matches its recorded digest is refused rather
        // than copied over the good one.
        store.put(&chunk, b"bit rot!!!!").unwrap();
        s3.fake().objects.clear();
        assert!(upload_keys(&store, &client().unwrap(), &s3.target(), &keys, &known).is_err());
        assert!(!s3.fake().objects.contains_key(chunk.as_str()));
    }

    #[test]
    fn known_digests_cover_chunks_and_a_kept_original() {
        let mut manifest: RecordingManifest = serde_json::from_value(serde_json::json!({
            "manifest_version": "recording-manifest/v1",
            "recording_id": "00000000000000000000000000",
            "started_at": "2026-07-25T14:12:03Z",
            "ended_at": null,
            "canonical_clock": {"kind": "boottime_ns", "started_at_ns": 1},
            "timeline": {"master_track_id": "a_imported_01", "nominal_sample_rate_hz": 48000},
            "tracks": [{
                "track_id": "a_imported_01",
                "media_type": "audio",
                "role": "unattributed",
                "source": {"kind": "imported_file", "stream_index": 1},
                "clock_domain": {"source_clock": "pipewire", "device_clock_id": null},
                "format": {"container": "flac", "codec": "flac", "sample_rate_hz": 48000,
                           "channels": 1, "sample_format": "s16"},
                "chunks": [{
                    "seq": 0,
                    "blob": "recordings/r/tracks/a_imported_01/000000.flac",
                    "sha256": "cd".repeat(32),
                    "bytes": 10, "sample_count": 48000,
                    "boottime_start_ns": 0, "boottime_end_ns": 1000000000,
                    "source_pts_start_ns": null, "source_pts_end_ns": null,
                    "discontinuity": false, "gap_before_ns": 0, "drops_before_chunk": 0
                }]
            }],
            "notes": {"headphones_expected": false, "echo_risk": "unknown"}
        }))
        .unwrap();
        let chunk = BlobKey::new("recordings/r/tracks/a_imported_01/000000.flac").unwrap();
        let known = known_digests(&manifest);
        assert_eq!(known.len(), 1);
        assert_eq!(known.get(&chunk), Some(&"cd".repeat(32)));

        manifest.source = Some(kaseta_contracts::ImportSource {
            original_filename: "talk.mp4".into(),
            original_key: Some(BlobKey::new("recordings/r/source/original.mp4").unwrap()),
            original_bytes: 3,
            original_sha256: "ab".repeat(32),
            container: "mov".into(),
            codec: "aac".into(),
            media_kind: kaseta_contracts::MediaType::Video,
            media_created_at: None,
            imported_at: time::macros::datetime!(2026-10-09 08:00:00 UTC),
            duration_s: 1.0,
        });
        let known = known_digests(&manifest);
        assert_eq!(known.len(), 2);
        assert_eq!(
            known.get(&BlobKey::new("recordings/r/source/original.mp4").unwrap()),
            Some(&"ab".repeat(32))
        );
    }
}
