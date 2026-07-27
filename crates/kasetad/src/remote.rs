//! Copying recordings to S3-compatible object storage.
//!
//! Signing is implemented here rather than pulled in as a vendor SDK. The
//! request surface Kaseta needs is a single authenticated `PUT` and a `HEAD`;
//! an SDK for that would add a hundred crates to a binary whose whole point is
//! being small, and the signing algorithm is short, fixed, and testable against
//! the published reference vectors.
//!
//! S3-compatible rather than S3-specific: Cloudflare R2, Backblaze, MinIO and
//! S3 itself all speak this, and only the endpoint distinguishes them.

use anyhow::{bail, Context, Result};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::config::RemoteStorageSettings;

type HmacSha256 = Hmac<Sha256>;

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

    fn host(&self) -> &str {
        self.endpoint
            .trim_start_matches("https://")
            .split('/')
            .next()
            .unwrap_or_default()
    }

    fn url_for(&self, key: &str) -> String {
        format!("{}/{}/{}", self.endpoint, self.bucket, encode_key(key))
    }
}

/// Percent-encodes an object key for use in a URL path.
///
/// Slashes are kept: they are the key's own hierarchy, not characters to
/// escape. Everything else outside the unreserved set is encoded, and the
/// signature must be computed over exactly this form or the request is
/// rejected.
fn encode_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for byte in key.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                out.push(byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

fn hmac(key: &[u8], data: &str) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

/// Derives the request signing key, scoped to a day, region and service.
///
/// The scoping is what limits a leaked signature's usefulness: it is valid for
/// one day, one region, and one service, rather than being the account secret.
fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k = hmac(format!("AWS4{secret}").as_bytes(), date);
    let k = hmac(&k, region);
    let k = hmac(&k, service);
    hmac(&k, "aws4_request")
}

/// A signed request, ready to send.
pub struct SignedRequest {
    pub url: String,
    pub authorization: String,
    pub amz_date: String,
    pub content_sha256: String,
}

/// Signs a request with AWS Signature Version 4.
///
/// The canonical request must match what the server reconstructs byte for byte,
/// which is why the header list, their order, and the payload hash are all
/// spelled out rather than assembled loosely.
pub fn sign(
    target: &RemoteTarget,
    method: &str,
    key: &str,
    payload: &[u8],
    now: time::OffsetDateTime,
) -> SignedRequest {
    let amz_date = format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    );
    let date = amz_date[..8].to_string();
    let content_sha256 = hex::encode(Sha256::digest(payload));

    let host = target.host().to_string();
    let canonical_uri = format!("/{}/{}", target.bucket, encode_key(key));

    let canonical_headers =
        format!("host:{host}\nx-amz-content-sha256:{content_sha256}\nx-amz-date:{amz_date}\n");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";

    let canonical_request = format!(
        "{method}\n{canonical_uri}\n\n{canonical_headers}\n{signed_headers}\n{content_sha256}"
    );

    let scope = format!("{date}/{}/s3/aws4_request", target.region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );

    let key_bytes = signing_key(&target.secret_access_key, &date, &target.region, "s3");
    let signature = hex::encode(hmac(&key_bytes, &string_to_sign));

    SignedRequest {
        url: target.url_for(key),
        authorization: format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            target.access_key_id
        ),
        amz_date,
        content_sha256,
    }
}

/// Uploads one object, skipping it if the same bytes are already there.
///
/// Uploads are retried after failures and re-run on restart, so this must be
/// idempotent and must not re-send a large object needlessly.
pub fn put_object(
    client: &reqwest::blocking::Client,
    target: &RemoteTarget,
    key: &str,
    payload: &[u8],
) -> Result<PutResult> {
    if object_matches(client, target, key, payload)? {
        return Ok(PutResult::AlreadyPresent);
    }

    let signed = sign(target, "PUT", key, payload, time::OffsetDateTime::now_utc());
    let response = client
        .put(&signed.url)
        .header("authorization", &signed.authorization)
        .header("x-amz-date", &signed.amz_date)
        .header("x-amz-content-sha256", &signed.content_sha256)
        .header("content-length", payload.len().to_string())
        .body(payload.to_vec())
        .send()
        .with_context(|| format!("uploading {key}"))?;

    let status = response.status();
    if !status.is_success() {
        // The body names the actual problem — a wrong region, a missing bucket,
        // an expired key — where the status alone makes them all look the same.
        let body = response.text().unwrap_or_default();
        let preview: String = body.chars().take(400).collect();
        bail!("storage rejected {key}: {status} {preview}");
    }
    Ok(PutResult::Uploaded)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutResult {
    Uploaded,
    AlreadyPresent,
}

/// Whether the object already holds exactly these bytes.
///
/// Compared by size rather than by downloading: the objects are audio, and
/// re-reading them to check would cost as much as uploading again.
fn object_matches(
    client: &reqwest::blocking::Client,
    target: &RemoteTarget,
    key: &str,
    payload: &[u8],
) -> Result<bool> {
    let signed = sign(target, "HEAD", key, &[], time::OffsetDateTime::now_utc());
    let response = client
        .head(&signed.url)
        .header("authorization", &signed.authorization)
        .header("x-amz-date", &signed.amz_date)
        .header("x-amz-content-sha256", &signed.content_sha256)
        .send();

    let Ok(response) = response else {
        // A failed check is not a failed upload; proceed and let the PUT report.
        return Ok(false);
    };
    if !response.status().is_success() {
        return Ok(false);
    }

    let remote_len = response
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok());

    Ok(remote_len == Some(payload.len()))
}

pub fn client() -> Result<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .context("building the storage client")
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

    #[test]
    fn the_signing_key_matches_the_published_reference() {
        // From AWS's own Signature Version 4 test vectors. If this drifts,
        // every request will be rejected with an opaque signature mismatch.
        let key = signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "iam",
        );
        assert_eq!(
            hex::encode(key),
            "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9"
        );
    }

    #[test]
    fn signing_is_deterministic_for_a_given_moment() {
        let at = datetime!(2026-07-27 12:00:00 UTC);
        let a = sign(&target(), "PUT", "recordings/a.flac", b"audio", at);
        let b = sign(&target(), "PUT", "recordings/a.flac", b"audio", at);
        assert_eq!(a.authorization, b.authorization);
    }

    #[test]
    fn different_content_produces_a_different_signature() {
        // The payload hash is part of what is signed, so a body swapped in
        // transit cannot be presented under the same signature.
        let at = datetime!(2026-07-27 12:00:00 UTC);
        let a = sign(&target(), "PUT", "k", b"one", at);
        let b = sign(&target(), "PUT", "k", b"two", at);
        assert_ne!(a.authorization, b.authorization);
        assert_ne!(a.content_sha256, b.content_sha256);
    }

    #[test]
    fn the_signature_is_scoped_to_a_day_and_region() {
        let t = target();
        let signed = sign(&t, "PUT", "k", b"x", datetime!(2026-07-27 12:00:00 UTC));
        assert!(signed.authorization.contains("20260727/us-east-1/s3/aws4_request"));
        assert!(signed.amz_date.starts_with("20260727T"));
    }

    #[test]
    fn keys_keep_their_hierarchy_but_escape_everything_else() {
        // Slashes are the key's structure, not characters to escape; escaping
        // them would create a differently-named object.
        assert_eq!(encode_key("recordings/2026/07/a.flac"), "recordings/2026/07/a.flac");
        assert_eq!(encode_key("a b.flac"), "a%20b.flac");
        assert_eq!(encode_key("a+b"), "a%2Bb");
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
    fn the_host_is_extracted_without_scheme_or_path() {
        let mut t = target();
        t.endpoint = "https://acc.r2.cloudflarestorage.com".into();
        assert_eq!(t.host(), "acc.r2.cloudflarestorage.com");
    }

    #[test]
    fn the_url_places_the_bucket_before_the_key() {
        let t = target();
        assert_eq!(
            t.url_for("recordings/a.flac"),
            "https://s3.us-east-1.amazonaws.com/examplebucket/recordings/a.flac"
        );
    }
}
