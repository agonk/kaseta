//! AWS Signature Version 4, as S3 and the services that copy it expect.
//!
//! Kept apart from the code that talks to a bucket because the algorithm is
//! fixed and the request shapes are not. A single `PUT` signed three headers
//! and nothing else; a multipart upload adds a query string that has its own
//! encoding and ordering rules, and object metadata that must be signed too.
//! Each of those is a way to produce a request the server reconstructs
//! differently, and the only answer it gives is "signature does not match".
//! So the canonical form is built here once, from explicit inputs, and checked
//! against the vectors AWS publishes for S3.
//!
//! The payload hash is always supplied by the caller. Bodies are hashed before
//! they are sent, because S3 verifies the declared hash against what arrives,
//! and a body read from a file twice is cheaper than holding it in memory once.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// The hash of an empty body, which every request without one declares.
pub const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Who is signing.
#[derive(Clone, Copy, Debug)]
pub struct Credentials<'a> {
    pub access_key_id: &'a str,
    pub secret_access_key: &'a str,
}

/// Everything about a request that its signature covers.
///
/// `x-amz-date` and `x-amz-content-sha256` are not listed in `headers`: they
/// are derived from the signing moment and `payload_sha256`, so the values
/// sent and the values signed cannot disagree.
#[derive(Clone, Copy, Debug)]
pub struct CanonicalRequest<'a> {
    pub method: &'a str,
    /// The path exactly as it is requested, already percent-encoded (see
    /// [`encode_path`]). Signed verbatim.
    pub uri: &'a str,
    /// Query parameters, not yet encoded. A parameter without a value, such as
    /// `uploads`, is given an empty one.
    pub query: &'a [(&'a str, &'a str)],
    /// Every other header to sign, `host` included, in any order.
    pub headers: &'a [(&'a str, &'a str)],
    /// Lower-case hex SHA-256 of the body.
    pub payload_sha256: &'a str,
}

/// A request's signature and the headers that must accompany it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Signature {
    /// The `Authorization` header value.
    pub authorization: String,
    /// `x-amz-date`, in the compact form that was signed.
    pub amz_date: String,
    /// The headers to send, `authorization`, `x-amz-date` and
    /// `x-amz-content-sha256` included, and `host` excluded: the HTTP client
    /// writes that one from the URL, and the caller has already signed the
    /// value it will write.
    pub headers: Vec<(String, String)>,
}

/// Signs `req` for `region` and `service` at `now`.
pub fn sign(
    req: &CanonicalRequest<'_>,
    creds: Credentials<'_>,
    region: &str,
    service: &str,
    now: time::OffsetDateTime,
) -> Signature {
    let amz_date = amz_date(now);
    let date = &amz_date[..8];

    let headers = canonical_headers(req, &amz_date);
    let signed_headers = headers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical = canonical_request_from(req, &headers, &signed_headers);

    let scope = format!("{date}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let key = signing_key(creds.secret_access_key, date, region, service);
    let signature = hex::encode(hmac(&key, &string_to_sign));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        creds.access_key_id
    );

    let mut send: Vec<(String, String)> = headers
        .into_iter()
        .filter(|(name, _)| name != "host")
        .collect();
    send.push(("authorization".into(), authorization.clone()));

    Signature {
        authorization,
        amz_date,
        headers: send,
    }
}

/// The canonical request text that [`sign`] hashes, so the tests can compare
/// it with the form AWS documents: when a signature differs, this is the text
/// that says why.
#[cfg(test)]
pub fn canonical_request(req: &CanonicalRequest<'_>, now: time::OffsetDateTime) -> String {
    let amz_date = amz_date(now);
    let headers = canonical_headers(req, &amz_date);
    let signed_headers = headers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");
    canonical_request_from(req, &headers, &signed_headers)
}

fn canonical_request_from(
    req: &CanonicalRequest<'_>,
    headers: &[(String, String)],
    signed_headers: &str,
) -> String {
    let mut lines = String::new();
    for (name, value) in headers {
        lines.push_str(name);
        lines.push(':');
        lines.push_str(value);
        lines.push('\n');
    }
    format!(
        "{}\n{}\n{}\n{lines}\n{signed_headers}\n{}",
        req.method,
        req.uri,
        canonical_query(req.query),
        req.payload_sha256
    )
}

/// The signed headers in canonical form: names lower-cased, values trimmed
/// with inner runs of spaces collapsed, sorted by name, repeated names joined
/// with commas.
fn canonical_headers(req: &CanonicalRequest<'_>, amz_date: &str) -> Vec<(String, String)> {
    let mut all: Vec<(String, String)> = req
        .headers
        .iter()
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), normalise_value(value)))
        .filter(|(name, _)| name != "x-amz-date" && name != "x-amz-content-sha256")
        .collect();
    all.push(("x-amz-content-sha256".into(), req.payload_sha256.to_string()));
    all.push(("x-amz-date".into(), amz_date.to_string()));
    // Stable, so repeated names keep the order they were given in, which is
    // the order their values are joined in.
    all.sort_by(|a, b| a.0.cmp(&b.0));

    let mut merged: Vec<(String, String)> = Vec::with_capacity(all.len());
    for (name, value) in all {
        match merged.last_mut() {
            Some((last, joined)) if *last == name => {
                joined.push(',');
                joined.push_str(&value);
            }
            _ => merged.push((name, value)),
        }
    }
    merged
}

fn normalise_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The query string, canonically: every name and value encoded, sorted by
/// name and then value, joined with `&`.
///
/// Also what goes into the URL, so that what is requested is byte for byte
/// what was signed. A parameter with no value keeps its `=`.
pub fn canonical_query(query: &[(&str, &str)]) -> String {
    let mut pairs: Vec<(String, String)> = query
        .iter()
        .map(|(name, value)| (encode(name, false), encode(value, false)))
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Percent-encodes a path, keeping its slashes.
///
/// The slashes are an object key's own hierarchy; encoding them would name a
/// different object.
pub fn encode_path(path: &str) -> String {
    encode(path, true)
}

/// RFC 3986 encoding as SigV4 defines it: everything outside the unreserved
/// set, by byte, in upper-case hex.
fn encode(raw: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char)
            }
            b'/' if keep_slash => out.push('/'),
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

fn amz_date(now: time::OffsetDateTime) -> String {
    let now = now.to_offset(time::UtcOffset::UTC);
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    )
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
pub fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k = hmac(format!("AWS4{secret}").as_bytes(), date);
    let k = hmac(&k, region);
    let k = hmac(&k, service);
    hmac(&k, "aws4_request")
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    /// The credentials and moment every S3 example in the AWS documentation
    /// ("Signature Calculations for the Authorization Header: Transferring
    /// Payload in a Single Chunk") is computed with.
    const S3_CREDS: Credentials<'static> = Credentials {
        access_key_id: "AKIAIOSFODNN7EXAMPLE",
        secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    };
    const S3_MOMENT: time::OffsetDateTime = datetime!(2013-05-24 00:00:00 UTC);
    const S3_HOST: &str = "examplebucket.s3.amazonaws.com";

    fn signature_of(sig: &Signature) -> &str {
        sig.authorization.rsplit_once("Signature=").unwrap().1
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

    /// GET Object, with a `Range` header that is signed.
    #[test]
    fn the_documented_get_object_example_is_reproduced() {
        let req = CanonicalRequest {
            method: "GET",
            uri: "/test.txt",
            query: &[],
            headers: &[("Host", S3_HOST), ("Range", "bytes=0-9")],
            payload_sha256: EMPTY_SHA256,
        };

        assert_eq!(
            canonical_request(&req, S3_MOMENT),
            "GET\n/test.txt\n\n\
             host:examplebucket.s3.amazonaws.com\n\
             range:bytes=0-9\n\
             x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n\
             x-amz-date:20130524T000000Z\n\n\
             host;range;x-amz-content-sha256;x-amz-date\n\
             e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );

        let sig = sign(&req, S3_CREDS, "us-east-1", "s3", S3_MOMENT);
        assert_eq!(
            sig.authorization,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    /// PUT Object, with `Date` and a storage class signed, and a key whose
    /// `$` must be encoded.
    #[test]
    fn the_documented_put_object_example_is_reproduced() {
        let body = b"Welcome to Amazon S3.";
        let payload = hex::encode(Sha256::digest(body));
        assert_eq!(
            payload,
            "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"
        );

        let uri = encode_path("/test$file.text");
        assert_eq!(uri, "/test%24file.text");
        let req = CanonicalRequest {
            method: "PUT",
            uri: &uri,
            query: &[],
            headers: &[
                ("Host", S3_HOST),
                ("Date", "Fri, 24 May 2013 00:00:00 GMT"),
                ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
            ],
            payload_sha256: &payload,
        };
        let sig = sign(&req, S3_CREDS, "us-east-1", "s3", S3_MOMENT);
        assert!(sig.authorization.contains(
            "SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,"
        ));
        assert_eq!(
            signature_of(&sig),
            "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
        );
    }

    /// GET Bucket Lifecycle: a query parameter with no value.
    #[test]
    fn the_documented_valueless_query_example_is_reproduced() {
        let req = CanonicalRequest {
            method: "GET",
            uri: "/",
            query: &[("lifecycle", "")],
            headers: &[("Host", S3_HOST)],
            payload_sha256: EMPTY_SHA256,
        };
        assert!(canonical_request(&req, S3_MOMENT).starts_with("GET\n/\nlifecycle=\n"));
        let sig = sign(&req, S3_CREDS, "us-east-1", "s3", S3_MOMENT);
        assert_eq!(
            signature_of(&sig),
            "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543"
        );
    }

    /// GET Bucket (List Objects): parameters given out of order.
    #[test]
    fn the_documented_list_objects_example_is_reproduced() {
        let req = CanonicalRequest {
            method: "GET",
            uri: "/",
            query: &[("prefix", "J"), ("max-keys", "2")],
            headers: &[("Host", S3_HOST)],
            payload_sha256: EMPTY_SHA256,
        };
        assert!(canonical_request(&req, S3_MOMENT).starts_with("GET\n/\nmax-keys=2&prefix=J\n"));
        let sig = sign(&req, S3_CREDS, "us-east-1", "s3", S3_MOMENT);
        assert_eq!(
            signature_of(&sig),
            "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
        );
    }

    /// The multipart calls are distinguished only by their query strings, and
    /// an upload ID is opaque text from the server that may hold anything.
    #[test]
    fn multipart_queries_are_encoded_and_ordered_canonically() {
        assert_eq!(canonical_query(&[("uploads", "")]), "uploads=");
        assert_eq!(
            canonical_query(&[("uploadId", "a b/c+d=e"), ("partNumber", "2")]),
            "partNumber=2&uploadId=a%20b%2Fc%2Bd%3De"
        );
        // Sorted by byte value, so an upper-case name sorts first.
        assert_eq!(canonical_query(&[("b", "1"), ("B", "2"), ("a", "3")]), "B=2&a=3&b=1");
        // A repeated name sorts by value.
        assert_eq!(canonical_query(&[("k", "2"), ("k", "1")]), "k=1&k=2");
        // Unreserved characters stay as they are; anything else by byte.
        assert_eq!(canonical_query(&[("k", "-._~\u{e9}")]), "k=-._~%C3%A9");
        assert_eq!(canonical_query(&[]), "");
    }

    #[test]
    fn paths_keep_their_slashes_and_escape_everything_else() {
        assert_eq!(encode_path("recordings/2026/07/a.flac"), "recordings/2026/07/a.flac");
        assert_eq!(encode_path("a b.flac"), "a%20b.flac");
        assert_eq!(encode_path("a+b"), "a%2Bb");
    }

    /// Header names are case-insensitive on the wire but not in the canonical
    /// form, and a value's padding is not part of it either.
    #[test]
    fn headers_are_normalised_before_signing() {
        let messy = CanonicalRequest {
            method: "PUT",
            uri: "/k",
            query: &[],
            headers: &[
                ("X-Amz-Meta-Kaseta-Sha256", "  abc  "),
                ("HOST", "h"),
                ("x-amz-meta-note", "a   b"),
            ],
            payload_sha256: EMPTY_SHA256,
        };
        let tidy = CanonicalRequest {
            headers: &[
                ("host", "h"),
                ("x-amz-meta-kaseta-sha256", "abc"),
                ("x-amz-meta-note", "a b"),
            ],
            ..messy
        };
        let at = datetime!(2026-07-27 12:00:00 UTC);
        assert_eq!(
            sign(&messy, S3_CREDS, "auto", "s3", at),
            sign(&tidy, S3_CREDS, "auto", "s3", at)
        );
        assert!(canonical_request(&messy, at).contains(
            "host:h\nx-amz-content-sha256:"
        ));
    }

    /// The two derived headers cannot be supplied twice or contradicted: what
    /// is sent is what was signed.
    #[test]
    fn the_date_and_payload_hash_come_from_the_signer() {
        let req = CanonicalRequest {
            method: "PUT",
            uri: "/k",
            query: &[],
            headers: &[("host", "h"), ("x-amz-date", "19700101T000000Z")],
            payload_sha256: EMPTY_SHA256,
        };
        let sig = sign(&req, S3_CREDS, "auto", "s3", datetime!(2026-07-27 12:00:00 UTC));
        let dates: Vec<_> = sig.headers.iter().filter(|(n, _)| n == "x-amz-date").collect();
        assert_eq!(dates.len(), 1);
        assert_eq!(dates[0].1, "20260727T120000Z");
        assert_eq!(sig.amz_date, "20260727T120000Z");
        assert!(sig
            .headers
            .iter()
            .any(|(n, v)| n == "x-amz-content-sha256" && v == EMPTY_SHA256));
        assert!(sig.headers.iter().all(|(n, _)| n != "host"));
        assert!(sig.headers.iter().any(|(n, _)| n == "authorization"));
    }

    #[test]
    fn a_moment_in_another_offset_is_signed_in_utc() {
        let req = CanonicalRequest {
            method: "GET",
            uri: "/",
            query: &[],
            headers: &[("host", "h")],
            payload_sha256: EMPTY_SHA256,
        };
        let utc = sign(&req, S3_CREDS, "auto", "s3", datetime!(2026-07-27 12:00:00 UTC));
        let local = sign(&req, S3_CREDS, "auto", "s3", datetime!(2026-07-27 14:00:00 +2));
        assert_eq!(utc, local);
    }
}
