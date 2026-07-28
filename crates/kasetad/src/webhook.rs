//! Handing a finished recording to Webhook.
//!
//! Webhook turns written material into tasks with a provenance trail. What it
//! wants from here is the transcript — the raw material — not conclusions:
//! Kaseta's own action items carry no source quote, no confidence and no
//! commitment/aside distinction, so importing them as tasks would put rows
//! beside extracted ones that look identical and are not. The summary travels
//! as context and is filed against the source event.
//!
//! The credential is an *intake* token: it may deposit recordings and nothing
//! else. It cannot read a task, change one, or create the client it files
//! against, which is the right shape for something living on a laptop.

use anyhow::{Context, Result};
use kaseta_contracts::{SummaryDocument, TranscriptDocument};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::config::WebhookSettings;

/// Webhook refuses a transcript larger than this, matching its own Intake
/// screen. Refusing here rather than sending it means the failure names the
/// reason instead of arriving as a rejected request.
const MAX_TRANSCRIPT_CHARS: usize = 500_000;

/// A failure that retrying cannot fix.
///
/// The scheduler treats every stage error as retryable, which is right for a
/// missing worker or a transient read and wrong for an HTTP 4xx: a payload
/// Webhook rejected will be rejected identically on every attempt until the
/// attempt budget runs out, and the failure somebody needs to see is buried
/// until then.
#[derive(Debug)]
pub struct Permanent(pub String);

impl std::fmt::Display for Permanent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Permanent {}

/// A short, stable name for *this* transcript's content.
///
/// The obvious key was the transcript revision, and it does not work:
/// re-transcribing deletes revision 1 and writes revision 1 again, so the number
/// never moves and a better transcript would arrive at Webhook looking like a
/// redelivery of the old one. A fingerprint of the text has the property the
/// revision was supposed to have and needs no bookkeeping to keep it true —
/// identical text is genuinely a retry, and different text is genuinely new
/// material.
///
/// Truncated because it names a version of one recording, not a document in a
/// global space: it is qualified by the recording id everywhere it is used.
pub fn fingerprint(transcript: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(transcript.as_bytes());
    hex::encode(&digest[..6])
}

/// The same fingerprint as a job revision, so that re-transcribing queues a new
/// hand-off while a retry of the same text does not. `enqueue_once` counts a
/// succeeded row as a duplicate, so without this a recording could be published
/// exactly once, for ever.
pub fn revision_of(transcript: &str) -> u32 {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(transcript.as_bytes());
    u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]])
}

#[derive(Debug, Serialize)]
struct IntakePayload<'a> {
    recording_id: String,
    transcript_fingerprint: String,
    title: &'a str,
    recorded_at: String,
    transcript: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_s: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    summary: Option<&'a serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct IntakeAck {
    pub status: String,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub filed: Option<String>,
}

/// The transcript as Webhook reads it: one attributed line per turn.
///
/// Attribution is the reason this is worth sending rather than the audio. Every
/// line already knows who said it, because it is derived from which track
/// carried it — so `you` is the person holding this machine and `them` is the
/// far end, and Webhook can tell a commitment from a request without guessing.
pub fn render_transcript(doc: &TranscriptDocument) -> String {
    let mut out = String::new();
    for line in &doc.lines {
        let who = match line.speaker.as_str() {
            "you" => "You",
            "them" => "Them",
            _ => "Unknown",
        };
        out.push_str(who);
        out.push_str(": ");
        out.push_str(line.text.trim());
        out.push('\n');
    }
    out
}

/// Everything the request needs, assembled before any network call so that a
/// payload problem is reported as one.
pub struct Recording<'a> {
    pub id: Ulid,
    pub title: &'a str,
    /// When the call happened, not when it is being sent. Webhook resolves
    /// "by Friday" against this, so a recording handed over on Monday still
    /// dates its deadlines from the call.
    pub recorded_at: time::OffsetDateTime,
    pub transcript: &'a TranscriptDocument,
    pub summary: Option<&'a SummaryDocument>,
    pub duration_s: Option<f64>,
}

fn build(rec: &Recording<'_>) -> Result<(String, serde_json::Value)> {
    let transcript = render_transcript(rec.transcript);
    if transcript.trim().is_empty() {
        return Err(Permanent("the transcript is empty".into()).into());
    }
    if transcript.len() > MAX_TRANSCRIPT_CHARS {
        return Err(Permanent(format!(
            "the transcript is {} characters; Webhook accepts {MAX_TRANSCRIPT_CHARS}",
            transcript.len()
        ))
        .into());
    }

    let summary_json = rec
        .summary
        .map(|s| serde_json::to_value(&s.body))
        .transpose()
        .context("serialising the summary")?;

    let recorded_at = rec
        .recorded_at
        .format(&time::format_description::well_known::Rfc3339)
        .context("formatting the recording time")?;

    let payload = IntakePayload {
        recording_id: rec.id.to_string(),
        transcript_fingerprint: fingerprint(&transcript),
        title: rec.title,
        recorded_at,
        transcript,
        duration_s: rec.duration_s,
        summary: summary_json.as_ref(),
    };

    let body = serde_json::to_value(&payload).context("serialising the recording")?;
    Ok((rec.id.to_string(), body))
}

/// Posts one recording. Blocking, because it runs inside a stage worker.
pub fn publish(settings: &WebhookSettings, rec: &Recording<'_>) -> Result<IntakeAck> {
    let url = settings
        .intake_url()
        .ok_or_else(|| Permanent("Webhook has no endpoint set in Settings".into()))?;
    let token = settings
        .token
        .as_deref()
        .filter(|t| !t.trim().is_empty())
        .ok_or_else(|| Permanent("Webhook has no token set in Settings".into()))?;

    let (_id, body) = build(rec)?;

    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .context("building the HTTP client")?;

    let res = client
        .post(&url)
        .bearer_auth(token.trim())
        .json(&body)
        .send()
        .context("posting to Webhook")?;

    let status = res.status();
    if status.is_success() {
        return res.json::<IntakeAck>().context("reading Webhook's answer");
    }

    let detail = res.text().unwrap_or_default();
    let detail = detail.chars().take(400).collect::<String>();

    // 4xx is a statement about the request, and the request will not change on
    // its own. 401 in particular is worth naming: a token that expired or was
    // revoked reads as "nothing is arriving" otherwise.
    if status.is_client_error() {
        let hint = match status.as_u16() {
            401 => "Webhook refused the token — it may have expired, been revoked, or be a work token rather than an intake one",
            413 => "Webhook refused the recording as too large",
            _ => "Webhook refused the recording",
        };
        return Err(Permanent(format!("{hint} ({status}): {detail}")).into());
    }

    anyhow::bail!("Webhook answered {status}: {detail}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaseta_contracts::TranscriptLine;

    fn doc(lines: Vec<(&str, &str)>) -> TranscriptDocument {
        TranscriptDocument {
            version: "1".into(),
            revision: 3,
            engine: None,
            language: None,
            lines: lines
                .into_iter()
                .map(|(speaker, text)| TranscriptLine {
                    at_s: 0.0,
                    speaker: speaker.into(),
                    text: text.into(),
                    track: None,
                })
                .collect(),
        }
    }

    #[test]
    fn renders_who_said_what() {
        let out = render_transcript(&doc(vec![
            ("you", "I will send the report by Friday."),
            ("them", "Thanks."),
            ("unknown", "..."),
        ]));
        assert_eq!(
            out,
            "You: I will send the report by Friday.\nThem: Thanks.\nUnknown: ...\n"
        );
    }

    #[test]
    fn the_same_text_fingerprints_the_same_and_different_text_does_not() {
        let a = render_transcript(&doc(vec![("you", "hello")]));
        let b = render_transcript(&doc(vec![("you", "hello there")]));
        assert_eq!(fingerprint(&a), fingerprint(&a));
        assert_ne!(fingerprint(&a), fingerprint(&b));
        assert_eq!(revision_of(&a), revision_of(&a));
        assert_ne!(revision_of(&a), revision_of(&b));
    }

    /// The transcript revision cannot carry this: re-transcribing rewrites
    /// revision 1 in place, so the number is the same for text that is not.
    #[test]
    fn a_re_transcription_is_new_material_even_though_the_revision_did_not_move() {
        let first = doc(vec![("you", "hello")]);
        let better = doc(vec![("you", "hello again")]);
        assert_eq!(first.revision, better.revision, "the premise of this test");

        let at = |d: &TranscriptDocument| {
            let rec = Recording {
                id: Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
                title: "Acme: sync",
                recorded_at: time::OffsetDateTime::UNIX_EPOCH,
                transcript: d,
                summary: None,
                duration_s: Some(60.0),
            };
            build(&rec).unwrap().1
        };

        let a = at(&first);
        let b = at(&better);
        assert_eq!(a["recorded_at"], "1970-01-01T00:00:00Z");
        assert_eq!(a["title"], "Acme: sync");
        assert_ne!(
            a["transcript_fingerprint"], b["transcript_fingerprint"],
            "a better transcript must not arrive looking like a redelivery"
        );
    }

    #[test]
    fn refuses_an_empty_transcript_permanently() {
        let d = doc(vec![]);
        let rec = Recording {
            id: Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            title: "t",
            recorded_at: time::OffsetDateTime::UNIX_EPOCH,
            transcript: &d,
            summary: None,
            duration_s: None,
        };
        let err = build(&rec).unwrap_err();
        assert!(err.downcast_ref::<Permanent>().is_some(), "must not retry");
    }

    #[test]
    fn refuses_an_oversized_transcript_permanently() {
        let long = "x".repeat(MAX_TRANSCRIPT_CHARS + 1);
        let d = doc(vec![("you", &long)]);
        let rec = Recording {
            id: Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            title: "t",
            recorded_at: time::OffsetDateTime::UNIX_EPOCH,
            transcript: &d,
            summary: None,
            duration_s: None,
        };
        let err = build(&rec).unwrap_err();
        assert!(err.downcast_ref::<Permanent>().is_some(), "must not retry");
    }

    #[test]
    fn builds_the_intake_url_from_a_base_however_it_is_typed() {
        for base in ["https://td.example", "https://td.example/", "https://td.example///"] {
            let s = WebhookSettings {
                enabled: true,
                endpoint: Some(base.into()),
                token: Some("td_live_x".into()),
                ..Default::default()
            };
            assert_eq!(s.intake_url().unwrap(), "https://td.example/api/agent/intake");
            assert!(s.is_configured());
        }
    }

    #[test]
    fn is_not_configured_without_an_endpoint_or_a_token() {
        let on_but_empty = WebhookSettings {
            enabled: true,
            endpoint: Some("  ".into()),
            token: Some("t".into()),
            ..Default::default()
        };
        assert!(!on_but_empty.is_configured());

        let no_token = WebhookSettings {
            enabled: true,
            endpoint: Some("https://td.example".into()),
            token: None,
            ..Default::default()
        };
        assert!(!no_token.is_configured());
    }
}
