//! Posting a finished recording to a webhook.
//!
//! What travels is the transcript — the raw material — rather than Kaseta's own
//! conclusions. Its action items carry no source quote, no confidence and no
//! commitment/aside distinction, so a receiver that extracts tasks would end up
//! with rows beside its own that look identical and are not. The summary goes
//! too, as context for the transcript rather than as a substitute for it.
//!
//! The URL is used exactly as configured and the response is not interpreted
//! beyond its status. Both are deliberate: a recorder that appended a path or
//! read a particular field would work with one receiver and silently fail with
//! every other.
//!
//! The credential should be scoped to depositing recordings and nothing else,
//! which is the right shape for something living on a laptop.

use anyhow::{Context, Result};
use kaseta_contracts::{Origin, SummaryDocument, TranscriptDocument};
use serde::Serialize;
use ulid::Ulid;

use crate::config::WebhookSettings;

/// Larger than this is refused before it is sent, so the failure names the
/// reason instead of arriving as a rejected request. Receivers commonly cap
/// request bodies, and a transcript this long is minutes of silence
/// mis-transcribed far more often than it is a real meeting.
const MAX_TRANSCRIPT_CHARS: usize = 500_000;

/// A failure that retrying cannot fix, such as an HTTP 4xx: a payload the
/// receiver rejected is rejected identically on every attempt. The scheduler's
/// own type, so one check covers every stage that can say this.
pub use crate::scheduler::Permanent;

/// A short, stable name for *this* transcript's content.
///
/// The obvious key was the transcript revision, and it does not work:
/// re-transcribing deletes revision 1 and writes revision 1 again, so the number
/// never moves and a better transcript would arrive at the receiver looking like a
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

/// The revision a hand-off of this recording's current transcript is queued
/// under, or `None` while it has no transcript.
///
/// Taken from the text exactly as it would be sent, so it moves when what a
/// receiver would see moves and at no other time.
pub fn transcript_revision(db: &crate::db::Db, recording_id: Ulid) -> Result<Option<u32>> {
    let Some(doc) = crate::derived::transcript_document(db, recording_id)? else {
        return Ok(None);
    };
    let origin = crate::library::origin(db, recording_id)?.unwrap_or(Origin::Captured);
    Ok(Some(revision_of(&render_transcript(&doc, origin))))
}

#[derive(Debug, Serialize)]
struct Payload<'a> {
    recording_id: String,
    transcript_fingerprint: String,
    title: &'a str,
    recorded_at: String,
    /// `capture` for a meeting recorded on this machine, `import` for a file
    /// brought to it. A receiver reading `You` and `Them` in one and only
    /// `Speaker` in the other deserves to know why.
    source: &'static str,
    /// The imported file's name, which is often the best description of what
    /// it holds. Absent for a capture, which has no file behind it.
    #[serde(skip_serializing_if = "Option::is_none")]
    original_filename: Option<&'a str>,
    transcript: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_s: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    summary: Option<&'a serde_json::Value>,
}

/// The transcript as it is sent: one attributed line per turn.
///
/// Attribution is the reason this is worth sending rather than the audio. Every
/// line already knows who said it, because it is derived from which track
/// carried it — so `you` is the person holding this machine and `them` is the
/// far end, and a receiver can tell a commitment from a request without guessing.
///
/// An imported file has no such split, and its lines read `Speaker`. The text
/// is also what the fingerprint is taken from, so the label is decided by the
/// recording's kind and nothing else: the same transcript always renders, and
/// fingerprints, the same.
pub fn render_transcript(doc: &TranscriptDocument, origin: Origin) -> String {
    let mut out = String::new();
    for line in &doc.lines {
        let who = kaseta_contracts::speaker::display(origin, &line.speaker);
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
    /// When the call happened, not when it is being sent. A receiver resolving
    /// "by Friday" needs the former, so a recording sent on Monday still dates
    /// its deadlines from the call. See [`recorded_at`] for an import.
    pub recorded_at: time::OffsetDateTime,
    pub origin: Origin,
    /// The imported file's name; `None` for a capture.
    pub original_filename: Option<&'a str>,
    pub transcript: &'a TranscriptDocument,
    pub summary: Option<&'a SummaryDocument>,
    pub duration_s: Option<f64>,
}

/// When a recording happened, for a receiver dating what was said in it.
///
/// A capture happened when it started. An imported file happened whenever it
/// was made, which may be months before it was imported; the file's own claim
/// is used when it makes one, and the import time only when it does not.
pub fn recorded_at(
    started_at: time::OffsetDateTime,
    media_created_at: Option<time::OffsetDateTime>,
) -> time::OffsetDateTime {
    media_created_at.unwrap_or(started_at)
}

fn build(rec: &Recording<'_>) -> Result<(String, serde_json::Value)> {
    let transcript = render_transcript(rec.transcript, rec.origin);
    if transcript.trim().is_empty() {
        return Err(Permanent("the transcript is empty".into()).into());
    }
    if transcript.len() > MAX_TRANSCRIPT_CHARS {
        return Err(Permanent(format!(
            "the transcript is {} characters; the limit for sending is {MAX_TRANSCRIPT_CHARS}",
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

    let payload = Payload {
        recording_id: rec.id.to_string(),
        transcript_fingerprint: fingerprint(&transcript),
        title: rec.title,
        recorded_at,
        source: match rec.origin {
            Origin::Captured => "capture",
            Origin::Imported => "import",
        },
        original_filename: rec
            .original_filename
            .filter(|_| rec.origin == Origin::Imported),
        transcript,
        duration_s: rec.duration_s,
        summary: summary_json.as_ref(),
    };

    let body = serde_json::to_value(&payload).context("serialising the recording")?;
    Ok((rec.id.to_string(), body))
}

/// Posts one recording. Blocking, because it runs inside a stage worker.
///
/// Returns the status that was accepted, which is the only thing the response
/// is read for. A receiver's own body — what it filed the recording as, which
/// client it matched — belongs to that receiver's model, and interpreting it
/// here would make this work with exactly one of them.
pub fn publish(settings: &WebhookSettings, rec: &Recording<'_>) -> Result<u16> {
    let url = settings
        .post_url()
        .ok_or_else(|| Permanent("no webhook URL is set in Settings".into()))?;
    let token = settings
        .token
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .ok_or_else(|| Permanent("no webhook token is set in Settings".into()))?;

    let (_id, body) = build(rec)?;

    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .context("building the HTTP client")?;

    let res = client
        .post(url)
        .bearer_auth(token)
        .json(&body)
        .send()
        .context("posting to the webhook")?;

    let status = res.status();
    if status.is_success() {
        return Ok(status.as_u16());
    }

    let detail = res.text().unwrap_or_default();
    let detail = detail.chars().take(400).collect::<String>();

    // 4xx is a statement about the request, and the request will not change on
    // its own. 401 and 404 are worth naming: a revoked token and a URL missing
    // its path both read as "nothing is arriving" otherwise, and the second is
    // the likely mistake when a base URL is pasted where a full one is wanted.
    if status.is_client_error() {
        let hint = match status.as_u16() {
            401 | 403 => "the webhook refused the token — it may have expired or been revoked",
            404 => "the webhook URL was not found — check it includes the full path, not just the host",
            413 => "the webhook refused the recording as too large",
            _ => "the webhook refused the recording",
        };
        return Err(Permanent(format!("{hint} ({status}): {detail}")).into());
    }

    anyhow::bail!("the webhook answered {status}: {detail}")
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
        let out = render_transcript(
            &doc(vec![
                ("you", "I will send the report by Friday."),
                ("them", "Thanks."),
                ("unknown", "..."),
            ]),
            Origin::Captured,
        );
        assert_eq!(
            out,
            "You: I will send the report by Friday.\nThem: Thanks.\nUnknown: ...\n"
        );
    }

    /// An imported file's lines are a "Speaker"; "Unknown" would read as a
    /// fault on every line of a recording that has none.
    #[test]
    fn an_imported_transcript_names_its_speakers_neutrally() {
        let d = doc(vec![("unknown", "Welcome."), ("unknown", "Thank you.")]);
        assert_eq!(
            render_transcript(&d, Origin::Imported),
            "Speaker: Welcome.\nSpeaker: Thank you.\n"
        );
    }

    fn payload(origin: Origin, original_filename: Option<&str>) -> serde_json::Value {
        let d = doc(vec![("unknown", "hello")]);
        let rec = Recording {
            id: Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            title: "Lecture 3",
            recorded_at: time::OffsetDateTime::UNIX_EPOCH,
            origin,
            original_filename,
            transcript: &d,
            summary: None,
            duration_s: None,
        };
        build(&rec).unwrap().1
    }

    /// Additive fields: a receiver that ignores them sees exactly what it saw
    /// before, and one that reads them can tell a file from a call.
    #[test]
    fn the_payload_says_whether_it_was_captured_or_imported() {
        let captured = payload(Origin::Captured, None);
        assert_eq!(captured["source"], "capture");
        assert!(captured.get("original_filename").is_none());
        assert_eq!(captured["transcript"], "Unknown: hello\n");

        let imported = payload(Origin::Imported, Some("Lecture 3.mp4"));
        assert_eq!(imported["source"], "import");
        assert_eq!(imported["original_filename"], "Lecture 3.mp4");
        assert_eq!(imported["transcript"], "Speaker: hello\n");

        // A filename on a capture is a caller's mistake, not something to send.
        assert!(payload(Origin::Captured, Some("x.mp4"))
            .get("original_filename")
            .is_none());
    }

    #[test]
    fn an_import_is_dated_by_when_its_media_was_made() {
        let imported = time::macros::datetime!(2026-10-09 08:00:00 UTC);
        let made = time::macros::datetime!(2025-03-01 14:30:00 UTC);
        assert_eq!(recorded_at(imported, Some(made)), made);
        assert_eq!(recorded_at(imported, None), imported);
    }

    #[test]
    fn the_same_text_fingerprints_the_same_and_different_text_does_not() {
        let a = render_transcript(&doc(vec![("you", "hello")]), Origin::Captured);
        let b = render_transcript(&doc(vec![("you", "hello there")]), Origin::Captured);
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
                title: "Weekly sync",
                recorded_at: time::OffsetDateTime::UNIX_EPOCH,
                origin: Origin::Captured,
                original_filename: None,
                transcript: d,
                summary: None,
                duration_s: Some(60.0),
            };
            build(&rec).unwrap().1
        };

        let a = at(&first);
        let b = at(&better);
        assert_eq!(a["recorded_at"], "1970-01-01T00:00:00Z");
        assert_eq!(a["title"], "Weekly sync");
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
            origin: Origin::Captured,
            original_filename: None,
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
            origin: Origin::Captured,
            original_filename: None,
            transcript: &d,
            summary: None,
            duration_s: None,
        };
        let err = build(&rec).unwrap_err();
        assert!(err.downcast_ref::<Permanent>().is_some(), "must not retry");
    }

    /// The whole point of taking a complete URL: whatever path a receiver uses
    /// survives untouched, and no path is invented for one that uses none.
    #[test]
    fn posts_to_the_url_exactly_as_configured() {
        for url in [
            "https://example.test/api/agent/intake",
            "https://example.test/hooks/kaseta?token=1",
            "https://example.test",
        ] {
            let s = WebhookSettings {
                enabled: true,
                url: Some(url.into()),
                token: Some("k_live_x".into()),
                ..Default::default()
            };
            assert_eq!(s.post_url().unwrap(), url);
            assert!(s.is_configured());
        }
    }

    /// Surrounding whitespace comes free with pasting and means nothing, so it
    /// is trimmed rather than posted to.
    #[test]
    fn a_pasted_url_is_trimmed_but_otherwise_untouched() {
        let s = WebhookSettings {
            enabled: true,
            url: Some("  https://example.test/intake/  ".into()),
            token: Some("k".into()),
            ..Default::default()
        };
        assert_eq!(s.post_url().unwrap(), "https://example.test/intake/");
    }

    #[test]
    fn is_not_configured_without_a_url_or_a_token() {
        let on_but_empty = WebhookSettings {
            enabled: true,
            url: Some("  ".into()),
            token: Some("t".into()),
            ..Default::default()
        };
        assert!(!on_but_empty.is_configured());
        assert!(on_but_empty.post_url().is_none(), "blank is not a URL");

        let no_token = WebhookSettings {
            enabled: true,
            url: Some("https://example.test/intake".into()),
            token: None,
            ..Default::default()
        };
        assert!(!no_token.is_configured());

        let switched_off = WebhookSettings {
            enabled: false,
            url: Some("https://example.test/intake".into()),
            token: Some("t".into()),
            ..Default::default()
        };
        assert!(!switched_off.is_configured(), "a switch off means off");
    }
}
