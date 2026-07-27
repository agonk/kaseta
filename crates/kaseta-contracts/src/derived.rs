//! What a recording becomes after it is captured.
//!
//! A transcript, a summary, and whatever someone later decided to call the
//! thing. All three used to live only in the daemon's index, which made them
//! invisible to anything that copies the store — including the backup. Audio
//! survived a lost laptop; everything derived from it did not.
//!
//! They are contracts because they cross a storage boundary. The index can be
//! rebuilt from them; they cannot be rebuilt from the index once the index is
//! gone.

use serde::{Deserialize, Serialize};

/// Schema marker carried by every derived document.
///
/// Written now so a later change can be recognised rather than guessed at from
/// which fields happen to be present.
pub const DERIVED_VERSION: &str = "derived/v1";

/// One thing somebody said.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TranscriptLine {
    /// Seconds from the start of the recording.
    ///
    /// Relative rather than on the canonical clock: the clock counts from boot,
    /// which means nothing to a reader and nothing on another machine.
    pub at_s: f64,
    /// `you`, `them`, or `unknown`.
    pub speaker: String,
    pub text: String,
    /// Which captured track carried this line.
    ///
    /// Attribution is derived from the track rather than from a speaker model,
    /// so a document that dropped it would restore into an index that could no
    /// longer say where its own answer came from. Optional because documents
    /// written before it existed do not have it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub track: Option<String>,
}

/// A transcript as stored beside the audio it came from.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TranscriptDocument {
    #[serde(default = "default_version")]
    pub version: String,
    pub revision: u32,
    #[serde(default)]
    pub engine: Option<String>,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub lines: Vec<TranscriptLine>,
}

/// Something a person agreed to do.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActionItem {
    pub what: String,
    /// `null` when the transcript does not say. The speaker labels separate the
    /// operator from the far end, but the far end may be several people, and a
    /// guessed name is worse than an absent one.
    #[serde(default)]
    pub owner: Option<String>,
}

/// A summary as stored beside the transcript it was drawn from.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SummaryBody {
    #[serde(default)]
    pub overview: String,
    #[serde(default)]
    pub decisions: Vec<String>,
    #[serde(default)]
    pub action_items: Vec<ActionItem>,
    #[serde(default)]
    pub topics: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SummaryDocument {
    #[serde(default = "default_version")]
    pub version: String,
    pub revision: u32,
    /// Which transcript revision this was drawn from.
    ///
    /// Without it, a re-transcription silently leaves a summary describing text
    /// that no longer exists, and nothing on either side says so.
    pub transcript_revision: u32,
    /// Who produced it, so a summary can be judged by what wrote it.
    pub provider: String,
    pub model: String,
    #[serde(flatten)]
    pub body: SummaryBody,
}

/// State a person changed after the recording was made.
///
/// The only mutable document under a recording's prefix. Everything else
/// describes what was captured and is sealed once written; this describes how
/// someone chose to file it, which is theirs to change whenever they like.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LibraryMetadata {
    #[serde(default = "default_version")]
    pub version: String,
    /// A title someone typed, replacing whatever capture inferred.
    #[serde(default)]
    pub title_override: Option<String>,
}

fn default_version() -> String {
    DERIVED_VERSION.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_summary_records_the_transcript_it_came_from() {
        let doc = SummaryDocument {
            version: DERIVED_VERSION.into(),
            revision: 2,
            transcript_revision: 1,
            provider: "openrouter".into(),
            model: "some/model".into(),
            body: SummaryBody {
                overview: "we talked".into(),
                ..Default::default()
            },
        };

        let json = serde_json::to_string(&doc).unwrap();
        let back: SummaryDocument = serde_json::from_str(&json).unwrap();
        assert_eq!(back.transcript_revision, 1);
        assert_eq!(back.revision, 2);
    }

    /// The body is flattened so a summary reads as one object rather than
    /// burying what it says under a key.
    #[test]
    fn a_summary_body_is_not_nested_under_a_key() {
        let doc = SummaryDocument {
            version: DERIVED_VERSION.into(),
            revision: 1,
            transcript_revision: 1,
            provider: "openrouter".into(),
            model: "m".into(),
            body: SummaryBody {
                overview: "hello".into(),
                ..Default::default()
            },
        };

        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&doc).unwrap()).unwrap();
        assert_eq!(json["overview"], "hello");
        assert!(json.get("body").is_none());
    }

    /// Documents written before the marker existed must still load.
    #[test]
    fn a_document_without_a_version_takes_the_current_one() {
        let doc: LibraryMetadata =
            serde_json::from_str(r#"{"title_override":"Standup"}"#).unwrap();
        assert_eq!(doc.version, DERIVED_VERSION);
        assert_eq!(doc.title_override.as_deref(), Some("Standup"));
    }

    #[test]
    fn a_transcript_survives_a_round_trip() {
        let doc = TranscriptDocument {
            version: DERIVED_VERSION.into(),
            revision: 1,
            engine: Some("parakeet".into()),
            language: Some("en".into()),
            lines: vec![TranscriptLine {
                at_s: 1.5,
                speaker: "you".into(),
                text: "morning".into(),
                track: Some("a_local-mic_01".into()),
            }],
        };

        let back: TranscriptDocument =
            serde_json::from_str(&serde_json::to_string(&doc).unwrap()).unwrap();
        assert_eq!(back.lines.len(), 1);
        assert_eq!(back.lines[0].speaker, "you");
        assert_eq!(back.lines[0].at_s, 1.5);
    }
}
