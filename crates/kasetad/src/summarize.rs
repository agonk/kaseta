//! Turning a transcript into something worth reading afterwards.
//!
//! Summarisation is the one part of the pipeline that leaves the machine, so it
//! is deliberately the only part that does. It runs behind a provider
//! abstraction: OpenRouter today, a local model or another vendor later, without
//! the rest of the daemon knowing which.
//!
//! # Long meetings
//!
//! A two-hour transcript does not fit in one request, and truncating it would
//! silently drop the end of the meeting — usually where the decisions are. Long
//! transcripts are therefore summarised in passes: each section is condensed,
//! then the condensed sections are summarised together. What comes back covers
//! the whole meeting rather than whatever fit.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::db::Db;
use crate::library::{Transcript, TranscriptLine};

/// Environment variable holding the API key.
///
/// Kept out of the database and out of blob storage: a key is a credential, not
/// recording data, and should not end up in a backup or a synced bucket.
pub const API_KEY_ENV: &str = "KASETA_OPENROUTER_KEY";

const OPENROUTER_URL: &str = "https://openrouter.ai/api/v1/chat/completions";

/// Default model. Chosen for being cheap and fast enough to run after every
/// meeting without thinking about cost, while still following instructions
/// reliably enough to produce structured output.
///
/// A transcript is a low-quality input — recognition errors, no punctuation
/// from the speaker, crosstalk — so the model's job is as much not inventing
/// detail as it is condensing. That argues for instruction-following over raw
/// capability, which is what puts a small model here rather than a large one.
pub const DEFAULT_MODEL: &str = "anthropic/claude-haiku-4.5";

/// Roughly how many characters of transcript go into one request.
///
/// A conservative stand-in for a token limit: counting tokens properly would
/// mean shipping a tokeniser per model, and the cost of being wrong here is an
/// extra pass, not a failure.
const SECTION_CHARS: usize = 24_000;

/// Most sections one recording may be summarised in.
///
/// Each section is a paid request, so an unbounded meeting is an unbounded
/// bill. Forty sections is roughly a day of continuous speech — far past any
/// real meeting — so hitting this means something is wrong, and the summary is
/// produced from what fits rather than silently costing more.
const MAX_SECTIONS: usize = 40;

/// What a summary contains.
///
/// Structured rather than prose so the interface can show decisions and actions
/// separately, and so an empty section is visibly empty rather than the model
/// inventing something to fill it.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Summary {
    /// A few sentences on what the meeting was about.
    #[serde(default)]
    pub overview: String,
    /// Points that were settled.
    #[serde(default)]
    pub decisions: Vec<String>,
    /// Things someone agreed to do.
    #[serde(default)]
    pub action_items: Vec<ActionItem>,
    /// What was discussed, in order.
    #[serde(default)]
    pub topics: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActionItem {
    pub what: String,
    /// Who agreed to it. `null` when the transcript does not say — the speaker
    /// labels distinguish the operator from the far end, but the far end may be
    /// several people, and guessing a name would be worse than admitting none.
    #[serde(default)]
    pub owner: Option<String>,
}

#[derive(Clone, Debug)]
pub struct SummarizeConfig {
    pub api_key: String,
    pub model: String,
}

impl SummarizeConfig {
    /// Resolves configuration from the environment, then from saved settings.
    ///
    /// The environment wins: a key exported for a one-off run, or set by a
    /// service unit, should not be silently overridden by something saved
    /// earlier through the interface.
    pub fn resolve(settings: &crate::config::Settings) -> Result<Self> {
        let api_key = std::env::var(API_KEY_ENV)
            .ok()
            .filter(|k| !k.trim().is_empty())
            .or_else(|| settings.summaries.api_key.clone());

        let Some(api_key) = api_key else {
            bail!("no API key is configured, so summaries cannot be produced");
        };

        let model = std::env::var("KASETA_OPENROUTER_MODEL")
            .ok()
            .filter(|m| !m.trim().is_empty())
            .or_else(|| settings.summaries.model.clone())
            .unwrap_or_else(|| DEFAULT_MODEL.to_string());

        Ok(Self { api_key, model })
    }
}

/// Renders a transcript the way the model should read it.
///
/// Speaker labels are kept: knowing who said what is most of what makes a
/// summary useful, and it costs almost nothing to include.
pub fn render(lines: &[TranscriptLine]) -> String {
    lines
        .iter()
        .map(|line| {
            let who = match line.speaker.as_str() {
                "you" => "Me",
                "them" => "Them",
                _ => "Unknown",
            };
            let minutes = (line.at_s / 60.0) as u64;
            let seconds = (line.at_s % 60.0) as u64;
            format!("[{minutes:02}:{seconds:02}] {who}: {}", line.text)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Splits a transcript into sections small enough to summarise in one request.
///
/// Splits between lines, never inside one, so no utterance is cut in half.
pub fn split_into_sections(rendered: &str, section_chars: usize) -> Vec<String> {
    if rendered.len() <= section_chars {
        return if rendered.is_empty() {
            Vec::new()
        } else {
            vec![rendered.to_string()]
        };
    }

    let mut sections = Vec::new();
    let mut current = String::new();

    for line in rendered.lines() {
        // A single line can exceed the limit on its own — the recogniser
        // collapses a whole track into one segment when it cannot produce
        // timings. Splitting only between lines would send that oversized line
        // as one request anyway, so long lines are cut too.
        for piece in split_long_line(line, section_chars) {
            if !current.is_empty() && current.len() + piece.len() + 1 > section_chars {
                sections.push(std::mem::take(&mut current));
            }
            if !current.is_empty() {
                current.push('\n');
            }
            current.push_str(&piece);
        }
    }
    if !current.is_empty() {
        sections.push(current);
    }
    sections
}

/// Cuts an oversized line, preferring a sentence boundary.
///
/// Splitting mid-word would corrupt the text the model reads; splitting at a
/// sentence keeps each piece independently readable.
fn split_long_line(line: &str, limit: usize) -> Vec<String> {
    if line.len() <= limit {
        return vec![line.to_string()];
    }

    let mut pieces = Vec::new();
    let mut rest = line;

    while rest.len() > limit {
        // Slicing at an arbitrary byte index panics on multi-byte text, so walk
        // back to a character boundary before looking at the window at all.
        let mut edge = limit.min(rest.len());
        while edge > 0 && !rest.is_char_boundary(edge) {
            edge -= 1;
        }
        if edge == 0 {
            break;
        }
        // Search backwards from there for somewhere sensible to cut.
        let window = &rest[..edge];
        let cut = window
            .rfind(". ")
            .map(|i| i + 2)
            .or_else(|| window.rfind(' ').map(|i| i + 1))
            .unwrap_or(edge);

        let (head, tail) = rest.split_at(cut);
        pieces.push(head.trim().to_string());
        rest = tail;
    }
    if !rest.trim().is_empty() {
        pieces.push(rest.trim().to_string());
    }
    pieces
}

const SYSTEM_PROMPT: &str = "\
You summarise meeting transcripts. The transcript labels each line with who \
spoke: `Me` is the person who recorded the meeting, `Them` is everyone else, \
and `Unknown` is audio that could not be attributed.

Report only what the transcript supports. Transcripts contain recognition \
errors, so if something is unclear, leave it out rather than guessing. If \
there were no decisions or no action items, return empty lists — do not invent \
them to fill space. Never attribute an action item to a named person unless \
that name appears in the transcript.

Reply with JSON only, matching exactly:
{\"overview\": string, \"decisions\": [string], \
\"action_items\": [{\"what\": string, \"owner\": string|null}], \"topics\": [string]}";

/// Summarises a transcript, in as many passes as its length requires.
pub fn summarize(config: &SummarizeConfig, transcript: &Transcript) -> Result<Summary> {
    if transcript.lines.is_empty() {
        bail!("this recording has no transcript to summarise");
    }

    let rendered = render(&transcript.lines);
    let mut sections = split_into_sections(&rendered, SECTION_CHARS);

    if sections.len() > MAX_SECTIONS {
        tracing::warn!(
            sections = sections.len(),
            kept = MAX_SECTIONS,
            "transcript exceeds the summarisation budget; summarising the earlier part"
        );
        sections.truncate(MAX_SECTIONS);
    }

    if sections.len() <= 1 {
        return request(config, sections.first().map(String::as_str).unwrap_or(&rendered), false);
    }

    // Condense each section, then summarise the condensations together. The
    // alternative — truncating — would drop the end of the meeting, which is
    // usually where the decisions are.
    tracing::info!(sections = sections.len(), "summarising in passes");
    let mut condensed = Vec::with_capacity(sections.len());
    for (i, section) in sections.iter().enumerate() {
        let partial = request(config, section, true)
            .with_context(|| format!("summarising section {} of {}", i + 1, sections.len()))?;
        condensed.push(format!(
            "Section {}:\n{}\nDecisions: {}\nActions: {}",
            i + 1,
            partial.overview,
            partial.decisions.join("; "),
            partial
                .action_items
                .iter()
                .map(|a| a.what.as_str())
                .collect::<Vec<_>>()
                .join("; ")
        ));
    }

    request(config, &condensed.join("\n\n"), false)
}

#[derive(Serialize)]
struct ChatRequest<'a> {
    model: &'a str,
    messages: Vec<ChatMessage<'a>>,
    /// Asking for JSON explicitly rather than hoping prose parses.
    response_format: ResponseFormat,
    temperature: f32,
    provider: ProviderPolicy,
}

/// Constraints on which host may serve the request.
///
/// An open-weight model is offered by many companies at once, and the router
/// picks between them per request. Left unconstrained, consecutive meetings can
/// go to different hosts in different jurisdictions, and the answer to "who has
/// this transcript" becomes "whoever was cheapest that second". Naming the
/// conditions is what makes that answer knowable.
#[derive(Serialize)]
struct ProviderPolicy {
    /// Excludes hosts that retain or train on what they are sent.
    data_collection: &'static str,
    /// Excludes hosts that would silently drop `response_format`.
    ///
    /// Ignoring it is not an error — it returns prose, which parses into an
    /// empty summary. Better to be routed elsewhere than to succeed emptily.
    require_parameters: bool,
}

impl Default for ProviderPolicy {
    fn default() -> Self {
        Self {
            data_collection: "deny",
            require_parameters: true,
        }
    }
}

#[derive(Serialize)]
struct ChatMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Serialize)]
struct ResponseFormat {
    #[serde(rename = "type")]
    kind: &'static str,
}

#[derive(Deserialize)]
struct ChatResponse {
    choices: Vec<Choice>,
}

#[derive(Deserialize)]
struct Choice {
    message: ResponseMessage,
}

#[derive(Deserialize)]
struct ResponseMessage {
    content: String,
}

fn request(config: &SummarizeConfig, transcript: &str, partial: bool) -> Result<Summary> {
    let instruction = if partial {
        "Summarise this section of a longer meeting."
    } else {
        "Summarise this meeting."
    };
    let user = format!("{instruction}\n\n{transcript}");

    let body = ChatRequest {
        model: &config.model,
        messages: vec![
            ChatMessage {
                role: "system",
                content: SYSTEM_PROMPT,
            },
            ChatMessage {
                role: "user",
                content: &user,
            },
        ],
        response_format: ResponseFormat {
            kind: "json_object",
        },
        // Low but not zero: summarising is not a task where creative variation
        // helps, and reproducibility makes a bad summary diagnosable.
        temperature: 0.2,
        provider: ProviderPolicy::default(),
    };

    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(180))
        .build()
        .context("building the HTTP client")?;

    let response = client
        .post(OPENROUTER_URL)
        .bearer_auth(&config.api_key)
        // Identifies the caller to OpenRouter, which asks for it.
        .header("HTTP-Referer", "https://github.com/agonk/kaseta")
        .header("X-Title", "Kaseta")
        .json(&body)
        .send()
        .context("calling OpenRouter")?;

    let status = response.status();
    let text = response.text().context("reading the response")?;

    if !status.is_success() {
        // The body usually says why — an unknown model, no credit, a bad key.
        // Reporting the status alone would make each of those look the same.
        let preview: String = text.chars().take(400).collect();
        bail!("OpenRouter returned {status}: {preview}");
    }

    let parsed: ChatResponse =
        serde_json::from_str(&text).context("OpenRouter returned an unexpected shape")?;
    let content = parsed
        .choices
        .first()
        .map(|c| c.message.content.as_str())
        .context("OpenRouter returned no choices")?;

    parse_summary(content)
}

/// Parses the model's reply into a summary.
///
/// Models sometimes wrap JSON in a code fence despite being asked not to, so
/// that is tolerated rather than treated as a failure.
pub fn parse_summary(content: &str) -> Result<Summary> {
    let trimmed = content.trim();
    let unfenced = trimmed
        .strip_prefix("```json")
        .or_else(|| trimmed.strip_prefix("```"))
        .and_then(|s| s.strip_suffix("```"))
        .unwrap_or(trimmed)
        .trim();

    serde_json::from_str(unfenced).with_context(|| {
        let preview: String = unfenced.chars().take(300).collect();
        format!("the summary was not valid JSON: {preview}")
    })
}

/// Stores a summary, replacing any previous one for this recording.
///
/// Written to the store as well as the index, so that a copy of the store —
/// which is what a backup is — contains the summary rather than only the audio
/// it was made from.
pub fn store(
    store: &dyn crate::blobstore::BlobStore,
    prefix: &kaseta_contracts::RecordingPrefix,
    db: &Db,
    recording_id: Ulid,
    model: &str,
    summary: &Summary,
) -> Result<()> {
    let json = serde_json::to_string(summary).context("serialising the summary")?;
    db.conn().execute(
        "INSERT INTO summaries (id, recording_id, revision, provider, model, content_json)
         VALUES (?1, ?2, 1, 'openrouter', ?3, ?4)
         ON CONFLICT(recording_id, revision) DO UPDATE SET
             model        = excluded.model,
             content_json = excluded.content_json,
             created_at   = strftime('%s','now')",
        rusqlite::params![Ulid::new().to_string(), recording_id.to_string(), model, json],
    )?;

    if let Some(document) = crate::derived::summary_document(db, recording_id)? {
        // As with a transcript: indexed but unstored is usable here and absent
        // from a backup, which the startup sweep repairs rather than paying for
        // the summary a second time.
        let key = prefix.summary(document.revision);
        match serde_json::to_vec_pretty(&document) {
            Ok(bytes) => {
                if let Err(e) = store.put(&key, &bytes) {
                    tracing::warn!(
                        %recording_id,
                        error = %format!("{e:#}"),
                        "the summary is indexed but not yet stored; it will be written again later"
                    );
                }
            }
            Err(e) => tracing::warn!(%recording_id, error = %e, "could not encode the summary"),
        }
        crate::derived::mark_dirty(db, recording_id)?;
    }
    Ok(())
}

/// Reads a recording's summary.
pub fn load(db: &Db, recording_id: Ulid) -> Result<Option<Summary>> {
    use rusqlite::OptionalExtension;
    let json: Option<String> = db
        .conn()
        .query_row(
            "SELECT content_json FROM summaries WHERE recording_id = ?1
             ORDER BY revision DESC LIMIT 1",
            rusqlite::params![recording_id.to_string()],
            |r| r.get(0),
        )
        .optional()?;

    json.map(|j| serde_json::from_str(&j).context("stored summary is unreadable"))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(at_s: f64, speaker: &str, text: &str) -> TranscriptLine {
        TranscriptLine {
            at_s,
            speaker: speaker.into(),
            text: text.into(),
        }
    }

    #[test]
    fn the_rendered_transcript_keeps_who_said_what() {
        // Attribution is most of what makes a summary useful, and the model
        // cannot recover it if the rendering drops it.
        let rendered = render(&[
            line(0.0, "you", "shall we ship on friday"),
            line(4.0, "them", "yes, if the tests pass"),
        ]);

        assert!(rendered.contains("Me: shall we ship on friday"));
        assert!(rendered.contains("Them: yes, if the tests pass"));
        assert!(rendered.contains("[00:00]"));
        assert!(rendered.contains("[00:04]"));
    }

    #[test]
    fn timestamps_render_past_a_minute() {
        let rendered = render(&[line(605.0, "you", "still here")]);
        assert!(rendered.contains("[10:05]"), "got {rendered}");
    }

    #[test]
    fn a_short_transcript_is_summarised_in_one_pass() {
        let sections = split_into_sections("one line", 1000);
        assert_eq!(sections.len(), 1);
        assert!(split_into_sections("", 1000).is_empty());
    }

    #[test]
    fn a_long_transcript_is_split_rather_than_truncated() {
        // Truncating would silently drop the end of a meeting, which is usually
        // where the decisions are.
        let long: String = (0..500)
            .map(|i| format!("[00:00] Me: line number {i} with some words in it"))
            .collect::<Vec<_>>()
            .join("\n");

        let sections = split_into_sections(&long, 2_000);
        assert!(sections.len() > 1, "expected several sections");

        let recombined: usize = sections.iter().map(|s| s.lines().count()).sum();
        assert_eq!(
            recombined,
            long.lines().count(),
            "every line must survive the split"
        );
    }

    #[test]
    fn splitting_never_cuts_a_line_in_half() {
        let long: String = (0..100)
            .map(|i| format!("[00:00] Me: utterance {i}"))
            .collect::<Vec<_>>()
            .join("\n");

        for section in split_into_sections(&long, 300) {
            for l in section.lines() {
                assert!(
                    l.starts_with("[00:00] Me: utterance"),
                    "a line was cut: {l:?}"
                );
            }
        }
    }

    #[test]
    fn an_oversized_single_line_is_split_rather_than_sent_whole() {
        // The recogniser collapses a track into one segment when it cannot
        // produce timings, which would otherwise bypass sectioning entirely.
        let huge = format!("[00:00] Me: {}", "word ".repeat(8_000));
        let sections = split_into_sections(&huge, 2_000);

        assert!(sections.len() > 1);
        for section in &sections {
            assert!(
                section.len() <= 2_000,
                "a section of {} exceeds the limit",
                section.len()
            );
        }
    }

    #[test]
    fn splitting_a_long_line_does_not_cut_a_word_in_half() {
        let line = "alpha beta gamma delta epsilon zeta eta theta iota kappa";
        for piece in split_long_line(line, 20) {
            for word in piece.split_whitespace() {
                assert!(
                    line.contains(word),
                    "{word:?} is not a whole word from the original"
                );
            }
        }
    }

    #[test]
    fn splitting_prefers_a_sentence_boundary() {
        let line = "First sentence here. Second sentence follows. Third one too.";
        let pieces = split_long_line(line, 30);
        assert!(
            pieces[0].ends_with('.'),
            "expected a cut at a sentence end, got {:?}",
            pieces[0]
        );
    }

    #[test]
    fn splitting_handles_text_with_no_spaces_at_all() {
        // Must terminate rather than loop, and must not panic on a char
        // boundary in multi-byte text.
        let line = "é".repeat(500);
        let pieces = split_long_line(&line, 40);
        assert!(pieces.len() > 1);
        assert_eq!(pieces.concat().chars().count(), 500);
    }

    #[test]
    fn splitting_never_slices_a_character_in_half() {
        // Three-byte characters against an even limit put the cut squarely
        // inside a character, which slicing by byte index would panic on.
        let line = "→".repeat(400);
        let pieces = split_long_line(&line, 50);
        assert!(pieces.len() > 1);
        assert_eq!(
            pieces.concat().chars().count(),
            400,
            "no character may be lost or corrupted"
        );
    }

    #[test]
    fn parses_a_plain_json_reply() {
        let summary = parse_summary(
            r#"{"overview":"we agreed to ship","decisions":["ship friday"],
                "action_items":[{"what":"run the tests","owner":"Me"}],
                "topics":["release"]}"#,
        )
        .unwrap();

        assert_eq!(summary.overview, "we agreed to ship");
        assert_eq!(summary.decisions, vec!["ship friday"]);
        assert_eq!(summary.action_items[0].what, "run the tests");
        assert_eq!(summary.action_items[0].owner.as_deref(), Some("Me"));
    }

    #[test]
    fn tolerates_a_reply_wrapped_in_a_code_fence() {
        // Models do this despite being told not to; failing on it would waste a
        // whole summarisation.
        let fenced = "```json\n{\"overview\":\"fenced\"}\n```";
        assert_eq!(parse_summary(fenced).unwrap().overview, "fenced");

        let bare_fence = "```\n{\"overview\":\"also fenced\"}\n```";
        assert_eq!(parse_summary(bare_fence).unwrap().overview, "also fenced");
    }

    #[test]
    fn missing_sections_become_empty_rather_than_failing() {
        // A meeting with no decisions is normal; the model omitting the key
        // must not discard the rest of the summary.
        let summary = parse_summary(r#"{"overview":"just a chat"}"#).unwrap();
        assert_eq!(summary.overview, "just a chat");
        assert!(summary.decisions.is_empty());
        assert!(summary.action_items.is_empty());
    }

    #[test]
    fn an_unowned_action_item_is_allowed() {
        // The far end may be several people, so guessing an owner would be
        // worse than admitting none.
        let summary =
            parse_summary(r#"{"action_items":[{"what":"book the room","owner":null}]}"#).unwrap();
        assert_eq!(summary.action_items[0].owner, None);
    }

    #[test]
    fn a_reply_that_is_not_json_is_reported_with_what_came_back() {
        let err = parse_summary("I'm sorry, I can't help with that.").unwrap_err();
        assert!(
            format!("{err:#}").contains("I'm sorry"),
            "the error should show what was actually returned"
        );
    }

    #[test]
    fn a_missing_key_is_reported_rather_than_silently_skipping_summaries() {
        let settings = crate::config::Settings::default();
        // Only meaningful when the environment does not supply one.
        if std::env::var(API_KEY_ENV).is_err() {
            let err = SummarizeConfig::resolve(&settings).unwrap_err();
            assert!(err.to_string().contains("no API key"));
        }
    }

    #[test]
    fn a_saved_key_is_used_when_the_environment_has_none() {
        let mut settings = crate::config::Settings::default();
        settings.summaries.api_key = Some("sk-or-saved".into());
        settings.summaries.model = Some("some/model".into());

        if std::env::var(API_KEY_ENV).is_err() {
            let config = SummarizeConfig::resolve(&settings).unwrap();
            assert_eq!(config.api_key, "sk-or-saved");
            assert_eq!(config.model, "some/model");
        }
    }

    #[test]
    fn the_model_falls_back_to_the_default_when_unset() {
        let mut settings = crate::config::Settings::default();
        settings.summaries.api_key = Some("sk-or-saved".into());

        if std::env::var("KASETA_OPENROUTER_MODEL").is_err()
            && std::env::var(API_KEY_ENV).is_err()
        {
            assert_eq!(SummarizeConfig::resolve(&settings).unwrap().model, DEFAULT_MODEL);
        }
    }

    /// The transcript is the one thing that leaves the machine, so the terms it
    /// leaves under are part of the request rather than a routing default that
    /// could change underneath us.
    #[test]
    fn every_request_refuses_hosts_that_retain_what_they_are_sent() {
        let body = ChatRequest {
            model: "some/model",
            messages: vec![ChatMessage { role: "user", content: "hello" }],
            response_format: ResponseFormat { kind: "json_object" },
            temperature: 0.2,
            provider: ProviderPolicy::default(),
        };

        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&body).unwrap()).unwrap();

        assert_eq!(json["provider"]["data_collection"], "deny");
        assert_eq!(json["provider"]["require_parameters"], true);
    }
}
