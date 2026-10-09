//! How a speaker is named to a person.
//!
//! Attribution is stored as a hint about where the audio came from, never as
//! a name: `local` is the operator's microphone, `remote` is everything the
//! machine played back, and `unknown` is audio no track could vouch for. The
//! word a person reads depends on what kind of recording the line came from as
//! well as on the hint, and that pairing used to be re-derived in every place
//! that printed a transcript. They drifted, so it lives here once.
//!
//! The same stored value means different things in the two kinds of recording.
//! In a captured meeting, `unknown` is a line the recorder genuinely could not
//! place, and saying "Unknown" is honest. In an imported file every line is
//! `unknown`, because one mixed track carries every voice and nothing was ever
//! going to attribute it; calling each of them "Unknown" would read as a fault
//! on every line of a recording that has none. Those lines are a "Speaker".

use crate::manifest::Origin;

/// The label a person reads for one transcript line.
///
/// `hint` accepts both vocabularies in use: the stored segment hint (`local`,
/// `remote`, `unknown`) and the transcript line's speaker (`you`, `them`,
/// `unknown`). Anything unrecognised is treated as unattributed rather than
/// guessed at.
pub fn display(origin: Origin, hint: &str) -> &'static str {
    match hint {
        "local" | "you" => "You",
        "remote" | "them" => "Them",
        _ => match origin {
            Origin::Captured => "Unknown",
            Origin::Imported => "Speaker",
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captured_lines_keep_their_meeting_roles() {
        for (hint, label) in [
            ("local", "You"),
            ("you", "You"),
            ("remote", "Them"),
            ("them", "Them"),
            ("unknown", "Unknown"),
            ("", "Unknown"),
            ("something-new", "Unknown"),
        ] {
            assert_eq!(display(Origin::Captured, hint), label, "hint {hint:?}");
        }
    }

    /// The stored value stays `unknown`; only the word shown changes.
    #[test]
    fn an_imported_line_is_a_speaker_not_an_unknown() {
        assert_eq!(display(Origin::Imported, "unknown"), "Speaker");
        assert_eq!(display(Origin::Imported, ""), "Speaker");
    }

    /// An import carries no attributed lines today, but a hint that does say
    /// who spoke is still worth more than the generic label.
    #[test]
    fn an_attributed_hint_wins_whatever_the_origin() {
        assert_eq!(display(Origin::Imported, "local"), "You");
        assert_eq!(display(Origin::Imported, "them"), "Them");
    }
}
