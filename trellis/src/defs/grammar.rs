//! The grammar version of a stored definition (ADR-0018).
//!
//! Every stored transform and relationship definition carries the
//! `grammar_version` its text was written under, stamped when the text is
//! written. Every read of stored transform text goes through [`load`], which
//! takes the text and its version and returns the definition in the current
//! grammar's terms. Nothing after it sees a version.

use super::ParseError;
use super::ast::TransformDef;
use super::parser::parse;

/// The grammar version this binary writes. Grammar 1 is the grammar when the
/// marker was introduced. A change that makes some accepted text parse,
/// resolve or evaluate differently, or be refused, bumps it (ADR-0018).
pub const CURRENT_GRAMMAR_VERSION: i32 = 1;

/// The oldest grammar version this binary reads. A version below it is outside
/// the support window. Each bump ships the previous version's compatibility
/// rule and moves this only once that rule leaves the window.
pub const OLDEST_SUPPORTED_GRAMMAR_VERSION: i32 = 1;

/// Where a stored version falls against a binary's window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Standing {
    /// Written under the current grammar: parses as is.
    Current,
    /// Older, inside the window: parses under that version's rules and its
    /// compatibility rule translates the tree.
    Older,
    /// Older than the window: not read.
    TooOld,
    /// Newer than the binary knows: not read or written.
    TooNew,
}

/// Classifies `stored` against the window `oldest..=current`.
pub(crate) fn standing_in(stored: i32, oldest: i32, current: i32) -> Standing {
    if stored > current {
        Standing::TooNew
    } else if stored == current {
        Standing::Current
    } else if stored < oldest {
        Standing::TooOld
    } else {
        Standing::Older
    }
}

/// Loads stored definition text written under grammar `stored`, in the current
/// grammar's terms. This is the one entry point for stored transform text.
pub fn load(text: &str, stored: i32) -> Result<TransformDef, ParseError> {
    load_in(
        text,
        stored,
        OLDEST_SUPPORTED_GRAMMAR_VERSION,
        CURRENT_GRAMMAR_VERSION,
    )
}

/// [`load`] against an explicit window, so each branch is testable while the
/// binary's own window holds a single version.
pub(crate) fn load_in(
    text: &str,
    stored: i32,
    oldest: i32,
    current: i32,
) -> Result<TransformDef, ParseError> {
    match standing_in(stored, oldest, current) {
        Standing::Current => parse(text),
        // No compatibility rule exists yet: only grammar 1 does, and it is the
        // current one. A bump adds a dispatch on `stored` here.
        Standing::Older => parse(text),
        Standing::TooOld => Err(ParseError::GrammarVersionTooOld { stored, oldest }),
        Standing::TooNew => Err(ParseError::GrammarVersionTooNew { stored, current }),
    }
}

/// The attach-time check (ADR-0018) against an explicit window: refuses when
/// any stored definition is newer than `current`, naming them, and pauses each
/// transform older than `oldest` with the reason in its `capture_failure`.
/// Returns how many it paused.
pub(crate) async fn check_stored_versions_in(
    client: &mut tokio_postgres::Client,
    oldest: i32,
    current: i32,
) -> Result<usize, crate::Error> {
    let newer = client
        .query(
            "select 'transform ' || target_table, grammar_version from transform_definitions \
               where grammar_version > $1 \
             union all \
             select 'relationship ' || from_schema || '.' || from_table || '.' || name, \
                    grammar_version from relationship_definitions where grammar_version > $1 \
             order by 1",
            &[&current],
        )
        .await?;
    if !newer.is_empty() {
        let named: Vec<String> = newer
            .iter()
            .map(|row| {
                format!(
                    "{} (grammar version {})",
                    row.get::<_, String>(0),
                    row.get::<_, i32>(1)
                )
            })
            .collect();
        return Err(crate::Error::IncompatibleInstance(format!(
            "stored definitions were written under a grammar newer than this binary reads \
             (version {current}): {}; run a binary that reads them",
            named.join(", ")
        )));
    }
    let txn = client.transaction().await?;
    let too_old = txn
        .query(
            "select id, source_table, grammar_version from transform_definitions \
             where grammar_version < $1 order by id",
            &[&oldest],
        )
        .await?;
    let mut paused = 0;
    for row in &too_old {
        let (id, source, stored): (i64, String, i32) = (row.get(0), row.get(1), row.get(2));
        let reason = ParseError::GrammarVersionTooOld { stored, oldest }.to_string();
        super::lifecycle::pause_for_grammar_version(&txn, id, &source, &reason)
            .await
            .map_err(|e| crate::Error::IncompatibleInstance(e.to_string()))?;
        paused += 1;
    }
    txn.commit().await?;
    Ok(paused)
}

/// [`check_stored_versions_in`] against this binary's window.
pub(crate) async fn check_stored_versions(
    client: &mut tokio_postgres::Client,
) -> Result<usize, crate::Error> {
    check_stored_versions_in(
        client,
        OLDEST_SUPPORTED_GRAMMAR_VERSION,
        CURRENT_GRAMMAR_VERSION,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEXT: &str = "TRANSFORM t FROM public.src SELECT id AS id";

    #[test]
    fn current_version_parses_as_today() {
        assert_eq!(
            load(TEXT, CURRENT_GRAMMAR_VERSION).unwrap(),
            parse(TEXT).unwrap()
        );
    }

    #[test]
    fn older_version_inside_the_window_parses() {
        assert!(load_in(TEXT, 2, 2, 4).is_ok());
        assert!(load_in(TEXT, 3, 2, 4).is_ok());
        assert_eq!(standing_in(3, 2, 4), Standing::Older);
    }

    #[test]
    fn older_version_outside_the_window_is_refused_naming_the_fix() {
        let err = load_in(TEXT, 1, 2, 4).unwrap_err();
        assert_eq!(
            err,
            ParseError::GrammarVersionTooOld {
                stored: 1,
                oldest: 2
            }
        );
        let msg = err.to_string();
        assert!(
            msg.contains("version 1") && msg.contains("2 and later"),
            "{msg}"
        );
        assert!(
            msg.contains("drop the transform and define it again"),
            "{msg}"
        );
    }

    #[test]
    fn newer_version_is_refused_even_when_the_text_parses() {
        let err = load_in(TEXT, 5, 2, 4).unwrap_err();
        assert_eq!(
            err,
            ParseError::GrammarVersionTooNew {
                stored: 5,
                current: 4
            }
        );
        assert!(load(TEXT, CURRENT_GRAMMAR_VERSION + 1).is_err());
    }
}
