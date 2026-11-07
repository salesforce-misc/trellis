//! The grammar version of a stored definition (ADR-0018).
//!
//! Every stored transform definition carries the
//! `grammar_version` its text was written under, stamped when the text is
//! written. Every read of stored transform text goes through [`load_stored`],
//! which first skips a definition this binary can't run (its floor or its
//! grammar is newer, ADR-0024 rule 3) without parsing it, then takes the text
//! and its version and returns the definition in the current grammar's terms.
//! Nothing after it sees a version.

use super::ParseError;
use super::ast::TransformDef;
use super::floor::Floors;
use super::parser::parse;

/// The grammar version this binary writes. Grammar 1 is the grammar when the
/// marker was introduced. A change that keeps some accepted text accepted but
/// makes it parse, resolve or evaluate differently bumps it; one that refuses
/// text Postgres rejects or reads differently doesn't (ADR-0018).
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

/// Loads a stored definition, in the current grammar's terms: `text`, written
/// under grammar `grammar_version`, of a definition whose stored floor is
/// `floor`. This is the one entry point for stored transform text.
///
/// `None`, without parsing the text, for a definition this binary can't run
/// ([`Floors::unrunnable`]): its floor is above the running release, or its
/// grammar is newer than this binary reads. Attaching paused it, so it reads
/// and writes nothing, and every caller skips it as it would a definition
/// that isn't there. An error is the text of a definition this binary can run
/// failing to load: older than the window, or refused.
pub(crate) fn load_stored(
    floors: &Floors,
    text: &str,
    grammar_version: i32,
    floor: &str,
) -> Result<Option<TransformDef>, ParseError> {
    if floors.unrunnable(floor, grammar_version).is_some() {
        return Ok(None);
    }
    load(text, grammar_version).map(Some)
}

/// Loads stored definition text written under grammar `stored`, in the current
/// grammar's terms, whatever its floor: only [`load_stored`] calls it.
fn load(text: &str, stored: i32) -> Result<TransformDef, ParseError> {
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

/// The attach-time check (ADR-0018, ADR-0024 rule 3) against the grammar
/// window `oldest..=current`, in `txn`. Pauses each definition this binary
/// can't run ([`pause_unrunnable_in`]), without parsing it; each older than
/// `oldest`; and each whose text this binary refuses. Each one pauses alone,
/// so every other definition keeps running. Returns how many it paused or
/// gave a new reason, and which of them it can't run, for the caller to log
/// once it commits ([`log_unrunnable_pauses`]).
pub(crate) async fn pause_unloadable_in(
    txn: &tokio_postgres::Transaction<'_>,
    floors: &Floors,
    oldest: i32,
    current: i32,
) -> Result<(usize, Vec<UnrunnableDefinition>), crate::Error> {
    let unrunnable = pause_unrunnable_in(txn, floors, current).await?;
    let mut paused = unrunnable.len();
    let stored = txn
        .query(
            "select id, source_table, target_table, definition_text, grammar_version, \
                    floor_version \
             from transform_definitions order by id",
            &[],
        )
        .await?;
    for row in &stored {
        let (id, source, target): (i64, String, String) = (row.get(0), row.get(1), row.get(2));
        let (text, grammar_version, floor): (&str, i32, &str) =
            (row.get(3), row.get(4), row.get(5));
        if floors
            .unrunnable_in(floor, grammar_version, current)
            .is_some()
        {
            continue;
        }
        if grammar_version < oldest {
            let reason = ParseError::GrammarVersionTooOld {
                stored: grammar_version,
                oldest,
            }
            .to_string();
            super::lifecycle::pause_for_grammar_version(txn, id, &source, &reason)
                .await
                .map_err(incompatible)?;
            paused += 1;
            continue;
        }
        // Stored text inside the window that this release refuses (a form a
        // newer release narrowed toward Postgres, ADR-0022) pauses the same
        // way, naming the refusal, rather than failing every reader of the
        // catalog.
        if let Err(e) = load_in(text, grammar_version, oldest, current) {
            let reason = unparsable_stored_text_reason(&target, &e);
            if super::lifecycle::pause_for_grammar_version(txn, id, &source, &reason)
                .await
                .map_err(incompatible)?
            {
                paused += 1;
            }
        }
    }
    // The key each definition stored before the record existed keys by
    // (#1719), and a pause of each one an earlier release built over its
    // source key's `INCLUDE` columns (#1722).
    paused += super::recorded_keys::backfill_in(txn, floors, oldest, current)
        .await
        .map_err(incompatible)?;
    Ok((paused, unrunnable))
}

/// A stored definition this binary can't run (ADR-0024 rule 3), read from
/// its stored columns alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnrunnableDefinition {
    id: i64,
    source: String,
    target: String,
    /// Its [`super::floor::Unrunnable::reason`].
    reason: String,
}

/// Each stored definition this binary can't run: its floor is above the
/// running release, its grammar is newer than `current`, or an enum-like
/// value stored for it is one a newer release wrote
/// ([`super::floor::unknown_value_sql`]). Reads only the stored columns,
/// never the text.
pub(crate) async fn unrunnable_definitions(
    client: &impl tokio_postgres::GenericClient,
    floors: &Floors,
    current: i32,
) -> Result<Vec<UnrunnableDefinition>, tokio_postgres::Error> {
    Ok(client
        .query(
            &format!(
                "select d.id, d.source_table, d.target_table, d.grammar_version, \
                        d.floor_version, {} \
                 from transform_definitions d order by d.id",
                super::floor::unknown_value_sql("d")
            ),
            &[],
        )
        .await?
        .iter()
        .filter_map(|row| {
            let unrunnable = floors
                .unrunnable_in(row.get(4), row.get(3), current)
                .or_else(|| {
                    row.get::<_, Option<String>>(5)
                        .map(|value| super::floor::Unrunnable::UnknownValue { value })
                })?;
            let target: String = row.get(2);
            Some(UnrunnableDefinition {
                id: row.get(0),
                source: row.get(1),
                reason: unrunnable.reason(&target),
                target,
            })
        })
        .collect())
}

/// Logs each pause [`pause_unrunnable_in`] made, once its transaction has
/// committed: a rolled-back check (`migrate`'s while another version is
/// live) paused nothing, so it logs nothing.
pub(crate) fn log_unrunnable_pauses(paused: &[UnrunnableDefinition]) {
    for definition in paused {
        crate::instance_log::warn!(
            transform = %definition.target,
            reason = %definition.reason,
            "paused a transform this release can't run"
        );
    }
}

/// Pauses, in `txn`, each definition this binary can't run (ADR-0024 rule
/// 3): its floor is above the running release, or its grammar is newer than
/// `current`. Reads only the stored columns, never the text, and records the
/// [`super::floor::Unrunnable::reason`] as its `capture_failure`, keeping any
/// reason it was already paused for. Returns the ones it paused or gave a new
/// reason, none once an earlier call recorded them, for the caller to log
/// once it commits ([`log_unrunnable_pauses`]).
///
/// Attaching runs it ([`pause_unloadable_in`]), and so does every handle
/// before it changes anything: a background process when it takes over, and
/// each state-changing call (ADR-0024 rule 2).
pub(crate) async fn pause_unrunnable_in(
    txn: &tokio_postgres::Transaction<'_>,
    floors: &Floors,
    current: i32,
) -> Result<Vec<UnrunnableDefinition>, crate::Error> {
    let mut paused = Vec::new();
    for definition in unrunnable_definitions(txn, floors, current).await? {
        if super::lifecycle::pause_for_unrunnable(
            txn,
            definition.id,
            &definition.source,
            &definition.reason,
        )
        .await
        .map_err(incompatible)?
        {
            paused.push(definition);
        }
    }
    Ok(paused)
}

/// A catalog failure of the attach's pauses, as the attach reports it.
fn incompatible(e: super::catalog::CatalogError) -> crate::Error {
    crate::Error::IncompatibleInstance(e.to_string())
}

/// The `capture_failure` of a transform whose stored text this release refuses.
pub(crate) fn unparsable_stored_text_reason(target: &str, error: &ParseError) -> String {
    format!(
        "the stored definition of transform {target} is refused by this release: {error}; \
         drop the transform and define it again in the current grammar"
    )
}

/// [`pause_unloadable_in`] against this binary's window, in a transaction of
/// its own on `client`, committed when `apply` and rolled back otherwise.
async fn pause_unloadable_on(
    client: &mut tokio_postgres::Client,
    floors: &Floors,
    apply: bool,
) -> Result<usize, crate::Error> {
    let txn = client.transaction().await?;
    let (paused, unrunnable) = pause_unloadable_in(
        &txn,
        floors,
        OLDEST_SUPPORTED_GRAMMAR_VERSION,
        CURRENT_GRAMMAR_VERSION,
    )
    .await?;
    if apply {
        txn.commit().await?;
        log_unrunnable_pauses(&unrunnable);
    } else {
        txn.rollback().await?;
    }
    Ok(paused)
}

/// The attach's check ([`pause_unloadable_in`]) against this binary's
/// window: before anything reads a stored definition, pauses the ones this
/// binary can't run or read. Returns how many it paused.
pub(crate) async fn check_stored_versions(
    client: &mut tokio_postgres::Client,
    floors: &Floors,
) -> Result<usize, crate::Error> {
    pause_unloadable_on(client, floors, true).await
}

/// [`check_stored_versions`] rolled back: how many definitions it would
/// pause, pausing none. `migrate` runs this while another version is live
/// (ADR-0023), when it may change nothing.
pub(crate) async fn check_stored_versions_dry_run(
    client: &mut tokio_postgres::Client,
    floors: &Floors,
) -> Result<usize, crate::Error> {
    pause_unloadable_on(client, floors, false).await
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

    /// A definition this binary can't run loads as nothing, and its text is
    /// never parsed: text no release of this grammar reads loads the same.
    /// One it can run parses, and fails as it always did.
    #[test]
    fn a_definition_this_binary_cant_run_is_skipped_unparsed() {
        let floors = Floors::of_release("0.2.0", std::sync::Arc::from([]));
        let unreadable = "TRANSFORM t FROM s SELECT FROBNICATE a AS a";
        for (text, grammar, floor) in [
            (unreadable, CURRENT_GRAMMAR_VERSION, "0.3.0"),
            (unreadable, CURRENT_GRAMMAR_VERSION + 1, "0.2.0"),
            (unreadable, CURRENT_GRAMMAR_VERSION, "not a release"),
        ] {
            assert_eq!(
                load_stored(&floors, text, grammar, floor),
                Ok(None),
                "{grammar} {floor}"
            );
        }
        assert_eq!(
            load_stored(&floors, TEXT, CURRENT_GRAMMAR_VERSION, "0.2.0"),
            Ok(Some(parse(TEXT).unwrap()))
        );
        assert!(load_stored(&floors, unreadable, CURRENT_GRAMMAR_VERSION, "0.2.0").is_err());
    }
}
