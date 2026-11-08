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
use super::ast::{KeySpace, TransformDef};
use super::floor::Floors;
use super::lexer::Identifiers;
use super::parser::parse_in;

/// The newest grammar version this binary reads. A change that keeps some
/// accepted text accepted but makes it parse, resolve or evaluate
/// differently bumps it; one that refuses text Postgres rejects or reads
/// differently doesn't (ADR-0018).
///
/// - Grammar 1 is the grammar when the marker was introduced: an unquoted
///   name keeps its case, and there are no quoted identifiers.
/// - Grammar 2 reads identifiers as Postgres does: an unquoted name folds to
///   lower case and a quoted one is exact. Grammar 1's compatibility rule
///   reads its text as if every name were quoted ([`load_in`]).
///
/// Text is stamped with the oldest version that reads it the same
/// ([`stamp_in`]), not always with this one.
pub const CURRENT_GRAMMAR_VERSION: i32 = 2;

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
pub(crate) fn load(text: &str, stored: i32) -> Result<TransformDef, ParseError> {
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
        Standing::Current => parse_in(text, Identifiers::Folded),
        Standing::Older => match stored {
            1 => grammar_1(text),
            // Only grammar 1 has a compatibility rule; a test's window may
            // name versions this binary never had.
            _ => parse_in(text, Identifiers::Folded),
        },
        Standing::TooOld => Err(ParseError::GrammarVersionTooOld { stored, oldest }),
        Standing::TooNew => Err(ParseError::GrammarVersionTooNew { stored, current }),
    }
}

/// Grammar 1's compatibility rule: its text read as if every name were
/// quoted. Lexing without folding gives the tree grammar 1 gave, whose names
/// are the catalog's exact names, which mean the same in grammar 2. The one
/// name grammar 1 folded is a table alias, compared in any case there: it is
/// lower-cased, the name grammar 2 gives it.
fn grammar_1(text: &str) -> Result<TransformDef, ParseError> {
    let mut def = parse_in(text, Identifiers::Exact)?;
    if let KeySpace::Join(join) | KeySpace::JoinOneToOne(join) = &mut def.key_space {
        for alias in std::iter::once(&mut join.source_alias)
            .chain(join.joins.iter_mut().map(|joined| &mut joined.right_alias))
            .flatten()
        {
            alias.make_ascii_lowercase();
        }
    }
    Ok(def)
}

/// The grammar version to stamp on `text` as it is written (define,
/// `ALTER TRANSFORM`, a rewrite): the oldest version in this binary's window
/// that reads it as the current one does (ADR-0018). Text with no quoted
/// identifier and no upper-case letter in a name reads the same in grammar 1,
/// so a release that only reads grammar 1 can still run it.
pub(crate) fn stamp(text: &str) -> i32 {
    stamp_in(
        text,
        OLDEST_SUPPORTED_GRAMMAR_VERSION,
        CURRENT_GRAMMAR_VERSION,
    )
}

/// `def` as the text a rewrite stores (`ALTER TRANSFORM`, a recorded key):
/// rendered in the current grammar, with the version [`stamp`] gives it. A
/// tree read from older text can hold a name the current grammar refuses (a
/// grammar-1 `AS` label over 63 bytes), whose rendering no load would read
/// back, so that is refused with the current grammar's error rather than
/// stored.
pub(crate) fn rewrite(def: &TransformDef) -> Result<(String, i32), ParseError> {
    let text = super::ast::render_definition_text(def);
    parse_in(&text, Identifiers::Folded)?;
    let version = stamp(&text);
    Ok((text, version))
}

/// [`stamp`] against the window `oldest..=current`. Text the current version
/// refuses gets the current version: nothing stores it ([`rewrite`] refuses
/// it).
pub(crate) fn stamp_in(text: &str, oldest: i32, current: i32) -> i32 {
    let Ok(reading) = load_in(text, current, oldest, current) else {
        return current;
    };
    (oldest..current)
        .find(|&version| load_in(text, version, oldest, current).as_ref() == Ok(&reading))
        .unwrap_or(current)
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

    use super::super::parser::parse;

    const TEXT: &str = "TRANSFORM t FROM public.src SELECT id AS id";

    #[test]
    fn current_version_parses_as_today() {
        assert_eq!(
            load(TEXT, CURRENT_GRAMMAR_VERSION).unwrap(),
            parse(TEXT).unwrap()
        );
    }

    /// `JOIN LATERAL` is refused by name in grammar 2, at any position, as
    /// Postgres reserves `LATERAL`; grammar 1, which never reserved it,
    /// still reads stored text naming a table `LATERAL` as it always did.
    #[test]
    fn join_lateral_is_refused_in_grammar_2_and_a_table_name_in_grammar_1() {
        let one = "TRANSFORM t FROM a JOIN LATERAL b ON a.k = b.k SELECT b.x AS x";
        let second = "TRANSFORM t FROM a JOIN c ON a.j = c.j \
                      JOIN LATERAL b ON a.k = b.k SELECT b.x AS x";
        for text in [one, second] {
            assert!(
                matches!(
                    load(text, CURRENT_GRAMMAR_VERSION),
                    Err(ParseError::UnsupportedKeySpace { ref construct, .. })
                        if construct == "JOIN LATERAL"
                ),
                "{text}"
            );
        }
        let stored = load(one, 1).expect("grammar 1 reads LATERAL as a table name");
        let join = super::super::join::spec(&stored).expect("a JOIN");
        assert_eq!(join.joins[0].right, "LATERAL");
        assert_eq!(join.joins[0].right_alias.as_deref(), Some("b"));
    }

    /// Each joined table's name folds in grammar 2 as the first one's does,
    /// a quoted one kept exact; grammar 1 keeps each as written and lower-
    /// cases every alias.
    #[test]
    fn every_joined_tables_name_folds_alike() {
        let names = |def: &TransformDef| -> Vec<(String, Option<String>)> {
            super::super::join::spec(def)
                .expect("a JOIN")
                .joins
                .iter()
                .map(|j| (j.right.clone(), j.right_alias.clone()))
                .collect()
        };
        let folded = "TRANSFORM t FROM a JOIN B1 ON a.k = B1.id \
                      LEFT JOIN \"B2\" ON a.j = \"B2\".id JOIN B3 AS X3 ON a.i = X3.id \
                      SELECT B1.x AS x";
        assert_eq!(
            names(&load(folded, CURRENT_GRAMMAR_VERSION).unwrap()),
            [
                ("b1".to_string(), None),
                ("B2".to_string(), None),
                ("b3".to_string(), Some("x3".to_string()))
            ]
        );
        let exact = "TRANSFORM t FROM a JOIN B1 ON a.k = B1.id \
                     LEFT JOIN B2 ON a.j = B2.id JOIN B3 AS X3 ON a.i = X3.id \
                     SELECT B1.x AS x";
        assert_eq!(
            names(&load(exact, 1).unwrap()),
            [
                ("B1".to_string(), None),
                ("B2".to_string(), None),
                ("B3".to_string(), Some("x3".to_string()))
            ]
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

    /// Grammar 1's compatibility rule reads its text as if every name were
    /// quoted: the same tree grammar 1 gave, which grammar 2 spells with
    /// quotes. Grammar 2 reads the same text folded.
    #[test]
    fn grammar_1_text_reads_as_if_every_name_were_quoted() {
        for (grammar_1, grammar_2) in [
            (
                "TRANSFORM cust_totals FROM invoices GROUP BY customer_id \
                 WITHOUT SOURCE INDEX SELECT SUM(Amount) AS total",
                "TRANSFORM cust_totals FROM invoices GROUP BY customer_id \
                 WITHOUT SOURCE INDEX SELECT SUM(\"Amount\") AS total",
            ),
            (
                "TRANSFORM OrderTotals FROM Shop.Orders GROUP BY OrderId \
                 WITHOUT SOURCE INDEX SELECT SUM(Qty) AS Total",
                "TRANSFORM \"OrderTotals\" FROM \"Shop\".\"Orders\" GROUP BY \"OrderId\" \
                 WITHOUT SOURCE INDEX SELECT SUM(\"Qty\") AS \"Total\"",
            ),
            // `FROM User` read a table named `User` in grammar 1.
            (
                "TRANSFORM t FROM User SELECT Id AS Id",
                "TRANSFORM t FROM \"User\" SELECT \"Id\" AS \"Id\"",
            ),
            // An alias folded in grammar 1 too: it is lower-cased.
            (
                "TRANSFORM t FROM Orders AS O JOIN items AS I ON o.Id = i.order_id \
                 GROUP BY O.Region SELECT COUNT(*) AS n",
                "TRANSFORM t FROM \"Orders\" AS o JOIN items AS i ON o.\"Id\" = i.order_id \
                 GROUP BY o.\"Region\" SELECT COUNT(*) AS n",
            ),
        ] {
            let loaded = load_in(grammar_1, 1, 1, 2).unwrap();
            assert_eq!(loaded, parse(grammar_2).unwrap(), "{grammar_1}");
            // Rewriting it renders the same tree in grammar 2.
            let rendered = super::super::ast::render_definition_text(&loaded);
            assert_eq!(parse(&rendered).unwrap(), loaded, "{rendered}");
            assert_ne!(
                load_in(grammar_1, 2, 1, 2).ok(),
                Some(loaded),
                "grammar 2 folds {grammar_1}"
            );
        }
        // Grammar 1 had no quoted identifiers.
        assert!(load_in("TRANSFORM t FROM s SELECT \"a\" AS a", 1, 1, 2).is_err());
    }

    /// Text is stamped with the oldest version that reads it as the current
    /// one does: grammar 1 for text with no quoted name and no upper-case
    /// letter in a name, whatever the case of its keywords and functions.
    #[test]
    fn text_is_stamped_with_the_oldest_grammar_that_reads_it_the_same() {
        for (text, stamped) in [
            ("TRANSFORM t FROM s SELECT a + a AS x", 1),
            ("transform t from public.s group by g select sum(a) as x", 1),
            (
                "TRANSFORM t FROM s AS o JOIN b ON o.k = b.k GROUP BY o.g SELECT COUNT(*) AS n",
                1,
            ),
            // An upper-case alias reads the same in both.
            (
                "TRANSFORM t FROM s AS O JOIN b ON o.k = b.k GROUP BY O.g SELECT COUNT(*) AS n",
                1,
            ),
            ("TRANSFORM t FROM s SELECT Amount AS x", 2),
            ("TRANSFORM T FROM s SELECT a AS x", 2),
            ("TRANSFORM t FROM Shop.s SELECT a AS x", 2),
            (
                "TRANSFORM t FROM s GROUP BY OrderId SELECT COUNT(*) AS n",
                2,
            ),
            ("TRANSFORM t FROM s SELECT a AS Total", 2),
            ("TRANSFORM t FROM s KEY (Id) SELECT a AS x", 2),
            ("TRANSFORM t FROM s SELECT \"a\" AS x", 2),
            ("TRANSFORM t FROM s SELECT \"Amount\" AS x", 2),
            // Refused text gets the current version; nothing stores it.
            ("TRANSFORM t FROM order SELECT a AS x", 2),
        ] {
            assert_eq!(stamp_in(text, 1, 2), stamped, "{text}");
        }
        assert_eq!(stamp("TRANSFORM t FROM s SELECT a AS x"), 1);
        // A window holding only the current version stamps it.
        assert_eq!(stamp_in("TRANSFORM t FROM s SELECT a AS x", 2, 2), 2);
    }

    /// A rewrite stores the tree's current rendering, stamped; a tree from
    /// grammar-1 text holding a name the current grammar refuses (a label
    /// over 63 bytes, which grammar 1 kept) is refused rather than stored as
    /// text no load reads back.
    #[test]
    fn a_rewrite_refuses_text_the_current_grammar_cant_read_back() {
        let def = load_in("TRANSFORM t FROM s SELECT Amount AS total", 1, 1, 2).unwrap();
        assert_eq!(
            rewrite(&def).unwrap(),
            (
                "TRANSFORM t FROM s SELECT \"Amount\" AS total".to_string(),
                2
            )
        );
        let def = load_in("TRANSFORM t FROM s SELECT a AS total", 1, 1, 2).unwrap();
        assert_eq!(rewrite(&def).unwrap().1, 1);
        let long = "L".repeat(70);
        let def = load_in(&format!("TRANSFORM t FROM s SELECT a AS {long}"), 1, 1, 2).unwrap();
        assert!(
            matches!(rewrite(&def), Err(ParseError::NameTooLong { name, .. }) if name == long),
            "{:?}",
            rewrite(&def)
        );
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
