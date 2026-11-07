//! `KEY (<column>, ...)` after a `FROM` or `JOIN` table (ADR-0026): the
//! columns Trellis keys that table by. Trellis's statement language, not
//! the expression grammar, so ADR-0022 doesn't apply; `KEY` not followed by
//! `(` is still an alias, as Postgres doesn't reserve the word.

use super::ast::{KeySpace, TransformDef, render_definition_text};
use super::error::ParseError;
use super::parse;

fn right_key(def: &TransformDef) -> Option<Vec<String>> {
    match &def.key_space {
        KeySpace::Join(join) | KeySpace::JoinOneToOne(join) => join.right_key.clone(),
        other => panic!("expected a JOIN, got {other:?}"),
    }
}

/// A definition's text, its `FROM` key, its `JOIN` key and its rendering.
type Case = (
    &'static str,
    Option<Vec<String>>,
    Option<Vec<String>>,
    &'static str,
);

fn key(columns: &[&str]) -> Option<Vec<String>> {
    Some(columns.iter().map(|c| c.to_string()).collect())
}

/// Every transform kind takes a `KEY` on each table it reads, in the order
/// written, and its rendered text carries it back, so a dump re-defines the
/// same key.
#[test]
fn a_key_parses_on_from_and_join_and_round_trips_through_its_rendered_text() {
    let cases: &[Case] = &[
        (
            "TRANSFORM t FROM orders KEY (customer_id, id) SELECT total AS total",
            key(&["customer_id", "id"]),
            None,
            "TRANSFORM t FROM orders KEY (customer_id, id) SELECT total AS total",
        ),
        (
            "TRANSFORM t from app.orders key(k) group by g select g as g, count(*) as n",
            key(&["k"]),
            None,
            "TRANSFORM t FROM app.orders KEY (k) GROUP BY g SELECT g AS g, COUNT(*) AS n",
        ),
        (
            "TRANSFORM t FROM a KEY (k) AS x JOIN b KEY (bk, n) AS y ON x.f = y.bk \
             GROUP BY x.g SELECT x.g AS g, COUNT(*) AS n",
            key(&["k"]),
            key(&["bk", "n"]),
            "TRANSFORM t FROM a KEY (k) AS x INNER JOIN b KEY (bk, n) AS y ON x.f = y.bk \
             GROUP BY x.g SELECT x.g AS g, COUNT(*) AS n",
        ),
        (
            "TRANSFORM t FROM a LEFT JOIN b KEY (bk) ON a.f = b.bk SELECT a.v AS v, b.w AS w",
            None,
            key(&["bk"]),
            "TRANSFORM t FROM a LEFT JOIN b KEY (bk) ON a.f = b.bk SELECT a.v AS v, b.w AS w",
        ),
    ];
    for (text, source_key, joined_key, rendered) in cases {
        let def = parse(text).unwrap_or_else(|e| panic!("{text}: {e:?}"));
        assert_eq!(&def.source_key, source_key, "{text}");
        if joined_key.is_some() || matches!(def.key_space, KeySpace::Join(_)) {
            assert_eq!(&right_key(&def), joined_key, "{text}");
        }
        assert_eq!(&render_definition_text(&def), rendered, "{text}");
        assert_eq!(parse(rendered).unwrap(), def, "{rendered}");
    }
}

/// Without `KEY` the tree has none, and a table aliased `key` still parses
/// as before: only `KEY (` starts the clause.
#[test]
fn key_without_a_parenthesis_is_still_an_alias() {
    let def = parse("TRANSFORM t FROM s SELECT a AS a").unwrap();
    assert_eq!(def.source_key, None);
    let def = parse(
        "TRANSFORM t FROM a key JOIN b ON key.f = b.k GROUP BY key.g SELECT key.g AS g, \
         COUNT(*) AS n",
    )
    .unwrap();
    assert_eq!(def.source_key, None);
    match &def.key_space {
        KeySpace::Join(join) => assert_eq!(join.source_alias.as_deref(), Some("key")),
        other => panic!("expected a JOIN, got {other:?}"),
    }
}

/// A `KEY` must name at least one column, each once, and comes directly
/// after its table, never after the alias.
#[test]
fn a_malformed_key_is_refused() {
    for text in [
        "TRANSFORM t FROM s KEY () SELECT a AS a",
        "TRANSFORM t FROM s KEY (a, ) SELECT a AS a",
        "TRANSFORM t FROM s KEY (a SELECT a AS a",
        "TRANSFORM t FROM a AS x KEY (k) JOIN b ON x.f = b.k SELECT x.v AS v",
    ] {
        assert!(parse(text).is_err(), "{text} parsed");
    }
    match parse("TRANSFORM t FROM s KEY (a, b, a) SELECT a AS a") {
        Err(ParseError::UnsupportedKeySpace { construct, detail }) => {
            assert_eq!(construct, "KEY");
            assert!(detail.contains("'a' twice"), "{detail}");
        }
        other => panic!("expected a refusal of a repeated column, got {other:?}"),
    }
}
