//! Table aliases in `JOIN` definitions (#1458): `FROM <table> [AS] <alias>`
//! and `JOIN <table> [AS] <alias>`. The parser resolves every qualifier that
//! names an aliased side to the side's bare table name, so the tree past it
//! is the tree of the same definition written without aliases, apart from
//! the aliases kept for rendering. Scoping, reserved words and the refusals
//! mirror Postgres (each was read off PostgreSQL 17): an alias hides its
//! table's name, an unquoted alias folds to lower case, and a reserved word
//! is never an alias.

use super::ast::{Expr, JoinSpec, KeySpace, QualifiedColumn, TransformDef, render_definition_text};
use super::error::ParseError;
use super::parse;

/// `def` with its aliases dropped: the tree every reader past the parser
/// works from.
fn without_aliases(mut def: TransformDef) -> TransformDef {
    if let KeySpace::Join(join) | KeySpace::JoinOneToOne(join) = &mut def.key_space {
        join.source_alias = None;
        join.right_alias = None;
    }
    def
}

fn join_of(def: &TransformDef) -> &JoinSpec {
    match &def.key_space {
        KeySpace::Join(join) | KeySpace::JoinOneToOne(join) => join,
        other => panic!("expected a JOIN, got {other:?}"),
    }
}

/// Each aliased definition and the same definition written with table
/// names. Their trees differ only in the aliases, so nothing that reads a
/// qualifier (validation, the oracle, `self_check`, the hidden tables'
/// SQL, capture) can tell them apart; a qualifier kept as written would
/// make them differ.
const ALIASED_AND_PLAIN: &[(&str, &str)] = &[
    (
        "TRANSFORM t FROM orders AS o JOIN customers AS c ON o.cid = c.id \
         GROUP BY o.region, c.tier SELECT o.region AS region, c.tier AS tier, \
         COUNT(*) AS n, SUM(o.total) AS total",
        "TRANSFORM t FROM orders JOIN customers ON orders.cid = customers.id \
         GROUP BY orders.region, customers.tier SELECT orders.region AS region, \
         customers.tier AS tier, COUNT(*) AS n, SUM(orders.total) AS total",
    ),
    // Without `AS`, a `LEFT` join, a schema on both tables.
    (
        "TRANSFORM t FROM shop.orders o LEFT JOIN crm.customers c ON c.id = o.cid \
         GROUP BY c.tier SELECT c.tier AS tier, COUNT(o.id) AS n",
        "TRANSFORM t FROM shop.orders LEFT JOIN crm.customers ON customers.id = orders.cid \
         GROUP BY customers.tier SELECT customers.tier AS tier, COUNT(orders.id) AS n",
    ),
    // Only the joined table aliased; the other side keeps its own name,
    // schema-qualified columns included.
    (
        "TRANSFORM t FROM shop.orders JOIN customers AS c ON shop.orders.cid = c.id \
         GROUP BY orders.region SELECT orders.region AS region, MAX(c.tier) AS tier",
        "TRANSFORM t FROM shop.orders JOIN customers ON shop.orders.cid = customers.id \
         GROUP BY orders.region SELECT orders.region AS region, MAX(customers.tier) AS tier",
    ),
    // Only the `FROM` table aliased, in a 1-1 JOIN, with the alias read in
    // nested expressions of every kind.
    (
        "TRANSFORM t FROM orders o JOIN customers ON o.cid = customers.id \
         SELECT CASE WHEN o.total IS NULL THEN 0 ELSE -o.total END AS a, \
         COALESCE(o.note, customers.name) AS b, CAST(o.total AS bigint) AS c, \
         NOT (o.total > 1 AND o.cid = 2) AS d",
        "TRANSFORM t FROM orders JOIN customers ON orders.cid = customers.id \
         SELECT CASE WHEN orders.total IS NULL THEN 0 ELSE -orders.total END AS a, \
         COALESCE(orders.note, customers.name) AS b, CAST(orders.total AS bigint) AS c, \
         NOT (orders.total > 1 AND orders.cid = 2) AS d",
    ),
    // An unquoted alias folds to lower case, as in Postgres.
    (
        "TRANSFORM t FROM orders AS O JOIN customers c ON o.cid = C.id \
         SELECT o.total AS total",
        "TRANSFORM t FROM orders JOIN customers ON orders.cid = customers.id \
         SELECT orders.total AS total",
    ),
    // An alias may repeat its own table's name.
    (
        "TRANSFORM t FROM shop.orders AS orders JOIN customers ON orders.cid = customers.id \
         SELECT orders.total AS total",
        "TRANSFORM t FROM shop.orders JOIN customers ON orders.cid = customers.id \
         SELECT orders.total AS total",
    ),
];

#[test]
fn an_aliased_definition_is_the_plain_one_past_the_parser() {
    for (aliased, plain) in ALIASED_AND_PLAIN {
        let def = parse(aliased).unwrap_or_else(|e| panic!("{aliased}: {e}"));
        let plain_def = parse(plain).unwrap_or_else(|e| panic!("{plain}: {e}"));
        assert_eq!(without_aliases(def), plain_def, "{aliased}");
    }
}

/// A definition re-parses from its rendered text to the same tree, aliases
/// written back with `AS` and every column of an aliased side qualified by
/// the alias.
#[test]
fn an_aliased_definition_round_trips_through_its_rendered_text() {
    for (aliased, _) in ALIASED_AND_PLAIN {
        let def = parse(aliased).unwrap();
        let rendered = render_definition_text(&def);
        assert_eq!(parse(&rendered).unwrap(), def, "{aliased} -> {rendered}");
        let join = join_of(&def);
        for alias in [&join.source_alias, &join.right_alias]
            .into_iter()
            .flatten()
        {
            assert!(rendered.contains(&format!(" AS {alias}")), "{rendered}");
        }
    }
    let def = parse(ALIASED_AND_PLAIN[1].0).unwrap();
    assert_eq!(
        render_definition_text(&def),
        "TRANSFORM t FROM shop.orders AS o LEFT JOIN crm.customers AS c ON c.id = o.cid \
         GROUP BY c.tier SELECT c.tier AS tier, COUNT(o.id) AS n"
    );
    let def = parse(ALIASED_AND_PLAIN[2].0).unwrap();
    assert_eq!(
        render_definition_text(&def),
        "TRANSFORM t FROM shop.orders INNER JOIN customers AS c ON shop.orders.cid = c.id \
         GROUP BY orders.region SELECT orders.region AS region, MAX(c.tier) AS tier"
    );
}

/// A definition without aliases renders exactly as it did before aliases
/// existed, so every stored definition keeps its text.
#[test]
fn a_plain_join_renders_as_before() {
    let def = parse(
        "TRANSFORM t FROM shop.orders LEFT OUTER JOIN customers ON shop.orders.cid = \
         customers.id GROUP BY orders.region SELECT orders.region AS region, \
         SUM(customers.x) AS x",
    )
    .unwrap();
    let join = join_of(&def);
    assert_eq!((&join.source_alias, &join.right_alias), (&None, &None));
    assert_eq!(
        render_definition_text(&def),
        "TRANSFORM t FROM shop.orders LEFT JOIN customers ON shop.orders.cid = customers.id \
         GROUP BY orders.region SELECT orders.region AS region, SUM(customers.x) AS x"
    );
}

/// A reserved word after a table is the next clause, never an alias:
/// `FROM a LEFT JOIN`, `JOIN b ON`, `GROUP BY`. So is `WITHOUT`, which
/// starts `WITHOUT SOURCE INDEX`.
#[test]
fn a_reserved_word_after_a_table_is_not_an_alias() {
    let def = parse(
        "TRANSFORM t FROM a LEFT JOIN b ON a.k = b.k GROUP BY a.g WITHOUT SOURCE INDEX \
         SELECT COUNT(*) AS n",
    )
    .unwrap();
    let join = join_of(&def);
    assert_eq!((&join.source_alias, &join.right_alias), (&None, &None));
    assert!(def.without_source_index);
    for text in [
        "TRANSFORM t FROM s GROUP BY g SELECT g AS g",
        "TRANSFORM t FROM s GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g",
        "TRANSFORM t FROM s SELECT a AS a",
    ] {
        assert!(parse(text).is_ok(), "{text}");
    }
    // `WITHOUT` straight after a 1-1 source is still the clause, refused
    // for a 1-1 target, not an alias.
    match parse("TRANSFORM t FROM s WITHOUT SOURCE INDEX SELECT a AS a").unwrap_err() {
        ParseError::UnsupportedKeySpace { construct, .. } => {
            assert_eq!(construct, "WITHOUT SOURCE INDEX")
        }
        other => panic!("{other:?}"),
    }
}

fn alias_refusal(text: &str) -> (String, String) {
    match parse(text) {
        Err(ParseError::TableAlias { alias, detail }) => (alias, detail),
        other => panic!("{text}: expected a TableAlias refusal, got {other:?}"),
    }
}

/// After `AS`, a word Postgres reserves is refused (Postgres: a syntax error).
#[test]
fn a_reserved_word_is_refused_as_an_alias() {
    for word in ["select", "LEFT", "join", "on", "group", "order", "outer"] {
        let (alias, detail) = alias_refusal(&format!(
            "TRANSFORM t FROM a AS {word} JOIN b ON a.k = b.k SELECT a.x AS x"
        ));
        assert_eq!(alias, word);
        assert!(detail.contains("reserves"), "{detail}");
        let (alias, _) = alias_refusal(&format!(
            "TRANSFORM t FROM a JOIN b AS {word} ON a.k = b.k SELECT a.x AS x"
        ));
        assert_eq!(alias, word);
    }
}

/// Two aliases alike, or an alias equal to the other table's name, would
/// leave a qualifier naming both sides (Postgres: "table name specified more
/// than once"). Case doesn't tell them apart.
#[test]
fn an_alias_naming_the_other_side_is_refused() {
    for (text, expected) in [
        (
            "TRANSFORM t FROM a AS x JOIN b AS x ON x.k = x.k SELECT 1 AS one",
            "x",
        ),
        (
            "TRANSFORM t FROM a AS X JOIN b AS x ON a.k = b.k SELECT 1 AS one",
            "x",
        ),
        (
            "TRANSFORM t FROM a AS b JOIN b ON b.k = b.k SELECT 1 AS one",
            "b",
        ),
        (
            "TRANSFORM t FROM a JOIN b AS a ON a.k = a.k SELECT 1 AS one",
            "a",
        ),
        (
            "TRANSFORM t FROM a JOIN b AS A ON a.k = b.k SELECT 1 AS one",
            "A",
        ),
    ] {
        let (alias, detail) = alias_refusal(text);
        assert_eq!(alias, expected, "{text}");
        assert!(detail.contains("either"), "{detail}");
    }
}

/// An alias hides its table's name: a column qualified with the table's
/// name, with or without its schema and in any clause, is refused, naming
/// the alias to write (Postgres: "missing FROM-clause entry" or "invalid
/// reference to FROM-clause entry").
#[test]
fn a_table_name_after_its_side_was_aliased_is_refused() {
    for (text, reference) in [
        (
            "TRANSFORM t FROM orders o JOIN c ON orders.k = c.k SELECT o.x AS x",
            "orders.k",
        ),
        (
            "TRANSFORM t FROM orders o JOIN c ON o.k = c.k GROUP BY Orders.g SELECT COUNT(*) AS n",
            "Orders.g",
        ),
        (
            "TRANSFORM t FROM orders o JOIN c ON o.k = c.k SELECT COALESCE(1, orders.x) AS x",
            "orders.x",
        ),
        (
            "TRANSFORM t FROM shop.orders AS orders JOIN c ON orders.k = c.k \
             SELECT shop.orders.x AS x",
            "shop.orders.x",
        ),
        (
            "TRANSFORM t FROM a JOIN shop.orders o ON a.k = o.k SELECT shop.orders.x AS x",
            "shop.orders.x",
        ),
    ] {
        match parse(text) {
            Err(err @ ParseError::AliasedTableName { .. }) => {
                let ParseError::AliasedTableName {
                    reference: found, ..
                } = &err
                else {
                    unreachable!()
                };
                assert_eq!(found, reference, "{text}");
                assert!(
                    err.to_string().contains("qualify the column with '"),
                    "{err}"
                );
            }
            other => panic!("{text}: expected AliasedTableName, got {other:?}"),
        }
    }
}

/// A schema before an alias names no side (Postgres: "invalid reference to
/// FROM-clause entry"): the qualifier is left as written, and the validator
/// refuses it as an unknown table.
#[test]
fn a_schema_qualified_alias_is_left_for_the_validator() {
    let def =
        parse("TRANSFORM t FROM shop.orders o JOIN c ON o.k = c.k SELECT shop.o.x AS x").unwrap();
    assert_eq!(
        def.fields[0].expr,
        Expr::SourceColumn {
            schema: Some("shop".to_string()),
            table: "o".to_string(),
            column: "x".to_string(),
        }
    );
    assert_eq!(
        join_of(&def).on[0].lhs,
        QualifiedColumn {
            schema: None,
            table: "orders".to_string(),
            column: "k".to_string(),
        }
    );
}

/// A single-table definition writes its columns bare, so an alias there is
/// refused rather than ignored.
#[test]
fn an_alias_on_a_single_table_definition_is_refused() {
    for text in [
        "TRANSFORM t FROM s AS x SELECT x.a AS a",
        "TRANSFORM t FROM s x SELECT a AS a",
        "TRANSFORM t FROM app.s x GROUP BY x.g SELECT COUNT(*) AS n",
    ] {
        let (alias, detail) = alias_refusal(text);
        assert_eq!(alias, "x", "{text}");
        assert!(detail.contains("single-table"), "{detail}");
    }
}

/// An alias followed by something no definition admits is reported as what
/// it is, never as an alias on a single-table definition: a comma join, or a
/// syntax error.
#[test]
fn text_after_an_alias_that_no_definition_admits_is_not_blamed_on_the_alias() {
    match parse("TRANSFORM t FROM a o, b SELECT o.x AS x").unwrap_err() {
        ParseError::UnsupportedKeySpace { construct, .. } => assert_eq!(construct, "comma join"),
        other => panic!("{other:?}"),
    }
    for text in [
        "TRANSFORM t FROM a AS o AS p JOIN b ON o.k = b.k SELECT o.x AS x",
        "TRANSFORM t FROM a o (c1) JOIN b ON o.k = b.k SELECT o.x AS x",
        "TRANSFORM t FROM a AS o.x JOIN b ON o.k = b.k SELECT o.x AS x",
        "TRANSFORM t FROM a o JION b ON o.k = b.k SELECT o.x AS x",
    ] {
        match parse(text).unwrap_err() {
            ParseError::UnexpectedToken { .. } => {}
            other => panic!("{text}: {other:?}"),
        }
    }
}

/// Two aliases over one table are still a self-join, refused saying why:
/// one captured change would feed both sides.
#[test]
fn a_self_join_through_two_aliases_is_refused() {
    for text in [
        "TRANSFORM t FROM orders a JOIN orders b ON a.id = b.parent SELECT a.x AS x",
        "TRANSFORM t FROM orders JOIN orders ON orders.id = orders.parent SELECT 1 AS x",
    ] {
        match parse(text).unwrap_err() {
            ParseError::UnsupportedKeySpace { construct, detail } => {
                assert_eq!(construct, "JOIN");
                assert!(detail.contains("self-join"), "{detail}");
                assert!(detail.contains("one captured change"), "{detail}");
            }
            other => panic!("{text}: {other:?}"),
        }
    }
}
