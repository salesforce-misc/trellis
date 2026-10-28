//! Transform-definition grammar and parser.
//!
//! Parses the concrete syntax documented in [`parser`] into the typed
//! [`ast::TransformDef`] the validator and the SQL renderer
//! consume. Per ADR-0004, only constructs the renderer can express in
//! Postgres SQL are accepted:
//!
//! - **Key-space**: 1-1 ([`ast::KeySpace::OneToOne`]), aggregate
//!   ([`ast::KeySpace::Aggregate`], a `GROUP BY <cols>` clause), or a `JOIN`
//!   aggregate ([`ast::KeySpace::Join`], ADR-0019).
//! - **Calculated fields**: column references, numeric and string literals,
//!   the `+` operator (Numeric-only), the `>` comparison operator
//!   (`Numeric, Numeric -> Boolean`), and `name(args)` function calls,
//!   all via the shared [`registry`]. Values carry a [`ast::ValueType`]
//!   (`Numeric`/`Text`/`Boolean`). General
//!   function-call syntax comes with four Text-argument functions
//!   (`strpos`, `octet_length`, `char_length`, `regexp_count`). In an
//!   aggregate definition, a non-grouping-key column must be wrapped in
//!   exactly one of `SUM`/`MIN`/`MAX`/`AVG` (Numeric-only), or a field may be
//!   the row-counting `COUNT(*)` (no column argument; `COUNT(<column>)` is
//!   not implemented). **Typed literals** — `DATE
//!   '2024-01-01'` / `CAST('2024-01-01' AS date)` — are the one way an
//!   expression can *produce* a [`ast::ValueType::Other`] value rather than
//!   merely pass one through; see [`typed_literal`].
//! - **Partial-data predicate**: a trivially-true predicate only.
//!
//! Everything else is rejected at parse time with an error naming the
//! specific unsupported construct (see [`error::ParseError`]).

pub mod alias_readers;
pub mod ast;
pub mod backfill;
pub mod catalog;
pub mod chunk_queue;
pub(crate) mod copies;
pub(crate) mod copy_type;
pub mod ddl;
pub mod error;
pub mod grammar;
pub mod hierarchy;
pub(crate) mod join;
pub(crate) mod join_key_types;
pub(crate) mod key_types;
pub mod ledger;
mod lexer;
pub mod lifecycle;
pub mod model;
pub mod oracle;
mod parser;
pub mod partition_tree;
pub mod pg_type;
pub mod registry;
pub mod row_security;
pub mod source_index;
pub mod subscription;
pub mod typed_literal;
pub mod validate;

// This module is tier 3 (`pub(crate)`, ADR-0012), so these flattened
// re-exports are a convenience for the engine itself and for the two gated
// doors onto it. Names the engine does not use are split out below and
// compiled only behind those gates, which is what keeps a plain
// `cargo build` free of `unused_imports` rather than an `allow`.
pub use ast::{AlterTransform, DefinitionRef, Statement, StatementKind, TransformRef, ValueType};
pub use catalog::{CatalogError, alter_transform, install_definition, tables_to_capture};
pub use error::ParseError;
pub use model::TransformStatus;
pub use parser::{MAX_EXPRESSION_DEPTH, parse_statement, statement_kind};
// Stored text loads through `grammar::load`; the integration tests parse
// fresh text directly.
#[cfg(any(test, feature = "internals", feature = "test-util"))]
pub use parser::parse;
pub use pg_type::PgType;
// The crate validates through `validate::validate`; the integration tests
// reach it here.
#[cfg(any(test, feature = "internals"))]
pub use validate::validate;
#[cfg(any(test, feature = "internals"))]
pub use validate::{JoinColumnInfo, validate_join};

// Reached from `crate::dev` (ADR-0012's sanctioned exception) by `generative`
// and `benchmark`.
#[cfg(any(test, feature = "test-util"))]
pub use catalog::create_definition_without_backfill;
#[cfg(any(test, feature = "test-util"))]
pub use ddl::{
    DdlError, create_aggregate_target_table, create_target_table, qualified_target_table,
    source_primary_key,
};
#[cfg(any(test, feature = "internals"))]
pub use oracle::{render_aggregate_select_sql, render_expr_sql, render_one_to_one_select_sql};

// Reached only by this crate's own `tests/*.rs`, through the `internals`
// feature (ADR-0012; see `Cargo.toml`). Not part of `dev`.
#[cfg(any(test, feature = "internals"))]
pub use ast::{
    Expr, FieldDef, JoinKind, JoinOn, JoinSpec, KeySpace, Operator, Predicate, QualifiedColumn,
    TransformDef,
};
#[cfg(any(test, feature = "internals"))]
pub use backfill::BackfillError;
#[cfg(any(test, feature = "internals"))]
pub use catalog::all_source_tables;
#[cfg(any(test, feature = "internals"))]
pub use catalog::{
    create_definition, dependents_of, edges_from, node_for_table, persist_edge, resolve_node,
    source_table_version, transforms_for_source,
};
#[cfg(any(test, feature = "internals"))]
pub use ddl::{PrimaryKeyColumn, neighbor_table_name};
#[cfg(any(test, feature = "internals"))]
#[cfg(any(test, feature = "internals"))]
pub use model::{EdgeKind, NodeKind, SchemaEdge, SchemaNode};
#[cfg(any(test, feature = "internals"))]
pub use validate::{
    KeyColumnRole, NondeterministicCollationTextFunction, NondeterministicKeyCollation,
    ValidationError,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_binary_add_expression() {
        let def = parse("TRANSFORM order_totals FROM orders SELECT a + b AS total").unwrap();

        assert_eq!(def.target, "order_totals");
        assert_eq!(def.source, "orders");
        assert_eq!(def.key_space, KeySpace::OneToOne);
        assert_eq!(def.predicate, Predicate::True);
        assert_eq!(
            def.fields,
            vec![FieldDef {
                name: "total".to_string(),
                expr: Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(Expr::Column("a".to_string())),
                    rhs: Box::new(Expr::Column("b".to_string())),
                },
            }]
        );
    }

    #[test]
    fn parses_numeric_literals_and_multiple_fields() {
        let def = parse("TRANSFORM t FROM s SELECT 1 + 2 AS literal_sum, price + 5 AS with_column")
            .unwrap();

        assert_eq!(
            def.fields,
            vec![
                FieldDef {
                    name: "literal_sum".to_string(),
                    expr: Expr::BinaryOp {
                        op: Operator::Add,
                        lhs: Box::new(Expr::NumberLiteral("1".to_string())),
                        rhs: Box::new(Expr::NumberLiteral("2".to_string())),
                    },
                },
                FieldDef {
                    name: "with_column".to_string(),
                    expr: Expr::BinaryOp {
                        op: Operator::Add,
                        lhs: Box::new(Expr::Column("price".to_string())),
                        rhs: Box::new(Expr::NumberLiteral("5".to_string())),
                    },
                },
            ]
        );
    }

    #[test]
    fn parses_a_decimal_literal() {
        let def = parse("TRANSFORM t FROM s SELECT price + 0.5 AS total").unwrap();
        assert_eq!(
            def.fields[0].expr,
            Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("price".to_string())),
                rhs: Box::new(Expr::NumberLiteral("0.5".to_string())),
            }
        );
    }

    #[test]
    fn accepts_an_explicit_true_predicate() {
        let def = parse("TRANSFORM t FROM s SELECT a AS x WHERE TRUE").unwrap();
        assert_eq!(def.predicate, Predicate::True);
    }

    #[test]
    fn parses_a_single_column_group_by_into_aggregate_key_space() {
        let def = parse(
            "TRANSFORM order_totals FROM order_line_items GROUP BY order_id \
             SELECT order_id AS order_id, SUM(amount) AS total_amount",
        )
        .unwrap();
        assert_eq!(
            def.key_space,
            KeySpace::Aggregate {
                group_by: vec!["order_id".to_string()]
            }
        );
        assert_eq!(
            def.fields[1].expr,
            Expr::FunctionCall {
                name: "SUM".to_string(),
                args: vec![Expr::Column("amount".to_string())],
            }
        );
    }

    #[test]
    fn parses_a_multi_column_group_by() {
        let def = parse("TRANSFORM t FROM s GROUP BY a, b SELECT a AS a, b AS b, SUM(c) AS total")
            .unwrap();
        assert_eq!(
            def.key_space,
            KeySpace::Aggregate {
                group_by: vec!["a".to_string(), "b".to_string()]
            }
        );
    }

    #[test]
    fn parses_min_max_avg_calls_in_an_aggregate_definition() {
        let def = parse(
            "TRANSFORM t FROM s GROUP BY id \
             SELECT MIN(x) AS lo, MAX(x) AS hi, AVG(x) AS avg_x",
        )
        .unwrap();
        assert_eq!(
            def.fields
                .iter()
                .map(|f| match &f.expr {
                    Expr::FunctionCall { name, .. } => name.as_str(),
                    _ => panic!("expected FunctionCall"),
                })
                .collect::<Vec<_>>(),
            vec!["MIN", "MAX", "AVG"]
        );
    }

    #[test]
    fn rejects_a_bare_non_grouping_column_reference_at_parse_time_is_not_the_job_here() {
        // Bare non-grouping-key column references parse successfully (the
        // parser has no source-column knowledge to reject them with) and are
        // caught by the validator instead — see
        // `validate::tests::rejects_a_bare_non_grouping_column_reference`.
        let def = parse("TRANSFORM t FROM s GROUP BY a SELECT b AS x").unwrap();
        assert_eq!(def.fields[0].expr, Expr::Column("b".to_string()));
    }

    #[test]
    fn parses_count_star_in_an_aggregate_definition() {
        let def = parse("TRANSFORM t FROM s GROUP BY a SELECT COUNT(*) AS x").unwrap();
        assert_eq!(
            def.fields[0].expr,
            Expr::FunctionCall {
                name: "COUNT".to_string(),
                args: Vec::new(),
            }
        );
    }

    /// `COUNT(<column>)` — counting non-null occurrences of a
    /// specific column, a different semantic from `COUNT(*)` — parses
    /// successfully in an aggregate definition.
    #[test]
    fn parses_count_of_a_column_in_an_aggregate_definition() {
        let def = parse("TRANSFORM t FROM s GROUP BY a SELECT COUNT(b) AS x").unwrap();
        assert_eq!(
            def.fields[0].expr,
            Expr::FunctionCall {
                name: "COUNT".to_string(),
                args: vec![Expr::Column("b".to_string())],
            }
        );
    }

    /// `COUNT` with more than one argument is a plain arity error, the same
    /// as any other registered function — Postgres itself has no `count(a,
    /// b)`.
    #[test]
    fn rejects_count_with_more_than_one_argument() {
        let err = parse("TRANSFORM t FROM s GROUP BY a SELECT COUNT(a, b) AS x").unwrap_err();
        match err {
            ParseError::FunctionArityMismatch {
                name,
                expected,
                found,
            } => {
                assert_eq!(name, "COUNT");
                assert_eq!(expected, 1);
                assert_eq!(found, 2);
            }
            other => panic!("expected FunctionArityMismatch, got {other:?}"),
        }
    }

    #[test]
    fn rejects_count_star_outside_an_aggregate_definition_too() {
        let err = parse("TRANSFORM t FROM s SELECT COUNT(*) AS x").unwrap_err();
        match err {
            ParseError::UnsupportedKeySpace { construct, .. } => {
                assert_eq!(construct, "COUNT(...)");
            }
            other => panic!("expected UnsupportedKeySpace, got {other:?}"),
        }
    }

    const JOIN_HEAD: &str = "TRANSFORM t FROM orders JOIN items ON orders.id = items.order_id";

    fn join_spec(text: &str) -> ast::JoinSpec {
        match parse(text).unwrap().key_space {
            KeySpace::Join(spec) => spec,
            other => panic!("expected a join, got {other:?}"),
        }
    }

    fn refusal(text: &str) -> (String, String) {
        match parse(text).unwrap_err() {
            ParseError::UnsupportedKeySpace { construct, detail } => (construct, detail),
            other => panic!("expected UnsupportedKeySpace, got {other:?}"),
        }
    }

    #[test]
    fn parses_an_inner_join_aggregate() {
        let def = parse(&format!(
            "{JOIN_HEAD} GROUP BY orders.region, items.sku \
             SELECT orders.region AS region, items.sku AS sku, \
             COUNT(*) AS n, SUM(items.qty) AS qty, AVG(orders.total) AS avg_total"
        ))
        .unwrap();
        let KeySpace::Join(spec) = &def.key_space else {
            panic!("expected a join");
        };
        assert_eq!(spec.kind, ast::JoinKind::Inner);
        assert_eq!(spec.right, "items");
        assert_eq!(spec.on.len(), 1);
        assert_eq!(spec.on[0].lhs.table, "orders");
        assert_eq!(spec.on[0].rhs.column, "order_id");
        assert_eq!(spec.group_by.len(), 2);
        assert_eq!(spec.group_by[1].table, "items");
        assert_eq!(
            def.fields[3].expr,
            Expr::FunctionCall {
                name: "SUM".to_string(),
                args: vec![Expr::SourceColumn {
                    schema: None,
                    table: "items".to_string(),
                    column: "qty".to_string(),
                }],
            }
        );
    }

    #[test]
    fn join_kind_spellings() {
        let kind = |head: &str| {
            join_spec(&format!(
                "TRANSFORM t FROM a {head} b ON a.k = b.k GROUP BY a.g SELECT COUNT(*) AS n"
            ))
            .kind
        };
        assert_eq!(kind("JOIN"), ast::JoinKind::Inner);
        assert_eq!(kind("INNER JOIN"), ast::JoinKind::Inner);
        assert_eq!(kind("left join"), ast::JoinKind::Left);
        assert_eq!(kind("LEFT OUTER JOIN"), ast::JoinKind::Left);
    }

    #[test]
    fn join_on_admits_and_of_equalities_in_either_order() {
        let spec = join_spec(
            "TRANSFORM t FROM a JOIN s.b ON b.k2 = a.k2 AND a.k = b.k \
             GROUP BY a.g SELECT COUNT(*) AS n",
        );
        assert_eq!(spec.explicit_right_schema.as_deref(), Some("s"));
        assert_eq!(spec.on.len(), 2);
        assert_eq!(spec.on[0].lhs.table, "b");
    }

    #[test]
    fn rejects_join_kinds_other_than_inner_and_left() {
        for kind in ["RIGHT", "FULL", "CROSS", "NATURAL"] {
            let (construct, _) = refusal(&format!(
                "TRANSFORM t FROM a {kind} JOIN b ON a.k = b.k GROUP BY a.g SELECT COUNT(*) AS n"
            ));
            assert_eq!(construct, format!("{kind} JOIN"));
        }
    }

    #[test]
    fn rejects_join_on_shapes_beyond_a_conjunction_of_equalities() {
        for on in [
            "a.k > b.k",
            "a.k = b.k OR a.j = b.j",
            "a.k = 5",
            "a.k = b.k AND b.x > 3",
            "k = b.k",
            "a.k + 1 = b.k",
        ] {
            let (construct, _) = refusal(&format!(
                "TRANSFORM t FROM a JOIN b ON {on} GROUP BY a.g SELECT COUNT(*) AS n"
            ));
            assert_eq!(construct, "JOIN ON", "{on}");
        }
    }

    #[test]
    fn rejects_a_self_join_and_a_join_without_group_by() {
        let (construct, detail) =
            refusal("TRANSFORM t FROM a JOIN a ON a.k = a.j GROUP BY a.g SELECT COUNT(*) AS n");
        assert_eq!(construct, "JOIN");
        assert!(detail.contains("itself"), "{detail}");
        let (construct, detail) =
            refusal("TRANSFORM t FROM s JOIN other ON s.id = other.id SELECT a AS x");
        assert_eq!(construct, "JOIN");
        assert!(detail.contains("GROUP BY"), "{detail}");
    }

    #[test]
    fn rejects_comma_joins_using_and_tables_sharing_a_bare_name() {
        let (construct, _) = refusal("TRANSFORM t FROM a, b GROUP BY a.g SELECT COUNT(*) AS n");
        assert_eq!(construct, "comma join");
        let (construct, _) =
            refusal("TRANSFORM t FROM a JOIN b USING (k) GROUP BY a.g SELECT COUNT(*) AS n");
        assert_eq!(construct, "JOIN USING");
        let (construct, detail) = refusal(
            "TRANSFORM t FROM shop.orders JOIN archive.orders ON orders.id = orders.id \
             GROUP BY orders.g SELECT COUNT(*) AS n",
        );
        assert_eq!(construct, "JOIN");
        assert!(detail.contains("bare name"), "{detail}");
        let (_, detail) = refusal(
            "TRANSFORM t FROM a RIGHT JOIN b ON a.k = b.k GROUP BY a.g SELECT COUNT(*) AS n",
        );
        assert!(detail.contains("swap the two tables"), "{detail}");
    }

    #[test]
    fn rejects_a_second_join() {
        let (construct, detail) = refusal(
            "TRANSFORM t FROM a JOIN b ON a.k = b.k LEFT JOIN c ON b.j = c.j \
             GROUP BY a.g SELECT COUNT(*) AS n",
        );
        assert_eq!(construct, "JOIN");
        assert!(detail.contains("exactly two tables"), "{detail}");
    }

    #[test]
    fn join_group_by_keys_must_be_qualified() {
        let (construct, _) =
            refusal("TRANSFORM t FROM a JOIN b ON a.k = b.k GROUP BY g SELECT COUNT(*) AS n");
        assert_eq!(construct, "JOIN GROUP BY");
    }

    #[test]
    fn deferred_aggregate_families_parse_inside_a_join() {
        for call in [
            "MIN(a.x)",
            "BOOL_AND(a.x)",
            "bit_or(b.x)",
            "ARRAY_AGG(a.x)",
            "STRING_AGG(a.x, ',')",
            "JSONB_AGG(a.x)",
        ] {
            let def = parse(&format!(
                "TRANSFORM t FROM a JOIN b ON a.k = b.k GROUP BY a.g SELECT {call} AS v"
            ))
            .unwrap();
            assert!(
                matches!(def.fields[0].expr, Expr::FunctionCall { .. }),
                "{call}"
            );
        }
    }

    #[test]
    fn rejects_group_by_trailing_after_select() {
        let err = parse("TRANSFORM t FROM s SELECT a AS x GROUP BY b").unwrap_err();
        match &err {
            ParseError::UnsupportedKeySpace { construct, .. } => {
                assert_eq!(construct, "GROUP BY");
            }
            other => panic!("expected UnsupportedKeySpace, got {other:?}"),
        }
        assert!(err.to_string().contains("GROUP BY"));
        assert!(err.to_string().contains("FROM <source>"));
    }

    #[test]
    fn rejects_join_trailing_after_select() {
        let err =
            parse("TRANSFORM t FROM s SELECT a AS x JOIN other ON s.id = other.id").unwrap_err();
        match err {
            ParseError::UnsupportedKeySpace { construct, .. } => {
                assert_eq!(construct, "JOIN");
            }
            other => panic!("expected UnsupportedKeySpace, got {other:?}"),
        }
    }

    /// Every two-part name in an expression parses to a qualified column; the
    /// validator decides whether its head names the FROM table.
    #[test]
    fn a_two_part_name_of_another_table_parses_as_a_qualified_column() {
        let def = parse("TRANSFORM t FROM s SELECT product.category_name AS x").unwrap();
        assert_eq!(
            def.fields[0].expr,
            source_column(None, "product", "category_name")
        );
    }

    #[test]
    fn rejects_non_plus_operator() {
        let err = parse("TRANSFORM t FROM s SELECT a - b AS x").unwrap_err();
        match err {
            ParseError::UnsupportedOperator { operator } => assert_eq!(operator, "-"),
            other => panic!("expected UnsupportedOperator, got {other:?}"),
        }
    }

    #[test]
    fn rejects_less_than_operator_as_unsupported_operator() {
        let err = parse("TRANSFORM t FROM s SELECT a < b AS x").unwrap_err();
        match err {
            ParseError::UnsupportedOperator { operator } => assert_eq!(operator, "<"),
            other => panic!("expected UnsupportedOperator, got {other:?}"),
        }
    }

    #[test]
    fn rejects_equals_operator_as_unsupported_operator() {
        let err = parse("TRANSFORM t FROM s SELECT a = b AS x").unwrap_err();
        match err {
            ParseError::UnsupportedOperator { operator } => assert_eq!(operator, "="),
            other => panic!("expected UnsupportedOperator, got {other:?}"),
        }
    }

    /// A two-character comparison is named whole: `>=` is not the supported
    /// `>` followed by a stray `=`, and `<=` is not `<`.
    #[test]
    fn rejects_a_two_character_comparison_by_its_whole_symbol() {
        for symbol in [">=", "<=", "<>"] {
            let err = parse(&format!("TRANSFORM t FROM s SELECT a {symbol} b AS x")).unwrap_err();
            match err {
                ParseError::UnsupportedOperator { operator } => assert_eq!(operator, symbol),
                other => panic!("`{symbol}`: expected UnsupportedOperator, got {other:?}"),
            }
        }
    }

    #[test]
    fn rejects_aggregate_function_call_with_key_space_specific_message() {
        let err = parse("TRANSFORM t FROM s SELECT SUM(a) AS x").unwrap_err();
        match err {
            ParseError::UnsupportedKeySpace { construct, .. } => {
                assert_eq!(construct, "SUM(...)");
            }
            other => panic!("expected UnsupportedKeySpace, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unregistered_function_call() {
        // Rejected via a `registry::lookup_function` lookup, not a
        // hardcoded parser branch — see ADR-0004's registry-sharing goal.
        assert!(registry::lookup_function("ROUND").is_none());
        let err = parse("TRANSFORM t FROM s SELECT ROUND(a) AS x").unwrap_err();
        match err {
            ParseError::UnsupportedFunction { name } => assert_eq!(name, "ROUND"),
            other => panic!("expected UnsupportedFunction, got {other:?}"),
        }
    }

    #[test]
    fn rejects_non_immutable_construct() {
        let err = parse("TRANSFORM t FROM s SELECT a + NOW() AS x").unwrap_err();
        match err {
            ParseError::NonImmutableConstruct { name } => assert_eq!(name, "NOW"),
            other => panic!("expected NonImmutableConstruct, got {other:?}"),
        }
    }

    #[test]
    fn parses_a_registered_function_call() {
        let def = parse("TRANSFORM t FROM s SELECT octet_length(name) AS len").unwrap();
        assert_eq!(
            def.fields,
            vec![FieldDef {
                name: "len".to_string(),
                expr: Expr::FunctionCall {
                    name: "OCTET_LENGTH".to_string(),
                    args: vec![Expr::Column("name".to_string())],
                },
            }]
        );
    }

    #[test]
    fn parses_a_two_argument_function_call() {
        let def = parse("TRANSFORM t FROM s SELECT strpos(name, 'x') AS pos").unwrap();
        assert_eq!(
            def.fields,
            vec![FieldDef {
                name: "pos".to_string(),
                expr: Expr::FunctionCall {
                    name: "STRPOS".to_string(),
                    args: vec![
                        Expr::Column("name".to_string()),
                        Expr::StringLiteral("x".to_string()),
                    ],
                },
            }]
        );
    }

    #[test]
    fn rejects_function_call_with_wrong_arity() {
        let err = parse("TRANSFORM t FROM s SELECT octet_length(a, b) AS x").unwrap_err();
        match err {
            ParseError::FunctionArityMismatch {
                name,
                expected,
                found,
            } => {
                assert_eq!(name, "octet_length");
                assert_eq!(expected, 1);
                assert_eq!(found, 2);
            }
            other => panic!("expected FunctionArityMismatch, got {other:?}"),
        }
    }

    #[test]
    fn parses_a_variadic_coalesce_call() {
        // COALESCE is variadic (unlike the fixed-arity registry functions),
        // so it isn't looked up in `registry::FUNCTIONS`; the parser accepts
        // any argument count >= 1, matching Postgres.
        let def = parse("TRANSFORM t FROM s SELECT COALESCE(a, b, 0) AS x").unwrap();
        assert_eq!(
            def.fields[0].expr,
            Expr::FunctionCall {
                name: "COALESCE".to_string(),
                args: vec![
                    Expr::Column("a".to_string()),
                    Expr::Column("b".to_string()),
                    Expr::NumberLiteral("0".to_string()),
                ],
            }
        );
    }

    #[test]
    fn parses_a_single_argument_coalesce_call() {
        // Postgres allows a one-argument COALESCE (it simply returns that
        // argument); the parser's arity floor is 1, so this is accepted.
        let def = parse("TRANSFORM t FROM s SELECT COALESCE(a) AS x").unwrap();
        assert_eq!(
            def.fields[0].expr,
            Expr::FunctionCall {
                name: "COALESCE".to_string(),
                args: vec![Expr::Column("a".to_string())],
            }
        );
    }

    #[test]
    fn rejects_coalesce_with_no_arguments() {
        // Postgres rejects `COALESCE()` as a syntax error; we reject it with a
        // dedicated at-least-one-argument message.
        let err = parse("TRANSFORM t FROM s SELECT COALESCE() AS x").unwrap_err();
        match err {
            ParseError::AtLeastOneArgumentRequired { name } => assert_eq!(name, "COALESCE"),
            other => panic!("expected AtLeastOneArgumentRequired, got {other:?}"),
        }
    }

    #[test]
    fn coalesce_with_a_null_literal_argument_is_unsupported() {
        // DIVERGENCE from Postgres (tracked toward full compatibility):
        // Postgres's most idiomatic COALESCE form takes a bare `NULL` literal
        // (e.g. `COALESCE(NULL, default)`), but this grammar has no NULL
        // literal (`ast.rs` has no such `Expr`/`Value` variant). `NULL` is
        // therefore parsed as an ordinary column reference and rejected at
        // validation as an unresolved column, rather than behaving as SQL
        // NULL.
        let def = parse("TRANSFORM t FROM s SELECT COALESCE(a, NULL) AS x").unwrap();
        assert_eq!(
            def.fields[0].expr,
            Expr::FunctionCall {
                name: "COALESCE".to_string(),
                args: vec![
                    Expr::Column("a".to_string()),
                    Expr::Column("NULL".to_string()),
                ],
            }
        );
        let source_columns =
            std::collections::HashMap::from([("a".to_string(), ValueType::Numeric)]);
        let err = validate(&def, &source_columns).unwrap_err();
        assert_eq!(
            err,
            ValidationError::UnresolvedColumn {
                field: "x".to_string(),
                column: "NULL".to_string(),
            }
        );
    }

    #[test]
    fn parses_a_greater_than_expression() {
        let def = parse("TRANSFORM t FROM s SELECT a > 0 AS positive").unwrap();
        assert_eq!(
            def.fields,
            vec![FieldDef {
                name: "positive".to_string(),
                expr: Expr::BinaryOp {
                    op: Operator::GreaterThan,
                    lhs: Box::new(Expr::Column("a".to_string())),
                    rhs: Box::new(Expr::NumberLiteral("0".to_string())),
                },
            }]
        );
    }

    /// Permanent regression pin for real operator precedence
    /// (see [`registry::OPERATORS`]'s doc comment): `a > b + c` must parse as
    /// `a > (b + c)`, since `+` ([`registry::precedence::ADDITIVE`]) binds
    /// tighter than `>` ([`registry::precedence::COMPARISON`]), matching
    /// Postgres's own grouping — not a flat left-to-right parse that
    /// would build `(a > b) + c` here. With real precedence, the types line
    /// up correctly by construction and `validate` accepts the expression.
    #[test]
    fn mixed_operators_respect_precedence() {
        let def = parse("TRANSFORM t FROM s SELECT a > b + c AS result").unwrap();
        assert_eq!(
            def.fields[0].expr,
            Expr::BinaryOp {
                op: Operator::GreaterThan,
                lhs: Box::new(Expr::Column("a".to_string())),
                rhs: Box::new(Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(Expr::Column("b".to_string())),
                    rhs: Box::new(Expr::Column("c".to_string())),
                }),
            }
        );

        let source_columns: std::collections::HashMap<String, ValueType> = [
            ("a".to_string(), ValueType::Numeric),
            ("b".to_string(), ValueType::Numeric),
            ("c".to_string(), ValueType::Numeric),
        ]
        .into_iter()
        .collect();
        validate(&def, &source_columns)
            .expect("a > (b + c) type-checks cleanly under real precedence");
    }

    /// Same operators as [`mixed_operators_respect_precedence`], reordered
    /// so the additive operator comes first: `a + b > c` groups as
    /// `(a + b) > c` under real precedence too (`+` still binds tighter
    /// than `>`, and there's only one way to slot a comparison around an
    /// already-complete additive expression). This happens to be the same
    /// tree the old flat left-to-right parser also produced for this
    /// particular ordering — but now for the right reason, not by
    /// coincidence of parse order.
    #[test]
    fn additive_before_comparison_still_binds_additive_first() {
        let def = parse("TRANSFORM t FROM s SELECT a + b > c AS result").unwrap();
        assert_eq!(
            def.fields[0].expr,
            Expr::BinaryOp {
                op: Operator::GreaterThan,
                lhs: Box::new(Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(Expr::Column("a".to_string())),
                    rhs: Box::new(Expr::Column("b".to_string())),
                }),
                rhs: Box::new(Expr::Column("c".to_string())),
            }
        );

        let source_columns: std::collections::HashMap<String, ValueType> = [
            ("a".to_string(), ValueType::Numeric),
            ("b".to_string(), ValueType::Numeric),
            ("c".to_string(), ValueType::Numeric),
        ]
        .into_iter()
        .collect();
        validate(&def, &source_columns).expect("(a + b) > c type-checks cleanly");
    }

    /// Parenthesized grouping (needed once a caller
    /// composes `+` and `>` and wants a grouping the precedence table alone
    /// would never produce): `(a > b) + c` must override `+`'s tighter
    /// binding and group the comparison first, unlike the unparenthesized
    /// `a > b + c` (pinned by `mixed_operators_respect_precedence` above),
    /// which groups `+` first. This is also the exact shape
    /// `generative::backend::manual::render_expr`'s round-trip test
    /// (`backend::manual::tests::render_expr_parenthesizes_nested_binary_ops_so_they_round_trip`)
    /// depends on the parser being able to re-parse.
    #[test]
    fn parenthesized_grouping_overrides_precedence() {
        let def = parse("TRANSFORM t FROM s SELECT (a > b) + c AS result").unwrap();
        assert_eq!(
            def.fields[0].expr,
            Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::BinaryOp {
                    op: Operator::GreaterThan,
                    lhs: Box::new(Expr::Column("a".to_string())),
                    rhs: Box::new(Expr::Column("b".to_string())),
                }),
                rhs: Box::new(Expr::Column("c".to_string())),
            }
        );
    }

    /// A same-precedence chain (`+`, `+`) stays left-associative under real
    /// precedence, exactly as it did under the old flat parser: `a + b + c`
    /// parses as `(a + b) + c`, not `a + (b + c)`.
    #[test]
    fn same_precedence_chain_is_left_associative() {
        let def = parse("TRANSFORM t FROM s SELECT a + b + c AS result").unwrap();
        assert_eq!(
            def.fields[0].expr,
            Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(Expr::Column("a".to_string())),
                    rhs: Box::new(Expr::Column("b".to_string())),
                }),
                rhs: Box::new(Expr::Column("c".to_string())),
            }
        );
    }

    #[test]
    fn parses_a_function_call_composed_with_greater_than() {
        let def = parse("TRANSFORM t FROM s SELECT strpos(name, 'foo') > 0 AS has_foo").unwrap();
        assert_eq!(
            def.fields,
            vec![FieldDef {
                name: "has_foo".to_string(),
                expr: Expr::BinaryOp {
                    op: Operator::GreaterThan,
                    lhs: Box::new(Expr::FunctionCall {
                        name: "STRPOS".to_string(),
                        args: vec![
                            Expr::Column("name".to_string()),
                            Expr::StringLiteral("foo".to_string()),
                        ],
                    }),
                    rhs: Box::new(Expr::NumberLiteral("0".to_string())),
                },
            }]
        );
    }

    #[test]
    fn rejects_general_predicates() {
        let err = parse("TRANSFORM t FROM s SELECT a AS x WHERE a = b").unwrap_err();
        match err {
            ParseError::UnsupportedPredicate { detail } => {
                assert!(detail.contains("TRUE"));
            }
            other => panic!("expected UnsupportedPredicate, got {other:?}"),
        }
    }

    // ADR-0011: `TRANSFORM`/`FROM` accept an
    // explicit `schema.table` spelling, in addition to the bare `<table>`
    // form these tests exercised above.

    #[test]
    fn bare_source_and_target_still_parse_with_no_explicit_schema() {
        let def = parse("TRANSFORM order_totals FROM orders SELECT a + b AS total").unwrap();

        assert_eq!(def.target, "order_totals");
        assert_eq!(def.explicit_target_schema, None);
        assert_eq!(def.source, "orders");
        assert_eq!(def.explicit_source_schema, None);
    }

    #[test]
    fn parses_an_explicitly_qualified_source() {
        let def = parse("TRANSFORM order_totals FROM custom.orders SELECT a + b AS total").unwrap();

        assert_eq!(def.target, "order_totals");
        assert_eq!(def.explicit_target_schema, None);
        assert_eq!(def.source, "orders");
        assert_eq!(def.explicit_source_schema, Some("custom".to_string()));
    }

    #[test]
    fn parses_an_explicitly_qualified_target() {
        let def = parse("TRANSFORM custom.order_totals FROM orders SELECT a + b AS total").unwrap();

        assert_eq!(def.target, "order_totals");
        assert_eq!(def.explicit_target_schema, Some("custom".to_string()));
        assert_eq!(def.source, "orders");
        assert_eq!(def.explicit_source_schema, None);
    }

    #[test]
    fn parses_explicitly_qualified_source_and_target_together() {
        let def = parse("TRANSFORM reporting.order_totals FROM sales.orders SELECT a + b AS total")
            .unwrap();

        assert_eq!(def.target, "order_totals");
        assert_eq!(def.explicit_target_schema, Some("reporting".to_string()));
        assert_eq!(def.source, "orders");
        assert_eq!(def.explicit_source_schema, Some("sales".to_string()));
    }

    #[test]
    fn rejects_a_table_reference_with_a_trailing_dot() {
        let err = parse("TRANSFORM t FROM s.").unwrap_err();
        match err {
            ParseError::UnexpectedEof { expected } => assert_eq!(expected, "an identifier"),
            other => panic!("expected UnexpectedEof, got {other:?}"),
        }
    }

    #[test]
    fn rejects_a_table_reference_with_an_empty_schema_component() {
        let err = parse("TRANSFORM t FROM .orders SELECT a AS x").unwrap_err();
        match err {
            ParseError::UnexpectedToken { expected, found } => {
                assert_eq!(expected, "an identifier");
                assert_eq!(found, "'.'");
            }
            other => panic!("expected UnexpectedToken, got {other:?}"),
        }
    }

    #[test]
    fn rejects_a_table_reference_with_more_than_two_qualified_parts() {
        let err = parse("TRANSFORM t FROM a.b.c SELECT x AS y").unwrap_err();
        match err {
            ParseError::TooManyQualifiedNameParts { reference } => {
                assert_eq!(reference, "a.b.c");
            }
            other => panic!("expected TooManyQualifiedNameParts, got {other:?}"),
        }
    }

    #[test]
    fn rejects_a_target_reference_with_more_than_two_qualified_parts() {
        let err = parse("TRANSFORM a.b.c FROM s SELECT x AS y").unwrap_err();
        match err {
            ParseError::TooManyQualifiedNameParts { reference } => {
                assert_eq!(reference, "a.b.c");
            }
            other => panic!("expected TooManyQualifiedNameParts, got {other:?}"),
        }
    }

    #[test]
    fn qualified_table_reference_does_not_collide_with_qualified_column_syntax() {
        // A qualified column reuses the same `ident '.' ident` token shape
        // inside a field expression. The two never compete for the same
        // tokens, since the table-reference form is only ever parsed in
        // `TRANSFORM`/`FROM` position, well before `SELECT`'s field list.
        let def = parse("TRANSFORM t FROM custom.orders SELECT product.category_name AS category")
            .unwrap();

        assert_eq!(def.source, "orders");
        assert_eq!(def.explicit_source_schema, Some("custom".to_string()));
        assert_eq!(
            def.fields,
            vec![FieldDef {
                name: "category".to_string(),
                expr: source_column(None, "product", "category_name"),
            }]
        );
    }

    fn source_column(schema: Option<&str>, table: &str, column: &str) -> Expr {
        Expr::SourceColumn {
            schema: schema.map(str::to_string),
            table: table.to_string(),
            column: column.to_string(),
        }
    }

    /// `<FROM table>.<column>` is a qualified source column; so is
    /// any other two-part name, which the validator then refuses.
    #[test]
    fn a_two_part_name_headed_by_the_from_table_is_a_source_column() {
        let def =
            parse("TRANSFORM t FROM orders SELECT orders.price + product.cost AS margin").unwrap();
        assert_eq!(
            def.fields[0].expr,
            Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(source_column(None, "orders", "price")),
                rhs: Box::new(source_column(None, "product", "cost")),
            }
        );
        // The FROM table's bare name heads it even when FROM names a schema.
        let def = parse("TRANSFORM t FROM app.orders SELECT orders.price AS p").unwrap();
        assert_eq!(def.fields[0].expr, source_column(None, "orders", "price"));
    }

    /// A three-part name is always a qualified source column, inside an
    /// aggregate's argument too; the validator checks which table it names.
    #[test]
    fn a_three_part_name_is_a_source_column() {
        let def = parse(
            "TRANSFORM t FROM app.orders GROUP BY region SELECT region AS region, \
             SUM(app.orders.price) AS total, SUM(x.y.z) AS other",
        )
        .unwrap();
        assert_eq!(
            def.fields[1].expr,
            Expr::FunctionCall {
                name: "SUM".to_string(),
                args: vec![source_column(Some("app"), "orders", "price")],
            }
        );
        assert_eq!(
            def.fields[2].expr,
            Expr::FunctionCall {
                name: "SUM".to_string(),
                args: vec![source_column(Some("x"), "y", "z")],
            }
        );
    }

    #[test]
    fn a_four_part_name_is_rejected() {
        assert_eq!(
            parse("TRANSFORM t FROM orders SELECT a.b.c.d AS x"),
            Err(ParseError::TooManyColumnNameParts {
                reference: "a.b.c.d".to_string(),
            })
        );
    }

    /// A `GROUP BY` key qualified by the FROM table is the plain column; a key
    /// qualified by any other head is refused, naming the FROM table.
    #[test]
    fn a_group_by_key_qualified_by_the_from_table_is_the_column() {
        let def = parse(
            "TRANSFORM t FROM app.orders GROUP BY orders.region, kind \
             SELECT region AS region, COUNT(*) AS n",
        )
        .unwrap();
        assert_eq!(
            def.key_space,
            KeySpace::Aggregate {
                group_by: vec!["region".to_string(), "kind".to_string(),],
            }
        );
        let err = parse(
            "TRANSFORM t FROM orders GROUP BY orders.region, product.kind \
             SELECT region AS region, COUNT(*) AS n",
        )
        .unwrap_err();
        assert_eq!(
            err,
            ParseError::GroupByKeyNotFromSource {
                reference: "product.kind".to_string(),
                source: "orders".to_string(),
            }
        );
        assert_eq!(
            err.to_string(),
            "GROUP BY key 'product.kind' does not name this transform's FROM table ('orders'); \
             a qualified key must name the FROM table, and a second table is read with a JOIN"
        );
    }

    /// A rendered definition (what `ALTER TRANSFORM` persists) keeps a
    /// qualified name qualified, as written.
    #[test]
    fn a_qualified_name_round_trips_through_render() {
        for text in [
            "TRANSFORM t FROM orders GROUP BY region SELECT region AS region, \
             (region + 1) AS price, SUM(orders.price) AS total",
            "TRANSFORM t FROM app.orders SELECT app.orders.price AS p",
            "TRANSFORM t FROM orders JOIN items ON orders.id = items.order_id \
             GROUP BY orders.region SELECT orders.region AS region, SUM(items.qty) AS q",
            "TRANSFORM t FROM app.orders LEFT OUTER JOIN app.items ON items.order_id = orders.id \
             AND orders.k = items.k GROUP BY items.sku, app.orders.region \
             SELECT MIN(items.qty) AS m",
        ] {
            let def = parse(text).unwrap();
            assert_eq!(
                parse(&ast::render_definition_text(&def)).unwrap(),
                def,
                "{text}"
            );
        }
    }

    /// An `ALTER TRANSFORM` clause is parsed without its source, into the same
    /// qualified columns a `TRANSFORM` statement's fields parse to.
    #[test]
    fn an_alter_clause_parses_two_part_names_as_qualified_columns() {
        let Statement::AlterTransform(alter) =
            parse_statement("ALTER TRANSFORM t ADD orders.price + product.cost AS margin").unwrap()
        else {
            panic!("not an ALTER");
        };
        let ast::AlterClause::Add(field) = alter.clauses[0].clone() else {
            panic!("not an ADD");
        };
        assert_eq!(
            field.expr,
            Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(source_column(None, "orders", "price")),
                rhs: Box::new(source_column(None, "product", "cost")),
            }
        );
    }
}

/// Grammar coverage for [`parse_statement`], the unified statement entry point
/// [`crate::Trellis::apply`] dispatches on. Kept apart from the `tests` module
/// above, which is about the expression/definition grammar `parse` covers, because
/// what's under test here is *which statement* a text is — the dispatch decision
/// itself.
#[cfg(test)]
mod statement_grammar_tests {
    use super::*;
    use ast::{DefinitionRef, Statement, TransformRef};
    use error::ParseError;

    /// A shorthand for the `PAUSE`/`RESUME` transform address that shows up in
    /// nearly every case below.
    fn transform(target: &str, column: Option<&str>) -> TransformRef {
        TransformRef {
            target: target.to_string(),
            column: column.map(str::to_string),
        }
    }

    // --- the defining form still routes through the unified entry point -----

    /// `apply`ing a definition must produce exactly what `define` used to
    /// parse, so folding dispatch inside the parser can't have changed what a
    /// `TRANSFORM` statement means.
    #[test]
    fn a_transform_statement_parses_to_the_same_def_parse_produces() {
        let text = "TRANSFORM reporting.order_totals FROM sales.orders SELECT a + b AS total";
        let Statement::DefineTransform(def) = parse_statement(text).unwrap() else {
            panic!("a TRANSFORM statement must parse as one");
        };
        assert_eq!(def, parse(text).unwrap());
        assert_eq!(def.target, "order_totals");
        assert_eq!(def.explicit_target_schema, Some("reporting".to_string()));
    }

    /// `RELATIONSHIP` and `DROP RELATIONSHIP` are not statements of this
    /// grammar: each fails as an ordinary parse error, like any other keyword
    /// that doesn't belong where it was written, and so does `RELATIONSHIP`
    /// after `PAUSE`/`RESUME`.
    #[test]
    fn relationship_statements_are_not_part_of_this_grammar() {
        for text in [
            "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
            "relationship product from order_line_items.product_id to products.id",
        ] {
            let err = parse_statement(text).unwrap_err();
            assert!(
                matches!(&err, ParseError::UnexpectedToken { found, .. } if found == "identifier 'RELATIONSHIP'" || found == "identifier 'relationship'"),
                "got {err:?}"
            );
            assert_eq!(statement_kind(text).unwrap_err(), err);
        }
        for text in [
            "DROP RELATIONSHIP posts.author",
            "DROP RELATIONSHIP blog.posts.author",
            "PAUSE RELATIONSHIP posts.author",
            "RESUME RELATIONSHIP posts.author",
        ] {
            let err = parse_statement(text).unwrap_err();
            let ParseError::UnexpectedToken { expected, found } = &err else {
                panic!("expected an ordinary unexpected-token error for {text:?}, got {err:?}");
            };
            assert_eq!(expected, "'TRANSFORM'");
            assert_eq!(found, "identifier 'RELATIONSHIP'");
            assert_eq!(statement_kind(text).unwrap_err(), err);
        }
    }

    /// An aggregate definition reaches the same key-space clause through
    /// `parse_statement` — the dispatch arm delegates rather than
    /// reimplementing.
    #[test]
    fn an_aggregate_transform_statement_keeps_its_key_space() {
        let Statement::DefineTransform(def) = parse_statement(
            "TRANSFORM order_totals FROM order_line_items GROUP BY order_id \
             SELECT order_id AS order_id, SUM(amount) AS total_amount",
        )
        .unwrap() else {
            panic!("a TRANSFORM statement must parse as one");
        };
        assert_eq!(
            def.key_space,
            ast::KeySpace::Aggregate {
                group_by: vec!["order_id".to_string()]
            }
        );
    }

    // --- PAUSE / RESUME / DROP TRANSFORM ------------------------------------

    #[test]
    fn pause_resume_and_drop_a_whole_transform() {
        assert_eq!(
            parse_statement("PAUSE TRANSFORM order_totals").unwrap(),
            Statement::Pause(transform("order_totals", None))
        );
        assert_eq!(
            parse_statement("RESUME TRANSFORM order_totals").unwrap(),
            Statement::Resume(transform("order_totals", None))
        );
        assert_eq!(
            parse_statement("DROP TRANSFORM order_totals").unwrap(),
            Statement::Drop(DefinitionRef::Transform("order_totals".to_string()))
        );
    }

    /// Decision 2: column-level addressing, folding `QuarantineTarget`'s
    /// existing `"transform.column"` spelling into this grammar.
    #[test]
    fn pause_and_resume_accept_a_column_address() {
        assert_eq!(
            parse_statement("PAUSE TRANSFORM order_totals.total").unwrap(),
            Statement::Pause(transform("order_totals", Some("total")))
        );
        assert_eq!(
            parse_statement("RESUME TRANSFORM order_totals.total").unwrap(),
            Statement::Resume(transform("order_totals", Some("total")))
        );
    }

    /// `PAUSE TRANSFORM a.b` is the same token shape as a schema-qualified
    /// table reference. The grammar reads it as `<transform>.<column>`; the
    /// facade resolves `a.b` to the transform `a.b` when that is a registered
    /// qualified name (`Trellis::apply`).
    #[test]
    fn a_dotted_transform_address_is_a_column_not_a_schema() {
        assert_eq!(
            parse_statement("PAUSE TRANSFORM reporting.order_totals").unwrap(),
            Statement::Pause(transform("reporting", Some("order_totals"))),
        );
    }

    /// Which is also why a three-part transform address has no reading left,
    /// and says so rather than reporting a stray token.
    #[test]
    fn an_over_qualified_transform_address_is_refused_naming_the_whole_address() {
        let err = parse_statement("PAUSE TRANSFORM reporting.order_totals.total").unwrap_err();
        let ParseError::MalformedDefinitionAddress {
            statement, address, ..
        } = &err
        else {
            panic!("expected a malformed-address error, got {err:?}");
        };
        assert_eq!(statement, "PAUSE TRANSFORM");
        assert_eq!(address, "reporting.order_totals.total");
        assert!(err.to_string().contains("<schema>.<table>"), "{err}");
    }

    /// `DROP` is whole-definition only, so a dotted address is the
    /// transform's qualified name, never `<transform>.<column>`.
    #[test]
    fn drop_reads_a_dotted_address_as_a_qualified_name() {
        assert_eq!(
            parse_statement("DROP TRANSFORM public.order_totals").unwrap(),
            Statement::Drop(DefinitionRef::Transform("public.order_totals".to_string()))
        );
        assert_eq!(
            parse_statement("DROP TRANSFORM order_totals").unwrap(),
            Statement::Drop(DefinitionRef::Transform("order_totals".to_string()))
        );
        let err = parse_statement("DROP TRANSFORM a.b.c").unwrap_err();
        assert!(
            matches!(&err, ParseError::MalformedDefinitionAddress { address, .. } if address == "a.b.c"),
            "{err:?}"
        );
    }

    /// `ALTER TRANSFORM` takes the qualified name `apply` returns, too.
    #[test]
    fn alter_accepts_a_qualified_or_bare_target() {
        for (text, want) in [
            ("ALTER TRANSFORM public.t ADD a + 1 AS x", "public.t"),
            ("ALTER TRANSFORM t ADD a + 1 AS x", "t"),
        ] {
            let Statement::AlterTransform(alter) = parse_statement(text).unwrap() else {
                panic!("expected an ALTER for {text}");
            };
            assert_eq!(alter.target, want);
        }
    }

    // --- keyword handling and general malformation ---------------------------

    /// Every keyword in this grammar is case-insensitive and lexes as a plain
    /// identifier; the new ones are no exception.
    #[test]
    fn the_new_keywords_are_case_insensitive() {
        assert_eq!(
            parse_statement("pause transform order_totals").unwrap(),
            Statement::Pause(transform("order_totals", None))
        );
        assert_eq!(
            parse_statement("ReSuMe TrAnSfOrM order_totals.total").unwrap(),
            Statement::Resume(transform("order_totals", Some("total")))
        );
        assert_eq!(
            parse_statement("dRoP tRaNsFoRm order_totals").unwrap(),
            Statement::Drop(DefinitionRef::Transform("order_totals".to_string()))
        );
    }

    #[test]
    fn an_unrecognized_leading_keyword_names_every_statement_form() {
        let err = parse_statement("DELETE TRANSFORM order_totals").unwrap_err();
        let message = err.to_string();
        for keyword in ["TRANSFORM", "PAUSE", "RESUME", "DROP", "ALTER"] {
            assert!(message.contains(keyword), "{message} is missing {keyword}");
        }
        assert!(!message.contains("RELATIONSHIP"), "{message}");
    }

    #[test]
    fn empty_input_asks_for_a_statement_keyword() {
        let err = parse_statement("   ").unwrap_err();
        assert!(
            matches!(err, ParseError::UnexpectedEof { .. }),
            "got {err:?}"
        );
        assert!(err.to_string().contains("PAUSE"), "{err}");
    }

    /// Decision 1: the kind keyword is mandatory on the imperative forms —
    /// there is no unified namespace to drop it in favour of.
    #[test]
    fn the_kind_keyword_is_required_after_the_verb() {
        for text in ["PAUSE order_totals", "DROP order_totals"] {
            let err = parse_statement(text).unwrap_err();
            assert!(err.to_string().contains("TRANSFORM"), "{err}");
        }
    }

    #[test]
    fn a_verb_with_no_address_at_all_is_refused() {
        assert!(matches!(
            parse_statement("PAUSE TRANSFORM").unwrap_err(),
            ParseError::UnexpectedEof { .. }
        ));
        assert!(matches!(
            parse_statement("DROP TRANSFORM").unwrap_err(),
            ParseError::UnexpectedEof { .. }
        ));
    }

    /// One statement per call, with no `;` terminator (ADR-0004) — a trailing
    /// token is an error rather than the start of a second statement.
    #[test]
    fn trailing_tokens_after_an_address_are_refused() {
        let err = parse_statement("DROP TRANSFORM order_totals CASCADE").unwrap_err();
        assert!(
            matches!(&err, ParseError::UnexpectedToken { expected, .. } if expected == "end of input"),
            "got {err:?}"
        );
    }

    /// Decision 4: the flat imperative won for pause/resume/drop, so the
    /// SQL-DDL-flavoured alternative spelling of *those* is not quietly also
    /// accepted — `PAUSE` is not one of `ALTER TRANSFORM`'s own `ADD`/`DROP`/
    /// `ALTER` clause keywords (ADR-0015), so this
    /// refuses via the grammar's "expected a clause keyword" error.
    /// `ALTER TRANSFORM` itself is accepted grammar — see `statement_grammar_tests` in this
    /// same module for its own coverage — this test is only about the one
    /// spelling ADR-0015 still didn't resurrect.
    #[test]
    fn the_alter_flavored_pause_spelling_is_not_accepted() {
        assert!(parse_statement("ALTER TRANSFORM order_totals PAUSE").is_err());
    }

    /// Decision 3: `AMEND` is out of scope until it has its own ADR, so it must
    /// not parse — silently accepting it would be worse than rejecting it.
    #[test]
    fn amend_is_not_part_of_this_grammar_yet() {
        assert!(parse_statement("AMEND TRANSFORM order_totals").is_err());
    }

    // --- statement_kind -------------------------------------

    /// One well-formed statement of every form, with the kind it must
    /// classify as. Both `DROP` spellings (schema-qualified or not) and every
    /// `ALTER` clause are here, since each is its own path through the parser.
    const EVERY_FORM: [(&str, StatementKind); 10] = [
        (
            "TRANSFORM widget_prices FROM widgets SELECT price AS price",
            StatementKind::DefineTransform,
        ),
        (
            "TRANSFORM widget_totals FROM widgets GROUP BY owner_id \
             SELECT owner_id AS owner_id, SUM(price) AS total",
            StatementKind::DefineTransform,
        ),
        (
            "PAUSE TRANSFORM widget_prices",
            StatementKind::PauseTransform,
        ),
        (
            "PAUSE TRANSFORM widget_prices.price",
            StatementKind::PauseTransform,
        ),
        (
            "RESUME TRANSFORM widget_prices.price",
            StatementKind::ResumeTransform,
        ),
        ("DROP TRANSFORM widget_prices", StatementKind::DropTransform),
        (
            "DROP TRANSFORM shop.widget_prices",
            StatementKind::DropTransform,
        ),
        (
            "ALTER TRANSFORM widget_prices ADD price + price AS doubled",
            StatementKind::AlterTransform,
        ),
        (
            "ALTER TRANSFORM widget_prices DROP doubled",
            StatementKind::AlterTransform,
        ),
        (
            "ALTER TRANSFORM widget_prices ALTER doubled AS price + price + price",
            StatementKind::AlterTransform,
        ),
    ];

    #[test]
    fn statement_kind_names_every_form() {
        for (text, kind) in EVERY_FORM {
            assert_eq!(statement_kind(text).unwrap(), kind, "{text:?}");
            assert_eq!(parse_statement(text).unwrap().kind(), kind, "{text:?}");
            assert!(
                text.starts_with(&format!("{} ", kind.keywords())),
                "{text:?} doesn't open with {:?}",
                kind.keywords()
            );
        }
        let covered: std::collections::HashSet<StatementKind> =
            EVERY_FORM.iter().map(|&(_, kind)| kind).collect();
        assert_eq!(
            covered.len(),
            StatementKind::ALL.len(),
            "EVERY_FORM must hold a statement of every kind"
        );
    }

    /// The leading keyword is matched case-insensitively behind any
    /// whitespace the lexer skips, non-ASCII spaces included, and trailing
    /// whitespace changes nothing.
    #[test]
    fn statement_kind_ignores_case_and_surrounding_whitespace() {
        for (text, kind) in EVERY_FORM {
            let alternating: String = text
                .chars()
                .enumerate()
                .map(|(i, c)| {
                    if i % 2 == 0 {
                        c.to_ascii_lowercase()
                    } else {
                        c.to_ascii_uppercase()
                    }
                })
                .collect();
            for body in [text.to_string(), text.to_ascii_lowercase(), alternating] {
                for space in ["", " ", "\n\t  ", "\r\n", "\u{00a0}", "\u{2003}\u{3000}"] {
                    let variant = format!("{space}{body}{space}");
                    assert_eq!(statement_kind(&variant).unwrap(), kind, "{variant:?}");
                }
            }
        }
    }

    /// A statement that leads with a real keyword but doesn't parse is a
    /// parse error, not that keyword's kind: `apply` would refuse it just
    /// the same, so a binding checking the kind must not let it through as
    /// a well-formed statement of some form.
    #[test]
    fn a_malformed_statement_of_each_kind_is_a_parse_error() {
        for text in [
            "TRANSFORM oops",
            "TRANSFORM t FROM widgets SELECT price AS price garbage",
            "PAUSE widget_prices",
            "RESUME TRANSFORM",
            "DROP TRANSFORM public.widget_prices.price",
            "ALTER TRANSFORM widget_prices",
            "ALTER TRANSFORM widget_prices PAUSE",
        ] {
            let err = statement_kind(text).unwrap_err();
            assert_eq!(err, parse_statement(text).unwrap_err(), "{text:?}");
            assert_eq!(err.code(), crate::ErrorCode::Parse, "{text:?}");
        }
    }

    /// Text that leads with no statement keyword at all — nothing, an
    /// unknown verb, or a keyword with more identifier characters glued on —
    /// is a parse error too.
    #[test]
    fn text_with_no_statement_keyword_is_a_parse_error() {
        for text in [
            "",
            "   ",
            "\u{3000}",
            "AMEND TRANSFORM widget_prices",
            "DELETE TRANSFORM widget_prices",
            "TRANSFORMS t FROM widgets SELECT price AS price",
            "TRANSFORM_x t FROM widgets SELECT price AS price",
            "_TRANSFORM t FROM widgets SELECT price AS price",
            "SELECT 1",
            "'TRANSFORM'",
            "RELATIONSHIP owner FROM widgets.owner_id TO users.id",
            "DROP RELATIONSHIP widgets.owner",
        ] {
            assert!(statement_kind(text).is_err(), "{text:?}");
        }
    }

    /// [`StatementKind::ALL`] is exactly the enum's variants, and every
    /// variant has its own name.
    #[test]
    fn every_statement_kind_is_listed_in_all() {
        crate::error_code::assert_all_is_every_variant!(
            StatementKind: DefineTransform,
            PauseTransform,
            ResumeTransform,
            DropTransform,
            AlterTransform,
        );
        let names: std::collections::HashSet<&str> = StatementKind::ALL
            .iter()
            .map(|kind| kind.as_str())
            .collect();
        assert_eq!(names.len(), StatementKind::ALL.len());
        let keywords: std::collections::HashSet<&str> = StatementKind::ALL
            .iter()
            .map(|kind| kind.keywords())
            .collect();
        assert_eq!(keywords.len(), StatementKind::ALL.len());
        for kind in StatementKind::ALL {
            assert_eq!(kind.to_string(), kind.as_str());
        }
    }
}
