//! Transform-definition grammar and parser (issue #22).
//!
//! Parses the concrete syntax documented in [`parser`] into the typed
//! [`ast::TransformDef`] the validator (issue #23) and evaluator (issue #24)
//! consume. Per ADR-0004, only constructs this crate can actually evaluate
//! are accepted:
//!
//! - **Key-space**: 1-1 ([`ast::KeySpace::OneToOne`]) or aggregate
//!   ([`ast::KeySpace::Aggregate`], a `GROUP BY <cols>` clause, issue #11's
//!   groundwork) — no joins, no relationship paths.
//! - **Calculated fields**: column references, numeric and string literals,
//!   the `+` operator (Numeric-only), the `>` comparison operator (issue
//!   #65, `Numeric, Numeric -> Boolean`), and `name(args)` function calls,
//!   all via the shared [`registry`]. Values carry a [`ast::ValueType`]
//!   (`Numeric`/`Text`/`Boolean`, issue #63). Issue #64 adds general
//!   function-call syntax plus four Text-argument functions
//!   (`strpos`, `octet_length`, `char_length`, `regexp_count`). In an
//!   aggregate definition, a non-grouping-key column must be wrapped in
//!   exactly one of `SUM`/`MIN`/`MAX`/`AVG` (Numeric-only), or a field may be
//!   the row-counting `COUNT(*)` (no column argument; `COUNT(<column>)` is
//!   not implemented). Issue #109 adds **typed literals** — `DATE
//!   '2024-01-01'` / `CAST('2024-01-01' AS date)` — the one way an
//!   expression can *produce* a [`ast::ValueType::Other`] value rather than
//!   merely pass one through; see [`typed_literal`].
//! - **Partial-data predicate**: a trivially-true predicate only.
//!
//! Everything else is rejected at parse time with an error naming the
//! specific unsupported construct (see [`error::ParseError`]).

pub mod ast;
pub mod backfill;
pub mod catalog;
pub mod chunk_queue;
pub mod ddl;
pub mod error;
pub mod eval;
pub mod invertibility;
mod lexer;
pub mod lifecycle;
pub mod model;
pub mod oracle;
mod parser;
pub mod pg_type;
pub mod registry;
pub mod typed_literal;
pub mod validate;

// This module is tier 3 (`pub(crate)`, ADR-0012), so these flattened
// re-exports are a convenience for the engine itself and for the two gated
// doors onto it. Names the engine does not use are split out below and
// compiled only behind those gates, which is what keeps a plain
// `cargo build` free of `unused_imports` rather than an `allow`.
pub use ast::{DefinitionRef, Statement, TransformRef, ValueType};
pub use catalog::{CatalogError, all_source_tables, create_relationship, install_definition};
pub use error::ParseError;
pub use model::{Definition, RelationshipCardinality, RelationshipDefinition, TransformStatus};
pub use parser::{parse, parse_statement};
pub use pg_type::PgType;
pub use validate::validate;

// Reached from `crate::dev` (ADR-0012's sanctioned exception) by `generative`
// and `benchmark`.
#[cfg(any(test, feature = "test-util"))]
pub use backfill::backfill_definition;
#[cfg(any(test, feature = "test-util"))]
pub use catalog::create_definition_without_backfill;
#[cfg(any(test, feature = "test-util"))]
pub use ddl::{
    DdlError, create_aggregate_target_table, create_target_table, qualified_target_table,
    source_primary_key,
};
#[cfg(any(test, feature = "internals"))]
pub use oracle::{
    OracleError, Recomputed, recompute, recompute_aggregate,
    render_aggregate_relationship_select_sql, render_aggregate_select_sql, render_expr_sql,
    render_relationship_select_sql,
};

// Reached only by this crate's own `tests/*.rs`, through the `internals`
// feature (ADR-0012; see `Cargo.toml`). Not part of `dev`.
#[cfg(any(test, feature = "internals"))]
pub use ast::{
    Expr, FieldDef, GroupByKey, KeySpace, Operator, Predicate, RelationshipDef, TransformDef,
};
#[cfg(any(test, feature = "internals"))]
pub use backfill::BackfillError;
#[cfg(any(test, feature = "internals"))]
pub use catalog::{
    RelationshipProjection, create_definition, dependents_of, edges_from, node_for_table,
    persist_edge, relationship_by_name, relationship_projection, resolve_node,
    source_table_version, transforms_for_source,
};
#[cfg(any(test, feature = "internals"))]
pub use ddl::{PrimaryKeyColumn, neighbor_table_name};
#[cfg(any(test, feature = "internals"))]
pub use eval::{EvalError, RegexCache, Row, Value, evaluate, evaluate_aggregate};
#[cfg(any(test, feature = "internals"))]
pub use invertibility::{AggregateArg, CountArg, Invertibility, PartialField, Verdict, classify};
#[cfg(any(test, feature = "internals"))]
pub use model::{EdgeKind, NodeKind, SchemaEdge, SchemaNode};
#[cfg(any(test, feature = "internals"))]
pub use parser::parse_relationship;
#[cfg(any(test, feature = "internals"))]
pub use validate::{RelationshipTypeMismatch, RelationshipWarning, ValidationError};

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
                group_by: vec![GroupByKey::Column("order_id".to_string())]
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
                group_by: vec![
                    GroupByKey::Column("a".to_string()),
                    GroupByKey::Column("b".to_string())
                ]
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

    /// Issue #120: `COUNT(<column>)` — counting non-null occurrences of a
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

    #[test]
    fn rejects_join() {
        let err =
            parse("TRANSFORM t FROM s JOIN other ON s.id = other.id SELECT a AS x").unwrap_err();
        match err {
            ParseError::UnsupportedKeySpace { construct, .. } => {
                assert_eq!(construct, "JOIN");
            }
            other => panic!("expected UnsupportedKeySpace, got {other:?}"),
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

    #[test]
    fn parses_relationship_path() {
        let def = parse("TRANSFORM t FROM s SELECT product.category_name AS x").unwrap();
        assert_eq!(
            def.fields[0].expr,
            Expr::RelationshipPath {
                rel: "product".to_string(),
                column: "category_name".to_string(),
            }
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
        let err = validate(&def, &source_columns, &std::collections::HashMap::new()).unwrap_err();
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

    /// Permanent regression pin for real operator precedence (issue #67,
    /// see [`registry::OPERATORS`]'s doc comment): `a > b + c` must parse as
    /// `a > (b + c)`, since `+` ([`registry::precedence::ADDITIVE`]) binds
    /// tighter than `>` ([`registry::precedence::COMPARISON`]), matching
    /// Postgres's own grouping — not the old flat left-to-right parse that
    /// used to build `(a > b) + c` here (renamed from
    /// `flat_parse_of_mixed_operators_is_caught_by_type_checking`, which
    /// pinned that wrong behavior and relied on the type checker to catch
    /// the resulting type mismatch). With real precedence, the types line
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
        validate(&def, &source_columns, &std::collections::HashMap::new())
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
        validate(&def, &source_columns, &std::collections::HashMap::new())
            .expect("(a + b) > c type-checks cleanly");
    }

    /// Parenthesized grouping (issue #67's follow-up, needed once a caller
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
    fn parses_a_relationship_declaration() {
        let rel = parse_relationship(
            "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
        )
        .unwrap();
        assert_eq!(
            rel,
            RelationshipDef {
                name: "product".to_string(),
                from_table: "order_line_items".to_string(),
                from_col: "product_id".to_string(),
                to_table: "products".to_string(),
                to_col: "id".to_string(),
            }
        );
    }

    #[test]
    fn parses_a_second_relationship_declaration_with_different_names() {
        let rel =
            parse_relationship("RELATIONSHIP author FROM posts.author_id TO users.id").unwrap();
        assert_eq!(
            rel,
            RelationshipDef {
                name: "author".to_string(),
                from_table: "posts".to_string(),
                from_col: "author_id".to_string(),
                to_table: "users".to_string(),
                to_col: "id".to_string(),
            }
        );
    }

    #[test]
    fn relationship_keywords_are_case_insensitive() {
        let rel = parse_relationship(
            "relationship product from order_line_items.product_id to products.id",
        )
        .unwrap();
        assert_eq!(rel.name, "product");
    }

    #[test]
    fn rejects_relationship_missing_from() {
        let err =
            parse_relationship("RELATIONSHIP product order_line_items.product_id TO products.id")
                .unwrap_err();
        match err {
            ParseError::UnexpectedToken { expected, .. } => assert_eq!(expected, "'FROM'"),
            other => panic!("expected UnexpectedToken, got {other:?}"),
        }
    }

    #[test]
    fn rejects_relationship_missing_dot_on_from_column() {
        let err = parse_relationship("RELATIONSHIP product FROM order_line_items TO products.id")
            .unwrap_err();
        match err {
            ParseError::UnexpectedToken { expected, .. } => assert_eq!(expected, "'.'"),
            other => panic!("expected UnexpectedToken, got {other:?}"),
        }
    }

    #[test]
    fn rejects_relationship_missing_to() {
        let err =
            parse_relationship("RELATIONSHIP product FROM order_line_items.product_id products.id")
                .unwrap_err();
        match err {
            ParseError::UnexpectedToken { expected, .. } => assert_eq!(expected, "'TO'"),
            other => panic!("expected UnexpectedToken, got {other:?}"),
        }
    }

    #[test]
    fn rejects_relationship_missing_name() {
        let err =
            parse_relationship("RELATIONSHIP FROM order_line_items.product_id TO products.id")
                .unwrap_err();
        match err {
            ParseError::UnexpectedToken { expected, .. } => assert_eq!(expected, "'FROM'"),
            other => panic!("expected UnexpectedToken, got {other:?}"),
        }
    }

    #[test]
    fn rejects_relationship_trailing_garbage() {
        let err = parse_relationship(
            "RELATIONSHIP product FROM order_line_items.product_id TO products.id EXTRA",
        )
        .unwrap_err();
        match err {
            ParseError::UnexpectedToken { expected, .. } => assert_eq!(expected, "end of input"),
            other => panic!("expected UnexpectedToken, got {other:?}"),
        }
    }

    #[test]
    fn rejects_relationship_missing_dot_on_to_column() {
        let err =
            parse_relationship("RELATIONSHIP product FROM order_line_items.product_id TO products")
                .unwrap_err();
        match err {
            ParseError::UnexpectedEof { expected } => assert_eq!(expected, "'.'"),
            other => panic!("expected UnexpectedEof, got {other:?}"),
        }
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

    // Issue #76 / ADR-0007 grammar clause 4: `TRANSFORM`/`FROM` accept an
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
    fn qualified_table_reference_does_not_collide_with_relationship_path_syntax() {
        // The `<rel>.<column>` relationship-path grammar (issue #25) reuses
        // the same `ident '.' ident` token shape inside a field expression —
        // confirms the two never compete for the same tokens, since the
        // table-reference form is only ever parsed in `TRANSFORM`/`FROM`
        // position, well before `SELECT`'s field list is reached.
        let def = parse("TRANSFORM t FROM custom.orders SELECT product.category_name AS category")
            .unwrap();

        assert_eq!(def.source, "orders");
        assert_eq!(def.explicit_source_schema, Some("custom".to_string()));
        assert_eq!(
            def.fields,
            vec![FieldDef {
                name: "category".to_string(),
                expr: Expr::RelationshipPath {
                    rel: "product".to_string(),
                    column: "category_name".to_string(),
                },
            }]
        );
    }
}

/// Grammar coverage for [`parse_statement`], the unified statement entry point
/// issue #227 dispatches [`crate::Trellis::apply`] on (issue #228's settled
/// grammar). Kept apart from the `tests` module above, which is about the
/// expression/definition grammar `parse` covers, because what's under test
/// here is *which statement* a text is — the dispatch decision itself.
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

    fn relationship(schema: Option<&str>, from_table: &str, name: &str) -> DefinitionRef {
        DefinitionRef::Relationship {
            schema: schema.map(str::to_string),
            from_table: from_table.to_string(),
            name: name.to_string(),
        }
    }

    // --- the two defining forms still route through the unified entry point --

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

    #[test]
    fn a_relationship_statement_parses_to_the_same_def_parse_relationship_produces() {
        let text = "RELATIONSHIP product FROM order_line_items.product_id TO products.id";
        let Statement::DefineRelationship(def) = parse_statement(text).unwrap() else {
            panic!("a RELATIONSHIP statement must parse as one");
        };
        assert_eq!(def, parser::parse_relationship(text).unwrap());
        assert_eq!(def.name, "product");
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
                group_by: vec![ast::GroupByKey::Column("order_id".to_string())]
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

    /// The addressing fork issue #228 left open: `PAUSE TRANSFORM a.b` is the
    /// same token shape as a schema-qualified table reference, and this grammar
    /// reads it as `<transform>.<column>` — the only reading the engine can
    /// resolve, since a transform's operator-facing identity is its bare target
    /// name everywhere below the facade.
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
        assert!(err.to_string().contains("bare target table"), "{err}");
    }

    /// Decision 2's other half: `DROP` is whole-definition only.
    #[test]
    fn drop_refuses_a_column_address() {
        let err = parse_statement("DROP TRANSFORM order_totals.total").unwrap_err();
        let ParseError::MalformedDefinitionAddress {
            statement, address, ..
        } = &err
        else {
            panic!("expected a malformed-address error, got {err:?}");
        };
        assert_eq!(statement, "DROP TRANSFORM");
        assert_eq!(address, "order_totals.total");
        assert!(err.to_string().contains("ALTER TRANSFORM"), "{err}");
    }

    // --- DROP RELATIONSHIP --------------------------------------------------

    /// Decision 1: always scoped to the from-table, optionally schema-qualified.
    #[test]
    fn drop_relationship_accepts_both_scoped_spellings() {
        assert_eq!(
            parse_statement("DROP RELATIONSHIP posts.author").unwrap(),
            Statement::Drop(relationship(None, "posts", "author"))
        );
        assert_eq!(
            parse_statement("DROP RELATIONSHIP blog.posts.author").unwrap(),
            Statement::Drop(relationship(Some("blog"), "posts", "author"))
        );
    }

    /// Decision 1's refusal: a bare relationship name identifies nothing,
    /// since `relationship_definitions` is unique on `(from_table, name)`.
    #[test]
    fn drop_relationship_refuses_a_bare_unscoped_name() {
        let err = parse_statement("DROP RELATIONSHIP author").unwrap_err();
        assert!(
            matches!(&err, ParseError::UnscopedRelationshipAddress { address } if address == "author"),
            "expected an unscoped-address error, got {err:?}"
        );
        assert!(err.to_string().contains("posts.author"), "{err}");
    }

    #[test]
    fn drop_relationship_refuses_a_four_part_address() {
        let err = parse_statement("DROP RELATIONSHIP a.b.c.d").unwrap_err();
        let ParseError::MalformedDefinitionAddress { address, .. } = &err else {
            panic!("expected a malformed-address error, got {err:?}");
        };
        assert_eq!(address, "a.b.c.d");
    }

    /// **Pausing is transform-only.** A relationship is a reusable component of
    /// a transform, not something that does work of its own, so there is
    /// nothing for a pause to suspend — `PAUSE`/`RESUME RELATIONSHIP` is simply
    /// not a form of this grammar. It must therefore fail the way any other
    /// misplaced keyword fails: an ordinary
    /// [`ParseError::UnexpectedToken`] saying `TRANSFORM` was expected, with no
    /// special case anywhere for the phrase.
    #[test]
    fn pause_and_resume_are_transform_only() {
        for text in [
            "PAUSE RELATIONSHIP posts.author",
            "RESUME RELATIONSHIP posts.author",
            // The same whether or not the address that follows is well-formed:
            // the keyword is rejected before any address is looked at.
            "PAUSE RELATIONSHIP author",
            "RESUME RELATIONSHIP blog.posts.author",
        ] {
            let err = parse_statement(text).unwrap_err();
            let ParseError::UnexpectedToken { expected, found } = &err else {
                panic!("expected an ordinary unexpected-token error for {text:?}, got {err:?}");
            };
            assert_eq!(expected, "'TRANSFORM'");
            assert_eq!(found, "identifier 'RELATIONSHIP'");
        }
    }

    /// The flip side of the rule, pinned so a future refactor can't quietly
    /// make `DROP` transform-only too: `DROP` is the one verb that takes either
    /// kind, because a relationship is a definition and definitions can be
    /// retired. (The accepting cases are
    /// `drop_relationship_accepts_both_scoped_spellings` above; this is the
    /// contrast against `PAUSE`/`RESUME`.)
    #[test]
    fn drop_is_the_one_verb_that_accepts_either_kind() {
        assert!(parse_statement("DROP RELATIONSHIP posts.author").is_ok());
        assert!(parse_statement("DROP TRANSFORM order_totals").is_ok());
        assert!(parse_statement("PAUSE RELATIONSHIP posts.author").is_err());
        assert!(parse_statement("RESUME RELATIONSHIP posts.author").is_err());
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
            parse_statement("dRoP rElAtIoNsHiP posts.author").unwrap(),
            Statement::Drop(relationship(None, "posts", "author"))
        );
    }

    #[test]
    fn an_unrecognized_leading_keyword_names_every_statement_form() {
        let err = parse_statement("DELETE TRANSFORM order_totals").unwrap_err();
        let message = err.to_string();
        for keyword in ["TRANSFORM", "RELATIONSHIP", "PAUSE", "RESUME", "DROP"] {
            assert!(message.contains(keyword), "{message} is missing {keyword}");
        }
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
        // `PAUSE`/`RESUME` only ever take `TRANSFORM`, so that is all their
        // message names; `DROP` takes either kind and names both.
        let err = parse_statement("PAUSE order_totals").unwrap_err();
        assert!(err.to_string().contains("TRANSFORM"), "{err}");

        let err = parse_statement("DROP order_totals").unwrap_err();
        let message = err.to_string();
        assert!(message.contains("TRANSFORM"), "{message}");
        assert!(message.contains("RELATIONSHIP"), "{message}");
    }

    #[test]
    fn a_verb_with_no_address_at_all_is_refused() {
        assert!(matches!(
            parse_statement("PAUSE TRANSFORM").unwrap_err(),
            ParseError::UnexpectedEof { .. }
        ));
        assert!(matches!(
            parse_statement("DROP RELATIONSHIP").unwrap_err(),
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

    /// Decision 4: the flat imperative won, so the SQL-DDL-flavoured
    /// alternative is not quietly also accepted.
    #[test]
    fn the_alter_flavored_spelling_is_not_accepted() {
        assert!(parse_statement("ALTER TRANSFORM order_totals PAUSE").is_err());
    }

    /// Decision 3: `AMEND` is out of scope until it has its own ADR, so it must
    /// not parse — silently accepting it would be worse than rejecting it.
    #[test]
    fn amend_is_not_part_of_this_grammar_yet() {
        assert!(parse_statement("AMEND TRANSFORM order_totals").is_err());
    }
}
