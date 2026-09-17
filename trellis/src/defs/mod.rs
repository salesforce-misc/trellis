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
//!   not implemented).
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
pub mod model;
pub mod oracle;
mod parser;
pub mod registry;
pub mod validate;

pub use ast::{
    Expr, FieldDef, GroupByKey, KeySpace, Operator, Predicate, RelationshipDef, TransformDef,
    ValueType,
};
pub use backfill::{BackfillError, backfill_definition};
pub use catalog::{
    CatalogError, RelationshipProjection, all_source_tables, create_definition,
    create_definition_without_backfill, create_relationship, dependents_of, edges_from,
    install_definition, node_for_table, persist_edge, relationship_by_name,
    relationship_projection, resolve_node, source_table_version, transforms_for_source,
};
pub use ddl::{
    DdlError, PrimaryKeyColumn, create_aggregate_target_table, create_target_table,
    neighbor_table_name, qualified_target_table, require_single_column_pk, source_primary_key,
};
pub use error::ParseError;
pub use eval::{EvalError, RegexCache, Row, Value, evaluate, evaluate_aggregate};
pub use invertibility::{AggregateArg, CountArg, Invertibility, PartialField, Verdict, classify};
pub use model::{
    Definition, EdgeKind, NodeKind, RelationshipCardinality, RelationshipDefinition, SchemaEdge,
    SchemaNode, TransformStatus,
};
pub use oracle::{
    OracleError, Recomputed, recompute, recompute_aggregate,
    render_aggregate_relationship_select_sql, render_aggregate_select_sql, render_expr_sql,
    render_relationship_select_sql,
};
pub use parser::{parse, parse_relationship};
pub use validate::{RelationshipTypeMismatch, RelationshipWarning, ValidationError, validate};

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

    #[test]
    fn rejects_count_of_a_column_in_an_aggregate_definition() {
        let err = parse("TRANSFORM t FROM s GROUP BY a SELECT COUNT(a) AS x").unwrap_err();
        match err {
            ParseError::UnsupportedAggregateFunction { name } => assert_eq!(name, "COUNT"),
            other => panic!("expected UnsupportedAggregateFunction, got {other:?}"),
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
