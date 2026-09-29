//! The per-target ledger (ADR-0002, #623 part D2): one row per source key a
//! target has applied, recording what that key last contributed and the
//! ordering state (`applied_lsn`, `applied_seg`, `basis`, `tombstone`) that
//! decides whether a later change for the key is applied or skipped.
//!
//! Every target gets one, created beside it in the registration transaction
//! (`<target>__ledger`, in the target's schema) and dropped with it:
//!
//! - **Aggregate targets**: the key, the row's `GROUP BY` values (typed as the
//!   target's), whether the row is a member of its group, one typed column
//!   per distinct aggregate argument ([`contributions`]), the relationship
//!   join key, and the ordering state. The group row is a pure function of
//!   its members' entries: `SUM(x)` is `sum(<x's column>)` over them, and so
//!   on for every field, which is how the one-pass build writes the groups
//!   ([`super::backfill`]).
//! - **1-1 targets**: the key and the ordering state only. The target row
//!   holds the values.
//!
//! **Nothing on the apply path reads a ledger yet.** The build writes an
//! aggregate target's ledger; apply doesn't maintain it, so it goes stale
//! after the target's first change. #623 part D3 is its first reader.
//!
//! The bookkeeping columns are `__`-prefixed so they can't collide with a
//! `GROUP BY` column, which validation keeps out of that prefix
//! ([`super::validate::RESERVED_COLUMN_PREFIX`]).

use std::collections::HashMap;

use super::ast::{Expr, FieldDef, GroupByKey, group_by_contains};
use super::registry::lookup_aggregate_function;
use crate::pool::quote_ident;

/// The suffix a target's ledger table adds to the target's name. Validation
/// refuses a target named with it ([`super::validate::ValidationError::ReservedTargetSuffix`]),
/// so one target's ledger can never be another target.
pub const LEDGER_SUFFIX: &str = "__ledger";

/// The longest target name whose ledger name still fits Postgres's 63-byte
/// identifier limit (`NAMEDATALEN - 1`). A longer ledger name would be
/// silently truncated, onto the target's own name at the limit.
pub const MAX_TARGET_NAME_LEN: usize = 63 - LEDGER_SUFFIX.len();

/// The source row's key, encoded exactly as the ring's `key`
/// ([`super::ddl::pk_key_sql_expr`]).
pub(crate) const KEY_COLUMN: &str = "__from_key";
/// Whether the row is a member of its group. False for a row the
/// definition's filter excludes (no definition has one yet, so every entry
/// the build writes is a member).
pub(crate) const MEMBER_COLUMN: &str = "__member";
/// The row's relationship join values, for a relationship-fed target's
/// reverse lookups. Written from #623 part D5; null until then.
pub(crate) const JOIN_KEY_COLUMN: &str = "__join_key";
/// The `lsn` of the last change applied to the entry. A Re-derive leaves it
/// unchanged (#623 Q1), so the build writes null.
pub(crate) const APPLIED_LSN_COLUMN: &str = "__applied_lsn";
/// The ring segment (`seg_seq`) of the last change applied to the entry: the
/// tombstone GC watermark (#623 Q7).
pub(crate) const APPLIED_SEG_COLUMN: &str = "__applied_seg";
/// The `pg_current_snapshot()` of the read that last wrote the entry, taken
/// in the same statement as that read (ADR-0002 I1). A change whose
/// transaction is visible in it is already counted.
pub(crate) const BASIS_COLUMN: &str = "__basis";
/// Whether the key's last applied change deleted it.
pub(crate) const TOMBSTONE_COLUMN: &str = "__tombstone";

/// A target's ledger table name.
pub(crate) fn ledger_table_name(target: &str) -> String {
    format!("{target}{LEDGER_SUFFIX}")
}

/// A target's ledger, schema-qualified and quoted for SQL text, from the
/// target's schema and bare name.
pub(crate) fn qualified_ledger_table(target_schema: &str, target: &str) -> String {
    format!(
        "{}.{}",
        quote_ident(target_schema),
        quote_ident(&ledger_table_name(target))
    )
}

/// One typed contribution column of an aggregate ledger: the value of one
/// aggregate argument over the entry's source row.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Contribution {
    /// The ledger column, `__arg<n>`.
    pub column: String,
    /// The argument expression, over the source row.
    pub arg: Expr,
}

/// The contribution columns an aggregate definition's ledger carries: one per
/// distinct argument of an aggregate call in a non-`GROUP BY` field, in field
/// declaration order, then left to right within a field.
///
/// The column holds the argument's value, so each aggregate over the source
/// is the same aggregate over the ledger: `SUM(x)` is `sum(c)`, its hidden
/// NULL-tracking count `count(c)`, `AVG(x)` `sum(c)` over `count(c)`,
/// `COUNT(x)` `count(c)`, `MIN(x)` `min(c)`. Calls sharing an argument share
/// its column, as `SUM(x)` and `AVG(x)` share a hidden count on the target
/// (issue #48). `COUNT(*)` has no argument and needs none: it counts the
/// group's member entries.
///
/// `substituted` is every field's cross-field-alias-resolved expression
/// ([`super::backfill::substituted_field_exprs`]), so an aliased aggregate
/// (`t2 = t` where `t = SUM(x)`) resolves to the same column as its source.
pub(crate) fn contributions(
    fields: &[FieldDef],
    group_by: &[GroupByKey],
    substituted: &HashMap<String, Expr>,
) -> Vec<Contribution> {
    fn walk(expr: &Expr, out: &mut Vec<Contribution>) {
        match expr {
            Expr::FunctionCall { name, args } if is_aggregate_leaf(name, args) => {
                if !out.iter().any(|c| c.arg == args[0]) {
                    out.push(Contribution {
                        column: format!("__arg{}", out.len()),
                        arg: args[0].clone(),
                    });
                }
            }
            Expr::FunctionCall { args, .. } => args.iter().for_each(|a| walk(a, out)),
            Expr::BinaryOp { lhs, rhs, .. } => {
                walk(lhs, out);
                walk(rhs, out);
            }
            Expr::Column(_)
            | Expr::NumberLiteral(_)
            | Expr::StringLiteral(_)
            | Expr::TypedLiteral { .. }
            | Expr::RelationshipPath { .. } => {}
        }
    }
    let mut out = Vec::new();
    for field in fields {
        if !group_by_contains(group_by, &field.name) {
            walk(&substituted[&field.name], &mut out);
        }
    }
    out
}

/// An aggregate call with one argument: a leaf whose argument becomes a
/// contribution column. (`COUNT(*)` has none, and the validator refuses an
/// aggregate nested in another's argument.)
fn is_aggregate_leaf(name: &str, args: &[Expr]) -> bool {
    args.len() == 1 && lookup_aggregate_function(name).is_some()
}

/// `expr` (a field's substituted expression) rewritten to read the ledger
/// instead of the source: each aggregate call's argument becomes its
/// contribution column, and a `GROUP BY` key read outside an aggregate
/// becomes the key's column. Rendered with
/// [`super::oracle::render_expr_sql`] and grouped by the key columns over
/// the member entries, it computes the field exactly as the same expression
/// grouped over the source does.
pub(crate) fn over_ledger(
    expr: &Expr,
    contributions: &[Contribution],
    group_by: &[GroupByKey],
) -> Expr {
    match expr {
        Expr::FunctionCall { name, args } if is_aggregate_leaf(name, args) => {
            let column = contributions
                .iter()
                .find(|c| c.arg == args[0])
                .map(|c| c.column.clone())
                .unwrap_or_else(|| {
                    panic!("no ledger contribution for {name}'s argument {:?}", args[0])
                });
            Expr::FunctionCall {
                name: name.clone(),
                args: vec![Expr::Column(column)],
            }
        }
        _ if group_by.iter().any(|k| k.as_expr() == *expr) => {
            let key = group_by.iter().find(|k| k.as_expr() == *expr).unwrap();
            Expr::Column(key.target_column_name().to_string())
        }
        Expr::FunctionCall { name, args } => Expr::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| over_ledger(a, contributions, group_by))
                .collect(),
        },
        Expr::BinaryOp { op, lhs, rhs } => Expr::BinaryOp {
            op: *op,
            lhs: Box::new(over_ledger(lhs, contributions, group_by)),
            rhs: Box::new(over_ledger(rhs, contributions, group_by)),
        },
        other => other.clone(),
    }
}

/// One rendered column of a ledger's `create table`: its quoted name, its
/// type, and, for a text contribution, the collation its argument has over
/// the source (so `MIN`/`MAX` over the ledger order it as over the source).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LedgerColumn {
    pub name: String,
    pub pg_type: String,
    pub collation: Option<String>,
}

impl LedgerColumn {
    fn render(&self) -> String {
        match &self.collation {
            Some(collation) => format!(
                "{} {} collate {collation}",
                quote_ident(&self.name),
                self.pg_type
            ),
            None => format!("{} {}", quote_ident(&self.name), self.pg_type),
        }
    }
}

/// The ordering-state columns every ledger ends with.
fn ordering_state_columns() -> String {
    format!(
        "{} pg_lsn, {} bigint, {} pg_snapshot, {} boolean not null default false",
        quote_ident(APPLIED_LSN_COLUMN),
        quote_ident(APPLIED_SEG_COLUMN),
        quote_ident(BASIS_COLUMN),
        quote_ident(TOMBSTONE_COLUMN),
    )
}

/// An aggregate target's ledger DDL: `create table` plus its indexes, as
/// statements to append to the target's own.
///
/// `group_columns` are the target's `GROUP BY` columns with the target's
/// types; `contribution_columns` are [`contributions`]' columns, typed by
/// their arguments. See [`aggregate_ledger_index_ddl`] for the indexes.
pub(crate) fn aggregate_ledger_ddl(
    qualified_ledger: &str,
    group_columns: &[LedgerColumn],
    contribution_columns: &[LedgerColumn],
    reads_relationships: bool,
) -> String {
    let mut columns = vec![format!("{} text primary key", quote_ident(KEY_COLUMN))];
    columns.extend(group_columns.iter().map(LedgerColumn::render));
    columns.push(format!(
        "{} boolean not null default true",
        quote_ident(MEMBER_COLUMN)
    ));
    columns.extend(contribution_columns.iter().map(LedgerColumn::render));
    columns.push(format!("{} text[]", quote_ident(JOIN_KEY_COLUMN)));
    columns.push(ordering_state_columns());
    let group_idents: Vec<String> = group_columns.iter().map(|c| quote_ident(&c.name)).collect();
    format!(
        "; create table {qualified_ledger} ({}){}",
        columns.join(", "),
        aggregate_ledger_index_ddl(qualified_ledger, &group_idents, reads_relationships),
    )
}

/// An aggregate ledger's secondary indexes, as statements each prefixed with
/// `; `: the `GROUP BY` index, partial on live members (the only entries a
/// group is a sum of), and, on a target that reads a relationship (the only
/// kind whose entries carry a join key), the join-key index. `group_idents`
/// are the quoted `GROUP BY` columns.
///
/// Shared by the ledger's DDL and the aggregate build, which drops them for
/// its load and builds them again after it. They are left for Postgres to
/// name: it picks a free name, where `<ledger>_…` could pass the identifier
/// limit and truncate onto the ledger's own name.
pub(crate) fn aggregate_ledger_index_ddl(
    qualified_ledger: &str,
    group_idents: &[String],
    reads_relationships: bool,
) -> String {
    let mut sql = format!(
        "; create index on {qualified_ledger} ({}) where {} and not {}",
        group_idents.join(", "),
        quote_ident(MEMBER_COLUMN),
        quote_ident(TOMBSTONE_COLUMN),
    );
    if reads_relationships {
        sql.push_str(&format!(
            "; create index on {qualified_ledger} using gin ({})",
            quote_ident(JOIN_KEY_COLUMN),
        ));
    }
    sql
}

/// A 1-1 target's ledger DDL (#623 Q2 (c)): the key and the ordering state,
/// with no values (the target row holds them), as a statement to append to
/// the target's own. Created empty: the 1-1 build's writes are absolute, and
/// the Re-derives that follow it stamp the entries (from #623 part D6).
pub(crate) fn one_to_one_ledger_ddl(qualified_ledger: &str) -> String {
    format!(
        "; create table {qualified_ledger} ({} text primary key, {})",
        quote_ident(KEY_COLUMN),
        ordering_state_columns(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::parser::parse;

    fn plan(text: &str) -> (Vec<Contribution>, HashMap<String, Expr>, Vec<GroupByKey>) {
        let def = parse(text).expect("parse");
        let super::super::ast::KeySpace::Aggregate { group_by } = &def.key_space else {
            panic!("not an aggregate");
        };
        let substituted = super::super::backfill::substituted_field_exprs(&def).expect("subst");
        (
            contributions(&def.fields, group_by, &substituted),
            substituted,
            group_by.clone(),
        )
    }

    fn args(contribs: &[Contribution]) -> Vec<(&str, &Expr)> {
        contribs
            .iter()
            .map(|c| (c.column.as_str(), &c.arg))
            .collect()
    }

    fn col(name: &str) -> Expr {
        Expr::Column(name.to_string())
    }

    #[test]
    fn each_distinct_aggregate_argument_gets_one_column_in_declaration_order() {
        let (contribs, _, _) = plan(
            "TRANSFORM t FROM s GROUP BY g SELECT g AS g, SUM(a) AS total, AVG(a) AS mean, \
             COUNT(*) AS n, COUNT(b) AS nb, MIN(c) AS lo, MAX(c) AS hi",
        );
        assert_eq!(
            args(&contribs),
            vec![
                ("__arg0", &col("a")),
                ("__arg1", &col("b")),
                ("__arg2", &col("c"))
            ]
        );
    }

    #[test]
    fn count_star_alone_needs_no_contribution_column() {
        let (contribs, _, _) = plan("TRANSFORM t FROM s GROUP BY g SELECT COUNT(*) AS n");
        assert!(contribs.is_empty(), "{contribs:?}");
    }

    #[test]
    fn a_composed_field_contributes_each_of_its_aggregate_arguments() {
        let (contribs, substituted, group_by) =
            plan("TRANSFORM t FROM s GROUP BY g SELECT SUM(a) + MAX(b + 1) AS x");
        assert_eq!(contribs.len(), 2, "{contribs:?}");
        assert_eq!(contribs[0].arg, col("a"));
        assert!(
            matches!(&contribs[1].arg, Expr::BinaryOp { .. }),
            "{contribs:?}"
        );
        assert_eq!(
            super::super::oracle::render_expr_sql(&over_ledger(
                &substituted["x"],
                &contribs,
                &group_by
            )),
            r#"(sum("__arg0") + max("__arg1"))"#
        );
    }

    #[test]
    fn an_aliased_aggregate_shares_its_sources_column() {
        let (contribs, substituted, group_by) =
            plan("TRANSFORM t FROM s GROUP BY g SELECT SUM(a) AS t1, t1 AS t2");
        assert_eq!(args(&contribs), vec![("__arg0", &col("a"))]);
        assert_eq!(
            super::super::oracle::render_expr_sql(&over_ledger(
                &substituted["t2"],
                &contribs,
                &group_by
            )),
            r#"sum("__arg0")"#
        );
    }

    #[test]
    fn a_relationship_group_key_outside_an_aggregate_reads_its_ledger_column() {
        let (contribs, substituted, group_by) = plan(
            "TRANSFORM t FROM s GROUP BY post.author SELECT post.author AS author, \
             SUM(post.words) AS words",
        );
        assert_eq!(
            contribs[0].arg,
            Expr::RelationshipPath {
                rel: "post".to_string(),
                column: "words".to_string()
            }
        );
        assert_eq!(
            over_ledger(
                &Expr::RelationshipPath {
                    rel: "post".to_string(),
                    column: "author".to_string()
                },
                &contribs,
                &group_by
            ),
            col("author")
        );
        assert_eq!(
            super::super::oracle::render_expr_sql(&over_ledger(
                &substituted["words"],
                &contribs,
                &group_by
            )),
            r#"sum("__arg0")"#
        );
    }

    #[test]
    fn the_aggregate_ledger_ddl_keys_on_the_source_key_and_indexes_live_members_by_group() {
        let ddl = aggregate_ledger_ddl(
            r#""public"."t__ledger""#,
            &[LedgerColumn {
                name: "g".to_string(),
                pg_type: "integer".to_string(),
                collation: None,
            }],
            &[
                LedgerColumn {
                    name: "__arg0".to_string(),
                    pg_type: "numeric".to_string(),
                    collation: None,
                },
                LedgerColumn {
                    name: "__arg1".to_string(),
                    pg_type: "text".to_string(),
                    collation: Some(r#""C""#.to_string()),
                },
            ],
            false,
        );
        assert_eq!(
            ddl,
            r#"; create table "public"."t__ledger" ("__from_key" text primary key, "g" integer, "__member" boolean not null default true, "__arg0" numeric, "__arg1" text collate "C", "__join_key" text[], "__applied_lsn" pg_lsn, "__applied_seg" bigint, "__basis" pg_snapshot, "__tombstone" boolean not null default false); create index on "public"."t__ledger" ("g") where "__member" and not "__tombstone""#
        );
        let with_join = aggregate_ledger_ddl(r#""public"."t__ledger""#, &[], &[], true);
        assert!(
            with_join
                .ends_with(r#"; create index on "public"."t__ledger" using gin ("__join_key")"#),
            "{with_join}"
        );
    }

    #[test]
    fn the_one_to_one_ledger_holds_only_the_key_and_the_ordering_state() {
        assert_eq!(
            one_to_one_ledger_ddl(r#""public"."t__ledger""#),
            r#"; create table "public"."t__ledger" ("__from_key" text primary key, "__applied_lsn" pg_lsn, "__applied_seg" bigint, "__basis" pg_snapshot, "__tombstone" boolean not null default false)"#
        );
    }

    #[test]
    fn a_ledger_name_derives_from_the_target_and_fits_the_identifier_limit() {
        assert_eq!(ledger_table_name("totals"), "totals__ledger");
        assert_eq!(
            qualified_ledger_table("app", "totals"),
            r#""app"."totals__ledger""#
        );
        assert_eq!(
            ledger_table_name(&"x".repeat(MAX_TARGET_NAME_LEN)).len(),
            63
        );
    }
}
