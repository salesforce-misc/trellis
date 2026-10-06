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
//! The build writes an aggregate target's ledger, and Apply maintains it
//! (`staging::ledger`, #623 D3–D5): each page locks, reads and rewrites the
//! entries of the keys it applies, and their group rows are the sums of the
//! moves.
//!
//! A routed target also gets `<target>__deltas` ([`aggregate_deltas_ddl`],
//! #625 F1): the per-group increments a build chunk records instead of
//! writing group rows, which the merger (`staging::build`) folds into them.
//!
//! The bookkeeping columns are `__`-prefixed so they can't collide with a
//! `GROUP BY` column, which validation keeps out of that prefix
//! ([`super::validate::RESERVED_COLUMN_PREFIX`]).

use std::collections::HashMap;

use super::ast::{Expr, FieldDef, GroupByKey, ValueType, group_by_contains};
use super::pg_type::PgType;
use super::registry::lookup_aggregate_function;
use crate::pool::quote_ident;

/// The suffix a target's ledger table adds to the target's name. Validation
/// refuses a target named with it ([`super::validate::ValidationError::ReservedTargetSuffix`]),
/// so one target's ledger can never be another target.
pub const LEDGER_SUFFIX: &str = "__ledger";

/// The suffix a target's group-delta table adds to the target's name (#625
/// F1). Reserved like [`LEDGER_SUFFIX`], and no longer than it, so
/// [`MAX_TARGET_NAME_LEN`] covers both.
pub const DELTAS_SUFFIX: &str = "__deltas";

/// The longest target name whose ledger and delta table names still fit
/// Postgres's 63-byte identifier limit (`NAMEDATALEN - 1`). A longer name
/// would be silently truncated, onto the target's own name at the limit.
pub const MAX_TARGET_NAME_LEN: usize = 63 - LEDGER_SUFFIX.len();
const _: () = assert!(DELTAS_SUFFIX.len() <= LEDGER_SUFFIX.len());

/// The reserved suffixes of the tables Trellis keeps beside a target.
pub const RESERVED_TARGET_SUFFIXES: [&str; 2] = [LEDGER_SUFFIX, DELTAS_SUFFIX];

/// The source row's key, encoded exactly as the ring's `key`
/// ([`super::ddl::pk_key_sql_expr`]).
pub(crate) const KEY_COLUMN: &str = "__from_key";
/// Whether the row is a member of its group. False for a row the
/// definition's filter excludes (no definition has one yet, so every entry
/// the build writes is a member).
pub(crate) const MEMBER_COLUMN: &str = "__member";
/// The `lsn` of the last change applied to the entry. A Re-derive leaves it
/// unchanged (#623 Q1), so the build writes null.
pub(crate) const APPLIED_LSN_COLUMN: &str = "__applied_lsn";
/// The tombstone GC's watermark (#623 Q7): while the entry is a tombstone, a
/// ring segment (`seg_seq`) at or above every batch that can hold a change
/// the entry would refuse. Only a write that leaves the entry a tombstone
/// sets it ([`tombstone_seg_sql`], #775); a live entry's is whatever its last
/// tombstone left, or null, and nothing reads it.
pub(crate) const APPLIED_SEG_COLUMN: &str = "__applied_seg";
/// The `pg_current_snapshot()` of the read that last wrote the entry, taken
/// in the same statement as that read (ADR-0002 I1). A change whose
/// transaction is visible in it is already counted.
pub(crate) const BASIS_COLUMN: &str = "__basis";
/// Whether the key's last applied change deleted it.
pub(crate) const TOMBSTONE_COLUMN: &str = "__tombstone";

/// The value a write gives an entry's [`APPLIED_SEG_COLUMN`], as SQL:
/// `greatest(old, seg)` when the write leaves the entry a tombstone
/// (`tombstone`, the SQL of the entry's new `__tombstone`), the old value
/// otherwise. `old` is the entry's current value (`None` for an insert,
/// which has none) and `seg` the segment the write stamps: a page's latest
/// segment, or for a Re-derive the newest one its read sees, if that is
/// newer (#742).
///
/// **Only a tombstone carries the stamp (#775).** The GC
/// (`staging::retire::collect_tombstones`) reads `applied_seg` only on
/// tombstones, through a partial index keyed on it. An update that changes
/// an indexed column can't be HOT, so if every write moved it, no Apply to a
/// live entry could be. Left alone on a live entry, it changes only when an
/// entry is, or becomes, a tombstone, which changes `__tombstone` (an
/// indexed column too) or is already the rare case.
///
/// **Why a live entry's stale or null stamp is enough** (ADR-0002 I4: a
/// tombstone outlives every batch that could hold a change it refuses). The
/// stamp only has to cover the entry's state once the write that makes or
/// keeps it a tombstone is done, and each such write stamps what that state
/// needs, whatever the entry carried while it was live:
///
/// - **An Apply of a change D** (a delete) stamps at least D's batch, since a
///   page's latest segment holds every batch it drains. The entry then
///   refuses (I2) the changes at or below D's `lsn` and those visible in its
///   `basis`. The first are D's same-key predecessors, which committed before
///   D's trigger ran, so they are in D's batch or an earlier one. For the
///   second, D was applied only because it is not visible in `basis` (or
///   there is none): a change C that is visible completed before that
///   snapshot was taken and D had not, so C completed before D. D's batch's
///   fence sees D, so it sees C too: C is in D's batch or an earlier one. So
///   D's batch covers everything, including a `basis` an earlier Re-derive
///   wrote while the entry was live, unstamped.
/// - **A Re-derive** stamps at least the newest segment its read's snapshot
///   sees (#742), which holds every change that snapshot saw: everything its
///   `basis` refuses, and the change behind the entry's `applied_lsn`, which
///   an earlier page applied and committed before this one took the entry
///   lock and read.
/// - **`greatest`** keeps any higher stamp the entry already had: a stale one
///   (an earlier tombstone's, kept through a revival) only delays the GC.
///
/// A revival (a tombstone becoming live again) keeps the old stamp, and the
/// next write that makes the entry a tombstone stamps it afresh, as above.
/// A GC batch re-checks `__tombstone` and the stamp on the row it locks, so a
/// page that revives a tombstone under it keeps the entry.
pub(crate) fn tombstone_seg_sql(old: Option<&str>, tombstone: &str, seg: &str) -> String {
    match old {
        Some(old) => format!("case when {tombstone} then greatest({old}, {seg}) else {old} end"),
        None => format!("case when {tombstone} then {seg} end"),
    }
}

/// A target's ledger table name.
pub(crate) fn ledger_table_name(target: &str) -> String {
    format!("{target}{LEDGER_SUFFIX}")
}

/// A target's group-delta table name (#625 F1).
pub(crate) fn deltas_table_name(target: &str) -> String {
    format!("{target}{DELTAS_SUFFIX}")
}

/// A target's group-delta table, schema-qualified and quoted for SQL text,
/// from the target's schema and bare name.
pub(crate) fn qualified_deltas_table(target_schema: &str, target: &str) -> String {
    format!(
        "{}.{}",
        quote_ident(target_schema),
        quote_ident(&deltas_table_name(target))
    )
}

/// Empties a target's group-delta table, `qualified_deltas` (quoted): the
/// deltas go wherever the ledger is emptied (#625 F1's B4), since each
/// records a move between entries the emptying discards. Only a target
/// `staging::ledger::route` sends to the ledger has one
/// ([`super::ddl::aggregate_target_table_ddl`]), so the caller asks `route`
/// first, as the DDL did.
pub(crate) async fn truncate_deltas(
    client: &impl tokio_postgres::GenericClient,
    qualified_deltas: &str,
) -> Result<(), tokio_postgres::Error> {
    client
        .batch_execute(&format!("truncate {qualified_deltas}"))
        .await
}

/// A delta row's claim key: an identity, so rows are numbered in insert
/// order, with a btree index the merger claims through, oldest first (#625
/// F2b; see `staging::ledger::merge_statement`).
pub(crate) const DELTA_SEQ_COLUMN: &str = "__seq";

/// A delta row's merge partition (#717): a hash of its group, modulo
/// [`DELTA_PARTITIONS`], generated by the table from the row's `GROUP BY`
/// columns ([`delta_partition_sql`]). Every row of one group is in one
/// partition, so mergers of different partitions write disjoint groups and
/// run side by side without waiting on each other's group rows.
pub(crate) const DELTA_PART_COLUMN: &str = "__part";

/// How many merge partitions a target's group deltas are spread over
/// (#717): the most mergers that can work on one target at once. Twice the
/// default eight drain workers, so a merger looking for a partition usually
/// finds its first choice free and a few hot groups don't leave a worker
/// with nothing to merge; more partitions only add probes to each merger's
/// look for one with rows (`staging::build::merge_deltas`). Baked into each
/// delta table's generated column when the target is created, so every
/// writer of a table agrees; the merger reads which partitions have rows
/// rather than assume this number.
pub(crate) const DELTA_PARTITIONS: i16 = 16;

/// The generation expression of [`DELTA_PART_COLUMN`] over a delta table's
/// `group_columns`: `hash_record_extended` of the group, which hashes each
/// column with its type's own hash function, the one its hash opclass pairs
/// with its `=`. So two spellings of one group (`1.5` and `1.50` of a
/// `numeric`, `0` and `-0` of a float) hash alike, as the target's unique key
/// and the merger's `group by` take them as one group, and a `NULL` column
/// hashes as 0, the same for every `NULL` group. A text rendering would split
/// such a group across partitions.
///
/// `bit varying` (and `bit`) has no hash function, so its column is hashed
/// as `hashtext` of its text, which is exact for it: its text is every bit,
/// in order, and two bit strings are equal only with the same bits and
/// length. A target with no `GROUP BY` has one group, in partition 0.
fn delta_partition_sql(group_columns: &[LedgerColumn]) -> String {
    if group_columns.is_empty() {
        return "0".to_string();
    }
    let hashed: Vec<String> = group_columns
        .iter()
        .map(|c| {
            let column = quote_ident(&c.name);
            match c.pg_type.as_str() {
                "bit" | "varbit" | "bit varying" => format!("hashtext({column}::text)"),
                _ => column,
            }
        })
        .collect();
    let n = DELTA_PARTITIONS;
    format!(
        "((hash_record_extended(row({}), 0) % {n} + {n}) % {n})::smallint",
        hashed.join(", ")
    )
}

/// A delta row's member-count increment column.
pub(crate) const DELTA_MEMBERS_COLUMN: &str = "__dm";

/// A delta row's increment of contribution `i`'s non-null count.
pub(crate) fn delta_count_column(i: usize) -> String {
    format!("__dc{i}")
}

/// A delta row's increment of summed contribution `i`'s sum.
pub(crate) fn delta_sum_column(i: usize) -> String {
    format!("__ds{i}")
}

/// A recomputing target's delta row's flag (#625 F5): some entry the chunk
/// changed counted in the group before it, so a value may have left the
/// group and the merger recomputes it from all of its entries.
pub(crate) const DELTA_OUT_COLUMN: &str = "__out";

/// A recomputing target's delta row's keys (#625 F5): the entries the chunk
/// changed into the group, which an add-only merge folds into the group's
/// `MIN`/`MAX`-style fields by their current values.
pub(crate) const DELTA_KEYS_COLUMN: &str = "__keys";

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

/// The column type of a contribution whose argument has type `value_type`:
/// the argument's own type, except that fixed-length `bit` is declared `bit
/// varying`. A bare `bit` column is `bit(1)`, which rejects every wider
/// argument value (`bit string length 3 does not match type bit(1)`), while
/// `bit varying` holds any width, and `BIT_AND`/`BIT_OR`/`COUNT` over it give
/// what they give over the `bit(n)` source column. It is the trap the
/// registry's `BIT_AND`/`BIT_OR` result type widens around.
pub(crate) fn contribution_pg_type(value_type: ValueType) -> String {
    let declared = match value_type {
        ValueType::Other(PgType::Bit) => ValueType::Other(PgType::VarBit),
        other => other,
    };
    super::ddl::pg_type_name(declared).into_owned()
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
/// their arguments. See [`aggregate_ledger_index_ddl`] for the indexes and
/// `group_index`.
pub(crate) fn aggregate_ledger_ddl(
    qualified_ledger: &str,
    group_columns: &[LedgerColumn],
    contribution_columns: &[LedgerColumn],
    group_index: bool,
) -> String {
    let mut columns = vec![format!("{} text primary key", quote_ident(KEY_COLUMN))];
    columns.extend(group_columns.iter().map(LedgerColumn::render));
    columns.push(format!(
        "{} boolean not null default true",
        quote_ident(MEMBER_COLUMN)
    ));
    columns.extend(contribution_columns.iter().map(LedgerColumn::render));
    columns.push(ordering_state_columns());
    let group_idents: Vec<String> = group_columns.iter().map(|c| quote_ident(&c.name)).collect();
    format!(
        "; create table {qualified_ledger} ({}){}",
        columns.join(", "),
        aggregate_ledger_index_ddl(qualified_ledger, &group_idents, group_index),
    )
}

/// An aggregate ledger's secondary indexes, as statements each prefixed with
/// `; `: the `GROUP BY` index, partial on live members (the only entries a
/// group is a sum of), and the tombstones by `applied_seg` (what
/// `staging::retire::collect_tombstones` reads, #623 D7), so that a GC batch
/// seeks the collectable ones however many are not yet collectable.
/// `group_idents` are the quoted `GROUP BY` columns.
///
/// The `GROUP BY` index is there only with `group_index`
/// (`staging::build::ledger_indexes_groups`, #723): a target the Re-derive
/// build takes, with no recomputed field, never reads its ledger by group.
/// The tombstone index is always there.
///
/// Neither reads a column an Apply to a live entry that keeps its group
/// changes: such an Apply leaves `applied_seg` alone ([`tombstone_seg_sql`]),
/// so it can be HOT (#775).
///
/// Shared by the ledger's DDL and the aggregate build, which drops them for
/// its load and builds them again after it. They are left for Postgres to
/// name: it picks a free name, where `<ledger>_…` could pass the identifier
/// limit and truncate onto the ledger's own name.
pub(crate) fn aggregate_ledger_index_ddl(
    qualified_ledger: &str,
    group_idents: &[String],
    group_index: bool,
) -> String {
    let mut sql = String::new();
    if group_index {
        sql.push_str(&format!(
            "; create index on {qualified_ledger} ({}) where {} and not {}",
            group_idents.join(", "),
            quote_ident(MEMBER_COLUMN),
            quote_ident(TOMBSTONE_COLUMN),
        ));
    }
    sql.push_str(&format!(
        "; create index on {qualified_ledger} ({}) where {}",
        quote_ident(APPLIED_SEG_COLUMN),
        quote_ident(TOMBSTONE_COLUMN),
    ));
    sql
}

/// A ledger-routed aggregate target's group-delta table DDL (#625 F1), as a
/// statement to append to the target's own: the `GROUP BY` columns typed as
/// the target's, the member-count increment, and per contribution `i` its
/// non-null count increment and, when `summed[i]`, its sum increment.
///
/// A build chunk appends one row per group it moved instead of writing the
/// group row, and the merger (`staging::build`) claims, deletes and sums
/// them into the groups. Rows are only ever appended and claimed. Each is
/// numbered by an identity, [`DELTA_SEQ_COLUMN`], and carries its group's
/// merge partition, [`DELTA_PART_COLUMN`] (#717). The index on both lets a
/// merger claim a partition's oldest live rows through the index instead of
/// walking the heap's dead or emptied pages from block 0 (#625 F2b), and
/// find which partitions have rows at all. A target with a
/// recomputed field (`recomputes`) also gets [`DELTA_OUT_COLUMN`] and
/// [`DELTA_KEYS_COLUMN`], what the merger's recompute reads (#625 F5). No primary key:
/// the claim locks rows by `ctid`. Logged: a delta row records an entry move
/// that already committed, so losing it on a crash would lose the move for
/// good.
pub(crate) fn aggregate_deltas_ddl(
    qualified_deltas: &str,
    group_columns: &[LedgerColumn],
    summed: &[bool],
    recomputes: bool,
) -> String {
    let seq = quote_ident(DELTA_SEQ_COLUMN);
    let mut columns = vec![format!("{seq} bigint generated always as identity")];
    columns.extend(group_columns.iter().map(LedgerColumn::render));
    columns.push(format!(
        "{} bigint not null",
        quote_ident(DELTA_MEMBERS_COLUMN)
    ));
    for (i, summed) in summed.iter().enumerate() {
        columns.push(format!(
            "{} bigint not null",
            quote_ident(&delta_count_column(i))
        ));
        if *summed {
            columns.push(format!(
                "{} numeric not null",
                quote_ident(&delta_sum_column(i))
            ));
        }
    }
    if recomputes {
        columns.push(format!(
            "{} boolean not null",
            quote_ident(DELTA_OUT_COLUMN)
        ));
        columns.push(format!("{} text[]", quote_ident(DELTA_KEYS_COLUMN)));
    }
    let part = quote_ident(DELTA_PART_COLUMN);
    columns.push(format!(
        "{part} smallint not null generated always as ({}) stored",
        delta_partition_sql(group_columns)
    ));
    format!(
        "; create table {qualified_deltas} ({}); \
         create index on {qualified_deltas} ({part}, {seq})",
        columns.join(", ")
    )
}

/// A 1-1 target's ledger DDL (#623 Q2 (c)): the key and the ordering state,
/// with no values (the target row holds them), as a statement to append to
/// the target's own. Created empty: the 1-1 build's writes are absolute, and
/// the Re-derives that follow it stamp the entries (#623 D6). The partial
/// index is the tombstone GC's (`staging::retire::collect_tombstones`), keyed
/// on `applied_seg` as an aggregate ledger's is, so that a GC batch seeks the
/// collectable tombstones rather than walking every one above the drained
/// prefix. An Apply to a live entry leaves `applied_seg` alone
/// ([`tombstone_seg_sql`]), so it changes no indexed column and can be a HOT
/// update (#775). The key sorts in `"C"`: every lock is taken in key order,
/// and a byte comparison is far cheaper than a locale's.
pub(crate) fn one_to_one_ledger_ddl(qualified_ledger: &str) -> String {
    format!(
        "; create table {qualified_ledger} ({} text collate \"C\" primary key, {}); \
         create index on {qualified_ledger} ({}) where {}",
        quote_ident(KEY_COLUMN),
        ordering_state_columns(),
        quote_ident(APPLIED_SEG_COLUMN),
        quote_ident(TOMBSTONE_COLUMN),
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

    /// Issue #695: a field named after a source column that another field
    /// aggregates doesn't change what the other aggregates read.
    #[test]
    fn a_field_named_like_a_source_column_shares_the_source_columns_ledger_column() {
        let (contribs, substituted, group_by) = plan(
            "TRANSFORM t FROM s GROUP BY g SELECT g AS g, SUM(val) AS val, MIN(val) AS lo, \
             MAX(val) AS hi",
        );
        assert_eq!(args(&contribs), vec![("__arg0", &col("val"))]);
        assert_eq!(
            super::super::oracle::render_expr_sql(&over_ledger(
                &substituted["lo"],
                &contribs,
                &group_by
            )),
            r#"min("__arg0")"#
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
            true,
        );
        assert_eq!(
            ddl,
            r#"; create table "public"."t__ledger" ("__from_key" text primary key, "g" integer, "__member" boolean not null default true, "__arg0" numeric, "__arg1" text collate "C", "__applied_lsn" pg_lsn, "__applied_seg" bigint, "__basis" pg_snapshot, "__tombstone" boolean not null default false); create index on "public"."t__ledger" ("g") where "__member" and not "__tombstone"; create index on "public"."t__ledger" ("__applied_seg") where "__tombstone""#
        );
    }

    /// Without its `GROUP BY` index (#723), a ledger keeps its tombstone
    /// index, and the rebuild after an old build's load adds the same set.
    #[test]
    fn a_ledger_without_its_group_index_keeps_its_tombstone_index() {
        let ddl = aggregate_ledger_ddl(
            r#""public"."t__ledger""#,
            &[LedgerColumn {
                name: "g".to_string(),
                pg_type: "integer".to_string(),
                collation: None,
            }],
            &[LedgerColumn {
                name: "__arg0".to_string(),
                pg_type: "numeric".to_string(),
                collation: None,
            }],
            false,
        );
        assert_eq!(
            ddl,
            r#"; create table "public"."t__ledger" ("__from_key" text primary key, "g" integer, "__member" boolean not null default true, "__arg0" numeric, "__applied_lsn" pg_lsn, "__applied_seg" bigint, "__basis" pg_snapshot, "__tombstone" boolean not null default false); create index on "public"."t__ledger" ("__applied_seg") where "__tombstone""#
        );
        assert_eq!(
            aggregate_ledger_index_ddl(r#""public"."t__ledger""#, &[r#""g""#.to_string()], false),
            r#"; create index on "public"."t__ledger" ("__applied_seg") where "__tombstone""#
        );
    }

    #[test]
    fn the_one_to_one_ledger_holds_only_the_key_and_the_ordering_state() {
        assert_eq!(
            one_to_one_ledger_ddl(r#""public"."t__ledger""#),
            r#"; create table "public"."t__ledger" ("__from_key" text collate "C" primary key, "__applied_lsn" pg_lsn, "__applied_seg" bigint, "__basis" pg_snapshot, "__tombstone" boolean not null default false); create index on "public"."t__ledger" ("__applied_seg") where "__tombstone""#
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
        assert_eq!(deltas_table_name("totals"), "totals__deltas");
        assert!(deltas_table_name(&"x".repeat(MAX_TARGET_NAME_LEN)).len() <= 63);
    }

    #[test]
    fn the_deltas_table_holds_a_claim_key_the_group_and_one_increment_per_accumulator() {
        assert_eq!(
            aggregate_deltas_ddl(
                r#""public"."t__deltas""#,
                &[LedgerColumn {
                    name: "g".to_string(),
                    pg_type: "integer".to_string(),
                    collation: None,
                }],
                &[true, false],
                false,
            ),
            r#"; create table "public"."t__deltas" ("__seq" bigint generated always as identity, "g" integer, "__dm" bigint not null, "__dc0" bigint not null, "__ds0" numeric not null, "__dc1" bigint not null, "__part" smallint not null generated always as (((hash_record_extended(row("g"), 0) % 16 + 16) % 16)::smallint) stored); create index on "public"."t__deltas" ("__part", "__seq")"#
        );
        assert_eq!(
            aggregate_deltas_ddl(
                r#""public"."t__deltas""#,
                &[LedgerColumn {
                    name: "g".to_string(),
                    pg_type: "integer".to_string(),
                    collation: None,
                }],
                &[false],
                true,
            ),
            r#"; create table "public"."t__deltas" ("__seq" bigint generated always as identity, "g" integer, "__dm" bigint not null, "__dc0" bigint not null, "__out" boolean not null, "__keys" text[], "__part" smallint not null generated always as (((hash_record_extended(row("g"), 0) % 16 + 16) % 16)::smallint) stored); create index on "public"."t__deltas" ("__part", "__seq")"#,
            "a recomputing target's delta rows say whether a value left the group, and \
             which keys entered it (#625 F5)"
        );
    }

    /// Creates a delta table grouped by `columns` (name, type) in `client`'s
    /// database as `public.<table>`, inserts one row per group of `groups`
    /// (each a row of SQL literals), and returns each row's partition, in
    /// the order given.
    async fn partitions_of(
        client: &tokio_postgres::Client,
        table: &str,
        columns: &[(&str, &str)],
        groups: &[&str],
    ) -> Vec<i16> {
        let group_columns: Vec<LedgerColumn> = columns
            .iter()
            .map(|(name, pg_type)| LedgerColumn {
                name: (*name).to_string(),
                pg_type: (*pg_type).to_string(),
                collation: None,
            })
            .collect();
        let qualified = format!("public.{}", quote_ident(table));
        let ddl = aggregate_deltas_ddl(&qualified, &group_columns, &[], false);
        client
            .batch_execute(ddl.trim_start_matches("; "))
            .await
            .unwrap_or_else(|e| panic!("create {table} over {columns:?}: {e}"));
        let mut names: Vec<String> = columns.iter().map(|(name, _)| quote_ident(name)).collect();
        names.push(quote_ident(DELTA_MEMBERS_COLUMN));
        let mut parts = Vec::new();
        for group in groups {
            let row = client
                .query_one(
                    &format!(
                        "insert into {qualified} ({}) values ({group}{}1) returning {}",
                        names.join(", "),
                        if columns.is_empty() { "" } else { ", " },
                        quote_ident(DELTA_PART_COLUMN)
                    ),
                    &[],
                )
                .await
                .unwrap_or_else(|e| panic!("insert ({group}) into {table}: {e}"));
            parts.push(row.get(0));
        }
        parts
    }

    /// A delta row's partition follows its group's own equality (#717):
    /// every pair of spellings of one value Postgres's `=` calls equal (and
    /// so the target's unique key and the merger's `group by` take as one
    /// group) lands in one partition, for every type a `GROUP BY` key may be
    /// and a few it may not, `bit varying` (which has no hash function)
    /// included. Every `NULL` group lands in one partition too, alone or
    /// beside other columns, and a target with no `GROUP BY` puts its one
    /// group in partition 0.
    #[tokio::test]
    async fn equal_groups_with_different_spellings_share_a_merge_partition() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = db.pool.get().await.expect("pool");
        client
            .batch_execute("create type public.mood as enum ('calm', 'busy')")
            .await
            .expect("an enum");
        let cases: &[(&str, &str, &str)] = &[
            ("smallint", "1", "'01'"),
            ("integer", "-7", "'-007'"),
            ("bigint", "42", "'42 '"),
            ("numeric", "1.5", "1.50"),
            ("numeric", "0", "'-0.000'"),
            ("numeric", "'NaN'", "'nan'"),
            ("double precision", "0", "'-0'"),
            ("real", "0", "'-0'"),
            ("text", "'abc'", "'ab' || 'c'"),
            (
                "uuid",
                "'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'",
                "'a0eebc999c0b4ef8bb6d6bb9bd380a11'",
            ),
            ("boolean", "'t'", "'true'"),
            ("oid", "'7'", "'007'"),
            ("bytea", r"'\x41'", "'A'"),
            ("date", "'2020-01-01'", "'Jan 1 2020'"),
            ("time", "'01:02'", "'01:02:00.000'"),
            ("timetz", "'01:02+00'", "'01:02:00+00:00'"),
            (
                "timestamp",
                "'2020-01-01 00:00'",
                "'2020-01-01T00:00:00.000'",
            ),
            (
                "timestamptz",
                "'2020-01-01 00:00+00'",
                "'2020-01-01 01:00+01'",
            ),
            ("interval", "'1 day'", "'24 hours'"),
            ("inet", "'10.0.0.1'", "'10.0.0.1/32'"),
            ("cidr", "'10.0.0.0/8'", "'10/8'"),
            ("macaddr", "'08:00:2b:01:02:03'", "'08002b010203'"),
            (
                "macaddr8",
                "'08:00:2b:01:02:03:04:05'",
                "'08002b0102030405'",
            ),
            ("varbit", "B'101'", "'101'"),
            ("\"public\".\"mood\"", "'busy'", "'busy'::text::public.mood"),
        ];
        for (i, (pg_type, a, b)) in cases.iter().enumerate() {
            let table = format!("t{i}");
            let parts =
                partitions_of(&client, &table, &[("g", pg_type)], &[a, b, "null", "null"]).await;
            let distinct: i64 = client
                .query_one(
                    &format!("select count(distinct g) from public.{table} where g is not null"),
                    &[],
                )
                .await
                .expect("count the groups")
                .get(0);
            assert_eq!(distinct, 1, "{pg_type}: {a} and {b} are one group");
            assert_eq!(
                parts[0], parts[1],
                "{pg_type}: {a} and {b} share a partition"
            );
            assert_eq!(
                parts[2], parts[3],
                "{pg_type}: the NULL group has one partition"
            );
            assert!(
                parts.iter().all(|p| (0..DELTA_PARTITIONS).contains(p)),
                "{pg_type}: {parts:?}"
            );
        }

        let parts = partitions_of(
            &client,
            "two",
            &[("n", "numeric"), ("t", "text")],
            &[
                "1.5, 'x'",
                "1.50, 'x'",
                "null, 'x'",
                "null, 'x'",
                "null, null",
                "null, null",
            ],
        )
        .await;
        assert_eq!(parts[0], parts[1], "{parts:?}");
        assert_eq!(parts[2], parts[3], "{parts:?}");
        assert_eq!(parts[4], parts[5], "{parts:?}");

        assert_eq!(
            partitions_of(&client, "none", &[], &["", ""]).await,
            [0, 0],
            "no GROUP BY: one group, in partition 0"
        );

        client
            .batch_execute(
                "insert into public.\"t1\" (g, __dm) select i, 1 from generate_series(1, 1000) i",
            )
            .await
            .expect("many groups");
        let used: i64 = client
            .query_one("select count(distinct __part) from public.\"t1\"", &[])
            .await
            .expect("count the partitions")
            .get(0);
        assert_eq!(
            used,
            i64::from(DELTA_PARTITIONS),
            "a thousand groups use every partition"
        );
    }
}
