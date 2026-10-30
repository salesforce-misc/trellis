//! A 1-1 `JOIN` transform's apply step (ADR-0021): a page of either side's
//! changes.
//!
//! A 1-1 `JOIN` keeps one target row per `a` row, joined to at most one `b`
//! row, over four hidden tables ([`crate::defs::join_one_to_one`]): `R`, an
//! entry per `a` row on the 1-1 ledger's ordering state, holding the join key
//! and the `a` columns the fields read; `M`, the same per `b` row; `J`, a lock
//! row per join key; and the pair's truncate lock row. Neither side reads the
//! other's source, only its copy under `J`.
//!
//! # One page ([`apply_page`])
//!
//! A page carries changes and Re-derives of either side, in one transaction
//! under `READ COMMITTED`, in the model's order (#1375). Each step is a rule
//! an ablation showed necessary; the plant that removes it is in brackets.
//!
//! 1. **Fold.** Only the newest change per id of each side goes through I2;
//!    a Re-derive of an id replaces that id's changes. A page whose segment
//!    may predate the build re-derives every id
//!    ([`super::ledger::page_may_predate_build`]).
//! 2. **Lock row and key set.** The pair's lock row `FOR SHARE`. Then the key
//!    set K: for each side, every change's new key, the key its entry (`R`
//!    or `M`) holds for each id, and the live key of each Re-derive row (read
//!    unlocked from its table, which keeps the retry below rare); for the `b`
//!    side also each change's old key from its first change's pre-image
//!    [`join1_old_key_unlocked`]. A key with a `NULL` part is left out: it
//!    matches nothing and takes no `J` row.
//! 3. **`J`.** K, both sides' keys together, inserted sorted `on conflict do
//!    nothing`, then locked `FOR UPDATE` in one sorted statement
//!    [`join1_no_j_lock`]. Nothing reads `M`, or `R` for a fan-out, before
//!    this [`join1_m_before_j`].
//! 4. **Entries.** The `R` placeholders and entry lock, then `M`'s
//!    ([`super::one_to_one_ledger::lock_entries`], I5)
//!    [`join1_no_placeholder`, `join1_no_entry_lock`]. After each, every
//!    held key is checked against K: one outside it moved after K was read,
//!    so the page rolls back with [`ApplyError::JoinKeyMoved`] and retries
//!    [`join1_no_recheck`]. The entry lock also orders the writers of a
//!    `NULL`-keyed row, which has no `J` row.
//! 5. **Re-derive read.** For each side, one statement reads its Re-derive
//!    rows live and `pg_current_snapshot()`, which becomes those entries'
//!    basis [`join1_snapshot_after_read`]. A key it reads outside K (the row
//!    moved after the unlocked read) rolls the page back the same way
//!    [`join1_rd_no_key`, `join1_brd_no_key`]: taking that key's `J` late
//!    would break the lock order. A `b` Re-derive's entry fans out like a
//!    `b` change's.
//! 6. **I2.** The entries are updated by ADR-0002's I2 (an Apply must not be
//!    in the entry's basis, must be past its applied position and above the
//!    floor) [`join1_vis_only`, `join1_lsn_only`]; a Re-derive sets the basis
//!    and leaves the applied position alone. Then each side's key and read
//!    columns are written for the entries that changed; a row that is gone
//!    has its key cleared, so it matches nothing.
//! 7. **Recompute and write.** Each changed `a` row's target row is rendered
//!    from `R` and the newest live `M` entry on its key (`LEFT JOIN
//!    LATERAL`); a row that no longer exists, or an `INNER` row with no
//!    match, is deleted. Then the fan-out ([`fan_out`]): every other live `R`
//!    entry on a key a changed `M` entry held before the page or holds after
//!    it is recomputed the same way, in sorted keyset batches of 10k, one
//!    statement each. Fields are rendered SQL, never evaluated in Rust. More
//!    rows than the cap ([`crate::defs::join_one_to_one::max_fanout`]) halt
//!    the transform; so does a field that fails on a fan-out row, once a
//!    bisection under savepoints has named the rows and fields.
//!
//! The lock order is the lock row, `J` sorted, `R` entries sorted, `M`
//! entries sorted, then target rows, sorted within each statement. The
//! target rows are not locked ahead across statements, and need not be: each
//! one is written only by a page that holds `J` for the key its `R` entry
//! has. An `a` page holds `J` for every key its entries hold (the held-key
//! check), and a `b` page reads the entries it fans out to after locking
//! their key, which no writer of an entry can leave or reach without the
//! same lock. So two pages that write one target row queue on a `J` row
//! before either locks it, whatever order their statements take. Nothing
//! reads a 1-1 `JOIN` target in v1 (define refuses it), so a page records
//! nothing for the seam.
//!
//! # A `TRUNCATE` ([`truncate_sides`], [`lock_truncated_definitions`], [`start_rebuilds`])
//!
//! A page that carries a `TRUNCATE` of either side takes the lock row `FOR
//! UPDATE` instead, so it waits for the pages and chunks in flight and every
//! other page waits for it, and settles the truncate before its changes: the
//! side's ledger is emptied and its floor raised, and a `TRUNCATE` of `a`
//! empties the target. `J` keeps its rows. The ledger is emptied by `DELETE`,
//! not `TRUNCATE`: a sweep or `M` pass batch reads `R` or `M` before it takes
//! the lock row, and a `TRUNCATE`'s `ACCESS EXCLUSIVE` lock would wait for
//! that read while the batch waited for the lock row. A `TRUNCATE` of `b`
//! changes every target row, which one transaction would not hold, so the
//! page starts a rebuild of the target and the build's chunks rewrite the
//! rows (a `LEFT` row keeps its old `b` columns until the chunk reaches it).
//! It takes the definitions' rows of every target whose `b` it truncates, in
//! id order, before its first lock row, as a rebuild's start does
//! (`super::build::start_rebuilds`), and starts the rebuilds after it has
//! marked its segments drained, as seal phase 1 takes a segment's row before
//! the segment pointer. The drain's barrier orders the truncate's batch
//! against every other.
//!
//! # A build ([`build`])
//!
//! A fresh build, a `RESUME` or a rebuild in place (`request_backfill`, a
//! catch-up) is made of pages: Re-derive pages of every `b` row without the
//! fan-out, then chunks of `a`'s primary key, then a rebuild sweep of the
//! stale entries and of the target rows no `R` entry backs. Each runs the
//! steps above, so it orders against the drain's pages as a page does.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use tokio_postgres::Transaction;
use tokio_postgres::types::{PgLsn, ToSql};

pub(crate) mod build;

use crate::defs::ast::{Expr, JoinKind, TransformDef};
use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::join::{self as spec, Side};
use crate::defs::join_one_to_one::HiddenTable;
use crate::defs::ledger as schema;
use crate::defs::model::Definition;
use crate::defs::oracle::render_qualified_expr_sql;
use crate::pool::{Pool, quote_ident, quote_literal};

use super::apply::ApplyError;
use super::fold::FoldedChange;
use super::one_to_one_ledger::{self as ledger, EntryChange, LedgerRef};
use super::one_to_one_render::{ImageColumn, OneToOneRender};
use super::target_mutations::TargetMutations;

/// The most `R` entries one target-write statement renders.
const WRITE_BATCH: usize = 10_000;

/// A definition's static shape: the hidden tables, the join key, the columns
/// each hidden ledger holds and the fields rendered over a row of `R` and the
/// newest live row of `M`.
#[derive(Debug, Clone)]
pub(crate) struct JoinShape {
    /// The definition's id: the key of its `transform_definitions` row.
    id: i64,
    /// The target, qualified and unquoted: the pause label of its points.
    pub(crate) target: String,
    /// The `FROM` table, qualified.
    source_table: String,
    /// The joined table, qualified.
    joined_table: String,
    left: bool,
    pk: Vec<PrimaryKeyColumn>,
    r: LedgerRef,
    m: LedgerRef,
    j: String,
    lock: String,
    /// `a`'s join key columns in `ON` order.
    key: Vec<ImageColumn>,
    /// `b`'s join key column names in `ON` order.
    m_key: Vec<String>,
    /// The columns `R` holds besides its ledger: the key, then the `a`
    /// columns the fields read.
    r_columns: Vec<ImageColumn>,
    /// The columns `M` holds besides its ledger.
    m_columns: Vec<ImageColumn>,
    /// Each written field's name and its SQL over the lateral row `s`.
    fields: Vec<(String, String)>,
    /// The columns of `s`: the SQL over `r` and `b`, and the name `fields`
    /// read it by.
    s_columns: Vec<(String, String)>,
}

/// What a field reads of one side, and the SQL name `s` gives it.
fn s_name(side: Side, column: &str) -> String {
    let alias = match side {
        Side::From => "a",
        Side::Joined => "b",
    };
    format!("{alias}.{column}")
}

/// `expr` with each qualified column renamed to its name in `s`
/// ([`s_name`]), and the columns read inserted into `reads`. A bare column
/// can't reach here: define refuses it.
fn qualify(
    def: &TransformDef,
    target: &str,
    expr: &Expr,
    reads: &mut BTreeSet<(Side, String)>,
) -> Result<Expr, ApplyError> {
    let mut sub = |e: &Expr| qualify(def, target, e, reads);
    Ok(match expr {
        Expr::SourceColumn { table, column, .. } => {
            let side = spec::side_of_table(def, table).ok_or_else(|| {
                ApplyError::GroupBasisUnsupported {
                    target: target.to_string(),
                    what: "a field reading a table that is neither side of its 1-1 JOIN",
                }
            })?;
            reads.insert((side, column.clone()));
            Expr::Column(s_name(side, column))
        }
        Expr::Column(_) => {
            return Err(ApplyError::GroupBasisUnsupported {
                target: target.to_string(),
                what: "an unqualified column in a 1-1 JOIN field",
            });
        }
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::TypedLiteral { .. } => expr.clone(),
        Expr::BinaryOp { op, lhs, rhs } => Expr::BinaryOp {
            op: *op,
            lhs: Box::new(sub(lhs)?),
            rhs: Box::new(sub(rhs)?),
        },
        Expr::Negate(operand) => Expr::Negate(Box::new(sub(operand)?)),
        Expr::Cast { expr, target: ty } => Expr::Cast {
            expr: Box::new(sub(expr)?),
            target: *ty,
        },
        Expr::FunctionCall { name, args } => Expr::FunctionCall {
            name: name.clone(),
            args: args.iter().map(&mut sub).collect::<Result<_, _>>()?,
        },
        Expr::Not(_) | Expr::IsNull { .. } | Expr::Case { .. } => {
            match expr.map_logic_children(&mut sub) {
                Some(rebuilt) => rebuilt?,
                None => expr.clone(),
            }
        }
    })
}

/// The cast of the SQL `expr` to `column`'s type, with its collation.
fn cast_to(column: &ImageColumn, expr: &str) -> String {
    let collate = column
        .collation
        .as_ref()
        .map(|name| format!(" collate {name}"))
        .unwrap_or_default();
    format!("({expr})::{}{collate}", column.type_sql)
}

impl JoinShape {
    /// The shape of `definition`, a 1-1 `JOIN`, with the `paused` fields left
    /// out; `pk` is `a`'s primary key.
    pub(crate) async fn load(
        pool: &Pool,
        definition: &Definition,
        pk: Vec<PrimaryKeyColumn>,
        paused: &HashSet<String>,
    ) -> Result<Self, ApplyError> {
        let def = &definition.def;
        let target = definition.target_table.clone();
        let (joined, a_columns, b_columns) = {
            let client = pool.get().await?;
            let joined = crate::defs::catalog::join_table_of(&**client, definition.id)
                .await?
                .ok_or_else(|| ApplyError::GroupBasisUnsupported {
                    target: target.clone(),
                    what: "a 1-1 JOIN with no joined table",
                })?;
            let a = super::one_to_one_render::image_columns(&**client, &definition.source_table)
                .await?;
            let b = super::one_to_one_render::image_columns(&**client, &joined).await?;
            (joined, a, b)
        };
        let by_name = |columns: &[ImageColumn], names: &[String]| -> Vec<ImageColumn> {
            names
                .iter()
                .filter_map(|n| columns.iter().find(|c| &c.name == n).cloned())
                .collect()
        };
        let key_names = spec::join_key(def, Side::From);
        let m_key = spec::join_key(def, Side::Joined);
        let key = by_name(&a_columns, &key_names);
        let read = |side: Side, columns: &[ImageColumn]| -> Vec<ImageColumn> {
            // The key first, then the rest in column order.
            let wanted = spec::side_columns(def, side);
            let key_of_side = spec::join_key(def, side);
            let mut out = by_name(columns, &key_of_side);
            out.extend(
                columns
                    .iter()
                    .filter(|c| wanted.contains(&c.name) && !key_of_side.contains(&c.name))
                    .cloned(),
            );
            out
        };
        let r_columns = read(Side::From, &a_columns);
        let m_columns = read(Side::Joined, &b_columns);

        let mut reads: BTreeSet<(Side, String)> = BTreeSet::new();
        let mut fields = Vec::with_capacity(def.fields.len());
        for field in &def.fields {
            if paused.contains(&field.name) {
                continue;
            }
            let expr = qualify(def, &target, &field.expr, &mut reads)?;
            fields.push((field.name.clone(), render_qualified_expr_sql(&expr, "s")));
        }
        let s_columns = reads
            .iter()
            .map(|(side, column)| {
                let alias = match side {
                    Side::From => "r",
                    Side::Joined => "b",
                };
                (
                    s_name(*side, column),
                    format!("{alias}.{}", quote_ident(column)),
                )
            })
            .collect();
        let left = matches!(spec::spec(def).map(|s| s.kind), Some(JoinKind::Left));
        let schema_name = pool.schema();
        Ok(JoinShape {
            id: definition.id,
            source_table: definition.source_table.clone(),
            joined_table: joined,
            left,
            pk,
            r: LedgerRef::hidden(
                schema_name,
                definition.id,
                HiddenTable::FromEntries,
                &target,
            ),
            m: LedgerRef::hidden(
                schema_name,
                definition.id,
                HiddenTable::JoinedEntries,
                &target,
            ),
            j: HiddenTable::Keys.qualified(schema_name, definition.id),
            lock: HiddenTable::TruncateLock.qualified(schema_name, definition.id),
            key,
            m_key,
            r_columns,
            m_columns,
            fields,
            s_columns,
            target,
        })
    }

    /// The `FROM` table, qualified.
    pub(crate) fn source_table(&self) -> &str {
        &self.source_table
    }

    /// The joined table, qualified.
    pub(crate) fn joined_table(&self) -> &str {
        &self.joined_table
    }

    /// `R`, qualified and quoted.
    pub(crate) fn r_ident(&self) -> &str {
        &self.r.ident
    }

    /// `side`'s table, qualified.
    fn table(&self, side: Side) -> &str {
        match side {
            Side::From => &self.source_table,
            Side::Joined => &self.joined_table,
        }
    }

    /// `side`'s hidden ledger: `R` or `M`.
    fn ledger(&self, side: Side) -> &LedgerRef {
        match side {
            Side::From => &self.r,
            Side::Joined => &self.m,
        }
    }

    /// `side`'s join key column names, in `ON` order.
    fn side_key(&self, side: Side) -> Vec<String> {
        match side {
            Side::From => self.key.iter().map(|c| c.name.clone()).collect(),
            Side::Joined => self.m_key.clone(),
        }
    }

    /// The columns `side`'s ledger holds besides its ordering state.
    fn side_columns(&self, side: Side) -> &[ImageColumn] {
        match side {
            Side::From => &self.r_columns,
            Side::Joined => &self.m_columns,
        }
    }

    /// The text of each join key column of `side`'s ledger row `alias`, cast
    /// to `J`'s type first, so both sides' keys compare as `K`'s texts do.
    fn key_texts(&self, side: Side, alias: &str) -> Vec<String> {
        self.key
            .iter()
            .zip(self.side_key(side))
            .map(|(c, name)| {
                format!(
                    "({})::text",
                    cast_to(c, &format!("{alias}.{}", quote_ident(&name)))
                )
            })
            .collect()
    }

    /// The row of `R`'s entry `r` and the newest live `M` entry on its key
    /// `b` (a `LEFT JOIN LATERAL`), and the lateral row `s` the fields read.
    /// `from` is the relation that supplies `r`. Newest is by applied
    /// position, an entry no Apply has written (a build's or a Re-derive's)
    /// last, then by `b`'s key, as the #1375 model reads it. Two live entries
    /// share a key only while a hand-off between `b` rows is applied out of
    /// order, and the later of its two changes recomputes the key.
    fn joined_sql(&self, from: &str) -> String {
        let on: Vec<String> = self
            .m_key
            .iter()
            .zip(&self.key)
            .map(|(m, r)| format!("m.{} = r.{}", quote_ident(m), quote_ident(&r.name)))
            .collect();
        let s = if self.s_columns.is_empty() {
            "(select 1 as __none) s".to_string()
        } else {
            let cols: Vec<String> = self
                .s_columns
                .iter()
                .map(|(name, sql)| format!("{sql} as {}", quote_ident(name)))
                .collect();
            format!("(select {}) s", cols.join(", "))
        };
        format!(
            "{from} \
             left join lateral (select m.* from {m} m where {on} and not m.{tomb} \
                                order by m.{applied} desc nulls last, \
                                         m.{key} collate \"C\" desc \
                                limit 1) b on true \
             cross join lateral {s}",
            m = self.m.ident,
            on = on.join(" and "),
            tomb = quote_ident(schema::TOMBSTONE_COLUMN),
            applied = quote_ident(schema::APPLIED_LSN_COLUMN),
            key = quote_ident(schema::KEY_COLUMN),
        )
    }

    /// Whether the `r` row has a target row: not a tombstone, and for an
    /// `INNER` join a live `M` entry on its key.
    fn has_target_row_sql(&self) -> String {
        let alive = format!("not r.{}", quote_ident(schema::TOMBSTONE_COLUMN));
        if self.left {
            return alive;
        }
        let on: Vec<String> = self
            .m_key
            .iter()
            .zip(&self.key)
            .map(|(m, r)| format!("m.{} = r.{}", quote_ident(m), quote_ident(&r.name)))
            .collect();
        format!(
            "{alive} and exists (select 1 from {m} m where {on} and not m.{tomb})",
            m = self.m.ident,
            on = on.join(" and "),
            tomb = quote_ident(schema::TOMBSTONE_COLUMN),
        )
    }
}

/// One change of a page, of either side: an Apply of its last change,
/// written from its image (`None` for a delete), or a Re-derive (`apply`
/// `None`), written from the row the page reads under the entry lock.
/// `old_image` is its first change's pre-image, whose key a `b` change locks.
#[derive(Debug, Clone)]
pub(crate) struct Record {
    key: String,
    apply: Option<(PgLsn, String, Option<String>)>,
    old_image: Option<String>,
}

impl Record {
    /// This record as a Re-derive of its row.
    fn rederive(&self) -> Self {
        Record {
            key: self.key.clone(),
            apply: None,
            old_image: self.old_image.clone(),
        }
    }
}

/// A page's `TRUNCATE` of one side or both ([`JoinOneToOnePlan::truncate`]).
#[derive(Debug, Clone, Default)]
struct Truncate {
    /// Each side's truncate's `lsn`: the floor it raises.
    from: Option<PgLsn>,
    joined: Option<PgLsn>,
    hop_gen: i32,
    src_changed: Option<std::time::SystemTime>,
    origin_lsn: Option<PgLsn>,
}

/// One page's work on one 1-1 `JOIN` target: its changes of each side.
#[derive(Debug, Clone)]
pub(crate) struct JoinOneToOnePlan {
    pub(crate) shape: std::sync::Arc<JoinShape>,
    a: Vec<Record>,
    b: Vec<Record>,
    /// The page's `TRUNCATE` of either side, which it settles before its
    /// changes (the module doc).
    truncate: Option<Truncate>,
    /// Whether a changed `M` entry fans out to the `R` entries on its keys.
    /// Only the build's `M` pass ([`build::m_batch`],
    /// [`build::m_null_batch`], [`build::m_gone_batch`]) turns it off: the
    /// build's chunks write every target row after it.
    fan_out: bool,
}

impl JoinOneToOnePlan {
    pub(crate) fn new(shape: JoinShape) -> Self {
        Self {
            shape: std::sync::Arc::new(shape),
            a: Vec::new(),
            b: Vec::new(),
            truncate: None,
            fan_out: true,
        }
    }

    /// Whether the page truncates `b`, so it starts a rebuild of the target.
    fn truncates_b(&self) -> bool {
        self.truncate.as_ref().is_some_and(|t| t.joined.is_some())
    }

    /// Adds a `TRUNCATE` of the table on `side`, `change`, whose ring `lsn`
    /// is `lsn`, the floor it raises.
    pub(crate) fn truncate(&mut self, side: Side, lsn: PgLsn, change: &FoldedChange) {
        let truncate = self.truncate.get_or_insert_with(|| Truncate {
            hop_gen: change.hop_gen,
            src_changed: change.src_changed,
            origin_lsn: change.origin_lsn,
            ..Truncate::default()
        });
        truncate.hop_gen = truncate.hop_gen.max(change.hop_gen);
        truncate.src_changed =
            super::apply::earliest_src_changed(truncate.src_changed, change.src_changed);
        truncate.origin_lsn = super::fold::earliest_origin(truncate.origin_lsn, change.origin_lsn);
        let floor = match side {
            Side::From => &mut truncate.from,
            Side::Joined => &mut truncate.joined,
        };
        *floor = Some(floor.map_or(lsn, |f| f.max(lsn)));
    }

    /// Adds `change` of the table on `side`.
    pub(crate) fn push(&mut self, side: Side, change: &FoldedChange) -> Result<(), ApplyError> {
        let apply = match &change.last_change {
            Some(last) if !change.has_recompute => {
                Some((last.lsn, last.row_txid.clone(), change.new_image.clone()))
            }
            _ => None,
        };
        let record = Record {
            key: change.key.clone(),
            apply,
            old_image: change.old_image.clone(),
        };
        match side {
            Side::From => self.a.push(record),
            Side::Joined => self.b.push(record),
        }
        Ok(())
    }
}

/// Loads the plan for `definition` (a 1-1 `JOIN`) on a page whose first
/// change for it is of the table on `side`, `pk` being that table's primary
/// key. The target is keyed by the `FROM` table's, which a page that reaches
/// the target through the joined table first reads here.
pub(crate) async fn plan_for(
    pool: &Pool,
    definition: &Definition,
    side: Side,
    pk: Vec<PrimaryKeyColumn>,
) -> Result<JoinOneToOnePlan, ApplyError> {
    let pk = match side {
        Side::From => pk,
        Side::Joined => ddl::source_primary_key(pool, &definition.source_table).await?,
    };
    let paused = super::quarantine::paused_columns_for(pool, &definition.def).await?;
    Ok(JoinOneToOnePlan::new(
        JoinShape::load(pool, definition, pk, &paused).await?,
    ))
}

/// The unquoted names of the key tuple `row`, as text: a key with any `NULL`
/// part is `None`.
type KeyText = Vec<String>;

/// The key texts of `rows`' columns from `first`, `None` for a row with a
/// `NULL` part.
fn key_of(row: &tokio_postgres::Row, first: usize, n: usize) -> Option<KeyText> {
    (first..first + n)
        .map(|i| row.get::<_, Option<String>>(i))
        .collect()
}

/// Step 1, the fold: the record each key keeps. A Re-derive of a key
/// replaces its changes, and of two Applies the later one is kept.
fn fold(records: &[Record]) -> BTreeMap<&str, &Record> {
    let mut by_key: BTreeMap<&str, &Record> = BTreeMap::new();
    for record in records {
        by_key
            .entry(&record.key)
            .and_modify(|kept| {
                let supersedes = match (&record.apply, &kept.apply) {
                    (_, None) => false,
                    (None, Some(_)) => true,
                    (Some((lsn, ..)), Some((kept_lsn, ..))) => lsn > kept_lsn,
                };
                if supersedes {
                    *kept = record;
                }
            })
            .or_insert(record);
    }
    by_key
}

/// Settles one page of `plan` in `txn` (the module doc). Returns how many
/// target rows it wrote and deleted.
pub(crate) async fn apply_page(
    txn: &Transaction<'_>,
    plan: &JoinOneToOnePlan,
    first_seg: i64,
    seg_seq: i64,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    let shape = &*plan.shape;

    // 1. Fold.
    let rederived: (Vec<Record>, Vec<Record>);
    let (a_records, b_records): (&[Record], &[Record]) =
        if plan.a.iter().chain(&plan.b).any(|r| r.apply.is_some())
            && super::ledger::page_may_predate_build(txn, &shape.r, first_seg).await?
        {
            rederived = (
                plan.a.iter().map(Record::rederive).collect(),
                plan.b.iter().map(Record::rederive).collect(),
            );
            (&rederived.0, &rederived.1)
        } else {
            (&plan.a, &plan.b)
        };
    let a = fold(a_records);
    let b = fold(b_records);
    let truncate = plan.truncate.as_ref();
    if a.is_empty() && b.is_empty() && truncate.is_none() {
        return Ok((0, 0));
    }
    // Planted bug: apply every change without ADR-0002's I2. See
    // `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let predicate = !crate::plant::fires(
        crate::plant::Plant::StaleOneToOneWrite,
        a.values().chain(b.values()).any(|r| r.apply.is_some()),
    );
    #[cfg(not(any(test, feature = "test-util")))]
    let predicate = true;

    let rederive_keys = |records: &BTreeMap<&str, &Record>| -> Vec<String> {
        records
            .values()
            .filter(|r| r.apply.is_none())
            .map(|r| r.key.clone())
            .collect()
    };
    let a_rederive = rederive_keys(&a);
    let b_rederive = rederive_keys(&b);
    // `b`'s primary key, which a `b` Re-derive reads its rows by.
    let m_pk = if b_rederive.is_empty() {
        Vec::new()
    } else {
        ddl::source_primary_key_in_txn(txn, &shape.joined_table).await?
    };

    // 2. The lock row, a truncate's clear, then K over both sides' keys. A
    // page that truncates `b` holds its definition's row already: the caller
    // takes it before any lock row ([`lock_truncated_definitions`]).
    // Planted bug: the definition's row comes after the lock row. See
    // `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let def_late = crate::plant::fires(
        crate::plant::Plant::Join1TruncateDefAfterLock,
        plan.truncates_b(),
    );
    #[cfg(not(any(test, feature = "test-util")))]
    let def_late = false;
    // Planted bug: a truncate's page takes the lock row `FOR SHARE`, so a
    // page of the pair runs beside it. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let share = crate::plant::fires(
        crate::plant::Plant::Join1TruncateShareLock,
        truncate.is_some(),
    );
    #[cfg(not(any(test, feature = "test-util")))]
    let share = false;
    let mode = if truncate.is_some() && !share {
        "update"
    } else {
        "share"
    };
    txn.execute(&format!("select 1 from {} for {mode}", shape.lock), &[])
        .await?;
    if def_late {
        txn.execute(
            "select 1 from transform_definitions where id = $1 for update",
            &[&shape.id],
        )
        .await?;
    }
    let cleared = match truncate {
        Some(truncate) => truncate_sides(txn, shape, truncate, mutations).await?,
        None => 0,
    };
    let mut k: BTreeSet<KeyText> = BTreeSet::new();
    if !a.is_empty() {
        let live = live_keys(txn, shape, Side::From, &shape.pk, &a_rederive).await?;
        k.extend(key_set(txn, shape, Side::From, &a, &live).await?);
    }
    if !b.is_empty() {
        let live = live_keys(txn, shape, Side::Joined, &m_pk, &b_rederive).await?;
        k.extend(key_set(txn, shape, Side::Joined, &b, &live).await?);
    }
    // Test-only pause point. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::BeforeJoin1Lock,
        &shape.target,
    )
    .await?;

    // 3. `J`, one sorted statement over both sides' keys.
    // Planted bug: the page reads `M` and `R` before it locks `J`. See
    // `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let late_j = crate::plant::fires(crate::plant::Plant::Join1MBeforeJ, !k.is_empty());
    #[cfg(not(any(test, feature = "test-util")))]
    let late_j = false;
    if !late_j {
        lock_keys(txn, shape, &k).await?;
    }

    // 4. The `R` entries, then the `M` entries.
    let (a_inserted, _) = lock_side(txn, shape, Side::From, &a, seg_seq, predicate, &k).await?;
    let (b_inserted, b_held) =
        lock_side(txn, shape, Side::Joined, &b, seg_seq, predicate, &k).await?;

    // 5. The Re-derive reads, one statement per side.
    let a_read = rederive_read(txn, shape, Side::From, &shape.pk, &a_rederive, &k, seg_seq).await?;
    let b_read = rederive_read(txn, shape, Side::Joined, &m_pk, &b_rederive, &k, seg_seq).await?;

    // 6. I2, then each side's copied columns.
    let a_changed =
        update_side(txn, shape, Side::From, &a, &a_inserted, &a_read, predicate).await?;
    let b_changed = update_side(
        txn,
        shape,
        Side::Joined,
        &b,
        &b_inserted,
        &b_read,
        predicate,
    )
    .await?;

    // 7. Recompute and write: the `a` changes' rows, then the fan-out.
    // Test-only pause point. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::BeforeJoin1Recompute,
        &shape.target,
    )
    .await?;
    let mut changed_keys: Vec<&str> = a_changed.iter().map(String::as_str).collect();
    changed_keys.sort_unstable();
    let (written, deleted) = write_target(txn, shape, &changed_keys, mutations).await?;
    let fan_keys = if plan.fan_out {
        fan_out_keys(txn, shape, &b_changed, &b_held).await?
    } else {
        BTreeSet::new()
    };
    let (fanned, fan_deleted) = fan_out(txn, shape, &fan_keys, &a_changed, mutations).await?;
    if late_j {
        lock_keys(txn, shape, &k).await?;
    }
    Ok((written + fanned, deleted + fan_deleted + cleared))
}

/// Locks `FOR UPDATE` the definitions' rows of `plans` (one page's) whose
/// page truncates `b`, in id order, and returns their ids for
/// [`start_rebuilds`]. The page calls it before its first 1-1 `JOIN` lock
/// row, since a rebuild's start takes every definition's row it rebuilds, in
/// id order, before it takes their lock rows (`crate::intake::catch_up`,
/// `super::build::stamp_rebuild_seg`): a page that held one target's lock
/// row while it queued on another's definition row would close a cycle with
/// a start of both.
pub(crate) async fn lock_truncated_definitions<'a>(
    txn: &Transaction<'_>,
    plans: impl IntoIterator<Item = &'a JoinOneToOnePlan>,
) -> Result<Vec<i64>, ApplyError> {
    let mut ids: Vec<i64> = plans
        .into_iter()
        .filter(|plan| plan.truncates_b())
        .map(|plan| plan.shape.id)
        .collect();
    ids.sort_unstable();
    ids.dedup();
    // Planted bug: each definition's row is taken after its lock row, in
    // `apply_page`. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let late = crate::plant::fires(
        crate::plant::Plant::Join1TruncateDefAfterLock,
        !ids.is_empty(),
    );
    #[cfg(not(any(test, feature = "test-util")))]
    let late = false;
    if !ids.is_empty() && !late {
        txn.execute(
            "select 1 from transform_definitions where id = any($1) order by id for update",
            &[&ids],
        )
        .await?;
    }
    Ok(ids)
}

/// A page's `TRUNCATE` of one side or both, under the lock row held `FOR
/// UPDATE`, before the page's changes: each truncated side's ledger is
/// emptied and its floor raised to the truncate's `lsn`, so a change from
/// before it that still reaches a page is refused (ADR-0002 I2); and a
/// truncated `a` empties the target, through the seam. `J` keeps its rows,
/// as it keeps every key either table has held. A truncated `b` leaves the
/// target to the rebuild [`start_rebuilds`] starts. Returns how many target
/// rows it deleted.
async fn truncate_sides(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    truncate: &Truncate,
    mutations: &mut TargetMutations,
) -> Result<usize, ApplyError> {
    for (side, lsn) in [(Side::From, truncate.from), (Side::Joined, truncate.joined)] {
        let Some(lsn) = lsn else { continue };
        let ledger = shape.ledger(side);
        // Planted bugs: the entries stay, or the floor doesn't rise. See
        // `crate::plant`.
        #[cfg(any(test, feature = "test-util"))]
        let (keep, no_floor) = (
            crate::plant::fires(
                match side {
                    Side::From => crate::plant::Plant::Join1TruncateKeepsR,
                    Side::Joined => crate::plant::Plant::Join1TruncateKeepsM,
                },
                true,
            ),
            crate::plant::fires(crate::plant::Plant::Join1TruncateNoFloor, true),
        );
        #[cfg(not(any(test, feature = "test-util")))]
        let (keep, no_floor) = (false, false);
        // A `DELETE`, not a `TRUNCATE`: a sweep or `M` pass batch reads the
        // ledger before it takes the lock row this page holds, and would wait
        // for the page while a `TRUNCATE` waited for its read (the module
        // doc).
        if !keep {
            txn.batch_execute(&format!("delete from {}", ledger.ident))
                .await?;
        }
        if !no_floor {
            ledger::raise_floor(txn, ledger, lsn).await?;
        }
    }
    if truncate.from.is_none() {
        return Ok(0);
    }
    super::apply::clear_target(
        txn,
        &shape.target,
        &shape.pk,
        truncate.hop_gen,
        truncate.src_changed,
        truncate.origin_lsn,
        mutations,
    )
    .await
}

/// Starts the rebuilds of the targets `ids` ([`lock_truncated_definitions`])
/// after a `TRUNCATE` of their `b`, in the page's transaction (ADR-0021):
/// every target row may hold the `b` side of a row that is gone, so the
/// build's chunks, not the page, rewrite them. Each definition reads
/// `backfilling` from the commit, and keeps applying; a `LEFT` row keeps its
/// old `b` columns, and an `INNER` row its place, until the rebuild reaches
/// it. A definition that doesn't apply any more (paused since the page read
/// the catalog) is left to its `RESUME`, whose build recreates `R` and `M`
/// and re-derives every row. The caller holds the definitions' rows and
/// their lock rows `FOR UPDATE`, and calls this after it has locked its
/// segments' rows to mark them drained: the start's segment stamp takes the
/// segment pointer `FOR SHARE`, and seal phase 1 takes the sealed segment's
/// predecessor's row before it updates the pointer.
pub(crate) async fn start_rebuilds(txn: &Transaction<'_>, ids: &[i64]) -> Result<(), ApplyError> {
    // Planted bug: the truncate starts no rebuild. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    if crate::plant::fires(crate::plant::Plant::Join1TruncateNoRebuild, !ids.is_empty()) {
        return Ok(());
    }
    if ids.is_empty() {
        return Ok(());
    }
    let ids = super::build::rebuildable(txn, ids).await?;
    super::build::start_rebuilds(txn, &ids).await?;
    super::build::stamp_rebuild_seg(txn, &ids).await?;
    Ok(())
}

/// The live join key of each of `keys`' rows of `side`'s table (whose
/// primary key is `pk`), read unlocked: the Re-derives' part of K. A key is
/// its columns' text; a row with a `NULL` part, or no row, contributes none.
async fn live_keys(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    side: Side,
    pk: &[PrimaryKeyColumn],
    keys: &[String],
) -> Result<Vec<KeyText>, ApplyError> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
    let names = shape.side_key(side);
    let query = super::apply::live_rows_query(shape.table(side), pk, &names, &keys)?;
    let cols: Vec<String> = names
        .iter()
        .map(|n| format!("m.doc ->> {}", quote_literal(n)))
        .collect();
    let sql = format!("select {} from ({}) m", cols.join(", "), query.docs);
    let rows = super::ledger::query_by_entry_key(txn, &sql, &query.params()).await?;
    Ok(rows
        .iter()
        .filter_map(|row| key_of(row, 0, names.len()))
        .collect())
}

/// `side`'s part of K: every image's new key, for a `b` change also its old
/// key from its first change's pre-image, the key the side's entry holds for
/// each of `records`, and the `live` keys, as the sorted set of their texts.
/// Each key is cast to `J`'s types. A key with a `NULL` part is left out.
async fn key_set(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    side: Side,
    records: &BTreeMap<&str, &Record>,
    live: &[KeyText],
) -> Result<BTreeSet<KeyText>, ApplyError> {
    let n = shape.key.len();
    let names = shape.side_key(side);
    // Planted bug: only the changes' new keys are locked. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let only_new = crate::plant::fires(crate::plant::Plant::Join1OldKeyUnlocked, true);
    #[cfg(not(any(test, feature = "test-util")))]
    let only_new = false;
    let keys: Vec<&str> = records.keys().copied().collect();
    let images: Vec<Option<&str>> = records
        .values()
        .map(|r| match &r.apply {
            Some((_, _, Some(image))) => Some(image.as_str()),
            _ => None,
        })
        .collect();
    let old_images: Vec<Option<&str>> = records
        .values()
        .map(|r| match side {
            Side::Joined if !only_new => r.old_image.as_deref(),
            _ => None,
        })
        .collect();
    let live_columns: Vec<Vec<&str>> = (0..n)
        .map(|i| live.iter().map(|k| k[i].as_str()).collect())
        .collect();
    let aliases =
        |prefix: &str| -> Vec<String> { (0..n).map(|i| format!("{prefix}{i}")).collect() };
    let typed = |expr: &dyn Fn(usize, &str) -> String| -> Vec<String> {
        shape
            .key
            .iter()
            .zip(&names)
            .enumerate()
            .map(|(i, (c, name))| format!("{} as k{i}", cast_to(c, &expr(i, name))))
            .collect()
    };
    let from_image = typed(&|_, name| format!("ch.img ->> {}", quote_literal(name)));
    let from_held = typed(&|_, name| format!("l.{}", quote_ident(name)));
    let from_live = typed(&|i, _| format!("lv.c{i}"));
    let live_arrays: Vec<String> = (0..n).map(|i| format!("${}::text[]", 4 + i)).collect();
    let texts: Vec<String> = aliases("k").iter().map(|k| format!("{k}::text")).collect();
    let present: Vec<String> = aliases("k")
        .iter()
        .map(|k| format!("{k} is not null"))
        .collect();
    let held = if only_new {
        "$1::text[] = '{}'::text[] and"
    } else {
        ""
    };
    let sql = format!(
        "with ch as (select u.img::jsonb as img from unnest($2::text[]) as u(img) \
                     where u.img is not null \
                     union all \
                     select u.img::jsonb from unnest($3::text[]) as u(img) \
                     where u.img is not null), \
         cand as ( \
             select {from_image} from ch \
             union \
             select {from_held} from {ledger} l where {held} l.{key} = any($1::text[]) \
             union \
             select {from_live} from unnest({live_arrays}) as lv({live_names}) \
         ) \
         select {texts} from cand where {present}",
        from_image = from_image.join(", "),
        from_held = from_held.join(", "),
        from_live = from_live.join(", "),
        live_arrays = live_arrays.join(", "),
        live_names = aliases("c").join(", "),
        texts = texts.join(", "),
        present = present.join(" and "),
        ledger = shape.ledger(side).ident,
        key = quote_ident(schema::KEY_COLUMN),
    );
    let mut params: Vec<&(dyn ToSql + Sync)> = vec![&keys, &images, &old_images];
    for column in &live_columns {
        params.push(column);
    }
    let rows = super::ledger::query_by_entry_key(txn, &sql, &params).await?;
    Ok(rows.iter().filter_map(|row| key_of(row, 0, n)).collect())
}

/// `keys` as one text array per key column, for [`key_unnest`].
fn key_arrays(n: usize, keys: &BTreeSet<KeyText>) -> Vec<Vec<&str>> {
    (0..n)
        .map(|i| keys.iter().map(|key| key[i].as_str()).collect())
        .collect()
}

/// The `unnest` of [`key_arrays`] bound from parameter `$1` as `u(c0, ..)`,
/// and each column cast to its `J` type.
fn key_unnest(shape: &JoinShape) -> (String, Vec<String>) {
    let n = shape.key.len();
    let arrays: Vec<String> = (0..n).map(|i| format!("${}::text[]", i + 1)).collect();
    let unnest = format!(
        "unnest({}) as u({})",
        arrays.join(", "),
        (0..n)
            .map(|i| format!("c{i}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let casts = shape
        .key
        .iter()
        .enumerate()
        .map(|(i, c)| cast_to(c, &format!("u.c{i}")))
        .collect();
    (unnest, casts)
}

/// Inserts K into `J` sorted and locks it, in one sorted statement each:
/// ADR-0021's insert-then-lock, which is exact because `J` rows are never
/// deleted.
async fn lock_keys(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    k: &BTreeSet<KeyText>,
) -> Result<(), ApplyError> {
    let n = shape.key.len();
    let columns = key_arrays(n, k);
    let params: Vec<&(dyn ToSql + Sync)> = columns.iter().map(|c| c as _).collect();
    let names: Vec<String> = shape.key.iter().map(|c| quote_ident(&c.name)).collect();
    let (unnest, casts) = key_unnest(shape);
    let ordinals: Vec<String> = (1..=n).map(|i| i.to_string()).collect();
    if !k.is_empty() {
        txn.execute(
            &format!(
                "insert into {j} ({names}) select {casts} from {unnest} \
                 order by {ordinals} on conflict do nothing",
                j = shape.j,
                names = names.join(", "),
                casts = casts.join(", "),
                ordinals = ordinals.join(", "),
            ),
            &params,
        )
        .await?;
    }
    // Test-only pause point. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterJoin1KeyInsert,
        &shape.target,
    )
    .await?;
    // Planted bug: the page doesn't lock `J`. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let skip = crate::plant::fires(crate::plant::Plant::Join1NoJLock, !k.is_empty());
    #[cfg(not(any(test, feature = "test-util")))]
    let skip = false;
    if !k.is_empty() && !skip {
        let qualified: Vec<String> = names.iter().map(|n| format!("j.{n}")).collect();
        txn.execute(
            &format!(
                "select 1 from {j} j where ({qualified}) in (select {casts} from {unnest}) \
                 order by {qualified} for update of j",
                j = shape.j,
                qualified = qualified.join(", "),
                casts = casts.join(", "),
            ),
            &params,
        )
        .await?;
    }
    // Test-only pause point. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterJoinLock,
        &shape.target,
    )
    .await?;
    Ok(())
}

/// Step 4 for one side: `records`' placeholders and sorted entry lock
/// ([`super::one_to_one_ledger::lock_entries`], I5), then the check that each
/// held key is in `k` ([`check_held_keys`]). Returns the keys whose entry it
/// inserted, and the key each entry held under the lock.
async fn lock_side(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    side: Side,
    records: &BTreeMap<&str, &Record>,
    seg_seq: i64,
    predicate: bool,
    k: &BTreeSet<KeyText>,
) -> Result<(ledger::Inserted, HashMap<String, KeyText>), ApplyError> {
    if records.is_empty() {
        return Ok((ledger::Inserted::default(), HashMap::new()));
    }
    let to_lock: Vec<EntryChange<'_>> = records
        .values()
        .map(|r| EntryChange {
            key: &r.key,
            apply: r.apply.as_ref().map(|(lsn, txid, _)| (*lsn, txid.as_str())),
            present: r.apply.as_ref().is_none_or(|(_, _, image)| image.is_some()),
        })
        .collect();
    // Planted bug: the page doesn't lock the entries it reads. See
    // `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let skip_lock = crate::plant::fires(crate::plant::Plant::Join1NoEntryLock, true);
    #[cfg(not(any(test, feature = "test-util")))]
    let skip_lock = false;
    let ledger = shape.ledger(side);
    let inserted =
        ledger::lock_entries(txn, ledger, &to_lock, seg_seq, predicate, skip_lock).await?;
    let keys: Vec<&str> = records.keys().copied().collect();
    let held = check_held_keys(txn, shape, side, &keys, k).await?;
    // Test-only pause point. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        match side {
            Side::From => super::interleave::PausePoint::AfterJoin1REntryLock,
            Side::Joined => super::interleave::PausePoint::AfterJoin1MEntryLock,
        },
        &ledger.label,
    )
    .await?;
    Ok((inserted, held))
}

/// Checks that the key each `keys` entry of `side`'s ledger holds, now that
/// the page has locked them, is in `k`: one outside it moved after `k` was
/// read. Returns each entry's held key (none for a `NULL` part).
async fn check_held_keys(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    side: Side,
    keys: &[&str],
    k: &BTreeSet<KeyText>,
) -> Result<HashMap<String, KeyText>, ApplyError> {
    let n = shape.key.len();
    let rows = super::ledger::query_by_entry_key(
        txn,
        &format!(
            "select l.{key}, {texts} from {ledger} l where l.{key} = any($1::text[])",
            texts = shape.key_texts(side, "l").join(", "),
            ledger = shape.ledger(side).ident,
            key = quote_ident(schema::KEY_COLUMN)
        ),
        &[&keys],
    )
    .await?;
    let held: HashMap<String, KeyText> = rows
        .iter()
        .filter_map(|row| Some((row.get::<_, String>(0), key_of(row, 1, n)?)))
        .collect();
    // Test-only: a key that moves on every attempt. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    if side == Side::From && super::interleave::take_forced_join_key_move() {
        super::interleave::note_join_key_moved();
        return Err(ApplyError::JoinKeyMoved {
            target: shape.target.clone(),
        });
    }
    // Planted bug: the page doesn't check its held keys. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let skip = crate::plant::fires(crate::plant::Plant::Join1NoRecheck, !rows.is_empty());
    #[cfg(not(any(test, feature = "test-util")))]
    let skip = false;
    if !skip && held.values().any(|key| !k.contains(key)) {
        #[cfg(any(test, feature = "test-util"))]
        super::interleave::note_join_key_moved();
        return Err(ApplyError::JoinKeyMoved {
            target: shape.target.clone(),
        });
    }
    Ok(held)
}

/// What one side's Re-derive read found ([`rederive_read`]): each Re-derive
/// key's live row (`None` for a row that is gone), the snapshot that is
/// their basis, and the segment their entries are stamped with.
#[derive(Default)]
struct Rederived {
    rows: HashMap<String, Option<String>>,
    snapshot: Option<String>,
    seg: i64,
}

/// Step 5 for one side: one statement reads the live rows of `keys` (the
/// side's Re-derives) from its table, whose primary key is `pk`, and
/// `pg_current_snapshot()` ([`super::one_to_one_ledger::read_rows`]). A key
/// a row has that isn't in `k` moved after the unlocked read, so the page
/// rolls back with [`ApplyError::JoinKeyMoved`] and retries. No `keys` read
/// nothing, and the entries are stamped with the page's `seg_seq`.
async fn rederive_read(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    side: Side,
    pk: &[PrimaryKeyColumn],
    keys: &[String],
    k: &BTreeSet<KeyText>,
    seg_seq: i64,
) -> Result<Rederived, ApplyError> {
    if keys.is_empty() {
        return Ok(Rederived {
            seg: seg_seq,
            ..Rederived::default()
        });
    }
    let names: Vec<String> = shape
        .side_columns(side)
        .iter()
        .map(|c| c.name.clone())
        .collect();
    let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    // The `a` side's pause points carry the target's label, the `b` side's
    // `M`'s.
    let label = match side {
        Side::From => shape.target.as_str(),
        Side::Joined => shape.m.label.as_str(),
    };
    let read = ledger::read_rows(txn, label, shape.table(side), pk, &names, &refs).await?;
    // Planted bugs: the page doesn't check the keys its read of one side
    // found. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let unchecked = crate::plant::fires(
        match side {
            Side::From => crate::plant::Plant::Join1RdNoKey,
            Side::Joined => crate::plant::Plant::Join1BrdNoKey,
        },
        true,
    );
    #[cfg(not(any(test, feature = "test-util")))]
    let unchecked = false;
    if !unchecked {
        check_read_keys(shape, side, &read.rows, k)?;
    }
    // Planted bug: the basis is a snapshot taken one statement after the
    // read. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let read = if crate::plant::fires(crate::plant::Plant::Join1SnapshotAfterRead, true) {
        let later: String = txn
            .query_one("select pg_catalog.pg_current_snapshot()::text", &[])
            .await?
            .get(0);
        ledger::ReadRows {
            snapshot: later,
            ..read
        }
    } else {
        read
    };
    // Test-only pause point. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(txn, super::interleave::PausePoint::AfterJoin1Read, label).await?;
    let mut found = read.rows;
    Ok(Rederived {
        rows: keys
            .iter()
            .map(|key| (key.clone(), found.remove(key)))
            .collect(),
        snapshot: Some(read.snapshot),
        seg: seg_seq.max(read.seg),
    })
}

/// Checks that the join key of each live row of `side` a Re-derive read is
/// in `k`. Define gives each `ON` pair one type and collation, so a `b`
/// column's text is the text its `J` key has.
fn check_read_keys(
    shape: &JoinShape,
    side: Side,
    rows: &HashMap<String, String>,
    k: &BTreeSet<KeyText>,
) -> Result<(), ApplyError> {
    let names = shape.side_key(side);
    for doc in rows.values() {
        let value: serde_json::Value =
            serde_json::from_str(doc).map_err(|_| ApplyError::JoinKeyMoved {
                target: shape.target.clone(),
            })?;
        let key: Option<KeyText> = names
            .iter()
            .map(|name| value.get(name).and_then(|v| v.as_str()).map(str::to_string))
            .collect();
        if let Some(key) = key
            && !k.contains(&key)
        {
            #[cfg(any(test, feature = "test-util"))]
            super::interleave::note_join_key_moved();
            return Err(ApplyError::JoinKeyMoved {
                target: shape.target.clone(),
            });
        }
    }
    Ok(())
}

/// Step 6 for one side: `records`' entries updated by I2 (a Re-derive sets
/// the basis from its read's snapshot), the placeholders nothing wrote
/// dropped, then the side's key and read columns written for the entries
/// that changed: a present row from its image or the Re-derive's read
/// (`read`), and a gone one with its key cleared, so it matches nothing.
/// Returns the keys that changed.
async fn update_side(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    side: Side,
    records: &BTreeMap<&str, &Record>,
    inserted: &ledger::Inserted,
    read: &Rederived,
    predicate: bool,
) -> Result<HashSet<String>, ApplyError> {
    if records.is_empty() {
        return Ok(HashSet::new());
    }
    let (rederived, snapshot, entry_seg) = (&read.rows, read.snapshot.as_deref(), read.seg);
    let ledger = shape.ledger(side);
    let mut changes: Vec<EntryChange<'_>> = Vec::with_capacity(records.len());
    for record in records.values() {
        if record.apply.is_some() && inserted.keys.contains(&record.key) {
            continue;
        }
        let present = match &record.apply {
            Some((_, _, image)) => image.is_some(),
            None => match rederived.get(record.key.as_str()) {
                Some(row) => row.is_some(),
                None => continue,
            },
        };
        changes.push(EntryChange {
            key: &record.key,
            apply: record
                .apply
                .as_ref()
                .map(|(lsn, txid, _)| (*lsn, txid.as_str())),
            present,
        });
    }
    let mut changed =
        ledger::update_entries(txn, ledger, &changes, snapshot, entry_seg, predicate).await?;
    changed.extend(inserted.applied.iter().cloned());
    let unwritten: Vec<&str> = inserted
        .keys
        .iter()
        .filter(|key| !changed.contains(*key))
        .map(String::as_str)
        .collect();
    ledger::drop_placeholders(txn, ledger, &unwritten).await?;

    let mut present_keys: Vec<&str> = Vec::new();
    let mut images: Vec<&str> = Vec::new();
    let mut gone: Vec<&str> = Vec::new();
    for record in records.values() {
        if !changed.contains(&record.key) {
            continue;
        }
        let image = match &record.apply {
            Some((_, _, image)) => image.as_deref(),
            None => rederived
                .get(record.key.as_str())
                .and_then(|row| row.as_deref()),
        };
        match image {
            Some(image) => {
                present_keys.push(&record.key);
                images.push(image);
            }
            None => gone.push(&record.key),
        }
    }
    write_entries(txn, shape, side, &present_keys, &images, &gone).await?;
    Ok(changed)
}

/// Writes `side`'s key and read columns for the entries that changed:
/// `present` ones from their `images`, and the key cleared for the `gone`.
async fn write_entries(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    side: Side,
    present: &[&str],
    images: &[&str],
    gone: &[&str],
) -> Result<(), ApplyError> {
    let key = quote_ident(schema::KEY_COLUMN);
    let ledger = &shape.ledger(side).ident;
    if !present.is_empty() {
        let columns = shape.side_columns(side);
        let sets: Vec<String> = columns
            .iter()
            .map(|c| format!("{n} = s.{n}", n = quote_ident(&c.name)))
            .collect();
        let render = OneToOneRender::of_columns(columns.to_vec());
        super::ledger::query_by_entry_key(
            txn,
            &format!(
                "update {ledger} l set {sets} \
                 from unnest($1::text[], $2::text[]::jsonb[]) as v(__key, __img) \
                      cross join {row} \
                 where l.{key} = any($1::text[]) and l.{key} = v.__key",
                sets = sets.join(", "),
                row = render.row_sql("v.__img"),
            ),
            &[&present, &images],
        )
        .await?;
    }
    if !gone.is_empty() {
        let sets: Vec<String> = shape
            .side_key(side)
            .iter()
            .map(|c| format!("{} = null", quote_ident(c)))
            .collect();
        super::ledger::query_by_entry_key(
            txn,
            &format!(
                "update {ledger} l set {sets} where l.{key} = any($1::text[])",
                sets = sets.join(", "),
            ),
            &[&gone],
        )
        .await?;
    }
    Ok(())
}

/// The keys a page's `b` changes fan out on: each changed `M` entry's key
/// before the page (`held`, read under its entry lock) and after it (read
/// back from `M`). Every one is in K, so the page holds its `J` lock.
async fn fan_out_keys(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    changed: &HashSet<String>,
    held: &HashMap<String, KeyText>,
) -> Result<BTreeSet<KeyText>, ApplyError> {
    if changed.is_empty() {
        return Ok(BTreeSet::new());
    }
    let mut keys: BTreeSet<KeyText> = changed
        .iter()
        .filter_map(|k| held.get(k))
        .cloned()
        .collect();
    let changed: Vec<&str> = changed.iter().map(String::as_str).collect();
    let rows = super::ledger::query_by_entry_key(
        txn,
        &format!(
            "select {texts} from {m} l where l.{key} = any($1::text[]) and not l.{tomb}",
            texts = shape.key_texts(Side::Joined, "l").join(", "),
            m = shape.m.ident,
            key = quote_ident(schema::KEY_COLUMN),
            tomb = quote_ident(schema::TOMBSTONE_COLUMN),
        ),
        &[&changed],
    )
    .await?;
    keys.extend(
        rows.iter()
            .filter_map(|row| key_of(row, 0, shape.key.len())),
    );
    Ok(keys)
}

/// The most attempts [`fan_out_failure`]'s bisection makes, as
/// `super::quarantine::MAX_ISOLATION_PROBES` bounds a page's.
const MAX_FAN_OUT_PROBES: usize = 256;

/// The most failing rows [`fan_out_failure`] names.
const MAX_NAMED_FAN_OUT_ROWS: usize = 8;

/// Step 9 of the page order (#1396's body), the fan-out: the target row of
/// every live `R` entry on `keys` is recomputed from `R` and the newest live
/// `M` entry on its key, but the `written` ones, which the page's `a` changes
/// wrote already. The page holds `J` for each of `keys` (they are in K), and
/// every writer of an `R` entry on a key, or of an `M` entry on it, holds that
/// key's `J` row too, so the entries read here are the ones the key has until
/// the page commits.
///
/// More entries than the cap ([`crate::defs::join_one_to_one::max_fanout`])
/// halt the transform with the reason; the entries are read with a `limit`
/// one past it. The rest are written in sorted keyset batches of
/// [`WRITE_BATCH`], one statement each ([`write_target`]), under one
/// savepoint. A data exception rolls the fan-out back to it and bisects the
/// batches down to the rows whose fields fail ([`fan_out_failure`]).
async fn fan_out(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    keys: &BTreeSet<KeyText>,
    written: &HashSet<String>,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    if keys.is_empty() {
        return Ok((0, 0));
    }
    let cap = crate::defs::join_one_to_one::max_fanout();
    let columns = key_arrays(shape.key.len(), keys);
    let params: Vec<&(dyn ToSql + Sync)> = columns.iter().map(|c| c as _).collect();
    let (unnest, casts) = key_unnest(shape);
    let qualified: Vec<String> = shape
        .key
        .iter()
        .map(|c| format!("r.{}", quote_ident(&c.name)))
        .collect();
    let rows = super::ledger::query_by_entry_key(
        txn,
        &format!(
            "select r.{key} from {r} r \
             where ({qualified}) in (select {casts} from {unnest}) and not r.{tomb} \
             limit {limit}",
            key = quote_ident(schema::KEY_COLUMN),
            r = shape.r.ident,
            qualified = qualified.join(", "),
            casts = casts.join(", "),
            tomb = quote_ident(schema::TOMBSTONE_COLUMN),
            limit = cap.saturating_add(1),
        ),
        &params,
    )
    .await?;
    if rows.len() as u64 > cap {
        return Err(ApplyError::JoinOneToOneHalt {
            target: shape.target.clone(),
            reason: format!(
                "a change of {} whose fan-out reaches more than {cap} rows of {} in one page, \
                 past the fan-out cap ({})",
                shape.joined_table,
                shape.source_table,
                crate::defs::join_one_to_one::MAX_FANOUT_ENV,
            ),
        });
    }
    let mut entries: Vec<String> = rows
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .filter(|key| !written.contains(key))
        .collect();
    if entries.is_empty() {
        return Ok((0, 0));
    }
    entries.sort_unstable();
    let entries: Vec<&str> = entries.iter().map(String::as_str).collect();
    txn.batch_execute("savepoint join1_fan_out").await?;
    let (mut written, mut deleted) = (0, 0);
    for batch in entries.chunks(WRITE_BATCH) {
        match write_target(txn, shape, batch, mutations).await {
            Ok((w, d)) => {
                written += w;
                deleted += d;
            }
            Err(err) if super::one_to_one_render::is_data_exception(&err) => {
                txn.batch_execute("rollback to savepoint join1_fan_out")
                    .await?;
                return Err(fan_out_failure(txn, shape, &entries, mutations).await?);
            }
            Err(err) => return Err(err),
        }
    }
    txn.batch_execute("release savepoint join1_fan_out").await?;
    Ok((written, deleted))
}

/// A fan-out over `entries` failed with a data exception: bisects them, each
/// attempt under a savepoint of its own, down to the rows whose fields fail
/// (within [`MAX_FAN_OUT_PROBES`] attempts, naming at most
/// [`MAX_NAMED_FAN_OUT_ROWS`]), names each one's failing fields with the
/// per-field probe ([`failing_fields`]), and returns the halt naming them.
/// A row the fan-out fails on is to be quarantined, and the `b` change and
/// the rest of the fan-out applied (ADR-0021); until that is built the
/// transform halts rather than leave the row stale. Any other failure
/// propagates.
async fn fan_out_failure(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    entries: &[&str],
    mutations: &mut TargetMutations,
) -> Result<ApplyError, ApplyError> {
    let mut failing: Vec<&str> = Vec::new();
    let mut probes = 0;
    let mut runs = vec![(0, entries.len())];
    while let Some((lo, hi)) = runs.pop() {
        if probes >= MAX_FAN_OUT_PROBES || failing.len() >= MAX_NAMED_FAN_OUT_ROWS {
            break;
        }
        probes += 1;
        txn.batch_execute("savepoint join1_fan_out_probe").await?;
        match write_target(txn, shape, &entries[lo..hi], mutations).await {
            Ok(_) => {
                txn.batch_execute("release savepoint join1_fan_out_probe")
                    .await?
            }
            Err(err) if super::one_to_one_render::is_data_exception(&err) => {
                txn.batch_execute(
                    "rollback to savepoint join1_fan_out_probe; \
                     release savepoint join1_fan_out_probe",
                )
                .await?;
                if hi - lo == 1 {
                    failing.push(entries[lo]);
                } else {
                    let mid = lo + (hi - lo) / 2;
                    runs.push((mid, hi));
                    runs.push((lo, mid));
                }
            }
            Err(err) => return Err(err),
        }
    }
    let mut named: Vec<String> = Vec::with_capacity(failing.len());
    for key in &failing {
        let fields = failing_fields(txn, shape, key).await?;
        let fields: Vec<String> = fields
            .iter()
            .map(|(field, error)| format!("{field}: {error}"))
            .collect();
        named.push(format!("{key} ({})", fields.join("; ")));
    }
    let more = if runs.is_empty() {
        ""
    } else {
        ", and maybe more"
    };
    Ok(ApplyError::JoinOneToOneHalt {
        target: shape.target.clone(),
        reason: format!(
            "a change of {} whose fan-out fails on rows of {}: {}{more} (quarantining a row \
             a fan-out fails on is not built yet)",
            shape.joined_table,
            shape.source_table,
            if named.is_empty() {
                "none named".to_string()
            } else {
                named.join(", ")
            },
        ),
    })
}

/// The per-field probe of one fan-out row, the `R` entry `key`: each field
/// alone over the row and the newest live `M` entry on its key, cast to its
/// target column's type as the upsert's assignment casts it, each under a
/// savepoint. Returns the fields that fail with a data exception, each with
/// its error, in definition order.
async fn failing_fields(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    key: &str,
) -> Result<Vec<(String, String)>, ApplyError> {
    let target_columns = super::one_to_one_render::image_columns(txn, &shape.target).await?;
    let from = format!("{} r", shape.r.ident);
    let mut failing = Vec::new();
    for (name, sql) in &shape.fields {
        let cast = target_columns
            .iter()
            .find(|c| &c.name == name)
            .map_or_else(String::new, |c| format!("::{}", c.type_sql));
        let probe = format!(
            "select (({sql}){cast})::text from {joined} where r.{key_col} = $1",
            joined = shape.joined_sql(&from),
            key_col = quote_ident(schema::KEY_COLUMN),
        );
        txn.batch_execute("savepoint join1_field_probe").await?;
        match txn.query(&probe, &[&key]).await {
            Ok(_) => {
                txn.batch_execute("release savepoint join1_field_probe")
                    .await?
            }
            Err(err) if super::one_to_one_render::is_data_exception(&err) => {
                txn.batch_execute(
                    "rollback to savepoint join1_field_probe; \
                     release savepoint join1_field_probe",
                )
                .await?;
                let message = err
                    .as_db_error()
                    .map_or_else(|| err.to_string(), |db| db.message().to_string());
                failing.push((name.clone(), message));
            }
            Err(err) => return Err(err.into()),
        }
    }
    Ok(failing)
}

/// Writes the target rows of `keys` (`R` entries a page changed, or a batch
/// of its fan-out): each live one with a target row is upserted from `R` and
/// the newest live `M` entry on its key, and the rest are deleted. The fields are rendered SQL. The
/// upsert takes its rows sorted by primary key. Returns how many rows it
/// wrote and deleted.
async fn write_target(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    keys: &[&str],
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    if keys.is_empty() {
        return Ok((0, 0));
    }
    // Nothing reads a 1-1 JOIN target in v1 (define refuses it), so there
    // are no prior images to capture and nothing to record for the seam.
    if mutations
        .image_sql(txn, &shape.target, "t")
        .await?
        .is_some()
    {
        return Err(ApplyError::GroupBasisUnsupported {
            target: shape.target.clone(),
            what: "a reader of a 1-1 JOIN target",
        });
    }
    let arity = shape.pk.len();
    let target = ddl::qualified_target_table_ident(&shape.target);
    let pk_cols: Vec<String> = shape.pk.iter().map(|c| quote_ident(&c.name)).collect();
    let field_cols: Vec<String> = shape.fields.iter().map(|(n, _)| quote_ident(n)).collect();
    let mut decoded: Vec<Vec<String>> = Vec::with_capacity(keys.len());
    for key in keys {
        let parts = super::apply::decode_target_pk_parts(&shape.pk, &shape.target, key)?;
        let parts: Option<Vec<String>> = parts.into_iter().collect();
        decoded.push(parts.ok_or_else(|| ApplyError::GroupBasisUnsupported {
            target: shape.target.clone(),
            what: "a 1-1 JOIN row whose primary key has a NULL part",
        })?);
    }

    let mut deleted = Vec::new();
    // Rows that have no target row: delete them. The key text compares only
    // for rows the index finds by primary key.
    {
        let columns = transpose(arity, &decoded);
        let params: Vec<&(dyn ToSql + Sync)> = columns.iter().map(|c| c as _).collect();
        let key_expr = ddl::pk_key_sql_expr(&shape.pk, Some("t"));
        let rows = txn
            .query(
                &format!(
                    "delete from {target} as t \
                     where exists (select 1 from {keyset} where {matches}) \
                       and not exists (select 1 from {r} r where r.{key} = {key_expr} and {has}) \
                     returning {key_expr}",
                    keyset = super::apply::pk_keyset_unnest(&shape.pk, 1),
                    matches = super::apply::pk_keyset_match(&shape.pk, "t"),
                    r = shape.r.ident,
                    key = quote_ident(schema::KEY_COLUMN),
                    has = shape.has_target_row_sql(),
                ),
                &params,
            )
            .await?;
        deleted.extend(rows.into_iter().map(|row| row.get::<_, String>(0)));
    }

    let mut written = 0;
    {
        let on_conflict = on_conflict_sql(&target, &pk_cols, &field_cols);
        let select_cols: Vec<String> = shape
            .pk
            .iter()
            .enumerate()
            .map(|(j, c)| format!("v.__p{j}::{}", c.data_type))
            .chain(shape.fields.iter().map(|(_, sql)| sql.clone()))
            .collect();
        let unnest_args: Vec<String> = (1..=arity + 1).map(|j| format!("${j}::text[]")).collect();
        let unnest_cols: Vec<String> = (0..arity)
            .map(|j| format!("__p{j}"))
            .chain(std::iter::once("__key".to_string()))
            .collect();
        let ordinals: Vec<String> = (1..=arity).map(|i| i.to_string()).collect();
        let col_list: Vec<String> = pk_cols.iter().chain(field_cols.iter()).cloned().collect();
        let returning = ddl::pk_key_sql_expr(&shape.pk, None);
        let from = format!(
            "unnest({}) as v({}) join {r} r on r.{key} = v.__key and {has}",
            unnest_args.join(", "),
            unnest_cols.join(", "),
            r = shape.r.ident,
            key = quote_ident(schema::KEY_COLUMN),
            has = shape.has_target_row_sql(),
        );
        let sql = format!(
            "insert into {target} ({cols}) select {select} from {joined} \
             order by {ordinals} {on_conflict} returning {returning} as pk",
            cols = col_list.join(", "),
            select = select_cols.join(", "),
            joined = shape.joined_sql(&from),
            ordinals = ordinals.join(", "),
        );
        for start in (0..keys.len()).step_by(WRITE_BATCH) {
            let end = (start + WRITE_BATCH).min(keys.len());
            let mut columns = transpose(arity, &decoded[start..end]);
            columns.push(keys[start..end].iter().map(|k| k.to_string()).collect());
            let params: Vec<&(dyn ToSql + Sync)> = columns.iter().map(|c| c as _).collect();
            written += txn.query(&sql, &params).await?.len();
        }
    }
    Ok((written, deleted.len()))
}

/// `rows` (each `arity` parts) as one array per part.
fn transpose(arity: usize, rows: &[Vec<String>]) -> Vec<Vec<String>> {
    (0..arity)
        .map(|j| rows.iter().map(|row| row[j].clone()).collect())
        .collect()
}

/// The `on conflict` clause of a target upsert: a write by text, as every
/// other 1-1 write compares (ADR-0020), and no write when every field is
/// paused.
fn on_conflict_sql(target: &str, pk_cols: &[String], field_cols: &[String]) -> String {
    if field_cols.is_empty() {
        return format!("on conflict ({}) do nothing", pk_cols.join(", "));
    }
    let set_list: Vec<String> = field_cols
        .iter()
        .map(|f| format!("{f} = excluded.{f}"))
        .collect();
    let target_cols: Vec<String> = field_cols.iter().map(|f| format!("{target}.{f}")).collect();
    let excluded: Vec<String> = field_cols.iter().map(|f| format!("excluded.{f}")).collect();
    format!(
        "on conflict ({}) do update set {} \
         where row({})::text is distinct from row({})::text",
        pk_cols.join(", "),
        set_list.join(", "),
        target_cols.join(", "),
        excluded.join(", ")
    )
}

/// The basic `self_check` of a 1-1 `JOIN` target, for tests: the primary keys
/// of every target row that differs from the live join of the two tables, or
/// that is missing or extra. The live join is `a [LEFT] JOIN b` evaluated as
/// the definition's fields say, each field cast to its target column's type
/// and compared by text, as every other 1-1 comparison is. It reads the live
/// tables once, in one statement, and takes no lock, so it is exact only on a
/// quiescent instance. The audit that pages, retries and reports is #1402's.
#[cfg(any(test, feature = "internals"))]
pub async fn diverging_keys(pool: &Pool, target_table: &str) -> Result<Vec<String>, ApplyError> {
    let definition = crate::defs::catalog::definition_by_target(pool, target_table)
        .await?
        .ok_or_else(|| ApplyError::TransformNotFound {
            transform: target_table.to_string(),
        })?;
    if !spec::is_one_to_one(&definition.def) {
        return Err(ApplyError::GroupBasisUnsupported {
            target: target_table.to_string(),
            what: "a self_check that is not of a 1-1 JOIN",
        });
    }
    let pk = ddl::source_primary_key(pool, &definition.source_table).await?;
    let paused = super::quarantine::paused_columns_for(pool, &definition.def).await?;
    let shape = JoinShape::load(pool, &definition, pk, &paused).await?;
    let client = pool.get().await?;
    let target_types =
        super::one_to_one_render::image_columns(&**client, &definition.target_table).await?;
    let on: Vec<String> = shape
        .key
        .iter()
        .zip(&shape.m_key)
        .map(|(a, b)| format!("r.{} = b.{}", quote_ident(&a.name), quote_ident(b)))
        .collect();
    let expected: Vec<String> = shape
        .fields
        .iter()
        .map(|(name, sql)| {
            let cast = target_types
                .iter()
                .find(|c| &c.name == name)
                .map_or_else(String::new, |c| format!("::{}", c.type_sql));
            format!("(({sql}){cast}) as {}", quote_ident(name))
        })
        .collect();
    let s = if shape.s_columns.is_empty() {
        "(select 1 as __none) s".to_string()
    } else {
        let cols: Vec<String> = shape
            .s_columns
            .iter()
            .map(|(name, sql)| format!("{sql} as {}", quote_ident(name)))
            .collect();
        format!("(select {}) s", cols.join(", "))
    };
    let names: Vec<String> = shape.fields.iter().map(|(n, _)| quote_ident(n)).collect();
    let row = |alias: &str| -> String {
        let cols: Vec<String> = names.iter().map(|n| format!("{alias}.{n}")).collect();
        format!("row({})::text", cols.join(", "))
    };
    let row_or_empty = |alias: &str| {
        if names.is_empty() {
            "''".to_string()
        } else {
            row(alias)
        }
    };
    let sql = format!(
        "with expected as ( \
             select {a_key} as k{comma}{fields} \
             from {a} r {kind} join {b} b on {on} cross join lateral {s} \
         ), actual as ( \
             select {t_key} as k{t_comma}{t_fields} from {target} t \
         ) \
         select coalesce(e.k, t.k) from expected e full join actual t on e.k = t.k \
         where e.k is null or t.k is null or {e_row} is distinct from {t_row} \
         order by 1",
        a_key = ddl::pk_key_sql_expr(&shape.pk, Some("r")),
        comma = if expected.is_empty() { "" } else { ", " },
        fields = expected.join(", "),
        a = ddl::qualified_source_table(&shape.source_table),
        b = ddl::qualified_source_table(&shape.joined_table),
        kind = if shape.left { "left" } else { "inner" },
        on = on.join(" and "),
        t_key = ddl::pk_key_sql_expr(&shape.pk, Some("t")),
        t_comma = if names.is_empty() { "" } else { ", " },
        t_fields = names
            .iter()
            .map(|n| format!("t.{n}"))
            .collect::<Vec<_>>()
            .join(", "),
        target = ddl::qualified_target_table_ident(&shape.target),
        e_row = row_or_empty("e"),
        t_row = row_or_empty("t"),
    );
    Ok(client
        .query(&sql, &[])
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect())
}
