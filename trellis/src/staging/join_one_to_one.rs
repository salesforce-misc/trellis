//! A 1-1 `JOIN` transform's apply step (ADR-0021): the `FROM` (`a`) side's
//! page.
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
//! A page carries `a` changes and Re-derives, in one transaction under `READ
//! COMMITTED`, in the model's order (#1375). Each step is a rule an ablation
//! showed necessary; the plant that removes it is in brackets.
//!
//! 1. **Fold.** Only the newest change per `a` id goes through I2; a
//!    Re-derive of an id replaces that id's changes. A page whose segment may
//!    predate the build re-derives every id
//!    ([`super::ledger::page_may_predate_build`]).
//! 2. **Lock row and key set.** The pair's lock row `FOR SHARE`. Then the key
//!    set K, in one statement: every change's new key, the key `R` holds for
//!    each id, and the live key of each Re-derive row (read unlocked, which
//!    keeps the retry below rare). A key with a `NULL` part is left out: it
//!    matches nothing and takes no `J` row.
//! 3. **`J`.** K inserted sorted `on conflict do nothing`, then locked `FOR
//!    UPDATE` in one sorted statement [`join1_no_j_lock`].
//! 4. **Entries.** The `R` placeholders and entry lock
//!    ([`super::one_to_one_ledger::lock_entries`], I5) [`join1_no_placeholder`,
//!    `join1_no_entry_lock`]. Then each held key is checked against K: one
//!    outside it moved after K was read, so the page rolls back with
//!    [`ApplyError::JoinKeyMoved`] and retries [`join1_no_recheck`]. The
//!    entry lock also orders the writers of a `NULL`-keyed row, which has no
//!    `J` row.
//! 5. **Re-derive read.** One statement reads the live `a` rows and
//!    `pg_current_snapshot()`; a key it reads outside K rolls the page back
//!    the same way [`join1_snapshot_after_read`].
//! 6. **I2.** The entries are updated by ADR-0002's I2 (an Apply must not be
//!    in the entry's basis, must be past its applied position and above the
//!    floor) [`join1_vis_only`, `join1_lsn_only`]; a Re-derive sets the basis
//!    and leaves the applied position alone. Then `R`'s key and read columns
//!    are written for the entries that changed; a row that is gone has its
//!    key cleared, so it matches no `b` change.
//! 7. **Recompute and write.** Each changed row's target row is rendered from
//!    `R` and the newest live `M` entry on its key (`LEFT JOIN LATERAL`), read
//!    only now, after `J` is locked; a row that no longer
//!    exists, or an `INNER` row with no match, is deleted. Fields are
//!    rendered SQL, never evaluated in Rust.
//!
//! The lock order is the lock row, `J` sorted, `R` entries sorted, target
//! rows sorted. `b`-side changes and Re-derives, `TRUNCATE` and the fan-out
//! are later steps (#1397, #1399): a page that carries one halts.
//!
//! # A build ([`build`])
//!
//! A fresh build, a `RESUME` or a rebuild in place (`request_backfill`, a
//! catch-up) fills `M`, `R`, `J` and the target in one transaction under the
//! lock row `FOR UPDATE`, and has no sweep job; the chunked build replaces it
//! (#1400).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use tokio_postgres::Transaction;
use tokio_postgres::types::{PgLsn, ToSql};

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

    fn r_names(&self) -> Vec<String> {
        self.r_columns.iter().map(|c| c.name.clone()).collect()
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

/// One `a` change of a page: an Apply of its last change, written from its
/// image (`None` for a delete), or a Re-derive (`apply` `None`), written
/// from the row the page reads under the entry lock.
#[derive(Debug, Clone)]
pub(crate) struct ARecord {
    key: String,
    apply: Option<(PgLsn, String, Option<String>)>,
}

/// One page's work on one 1-1 `JOIN` target.
#[derive(Debug, Clone)]
pub(crate) struct JoinOneToOnePlan {
    pub(crate) shape: std::sync::Arc<JoinShape>,
    records: Vec<ARecord>,
}

impl JoinOneToOnePlan {
    pub(crate) fn new(shape: JoinShape) -> Self {
        Self {
            shape: std::sync::Arc::new(shape),
            records: Vec::new(),
        }
    }

    /// Adds `change` of the table on `side`. The `b` side is the next step's
    /// (#1397): a change of it halts the page rather than run wrong.
    pub(crate) fn push(&mut self, side: Side, change: &FoldedChange) -> Result<(), ApplyError> {
        if side == Side::Joined {
            return Err(ApplyError::GroupBasisUnsupported {
                target: self.shape.target.clone(),
                what: "a change of the joined table of a 1-1 JOIN (its apply path is not built yet)",
            });
        }
        let apply = match &change.last_change {
            Some(last) if !change.has_recompute => {
                Some((last.lsn, last.row_txid.clone(), change.new_image.clone()))
            }
            _ => None,
        };
        self.records.push(ARecord {
            key: change.key.clone(),
            apply,
        });
        Ok(())
    }
}

/// Loads the plan for `definition` (a 1-1 `JOIN`), `pk` being the `FROM`
/// table's primary key.
pub(crate) async fn plan_for(
    pool: &Pool,
    definition: &Definition,
    pk: Vec<PrimaryKeyColumn>,
) -> Result<JoinOneToOnePlan, ApplyError> {
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
    let rederived: Vec<ARecord>;
    let records: &[ARecord] = if plan.records.iter().any(|r| r.apply.is_some())
        && super::ledger::page_may_predate_build(txn, &shape.r, first_seg).await?
    {
        rederived = plan
            .records
            .iter()
            .map(|r| ARecord {
                key: r.key.clone(),
                apply: None,
            })
            .collect();
        &rederived
    } else {
        &plan.records
    };
    let mut by_key: BTreeMap<&str, &ARecord> = BTreeMap::new();
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
    if by_key.is_empty() {
        return Ok((0, 0));
    }
    // Planted bug: apply every change without ADR-0002's I2. See
    // `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let predicate = !crate::plant::fires(
        crate::plant::Plant::StaleOneToOneWrite,
        by_key.values().any(|r| r.apply.is_some()),
    );
    #[cfg(not(any(test, feature = "test-util")))]
    let predicate = true;

    let keys: Vec<&str> = by_key.keys().copied().collect();
    let rederive_keys: Vec<&str> = by_key
        .values()
        .filter(|r| r.apply.is_none())
        .map(|r| r.key.as_str())
        .collect();

    // 2. The lock row, then K.
    txn.execute(&format!("select 1 from {} for share", shape.lock), &[])
        .await?;
    let live = live_keys(txn, shape, &rederive_keys).await?;
    let k = key_set(txn, shape, &by_key, &live).await?;
    // Test-only pause point. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::BeforeJoin1Lock,
        &shape.target,
    )
    .await?;

    // 3. `J`.
    lock_keys(txn, shape, &k).await?;

    // 4. The `R` entries.
    let to_lock: Vec<EntryChange<'_>> = by_key
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
    let inserted =
        ledger::lock_entries(txn, &shape.r, &to_lock, seg_seq, predicate, skip_lock).await?;
    check_held_keys(txn, shape, &keys, &k).await?;
    // Test-only pause point. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterJoin1REntryLock,
        &shape.r.label,
    )
    .await?;

    // 5. The Re-derive read.
    let (mut read, snapshot, entry_seg) = if rederive_keys.is_empty() {
        (HashMap::new(), None, seg_seq)
    } else {
        let read = ledger::read_rows(
            txn,
            &shape.target,
            &shape.source_table,
            &shape.pk,
            &shape.r_names(),
            &rederive_keys,
        )
        .await?;
        check_read_keys(shape, &read.rows, &k)?;
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
        super::interleave::pause_at(
            txn,
            super::interleave::PausePoint::AfterJoin1Read,
            &shape.target,
        )
        .await?;
        (read.rows, Some(read.snapshot), seg_seq.max(read.seg))
    };
    let rederived: HashMap<&str, Option<String>> = rederive_keys
        .iter()
        .map(|key| (*key, read.remove(*key)))
        .collect();

    // 6. I2, then `R`'s columns.
    let mut changes: Vec<EntryChange<'_>> = Vec::with_capacity(by_key.len());
    for record in by_key.values() {
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
    let mut changed = ledger::update_entries(
        txn,
        &shape.r,
        &changes,
        snapshot.as_deref(),
        entry_seg,
        predicate,
    )
    .await?;
    changed.extend(inserted.applied.iter().cloned());
    let unwritten: Vec<&str> = inserted
        .keys
        .iter()
        .filter(|key| !changed.contains(*key))
        .map(String::as_str)
        .collect();
    ledger::drop_placeholders(txn, &shape.r, &unwritten).await?;

    let mut present_keys: Vec<&str> = Vec::new();
    let mut images: Vec<&str> = Vec::new();
    let mut gone: Vec<&str> = Vec::new();
    for record in by_key.values() {
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
    write_entries(txn, shape, &present_keys, &images, &gone).await?;

    // 7. Recompute and write.
    // Test-only pause point. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::BeforeJoin1Recompute,
        &shape.target,
    )
    .await?;
    let mut changed_keys: Vec<&str> = changed.iter().map(String::as_str).collect();
    changed_keys.sort_unstable();
    write_target(txn, shape, &changed_keys, mutations).await
}

/// The live key of each of `keys`' rows, read unlocked: the third part of
/// K. A key is its columns' text; a row with a `NULL` part, or no row,
/// contributes none.
async fn live_keys(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    keys: &[&str],
) -> Result<Vec<KeyText>, ApplyError> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let names: Vec<String> = shape.key.iter().map(|c| c.name.clone()).collect();
    let query = super::apply::live_rows_query(&shape.source_table, &shape.pk, &names, keys)?;
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

/// K: every image's new key, the key `R` holds for each of `by_key`, and the
/// `live` keys, as the sorted set of their texts. A key with a `NULL` part is
/// left out.
async fn key_set(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    by_key: &BTreeMap<&str, &ARecord>,
    live: &[KeyText],
) -> Result<BTreeSet<KeyText>, ApplyError> {
    let n = shape.key.len();
    let keys: Vec<&str> = by_key.keys().copied().collect();
    let images: Vec<Option<&str>> = by_key
        .values()
        .map(|r| match &r.apply {
            Some((_, _, Some(image))) => Some(image.as_str()),
            _ => None,
        })
        .collect();
    let live_columns: Vec<Vec<&str>> = (0..n)
        .map(|i| live.iter().map(|k| k[i].as_str()).collect())
        .collect();
    let aliases =
        |prefix: &str| -> Vec<String> { (0..n).map(|i| format!("{prefix}{i}")).collect() };
    let from_image: Vec<String> = shape
        .key
        .iter()
        .enumerate()
        .map(|(i, c)| {
            format!(
                "{} as k{i}",
                cast_to(c, &format!("ch.img ->> {}", quote_literal(&c.name)))
            )
        })
        .collect();
    let from_r: Vec<String> = shape
        .key
        .iter()
        .enumerate()
        .map(|(i, c)| format!("r.{} as k{i}", quote_ident(&c.name)))
        .collect();
    let from_live: Vec<String> = shape
        .key
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{} as k{i}", cast_to(c, &format!("l.c{i}"))))
        .collect();
    let live_arrays: Vec<String> = (0..n).map(|i| format!("${}::text[]", 3 + i)).collect();
    let texts: Vec<String> = aliases("k").iter().map(|k| format!("{k}::text")).collect();
    let present: Vec<String> = aliases("k")
        .iter()
        .map(|k| format!("{k} is not null"))
        .collect();
    // Planted bug: the key `R` holds isn't part of the key set. See
    // `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let held = if crate::plant::fires(crate::plant::Plant::Join1OldKeyUnlocked, true) {
        "$1::text[] = '{}'::text[] and"
    } else {
        ""
    };
    #[cfg(not(any(test, feature = "test-util")))]
    let held = "";
    let sql = format!(
        "with ch as (select u.k, u.img::jsonb as img \
                     from unnest($1::text[], $2::text[]) as u(k, img) \
                     where u.img is not null), \
         cand as ( \
             select {from_image} from ch \
             union \
             select {from_r} from {r} r where {held} r.{key} = any($1::text[]) \
             union \
             select {from_live} from unnest({live_arrays}) as l({live_names}) \
         ) \
         select {texts} from cand where {present}",
        from_image = from_image.join(", "),
        from_r = from_r.join(", "),
        from_live = from_live.join(", "),
        live_arrays = live_arrays.join(", "),
        live_names = aliases("c").join(", "),
        texts = texts.join(", "),
        present = present.join(" and "),
        r = shape.r.ident,
        key = quote_ident(schema::KEY_COLUMN),
    );
    let mut params: Vec<&(dyn ToSql + Sync)> = vec![&keys, &images];
    for column in &live_columns {
        params.push(column);
    }
    let rows = super::ledger::query_by_entry_key(txn, &sql, &params).await?;
    Ok(rows.iter().filter_map(|row| key_of(row, 0, n)).collect())
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
    let columns: Vec<Vec<&str>> = (0..n)
        .map(|i| k.iter().map(|key| key[i].as_str()).collect())
        .collect();
    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(n);
    for column in &columns {
        params.push(column);
    }
    let names: Vec<String> = shape.key.iter().map(|c| quote_ident(&c.name)).collect();
    let casts: Vec<String> = shape
        .key
        .iter()
        .enumerate()
        .map(|(i, c)| cast_to(c, &format!("u.c{i}")))
        .collect();
    let arrays: Vec<String> = (0..n).map(|i| format!("${}::text[]", i + 1)).collect();
    let unnest = format!(
        "unnest({}) as u({})",
        arrays.join(", "),
        (0..n)
            .map(|i| format!("c{i}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
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

/// Checks that the key each `keys` entry of `R` holds, now that the page has
/// locked them, is in `k`: one outside it moved after `k` was read.
async fn check_held_keys(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    keys: &[&str],
    k: &BTreeSet<KeyText>,
) -> Result<(), ApplyError> {
    let n = shape.key.len();
    let texts: Vec<String> = shape
        .key
        .iter()
        .map(|c| format!("r.{}::text", quote_ident(&c.name)))
        .collect();
    let rows = super::ledger::query_by_entry_key(
        txn,
        &format!(
            "select {} from {} r where r.{} = any($1::text[])",
            texts.join(", "),
            shape.r.ident,
            quote_ident(schema::KEY_COLUMN)
        ),
        &[&keys],
    )
    .await?;
    // Test-only: a key that moves on every attempt. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    if super::interleave::take_forced_join_key_move() {
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
    if skip {
        return Ok(());
    }
    for row in &rows {
        if let Some(held) = key_of(row, 0, n)
            && !k.contains(&held)
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

/// Checks that the key of each live row a Re-derive read is in `k`.
fn check_read_keys(
    shape: &JoinShape,
    rows: &HashMap<String, String>,
    k: &BTreeSet<KeyText>,
) -> Result<(), ApplyError> {
    for doc in rows.values() {
        let value: serde_json::Value =
            serde_json::from_str(doc).map_err(|_| ApplyError::JoinKeyMoved {
                target: shape.target.clone(),
            })?;
        let key: Option<KeyText> = shape
            .key
            .iter()
            .map(|c| {
                value
                    .get(&c.name)
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
            })
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

/// Writes `R`'s key and read columns for the entries that changed:
/// `present` ones from their `images`, and the key cleared for the `gone`.
async fn write_entries(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    present: &[&str],
    images: &[&str],
    gone: &[&str],
) -> Result<(), ApplyError> {
    let key = quote_ident(schema::KEY_COLUMN);
    if !present.is_empty() {
        let sets: Vec<String> = shape
            .r_columns
            .iter()
            .map(|c| format!("{n} = s.{n}", n = quote_ident(&c.name)))
            .collect();
        let render = OneToOneRender::of_columns(shape.r_columns.clone());
        super::ledger::query_by_entry_key(
            txn,
            &format!(
                "update {r} r set {sets} \
                 from unnest($1::text[], $2::text[]::jsonb[]) as v(__key, __img) \
                      cross join {row} \
                 where r.{key} = any($1::text[]) and r.{key} = v.__key",
                r = shape.r.ident,
                sets = sets.join(", "),
                row = render.row_sql("v.__img"),
            ),
            &[&present, &images],
        )
        .await?;
    }
    if !gone.is_empty() {
        let sets: Vec<String> = shape
            .key
            .iter()
            .map(|c| format!("{} = null", quote_ident(&c.name)))
            .collect();
        super::ledger::query_by_entry_key(
            txn,
            &format!(
                "update {r} r set {sets} where r.{key} = any($1::text[])",
                r = shape.r.ident,
                sets = sets.join(", "),
            ),
            &[&gone],
        )
        .await?;
    }
    Ok(())
}

/// Writes the target rows of `keys` (`R` entries that changed): each live
/// one with a target row is upserted from `R` and the newest live `M` entry
/// on its key, and the rest are deleted. The fields are rendered SQL. The
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

/// The joined table's primary key, for a build's `M` keys.
pub(crate) async fn joined_primary_key(
    pool: &Pool,
    shape: &JoinShape,
) -> Result<Vec<PrimaryKeyColumn>, ApplyError> {
    Ok(ddl::source_primary_key(pool, &shape.joined_table).await?)
}

/// Fills `M`, `R`, `J` and the target of `shape` from the live tables, in
/// `txn` (a `READ COMMITTED` build transaction): the minimal build, one
/// transaction under the lock row `FOR UPDATE`, which the chunked build
/// replaces (#1400, the module doc). `m_pk` is the joined table's primary key.
/// Returns how many target rows it wrote and deleted.
///
/// Each ledger table is filled by one statement that reads its table with
/// `pg_current_snapshot()`, which becomes every entry's basis, so a change
/// that statement's snapshot saw is refused by I2 and one it didn't is
/// applied. An entry a page already wrote keeps its applied position, and one
/// whose row is gone (a rebuild in place keeps the ledgers) becomes a
/// tombstone by a second statement, whose snapshot is its basis. The target is
/// then written from `R` and `M` as a page writes it, and a target row no
/// entry backs is deleted.
pub(crate) async fn build(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    m_pk: &[PrimaryKeyColumn],
) -> Result<(usize, usize), ApplyError> {
    txn.execute(&format!("select 1 from {} for update", shape.lock), &[])
        .await?;
    let key = quote_ident(schema::KEY_COLUMN);
    let basis = quote_ident(schema::BASIS_COLUMN);
    let tombstone = quote_ident(schema::TOMBSTONE_COLUMN);
    let seg = quote_ident(schema::APPLIED_SEG_COLUMN);
    for (ledger, columns, key_columns, table, pk) in [
        (
            &shape.m.ident,
            &shape.m_columns,
            shape.m_key.clone(),
            &shape.joined_table,
            m_pk,
        ),
        (
            &shape.r.ident,
            &shape.r_columns,
            shape.key.iter().map(|c| c.name.clone()).collect(),
            &shape.source_table,
            &shape.pk[..],
        ),
    ] {
        let names: Vec<String> = columns.iter().map(|c| quote_ident(&c.name)).collect();
        let sets: Vec<String> = names
            .iter()
            .map(|n| format!("{n} = excluded.{n}"))
            .chain([
                format!("{basis} = excluded.{basis}"),
                format!("{tombstone} = false"),
            ])
            .collect();
        txn.execute(
            &format!(
                "insert into {ledger} ({key}, {basis}, {names}) \
                 select {key_expr}, pg_catalog.pg_current_snapshot(), {from_t} \
                 from {table} t \
                 on conflict ({key}) do update set {sets}",
                names = names.join(", "),
                from_t = names
                    .iter()
                    .map(|n| format!("t.{n}"))
                    .collect::<Vec<_>>()
                    .join(", "),
                key_expr = ddl::pk_key_sql_expr(pk, Some("t")),
                table = ddl::qualified_source_table(table),
                sets = sets.join(", "),
            ),
            &[],
        )
        .await?;
        // An entry whose row is gone (a rebuild in place keeps the
        // ledgers) is re-derived as gone: a tombstone whose basis is this
        // statement's snapshot, its key cleared so it matches nothing, and
        // stamped with the newest segment, as every Re-derive's tombstone is
        // (`super::retire::collect_tombstones`).
        let cleared: Vec<String> = key_columns
            .iter()
            .map(|c| format!("{} = null", quote_ident(c)))
            .chain([
                format!("{basis} = pg_catalog.pg_current_snapshot()"),
                format!("{tombstone} = true"),
                format!(
                    "{seg} = {}",
                    schema::tombstone_seg_sql(
                        Some(&format!("l.{seg}")),
                        "true",
                        "coalesce((select max(seg_seq) from segments), 0)",
                    )
                ),
            ])
            .collect();
        txn.execute(
            &format!(
                "update {ledger} l set {cleared} \
                 where not l.{tombstone} \
                   and not exists (select 1 from {table} t where {key_expr} = l.{key})",
                cleared = cleared.join(", "),
                key_expr = ddl::pk_key_sql_expr(pk, Some("t")),
                table = ddl::qualified_source_table(table),
            ),
            &[],
        )
        .await?;
    }
    // `J`: every key either side holds.
    let r_key: Vec<String> = shape
        .key
        .iter()
        .map(|c| format!("r.{}", quote_ident(&c.name)))
        .collect();
    let m_key: Vec<String> = shape
        .m_key
        .iter()
        .map(|c| format!("m.{}", quote_ident(c)))
        .collect();
    let not_null = |cols: &[String]| {
        cols.iter()
            .map(|c| format!("{c} is not null"))
            .collect::<Vec<_>>()
            .join(" and ")
    };
    let j_cols: Vec<String> = shape.key.iter().map(|c| quote_ident(&c.name)).collect();
    let ordinals: Vec<String> = (1..=j_cols.len()).map(|i| i.to_string()).collect();
    txn.execute(
        &format!(
            "insert into {j} ({j_cols}) \
             select * from (select {r_key} from {r} r where {r_ok} \
                            union select {m_key} from {m} m where {m_ok}) k \
             order by {ordinals} on conflict do nothing",
            j = shape.j,
            j_cols = j_cols.join(", "),
            r_key = r_key.join(", "),
            r = shape.r.ident,
            r_ok = not_null(&r_key),
            m_key = m_key.join(", "),
            m = shape.m.ident,
            m_ok = not_null(&m_key),
            ordinals = ordinals.join(", "),
        ),
        &[],
    )
    .await?;
    let write = write_all_target_rows(txn, shape).await?;
    let target = ddl::qualified_target_table_ident(&shape.target);
    let key_expr = ddl::pk_key_sql_expr(&shape.pk, Some("t"));
    let deleted = txn
        .execute(
            &format!(
                "delete from {target} t where not exists \
                 (select 1 from {r} r where r.{key} = {key_expr} and {has})",
                r = shape.r.ident,
                has = shape.has_target_row_sql(),
            ),
            &[],
        )
        .await?;
    Ok((write, deleted as usize))
}

/// Upserts the target row of every `R` entry that has one, from `R` and the
/// newest live `M` entry on its key, with the primary key read from `a`.
async fn write_all_target_rows(
    txn: &Transaction<'_>,
    shape: &JoinShape,
) -> Result<usize, ApplyError> {
    let target = ddl::qualified_target_table_ident(&shape.target);
    let pk_cols: Vec<String> = shape.pk.iter().map(|c| quote_ident(&c.name)).collect();
    let field_cols: Vec<String> = shape.fields.iter().map(|(n, _)| quote_ident(n)).collect();
    let on_conflict = on_conflict_sql(&target, &pk_cols, &field_cols);
    let select_cols: Vec<String> = pk_cols
        .iter()
        .map(|c| format!("a.{c}"))
        .chain(shape.fields.iter().map(|(_, sql)| sql.clone()))
        .collect();
    let col_list: Vec<String> = pk_cols.iter().chain(field_cols.iter()).cloned().collect();
    let from = format!(
        "{r} r join {a} a on {a_key} = r.{key}",
        r = shape.r.ident,
        a = ddl::qualified_source_table(&shape.source_table),
        a_key = ddl::pk_key_sql_expr(&shape.pk, Some("a")),
        key = quote_ident(schema::KEY_COLUMN),
    );
    let sql = format!(
        "insert into {target} ({cols}) select {select} from {joined} where {has} \
         order by {ordinals} {on_conflict}",
        cols = col_list.join(", "),
        select = select_cols.join(", "),
        joined = shape.joined_sql(&from),
        has = shape.has_target_row_sql(),
        ordinals = (1..=pk_cols.len())
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(", "),
    );
    Ok(txn.execute(&sql, &[]).await? as usize)
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
