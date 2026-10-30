//! A 1-1 `JOIN`'s build (ADR-0021, #1400), one transaction's work at a time:
//! the `M` pass, a chunk of `a`'s primary key, and a Re-derive page of the
//! rebuild sweep. The jobs that run them (claims, fences, the transactions)
//! are `crate::staging::build`'s (`staging/build/join_one_to_one.rs`).
//!
//! Every step takes a page's locks in a page's order (the parent module): the
//! pair's lock row `FOR SHARE`, `J` sorted, the entries sorted, then the
//! target rows. So a build step and a drain page on one row queue on a `J`
//! row or an entry, and the one that comes second reads what the first
//! committed.
//!
//! - **`M`** ([`m_batch`]): `b`'s rows in primary-key batches, each a page of
//!   `b` Re-derives without the fan-out. It is the plan job's first step,
//!   before it enqueues a chunk, so every chunk reads the `M` the build made.
//!   The chunks write every target row the fan-out would have.
//! - **A chunk** ([`run_chunk`]): `(lo, hi]` of `a`'s primary key.
//!   1. The lock row `FOR SHARE`. K is the live join keys of the range's `a`
//!      rows (read unlocked) plus the keys `R` holds for them.
//!   2. `J` for K, insert-then-lock.
//!   3. Insert-from-read: one statement reads the rows that have no `R`
//!      entry live, with `pg_current_snapshot()` as their entries' basis,
//!      and inserts their entries `on conflict do nothing`.
//!   4. The remaining entries are locked and re-derived as a page's are.
//!   5. Each row's target row is written from `R` and the newest live `M`
//!      entry on its key.
//!
//!   Three checks roll the chunk back with
//!   [`ApplyError::JoinKeyMoved`], a transient failure its retry gets past
//!   with the late key in K: the insert's keys, the locked entries' held
//!   keys and the Re-derive read's keys must each be in K (#1375 C10).
//! - **A sweep page** ([`rederive`]): a page of Re-derives of one side.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::Instant;

use tokio_postgres::Transaction;
use tokio_postgres::types::ToSql;

use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::join::Side;
use crate::defs::ledger as schema;
use crate::metrics::{self, BuildStatement};
use crate::pool::quote_ident;

use super::super::apply::ApplyError;
use super::super::target_mutations::TargetMutations;
use super::{
    JoinOneToOnePlan, JoinShape, KeyText, Record, apply_page, fold, key_of, key_set, lock_keys,
    lock_side, rederive_read, update_side, write_target,
};

/// What one build chunk did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ChunkOutcome {
    /// The `a` keys it re-derived.
    pub keys: usize,
    /// The target rows it inserted or changed.
    pub written: usize,
    /// The target rows it deleted.
    pub deleted: usize,
}

/// A page of Re-derives of `keys` (encoded primary keys of `side`'s table),
/// in `txn`, the parent module's page with every record a Re-derive.
/// `fan_out` false leaves out the fan-out of the `M` entries it changes (the
/// `M` pass). Returns the target rows it wrote and deleted.
pub(crate) async fn rederive(
    txn: &Transaction<'_>,
    shape: &std::sync::Arc<JoinShape>,
    side: Side,
    keys: &[String],
    fan_out: bool,
) -> Result<(usize, usize), ApplyError> {
    if keys.is_empty() {
        return Ok((0, 0));
    }
    let records: Vec<Record> = keys
        .iter()
        .map(|key| Record {
            key: key.clone(),
            apply: None,
            old_image: None,
        })
        .collect();
    let (a, b) = match side {
        Side::From => (records, Vec::new()),
        Side::Joined => (Vec::new(), records),
    };
    let plan = JoinOneToOnePlan {
        shape: shape.clone(),
        a,
        b,
        fan_out,
    };
    // A Re-derive page's segment is its read's; the page's own only stamps
    // Applies, of which it has none.
    apply_page(txn, &plan, 0, 0, &mut TargetMutations::new()).await
}

/// One batch of the `M` pass: up to `limit` keys of `b` after `after` (an
/// encoded key; `None` from the start) in `m_pk`'s order, as a page of `b`
/// Re-derives without the fan-out ([`rederive`]). `m_pk` is `b`'s primary
/// key, read once for the whole pass, so its collations hold across the
/// batches. Returns the last key it read, `None` when it read fewer than
/// `limit` keys: then the pass is done. A key with a `NULL` part is in no
/// batch's range; [`m_null_keys`] reads those.
pub(crate) async fn m_batch(
    txn: &Transaction<'_>,
    shape: &std::sync::Arc<JoinShape>,
    m_pk: &[PrimaryKeyColumn],
    after: Option<&str>,
    limit: i64,
) -> Result<Option<String>, ApplyError> {
    let idents: Vec<String> = m_pk
        .iter()
        .map(|c| format!("t.{}", quote_ident(&c.name)))
        .collect();
    let not_null: Vec<String> = idents.iter().map(|c| format!("{c} is not null")).collect();
    let order: Vec<String> = idents
        .iter()
        .zip(m_pk)
        .map(|(ident, c)| c.ordered(ident))
        .collect();
    let decoded: Vec<String> = match after {
        Some(after) => ddl::split_pk_key(m_pk, &shape.joined_table, after)?
            .into_iter()
            .map(|part| part.map(|c| c.into_owned()).unwrap_or_default())
            .collect(),
        None => Vec::new(),
    };
    let after_cmp = if after.is_some() {
        format!(
            " and {}",
            crate::defs::backfill::pk_row_cmp(&idents, m_pk, ">", 1)
        )
    } else {
        String::new()
    };
    let mut params: Vec<&(dyn ToSql + Sync)> = vec![&limit];
    params.extend(decoded.iter().map(|v| v as &(dyn ToSql + Sync)));
    let started = Instant::now();
    let keys: Vec<String> = txn
        .query(
            &format!(
                "select ({k}) from {b} t where {not_null}{after_cmp} order by {order} limit $1",
                k = ddl::pk_key_sql_expr(m_pk, Some("t")),
                b = ddl::qualified_source_table(&shape.joined_table),
                not_null = not_null.join(" and "),
                order = order.join(", "),
            ),
            &params,
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    metrics::record_build_statement(BuildStatement::ChunkKeys, started.elapsed());
    rederive(txn, shape, Side::Joined, &keys, false).await?;
    let full = i64::try_from(keys.len()).is_ok_and(|n| n >= limit);
    Ok(if full { keys.last().cloned() } else { None })
}

/// The keys of `b` with a `NULL` part, which no [`m_batch`] range holds: only
/// a `b` keyed by a nullable unique index (another transform's target) has
/// any. Empty for every other `b`.
pub(crate) async fn m_null_keys(
    txn: &impl tokio_postgres::GenericClient,
    shape: &JoinShape,
    m_pk: &[PrimaryKeyColumn],
) -> Result<Vec<String>, ApplyError> {
    let nullable: Vec<String> = m_pk
        .iter()
        .filter(|c| c.nullable)
        .map(|c| format!("t.{} is null", quote_ident(&c.name)))
        .collect();
    if nullable.is_empty() {
        return Ok(Vec::new());
    }
    Ok(txn
        .query(
            &format!(
                "select ({}) from {} t where {}",
                ddl::pk_key_sql_expr(m_pk, Some("t")),
                ddl::qualified_source_table(&shape.joined_table),
                nullable.join(" or "),
            ),
            &[],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect())
}

/// Runs one build chunk, `(lo, hi]` of `a`'s primary key (encoded, as
/// `backfill_chunks` stores the bounds; `lo` `None` for the first chunk), in
/// `txn`, a `READ COMMITTED` transaction the caller commits. `range_pk` is
/// `a`'s key with the collations its plan job walked under. See the module
/// doc for the steps.
///
/// A key quarantined for the definition (`poison`) is left out, as a plain 1-1
/// chunk leaves it out, and so is a key with a `NULL` part, which no range
/// holds.
pub(crate) async fn run_chunk(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    range_pk: &[PrimaryKeyColumn],
    lo: Option<&str>,
    hi: &str,
) -> Result<ChunkOutcome, ApplyError> {
    let decode = |text: &str| -> Result<Vec<String>, ApplyError> {
        Ok(ddl::split_pk_key(range_pk, &shape.source_table, text)?
            .into_iter()
            .map(|part| part.map(|c| c.into_owned()).unwrap_or_default())
            .collect())
    };
    let (lo, hi) = (lo.map(decode).transpose()?, decode(hi)?);
    let idents: Vec<String> = range_pk
        .iter()
        .map(|c| format!("s.{}", quote_ident(&c.name)))
        .collect();
    let range = crate::defs::backfill::pk_range_where(&idents, range_pk, &lo);
    let bounds = crate::defs::backfill::range_params(&lo, &hi);
    let n = bounds.len();
    let a = ddl::qualified_source_table(&shape.source_table);
    let k_src = ddl::pk_key_sql_expr(&shape.pk, Some("s"));

    // 1. The range's keys and their rows' join keys, unlocked: the
    // Re-derives' part of K.
    let started = Instant::now();
    let not_null: Vec<String> = idents.iter().map(|c| format!("{c} is not null")).collect();
    let live_cols: Vec<String> = shape
        .key
        .iter()
        .map(|c| format!("(s.{})::text", quote_ident(&c.name)))
        .collect();
    let mut params = bounds.clone();
    params.push(&shape.source_table);
    params.push(&shape.target);
    let rows = txn
        .query(
            &format!(
                "select ({k_src}), {live} from {a} s where {range} and {not_null} \
                 and not exists (select 1 from poison p \
                                 join transform_definitions d on d.id = p.transform_id \
                                 where p.src_table = ${src} and p.key = ({k_src}) \
                                   and d.target_table = ${tgt})",
                live = live_cols.join(", "),
                not_null = not_null.join(" and "),
                src = n + 1,
                tgt = n + 2,
            ),
            &params,
        )
        .await?;
    metrics::record_build_statement(BuildStatement::ChunkKeys, started.elapsed());
    if rows.is_empty() {
        return Ok(ChunkOutcome::default());
    }
    let arity = shape.key.len();
    let records: Vec<Record> = rows
        .iter()
        .map(|row| Record {
            key: row.get(0),
            apply: None,
            old_image: None,
        })
        .collect();
    let live: Vec<KeyText> = rows
        .iter()
        .filter_map(|row| key_of(row, 1, arity))
        .collect();
    let records = fold(&records);

    // 2. The lock row, K, then `J`.
    txn.execute(&format!("select 1 from {} for share", shape.lock), &[])
        .await?;
    let k = key_set(txn, shape, Side::From, &records, &live).await?;
    // Test-only pause point. See `super::super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::super::interleave::pause_at(
        txn,
        super::super::interleave::PausePoint::BeforeJoin1Lock,
        &shape.target,
    )
    .await?;
    lock_keys(txn, shape, &k).await?;

    // 3. Insert-from-read: the entries of the rows that have none.
    let keys: Vec<&str> = records.keys().copied().collect();
    let inserted = insert_from_read(txn, shape, &range, &bounds, &keys, &k).await?;

    // 4. The rest: the page's entry lock, held-key check, read and I2.
    let remaining: BTreeMap<&str, &Record> = records
        .iter()
        .filter(|(key, _)| !inserted.contains(**key))
        .map(|(key, record)| (*key, *record))
        .collect();
    let (placeholders, _) = lock_side(txn, shape, Side::From, &remaining, 0, true, &k).await?;
    let rederived: Vec<String> = remaining.keys().map(|key| key.to_string()).collect();
    let read = rederive_read(txn, shape, Side::From, &shape.pk, &rederived, &k, 0).await?;
    let changed = update_side(
        txn,
        shape,
        Side::From,
        &remaining,
        &placeholders,
        &read,
        true,
    )
    .await?;

    // 5. The target rows.
    // Test-only pause point. See `super::super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::super::interleave::pause_at(
        txn,
        super::super::interleave::PausePoint::BeforeJoin1Recompute,
        &shape.target,
    )
    .await?;
    let mut written_keys: Vec<&str> = inserted
        .iter()
        .chain(changed.iter())
        .map(String::as_str)
        .collect();
    written_keys.sort_unstable();
    written_keys.dedup();
    let (written, deleted) =
        write_target(txn, shape, &written_keys, &mut TargetMutations::new()).await?;
    Ok(ChunkOutcome {
        keys: records.len(),
        written,
        deleted,
    })
}

/// Step 3 of a chunk: one statement reads the live rows of `keys` in the
/// chunk's range (`range`, bound by `bounds`) and inserts an entry for each
/// that has none, its basis the statement's `pg_current_snapshot()`, sorted
/// as a page inserts its placeholders. An entry is this transaction's own row
/// from the insert, so it is locked already. Returns the keys it inserted.
/// One whose join key isn't in `k` moved after the chunk's unlocked read, so
/// the chunk rolls back with [`ApplyError::JoinKeyMoved`] and its retry locks
/// that key [`join1_chunk_no_key`, #1375 C10].
async fn insert_from_read(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    range: &str,
    bounds: &[&(dyn ToSql + Sync)],
    keys: &[&str],
    k: &BTreeSet<KeyText>,
) -> Result<HashSet<String>, ApplyError> {
    let key = quote_ident(schema::KEY_COLUMN);
    let basis = quote_ident(schema::BASIS_COLUMN);
    let names: Vec<String> = shape
        .r_columns
        .iter()
        .map(|c| quote_ident(&c.name))
        .collect();
    let from_s: Vec<String> = names.iter().map(|n| format!("s.{n}")).collect();
    let k_src = ddl::pk_key_sql_expr(&shape.pk, Some("s"));
    let mut params: Vec<&(dyn ToSql + Sync)> = bounds.to_vec();
    let keys_param = params.len() + 1;
    params.push(&keys);
    let started = Instant::now();
    let rows = txn
        .query(
            &format!(
                "insert into {r} as l ({key}, {basis}{comma}{names}) \
                 select ({k_src}), pg_catalog.pg_current_snapshot(){comma}{from_s} \
                 from {a} s where {range} and ({k_src}) = any(${keys_param}::text[]) \
                 order by ({k_src}) collate \"C\" \
                 on conflict ({key}) do nothing \
                 returning l.{key}, {texts}",
                r = shape.r.ident,
                comma = if names.is_empty() { "" } else { ", " },
                names = names.join(", "),
                from_s = from_s.join(", "),
                a = ddl::qualified_source_table(&shape.source_table),
                texts = shape.key_texts(Side::From, "l").join(", "),
            ),
            &params,
        )
        .await?;
    metrics::record_build_statement(BuildStatement::ChunkLock, started.elapsed());
    // Planted bug: the chunk doesn't check the keys its insert-from-read
    // found. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let unchecked = crate::plant::fires(crate::plant::Plant::Join1ChunkNoKey, !rows.is_empty());
    #[cfg(not(any(test, feature = "test-util")))]
    let unchecked = false;
    let arity = shape.key.len();
    if !unchecked
        && rows
            .iter()
            .filter_map(|row| key_of(row, 1, arity))
            .any(|key| !k.contains(&key))
    {
        #[cfg(any(test, feature = "test-util"))]
        super::super::interleave::note_join_key_moved();
        return Err(ApplyError::JoinKeyMoved {
            target: shape.target.clone(),
        });
    }
    Ok(rows.iter().map(|row| row.get(0)).collect())
}

/// Up to `scan` entries of `side`'s ledger after `after` in key order, and
/// the live ones among them the build hasn't re-derived: whose `basis` is
/// null (written by an Apply alone) or a snapshot taken before the build's
/// start, `start_xid`, as a plain 1-1 sweep decides
/// (`super::super::build::one_to_one::sweep_batch`). A key quarantined for
/// the definition is left as it is. Returns how many it read, the last key
/// it read, and the stale keys.
pub(crate) async fn stale_entries(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    side: Side,
    start_xid: &str,
    after: Option<&str>,
    scan: i64,
) -> Result<(i64, Option<String>, Vec<String>), ApplyError> {
    let key = quote_ident(schema::KEY_COLUMN);
    let tombstone = quote_ident(schema::TOMBSTONE_COLUMN);
    let basis = quote_ident(schema::BASIS_COLUMN);
    let started = Instant::now();
    let row = txn
        .query_one(
            &format!(
                "with w as ( \
                     select l.{key} as k, not l.{tombstone} \
                            and (l.{basis} is null \
                                 or pg_catalog.pg_snapshot_xmax(l.{basis}) <= $1::text::xid8) \
                            as stale \
                     from {ledger} l {after} order by l.{key} limit $2 \
                 ) \
                 select (select count(*) from w), \
                        (select w.k from w order by w.k desc limit 1), \
                        array(select w.k from w where w.stale \
                                and not exists (select 1 from poison p \
                                                join transform_definitions d \
                                                  on d.id = p.transform_id \
                                                where p.src_table = $4 and p.key = w.k \
                                                  and d.target_table = $5) \
                              order by w.k)",
                ledger = shape.ledger(side).ident,
                after = if after.is_some() {
                    format!("where l.{key} > $3")
                } else {
                    "where $3::text is null".to_string()
                },
            ),
            &[&start_xid, &scan, &after, &shape.table(side), &shape.target],
        )
        .await?;
    metrics::record_build_statement(BuildStatement::ChunkKeys, started.elapsed());
    Ok((row.get(0), row.get(1), row.get(2)))
}
