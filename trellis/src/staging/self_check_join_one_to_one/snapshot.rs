//! A 1-1 `JOIN` definition's hidden state, its target and its two sources as
//! its copies have applied them, all read under one snapshot: what the
//! generative suite's in-run invariant (`generative::invariant::join1`, #1709)
//! checks while a case's writes and drains run. Test-only, never API.
//!
//! **One snapshot.** The statements run in one `REPEATABLE READ READ ONLY`
//! transaction, so they share the snapshot its first query takes. Capture
//! writes a change's ring row in the change's own transaction, and a page
//! writes `R`, `M`, the target and its markers, and marks its ring rows
//! applied, in one transaction (ADR-0021). So under one snapshot a page in
//! flight is invisible whole, and each source is what its copy has applied
//! plus the ring rows still unapplied, which [`super::Side::applied`]
//! subtracts as `self_check` does.
//!
//! **Locks before the snapshot.** `TRUNCATE` and `DROP` are not MVCC-safe: a
//! snapshot taken before one commits reads the emptied or recreated table
//! as empty. A ring slot is retired by `TRUNCATE`, a source may be
//! truncated by the workload, and `RESUME` drops and recreates `R`, `M` and
//! `J`. So the transaction first takes `ACCESS SHARE` on every ring slot,
//! both sources, the hidden tables and the target, with `LOCK TABLE`, which
//! takes no snapshot: any such statement either committed before the
//! snapshot, which then sees its effects, or waits until the read ends.
//! Retirement takes its lock `NOWAIT` and skips a slot it can't lock. A lock
//! that isn't granted within [`LOCK_TIMEOUT`] (a `RESUME` holding one table
//! while it waits for another this read holds) skips the read:
//! [`hidden_snapshot`] returns `None`, as it does when a hidden table is gone.

use std::collections::BTreeSet;

use crate::defs::catalog;
use crate::defs::ddl;
use crate::defs::join::{self as spec, Side as JoinSide};
use crate::defs::join_one_to_one::{HiddenTable, read_columns};
use crate::defs::ledger as schema;
use crate::pool::{Pool, quote_ident};

use super::super::converge::per_ring_table;
use super::super::self_check::{SelfCheckError, paused_columns, walk_key};
use super::{Audit, Side, build};

/// How long the read waits for a lock before it skips (the module doc).
pub const LOCK_TIMEOUT: &str = "200ms";

/// One entry of `R` or `M`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HiddenEntry {
    /// The entry key: its row's primary key text.
    pub key: String,
    /// The join key's columns as text, in `ON` order.
    pub join_key: Vec<Option<String>>,
    /// The columns the ledger copies, as text, in [`SourceView::columns`]'s
    /// order (the join key first).
    pub values: Vec<Option<String>>,
    pub tombstone: bool,
    /// The entry's applied position, as `pg_lsn`'s number.
    pub applied_lsn: Option<u64>,
}

/// One target row: its key text and each field as text, in
/// [`HiddenSnapshot::fields`]'s order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetRow {
    pub key: String,
    pub values: Vec<Option<String>>,
}

/// The target row the definition's fields give a live `R` entry, read with
/// the newest live `M` entry on its join key (the apply's rule: the latest
/// applied position, then the highest key), evaluated by Postgres under the
/// snapshot by `self_check`'s renderer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinedRow {
    /// The `R` entry's key.
    pub key: String,
    /// The `M` entry it was read with, or `None` when its key has no live one.
    pub joined: Option<String>,
    pub values: Vec<Option<String>>,
}

/// A source as its copy has applied it (`self_check`'s view).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SourceView {
    /// The columns the copy holds, the join key first.
    pub columns: Vec<String>,
    /// Each row as applied, by primary key text, its values in
    /// [`Self::columns`]'s order.
    pub rows: Vec<(String, Vec<Option<String>>)>,
    /// Keys the ring can't vouch for: an unapplied change with no usable
    /// image, or a held key whose parked origin is unknown.
    pub uncertain: BTreeSet<String>,
    /// An unapplied change that touches every key (a `TRUNCATE`).
    pub global: bool,
}

/// A pending fan-out marker (ADR-0021): the rows of its join key after
/// `cursor` (in `R`'s entry-key order under `"C"`; every row when `None`)
/// may still join an older `M` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FanOutMarker {
    pub join_key: Vec<String>,
    pub cursor: Option<String>,
}

/// A 1-1 `JOIN` definition's state under one snapshot (the module doc).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HiddenSnapshot {
    pub target: String,
    /// `transform_definitions.status`.
    pub status: String,
    /// A build chunk of the definition is not done yet.
    pub building: bool,
    /// `LEFT` (else `INNER`).
    pub left: bool,
    /// The target's fields, except paused ones, in order.
    pub fields: Vec<String>,
    /// `R`, every entry, tombstones included.
    pub from_entries: Vec<HiddenEntry>,
    /// `M`, every entry.
    pub joined_entries: Vec<HiddenEntry>,
    /// `J`'s rows: each join key's columns as text.
    pub keys: Vec<Vec<Option<String>>>,
    /// The target's rows.
    pub rows: Vec<TargetRow>,
    /// One per live `R` entry.
    pub joined_rows: Vec<JoinedRow>,
    /// `a` as `R` has applied it.
    pub from_applied: SourceView,
    /// `b` as `M` has applied it.
    pub joined_applied: SourceView,
    pub markers: Vec<FanOutMarker>,
    /// Keys of `a` held in quarantine (`poison_held`).
    pub held: BTreeSet<String>,
}

/// SQLSTATEs of the `LOCK TABLE` that mean "skip this read": a lock not
/// granted in time, a hidden table a `RESUME` dropped and hasn't recreated.
fn skippable(error: &tokio_postgres::Error) -> bool {
    use tokio_postgres::error::SqlState;
    matches!(
        error.code(),
        Some(code) if *code == SqlState::LOCK_NOT_AVAILABLE
            || *code == SqlState::UNDEFINED_TABLE
            || *code == SqlState::T_R_DEADLOCK_DETECTED
    )
}

/// Reads `target_table`'s (a 1-1 `JOIN` target's) [`HiddenSnapshot`], or
/// `None` when the read is skipped (the module doc).
pub async fn hidden_snapshot(
    pool: &Pool,
    target_table: &str,
) -> Result<Option<HiddenSnapshot>, SelfCheckError> {
    let def = catalog::definition_by_target(pool, target_table)
        .await?
        .ok_or_else(|| SelfCheckError::TargetNotFound(target_table.to_string()))?;
    let pk = walk_key(pool, &def).await?;
    let paused = paused_columns(pool, &def.def).await?;
    let audit = build(pool, &def, &pk, &paused).await?;
    let mut client = pool.get().await?;
    let txn = client
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    let locked = txn
        .batch_execute(&format!(
            "set local lock_timeout = '{LOCK_TIMEOUT}'; \
             lock table {rings}, {a}, {b}, {r}, {m}, {j}, {target} in access share mode",
            rings = per_ring_table(", ", |_, table| table.to_string()),
            a = audit.a.ident,
            b = audit.b.ident,
            r = audit.a.ledger.ident,
            m = audit.b.ledger.ident,
            j = HiddenTable::Keys.qualified(pool.schema(), def.id),
            target = ddl::qualified_target_table_ident(&def.target_table),
        ))
        .await;
    match locked {
        Ok(()) => {}
        Err(e) if skippable(&e) => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    match read(&txn, &audit, &pk, pool.schema(), target_table).await {
        Ok(snapshot) => {
            txn.commit().await?;
            Ok(Some(snapshot))
        }
        Err(e) => Err(e.into()),
    }
}

/// The values a ledger's statement selects: its `columns` as text.
fn texts(alias: &str, columns: &[String]) -> String {
    let values: Vec<String> = columns
        .iter()
        .map(|c| format!("{alias}.{}::text", quote_ident(c)))
        .collect();
    format!("array[{}]::text[]", values.join(", "))
}

async fn read(
    txn: &tokio_postgres::Transaction<'_>,
    audit: &Audit<'_>,
    pk: &[ddl::PrimaryKeyColumn],
    catalog_schema: &str,
    target_table: &str,
) -> Result<HiddenSnapshot, tokio_postgres::Error> {
    let id = audit.id();
    let r = &audit.a.ledger.ident;
    let m = &audit.b.ledger.ident;
    let j = HiddenTable::Keys.qualified(catalog_schema, id);
    let target = ddl::qualified_target_table_ident(&audit.def.target_table);
    let status = txn
        .query_one(
            "select status, exists (select 1 from backfill_chunks \
                                    where definition_id = $1 and not done) \
             from transform_definitions where id = $1",
            &[&id],
        )
        .await?;
    let markers = txn
        .query(
            "select join_key, cursor from join1_fan_out where transform_id = $1",
            &[&id],
        )
        .await?
        .iter()
        .map(|row| FanOutMarker {
            join_key: row.get(0),
            cursor: row.get(1),
        })
        .collect();
    let held = txn
        .query(
            "select key from poison_held where transform_id = $1 and src_table = $2",
            &[&id, &audit.a.table],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();

    let from_columns = read_columns(&audit.def.def, JoinSide::From);
    let joined_columns = read_columns(&audit.def.def, JoinSide::Joined);
    let from_entries = entries(txn, r, &audit.a.key, &from_columns).await?;
    let joined_entries = entries(txn, m, &audit.b.key, &joined_columns).await?;
    let keys = txn
        .query(
            &format!(
                "select {} from {j} j",
                texts("j", &spec::join_key(&audit.def.def, JoinSide::From))
            ),
            &[],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();

    let field_texts: Vec<String> = audit
        .fields
        .iter()
        .map(|f| format!("{}::text", quote_ident(&f.name)))
        .collect();
    let rows = txn
        .query(
            &format!(
                "select {key}, array[{values}]::text[] from {target}",
                key = ddl::pk_key_sql_expr(pk, None),
                values = field_texts.join(", "),
            ),
            &[],
        )
        .await?
        .iter()
        .map(|row| TargetRow {
            key: row.get(0),
            values: row.get(1),
        })
        .collect();

    let key_col = quote_ident(schema::KEY_COLUMN);
    let tomb = quote_ident(schema::TOMBSTONE_COLUMN);
    let applied = quote_ident(schema::APPLIED_LSN_COLUMN);
    let on: Vec<String> = audit
        .a
        .key
        .iter()
        .zip(&audit.b.key)
        .map(|(a, b)| format!("x.{} = a.{}", quote_ident(b), quote_ident(a)))
        .collect();
    let joined_values: Vec<String> = audit
        .fields
        .iter()
        .map(|f| format!("({})::text", f.sql))
        .collect();
    let joined_rows = txn
        .query(
            &format!(
                "select a.{key_col}, b.{key_col}, array[{values}]::text[] \
                 from {r} a \
                 left join lateral (select x.* from {m} x \
                                    where {on} and not x.{tomb} \
                                    order by x.{applied} desc nulls last, \
                                             x.{key_col} collate \"C\" desc \
                                    limit 1) b on true \
                 where not a.{tomb}",
                values = joined_values.join(", "),
                on = on.join(" and "),
            ),
            &[],
        )
        .await?
        .iter()
        .map(|row| JoinedRow {
            key: row.get(0),
            joined: row.get(1),
            values: row.get(2),
        })
        .collect();

    let from_applied = applied_view(txn, audit, &audit.a, from_columns).await?;
    let joined_applied = applied_view(txn, audit, &audit.b, joined_columns).await?;

    Ok(HiddenSnapshot {
        target: target_table.to_string(),
        status: status.get(0),
        building: status.get(1),
        left: audit.left,
        fields: audit.fields.iter().map(|f| f.name.clone()).collect(),
        from_entries,
        joined_entries,
        keys,
        rows,
        joined_rows,
        from_applied,
        joined_applied,
        markers,
        held,
    })
}

/// Every entry of the ledger `ledger`, whose join key is `key`.
async fn entries(
    txn: &tokio_postgres::Transaction<'_>,
    ledger: &str,
    key: &[String],
    columns: &[String],
) -> Result<Vec<HiddenEntry>, tokio_postgres::Error> {
    Ok(txn
        .query(
            &format!(
                "select l.{key_col}, {join_key}, {values}, l.{tomb}, \
                        (l.{applied} - '0/0'::pg_lsn)::text \
                 from {ledger} l",
                key_col = quote_ident(schema::KEY_COLUMN),
                join_key = texts("l", key),
                values = texts("l", columns),
                tomb = quote_ident(schema::TOMBSTONE_COLUMN),
                applied = quote_ident(schema::APPLIED_LSN_COLUMN),
            ),
            &[],
        )
        .await?
        .iter()
        .map(|row| HiddenEntry {
            key: row.get(0),
            join_key: row.get(1),
            values: row.get(2),
            tombstone: row.get(3),
            applied_lsn: row
                .get::<_, Option<String>>(4)
                .map(|n| n.parse().expect("an lsn difference is a whole number")),
        })
        .collect())
}

/// `side`'s source as its ledger has applied it.
async fn applied_view(
    txn: &tokio_postgres::Transaction<'_>,
    audit: &Audit<'_>,
    side: &Side,
    columns: Vec<String>,
) -> Result<SourceView, tokio_postgres::Error> {
    let ring = side.ring(audit.id());
    let sql = format!(
        "with {ctes}__applied as materialized ({applied}) \
         select 1, {key}, {values} from __applied e \
         union all select 3, u.__k, null::text[] from ({unc}) u \
         union all select 4, '', null::text[] where {global}",
        ctes = ring.one_to_one_ctes(&side.ledger),
        applied = side.applied(&ring),
        key = ddl::pk_key_sql_expr(&side.pk, Some("e")),
        values = texts("e", &columns),
        unc = ring.uncertain_keys_sql(),
        global = ring.global_sql(),
    );
    let mut view = SourceView {
        columns,
        ..SourceView::default()
    };
    for row in txn.query(sql.as_str(), &[]).await? {
        match row.get::<_, i32>(0) {
            1 => view.rows.push((row.get(1), row.get(2))),
            3 => {
                view.uncertain.insert(row.get(1));
            }
            _ => view.global = true,
        }
    }
    Ok(view)
}
