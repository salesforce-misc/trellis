//! The blind append: the one write path all four producers (CDC intake,
//! reverse propagation, definition re-derive, and — server-side — backfill)
//! use to stage a change. See
//! docs/staging-and-claiming/02-the-staging-ring.md.
//!
//! [`StagedChange`] makes the image-bearing/image-less distinction the
//! design depends on first-class in the type: [`StagedChange::Cdc`] carries
//! `old_image`/`new_image`; [`StagedChange::Recompute`] — the shape all
//! three non-CDC producers use — carries neither, because it isn't a
//! change, it's an instruction ("recompute this key") that the fold (stage
//! 04) must not confuse with one. [`StagedChange::Truncate`] (issue #60) is
//! a third, distinct shape: key-less and image-less, one row per truncated
//! `src_table`, carrying only the commit position a whole-keyspace clear
//! happened at.

use std::time::SystemTime;

use tokio_postgres::types::{PgLsn, ToSql};
use tokio_postgres::{GenericClient, Transaction};

use super::error::StagingError;

/// Number of ring slots. Fixed at 4 for now (issue #6 asks whether it should
/// be operator-tunable); `seg_0..seg_3` are the only place the count is
/// spelled out.
pub const RING_SIZE: i16 = 4;

/// The three CDC row kinds — the image-bearing half of [`StagedChange`].
/// An enum rather than a bare `&str` so a typo can't slip past to fail at
/// insert time against the ring tables' CHECK constraint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcOp {
    Insert,
    Update,
    Delete,
}

impl CdcOp {
    fn as_sql(self) -> &'static str {
        match self {
            CdcOp::Insert => "insert",
            CdcOp::Update => "update",
            CdcOp::Delete => "delete",
        }
    }
}

/// The sentinel key every [`StagedChange::Truncate`] row carries — a
/// truncate is whole-keyspace, not keyed to any one row, but the ring's
/// schema requires a `key` on every row (`V3__staging_ring.sql`'s `not
/// null`), and the fold groups by `(src_table, key)`. Prefixed with U+001F
/// (INFORMATION SEPARATOR ONE) — not a NUL byte: Postgres `text` cannot
/// store an embedded NUL at all (it's a null-terminated C string
/// internally; the wire protocol rejects it outright), which a real
/// truncate sentinel value would need to survive an actual round trip
/// through the ring. U+001F is exactly the separator
/// `intake::extract_key` itself already joins composite-key parts on — this
/// reuses that same "no real column value contains this control character"
/// assumption the codebase already relies on, rather than inventing a
/// second one. A real key can, in principle, still collide if source data
/// itself contains U+001F, but that would already corrupt
/// `intake::extract_key`'s own composite-key joining today, independent of
/// this sentinel.
pub const TRUNCATE_SENTINEL_KEY: &str = "\u{1f}trellis-truncate-sentinel";

/// One raw change to append into the active ring segment.
///
/// `old_image`/`new_image` are raw JSON text (serialized by the caller):
/// the engine crate has no JSON dependency, and tokio-postgres's
/// `serde_json::Value` feature isn't enabled here. The text is bound with an
/// explicit `::text::jsonb` cast so a malformed payload fails on the insert
/// rather than being mis-typed. The intermediate `::text` matters: Postgres
/// infers a placeholder's type from its immediate cast, so `$n::jsonb` alone
/// would make the driver describe the parameter as `jsonb` — which
/// `Option<&str>`'s `ToSql` rejects — failing the bind before the value ever
/// reaches Postgres.
#[derive(Debug, Clone)]
pub enum StagedChange {
    /// CDC intake's shape: an image-bearing decoded change.
    Cdc {
        src_table: String,
        /// The PostgreSQL source relation OID. `None` is reserved for legacy
        /// staged rows and manual producers that have no resolved relation.
        source_relation_oid: Option<u32>,
        key: String,
        op: CdcOp,
        lsn: Option<PgLsn>,
        old_image: Option<String>,
        new_image: Option<String>,
        origin_lsn: Option<PgLsn>,
        src_changed: Option<SystemTime>,
        hop_gen: i32,
        group_key: Option<String>,
    },
    /// The shape all three non-CDC producers (reverse propagation,
    /// definition re-derive, backfill) use: a bare, image-less recompute
    /// trigger. It asserts nothing about the row's state — only "recompute
    /// this key."
    Recompute {
        src_table: String,
        /// See [`StagedChange::Cdc::source_relation_oid`]. Resolved backfills
        /// supply this; legacy/manual recomputes may intentionally omit it.
        source_relation_oid: Option<u32>,
        key: String,
        hop_gen: i32,
        group_key: Option<String>,
    },
    /// A source `TRUNCATE` of `src_table` (issue #60): one row per truncated
    /// relation, key-less (see [`TRUNCATE_SENTINEL_KEY`]) and image-less —
    /// it asserts nothing about any one row's state, only "every row this
    /// source ever produced is gone as of this position." `lsn`/`src_changed`
    /// are stamped at commit exactly like [`StagedChange::Cdc`]'s, by
    /// `intake::stamp_commit_metadata`; `origin_lsn` stays `None` from intake,
    /// matching [`StagedChange::Cdc`] — a truncate always originates directly
    /// from the source, it is never a re-propagated downstream change.
    Truncate {
        src_table: String,
        /// PostgreSQL's OID for the truncated source relation.
        source_relation_oid: Option<u32>,
        lsn: Option<PgLsn>,
        origin_lsn: Option<PgLsn>,
        src_changed: Option<SystemTime>,
    },
}

impl StagedChange {
    /// The source table this change targets, regardless of variant — used by
    /// intake's hard-cap error (issue #8) to name which tables an oversized
    /// transaction touched.
    pub fn src_table(&self) -> &str {
        match self {
            StagedChange::Cdc { src_table, .. } => src_table,
            StagedChange::Recompute { src_table, .. } => src_table,
            StagedChange::Truncate { src_table, .. } => src_table,
        }
    }
}

/// Borrowed, column-shaped view of a [`StagedChange`], built once per
/// `append` call so the multi-row `INSERT`'s bound parameters can hold
/// references into it rather than cloning every field.
struct ChangeRow<'a> {
    src_table: &'a str,
    source_relation_oid: Option<u32>,
    key: &'a str,
    op: &'static str,
    lsn: Option<PgLsn>,
    old_image: Option<&'a str>,
    new_image: Option<&'a str>,
    origin_lsn: Option<PgLsn>,
    src_changed: Option<SystemTime>,
    hop_gen: i32,
    group_key: Option<&'a str>,
}

impl<'a> From<&'a StagedChange> for ChangeRow<'a> {
    fn from(change: &'a StagedChange) -> Self {
        match change {
            StagedChange::Cdc {
                src_table,
                source_relation_oid,
                key,
                op,
                lsn,
                old_image,
                new_image,
                origin_lsn,
                src_changed,
                hop_gen,
                group_key,
            } => ChangeRow {
                src_table,
                source_relation_oid: *source_relation_oid,
                key,
                op: op.as_sql(),
                lsn: *lsn,
                old_image: old_image.as_deref(),
                new_image: new_image.as_deref(),
                origin_lsn: *origin_lsn,
                src_changed: *src_changed,
                hop_gen: *hop_gen,
                group_key: group_key.as_deref(),
            },
            StagedChange::Recompute {
                src_table,
                source_relation_oid,
                key,
                hop_gen,
                group_key,
            } => ChangeRow {
                src_table,
                source_relation_oid: *source_relation_oid,
                key,
                op: "recompute",
                lsn: None,
                old_image: None,
                new_image: None,
                origin_lsn: None,
                src_changed: None,
                hop_gen: *hop_gen,
                group_key: group_key.as_deref(),
            },
            StagedChange::Truncate {
                src_table,
                source_relation_oid,
                lsn,
                origin_lsn,
                src_changed,
            } => ChangeRow {
                src_table,
                source_relation_oid: *source_relation_oid,
                key: TRUNCATE_SENTINEL_KEY,
                op: "truncate",
                lsn: *lsn,
                old_image: None,
                new_image: None,
                origin_lsn: *origin_lsn,
                src_changed: *src_changed,
                hop_gen: 0,
                group_key: None,
            },
        }
    }
}

/// Resolves ring slot `n` to its table name via a `match` over the closed
/// set of literals — rather than `format!("seg_{n}")` — so an out-of-range
/// slot is a typed error, not a nonexistent table name. Defensive:
/// `ring_slot` only ever comes from this crate's own writes.
pub(crate) fn ring_table_name(ring_slot: i16) -> Result<&'static str, StagingError> {
    match ring_slot {
        0 => Ok("seg_0"),
        1 => Ok("seg_1"),
        2 => Ok("seg_2"),
        3 => Ok("seg_3"),
        other => Err(StagingError::InvalidRingSlot(other)),
    }
}

/// Appends `changes` into the active ring segment as a single blind
/// multi-row `INSERT` — no `ON CONFLICT`, no merge, one row per change.
/// `row_txid`, `appended_at`, and `route` come from the tables' own
/// defaults/generated expression (see `V3__staging_ring.sql`), so every
/// producer computes them identically.
///
/// Takes an already-open `Transaction` so the pointer read and the insert
/// share one transaction, and so callers can compose this atomically with
/// something else (stage 01's watermark advance) rather than have a
/// transaction opened and committed underneath them.
///
/// The pointer is read with a plain `SELECT`, no `FOR UPDATE`/`FOR SHARE`:
/// locking it would serialize every append against every seal. A stale read
/// is expected and is what the fence (stage 03) accounts for.
pub async fn append(txn: &Transaction<'_>, changes: &[StagedChange]) -> Result<(), StagingError> {
    if changes.is_empty() {
        return Ok(());
    }

    let ring_slot: i16 = txn
        .query_one("select ring_slot from segment_pointer", &[])
        .await?
        .get(0);
    let table = ring_table_name(ring_slot)?;

    const COLUMNS: &str = "src_table, source_relation_oid, key, op, lsn, old_image, new_image, origin_lsn, src_changed, hop_gen, group_key";
    const COLS_PER_ROW: usize = 11;
    // Postgres's wire protocol caps a Bind message's parameter count at
    // i16::MAX (65535); 5900 rows keeps every chunk's parameter count
    // (64900) safely under that regardless of column count.
    const MAX_ROWS_PER_STATEMENT: usize = 5900;

    let rows: Vec<ChangeRow<'_>> = changes.iter().map(ChangeRow::from).collect();

    for chunk in rows.chunks(MAX_ROWS_PER_STATEMENT) {
        let mut sql = format!("insert into {table} ({COLUMNS}) values ");
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(chunk.len() * COLS_PER_ROW);
        for (i, row) in chunk.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            let base = i * COLS_PER_ROW;
            sql.push_str(&format!(
                "(${}, ${}, ${}, ${}, ${}, ${}::text::jsonb, ${}::text::jsonb, ${}, ${}, ${}, ${})",
                base + 1,
                base + 2,
                base + 3,
                base + 4,
                base + 5,
                base + 6,
                base + 7,
                base + 8,
                base + 9,
                base + 10,
                base + 11,
            ));
            params.push(&row.src_table);
            params.push(&row.source_relation_oid);
            params.push(&row.key);
            params.push(&row.op);
            params.push(&row.lsn);
            params.push(&row.old_image);
            params.push(&row.new_image);
            params.push(&row.origin_lsn);
            params.push(&row.src_changed);
            params.push(&row.hop_gen);
            params.push(&row.group_key);
        }

        txn.execute(sql.as_str(), &params).await?;
    }
    Ok(())
}

/// The `RingFull` predicate: whether `ring_slot` is free to seal into, i.e.
/// no live `segments` row occupies it.
///
/// **Defined here, but not enforced here** — enforcement is stage 03's job.
/// This exists now so stage 03 has something correct to call.
pub async fn ring_slot_is_free(
    client: &impl GenericClient,
    ring_slot: i16,
) -> Result<bool, StagingError> {
    let row = client
        .query_one(
            "select not exists (select 1 from segments where ring_slot = $1)",
            &[&ring_slot],
        )
        .await?;
    Ok(row.get(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_table_name_resolves_the_fixed_slots() {
        assert_eq!(ring_table_name(0).unwrap(), "seg_0");
        assert_eq!(ring_table_name(3).unwrap(), "seg_3");
    }

    #[test]
    fn ring_table_name_rejects_out_of_range_slots() {
        match ring_table_name(4) {
            Err(StagingError::InvalidRingSlot(4)) => {}
            other => panic!("expected InvalidRingSlot(4), got {other:?}"),
        }
    }
}
