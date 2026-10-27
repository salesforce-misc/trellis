//! The blind append: the write path the Rust-side producers (the
//! target-mutation seam, quarantine's key release, definition re-derive,
//! and — server-side — backfill) use to stage a change. Capture triggers write the
//! same row shape in SQL (`capture::sql`). See
//! docs/staging-and-claiming/02-the-staging-ring.md.
//!
//! [`StagedChange`] makes the image-bearing/image-less distinction the
//! design depends on first-class in the type: [`StagedChange::Cdc`] carries
//! `old_image`/`new_image`; [`StagedChange::Recompute`] — the shape the
//! non-CDC producers use — carries neither, because it isn't a
//! change, it's an instruction ("recompute this key") that the fold (stage
//! 04) must not confuse with one. [`StagedChange::Truncate`] is
//! a third, distinct shape: key-less and image-less, one row per truncated
//! `src_table`, carrying only the commit position a whole-keyspace clear
//! happened at.

use std::time::SystemTime;

use tokio_postgres::types::{PgLsn, ToSql};
use tokio_postgres::{GenericClient, Transaction};

use super::error::StagingError;

/// Number of ring slots. Fixed at 4 for now (not
/// operator-tunable); `seg_0..seg_3` are the only place the count is
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
/// truncate is whole-keyspace, not keyed to any one row, but the ring's schema
/// requires a `key` on every row (the ring tables' `not null`), and the
/// fold groups by `(src_table, key)`. Prefixed with U+001F (INFORMATION
/// SEPARATOR ONE) — not a NUL byte: Postgres `text` cannot store an embedded
/// NUL at all (it's a null-terminated C string internally; the wire protocol
/// rejects it outright), which a real truncate sentinel value would need to
/// survive an actual round trip through the ring. U+001F is exactly the
/// separator a capture trigger and `defs::ddl::join_pk_key` join composite-key
/// parts on, and this sentinel — unlike a real key — never passes through that
/// encoding at all: it is one fixed literal, compared (and grouped by the fold)
/// as an opaque whole string, not split back into parts.
///
/// This is a *different* question from a real, in-value U+001F misparsing as
/// a field boundary (`defs::ddl` escapes one in a multi-column key, and a
/// single-column key is never split at all). That escaping does not narrow
/// this sentinel, which collides with any key whose text happens to *equal*
/// this exact literal, in two ways:
/// a single-column key whose own value literally spells
/// `"\u{1f}trellis-truncate-sentinel"` (arity-1 keys are deliberately
/// verbatim, see `defs::ddl::push_escaped_composite_key_part`'s "why arity 1
/// is exempt"), and a composite key whose first declared-order component is
/// a genuine empty string and whose remaining components spell
/// `"trellis-truncate-sentinel"` (the U+001F between them is a real,
/// correctly-unescaped field boundary). Both are accepted as the same class
/// of vanishingly unlikely risk every fixed reserved-token sentinel in this
/// crate carries — narrowing them further would mean this sentinel stops being a short,
/// fixed, greppable literal, which is judged not worth it here.
pub const TRUNCATE_SENTINEL_KEY: &str = "\u{1f}trellis-truncate-sentinel";

/// The ring key of a capture function's `schema_changed` marker:
/// a row a capture trigger writes, in place of or ahead of its images, when a
/// column it images has been renamed or dropped. Key-less like
/// [`TRUNCATE_SENTINEL_KEY`], and with the same collision caveat. Its
/// `new_image` says which columns went missing
/// (`capture::sql::SCHEMA_CHANGED_OP`'s doc has the shape).
pub const SCHEMA_CHANGED_SENTINEL_KEY: &str = "\u{1f}trellis-schema-changed";

/// The ring key of a partitioned source's `node_truncate` marker (ADR-0017
/// P5): the row a leaf's or middle's truncate writes in place of the root's
/// truncate sentinel. Key-less like [`TRUNCATE_SENTINEL_KEY`], and with the
/// same collision caveat. Its `new_image` names the node
/// (`capture::sql::NODE_TRUNCATE_OP`'s doc has the shape).
pub const NODE_TRUNCATE_SENTINEL_KEY: &str = "\u{1f}trellis-node-truncate";

/// One raw change to append into the active ring segment.
///
/// `old_image`/`new_image` are raw JSON text (serialized by the caller):
/// the trellis crate has no JSON dependency, and tokio-postgres's
/// `serde_json::Value` feature isn't enabled here. The text is bound with an
/// explicit `::text::jsonb` cast so a malformed payload fails on the insert
/// rather than being mis-typed. The intermediate `::text` matters: Postgres
/// infers a placeholder's type from its immediate cast, so `$n::jsonb` alone
/// would make the driver describe the parameter as `jsonb` — which
/// `Option<&str>`'s `ToSql` rejects — failing the bind before the value ever
/// reaches Postgres.
#[derive(Debug, Clone)]
pub enum StagedChange {
    /// A capture trigger's shape: an image-bearing change. The
    /// target-mutation seam stages this shape too, for a target it feeds
    /// images for (`staging::target_mutations`): `lsn` is
    /// then the writer's pre-commit write token, and `origin_lsn` is the
    /// origin of the change that produced the write (see
    /// [`StagedChange::Recompute::origin_lsn`]). A capture trigger stamps
    /// both with `pg_current_wal_insert_lsn()`.
    Cdc {
        src_table: String,
        key: String,
        op: CdcOp,
        lsn: Option<PgLsn>,
        old_image: Option<String>,
        new_image: Option<String>,
        origin_lsn: Option<PgLsn>,
        src_changed: Option<SystemTime>,
        hop_gen: i32,
    },
    /// The shape the non-CDC producers (forward propagation, a key release,
    /// definition re-derive, backfill) use: a bare, image-less recompute
    /// trigger. It asserts nothing about the row's state — only "recompute
    /// this key."
    ///
    /// `src_changed` (for multi-hop propagation) is the
    /// timestamp of the real source commit this recompute traces back to,
    /// when one is known — `Some` for forward propagation and quarantine
    /// replay (both of which inherit it from the triggering
    /// change), `None` for a build's staged pre-existing rows (there is no
    /// meaningful origin for a row nothing ever "changed"). Without this, an
    /// automatically-propagated hop-to-hop chain (transform A's output
    /// feeding transform B as B's input, entirely through `Recompute` rows)
    /// would carry no origin at all past the first hop, leaving both the
    /// per-transform latency histogram and the end-to-end latency
    /// histogram unable to observe anything for it — exactly the gap
    /// this field closes.
    Recompute {
        src_table: String,
        key: String,
        hop_gen: i32,
        src_changed: Option<SystemTime>,
        /// The key's row image as it stood before the write that
        /// staged this recompute, when the producer knows it — only
        /// `staging::target_mutations` sets it, for a key it changed in a
        /// target table. It is a hint, not a delta: the recompute still
        /// re-reads the key live. A downstream aggregate also re-derives the
        /// group this image names, so a row that moved groups (or was
        /// deleted) leaves its old group correct too. Stored in the ring's
        /// `old_image` column under `op = 'recompute'`, which the fold keeps
        /// apart from real images (`FoldedChange::prior_image`).
        prior_image: Option<String>,
        /// The source commit this recompute traces back to,
        /// carried forward from the triggering change exactly as
        /// `src_changed` is, so `converged_through` gates it only for tokens
        /// at or past that commit. `None` means unknown, which gates every
        /// token: a backfill-enumerated row, or a write with no source
        /// commit behind it.
        origin_lsn: Option<PgLsn>,
    },
    /// A source `TRUNCATE` of `src_table`: one row per truncated
    /// relation, key-less (see [`TRUNCATE_SENTINEL_KEY`]) and image-less —
    /// it asserts nothing about any one row's state, only "every row this
    /// source ever produced is gone as of this position." A truncate always
    /// originates directly from the source, never from a re-propagated
    /// downstream change. The capture trigger writes this row itself
    /// (`capture::sql`); only tests stage one through here.
    ///
    /// `lsn` is required, as the trigger always stamps one: the truncate's
    /// drain raises its targets' truncate floor to it (ADR-0002 I2), and a
    /// truncate with none would raise no floor.
    #[cfg(any(test, feature = "internals"))]
    Truncate {
        src_table: String,
        lsn: PgLsn,
        origin_lsn: Option<PgLsn>,
        src_changed: Option<SystemTime>,
    },
}

/// Borrowed, column-shaped view of a [`StagedChange`], built once per
/// `append` call so the multi-row `INSERT`'s bound parameters can hold
/// references into it rather than cloning every field.
struct ChangeRow<'a> {
    src_table: &'a str,
    key: &'a str,
    op: &'static str,
    lsn: Option<PgLsn>,
    old_image: Option<&'a str>,
    new_image: Option<&'a str>,
    origin_lsn: Option<PgLsn>,
    src_changed: Option<SystemTime>,
    hop_gen: i32,
}

impl<'a> From<&'a StagedChange> for ChangeRow<'a> {
    fn from(change: &'a StagedChange) -> Self {
        match change {
            StagedChange::Cdc {
                src_table,
                key,
                op,
                lsn,
                old_image,
                new_image,
                origin_lsn,
                src_changed,
                hop_gen,
            } => ChangeRow {
                src_table,
                key,
                op: op.as_sql(),
                lsn: *lsn,
                old_image: old_image.as_deref(),
                new_image: new_image.as_deref(),
                origin_lsn: *origin_lsn,
                src_changed: *src_changed,
                hop_gen: *hop_gen,
            },
            StagedChange::Recompute {
                src_table,
                key,
                hop_gen,
                src_changed,
                prior_image,
                origin_lsn,
            } => ChangeRow {
                src_table,
                key,
                op: "recompute",
                lsn: None,
                old_image: prior_image.as_deref(),
                new_image: None,
                origin_lsn: *origin_lsn,
                src_changed: *src_changed,
                hop_gen: *hop_gen,
            },
            #[cfg(any(test, feature = "internals"))]
            StagedChange::Truncate {
                src_table,
                lsn,
                origin_lsn,
                src_changed,
            } => ChangeRow {
                src_table,
                key: TRUNCATE_SENTINEL_KEY,
                op: "truncate",
                lsn: Some(*lsn),
                old_image: None,
                new_image: None,
                origin_lsn: *origin_lsn,
                src_changed: *src_changed,
                hop_gen: 0,
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

/// Resolves the ring slot a writer appends into
/// (docs/staging-and-claiming/03-sealing-and-the-fence.md, "The two-phase seal").
/// Every ring writer resolves its slot here, never from `segment_pointer`. Two
/// things make the read safe for the fence, and both are in this one statement:
///
/// - **It reads the mirror, not the table.** `ring_slot_mirror` is a
///   sequence, and `pg_sequence_last_value` is not transactional: it returns
///   the latest value at every isolation level. `select ring_slot from
///   segment_pointer` does not — at `REPEATABLE READ` or `SERIALIZABLE` it
///   answers from the transaction's snapshot, which can be any number of
///   flips old, and a row written to a slot that stale lands above its
///   batch's fence where no batch ever claims it.
/// - **The writer's xid is assigned before the read.** The fence argument is
///   "every writer that read the old slot has an xid below `xmax(S_k)`": the
///   read precedes phase 2's mirror set, which precedes its xid bump. That
///   only bounds the writer's xid if the xid already exists when it reads;
///   a writer that read first and took its xid at the `INSERT` could take it
///   after the bump, sit above `xmax(S_k)`, and slip past the seal gate.
///   Assigning it here costs nothing: the `INSERT` that follows would assign
///   it anyway, and `pg_current_xact_id()` returns the existing xid if the
///   transaction already has one. `CASE` evaluates its condition before its
///   result, so the order holds within one round trip.
///
/// The read takes no lock: locking would serialize every append against
/// every seal. A read of a slot the seal has just flipped away from is
/// expected and is exactly what the fence accounts for.
///
/// `segment_pointer` stays the registry's authority for `active_seq` and for
/// Trellis's own `READ COMMITTED` readers (the seal's `active_pointer` and
/// `Raced` check, convergence's conditions 2 and 3). Between phase 1's commit
/// and phase 2's mirror set the two disagree by one slot; that gap is part of
/// the argument above, so don't "unify" the two reads.
pub async fn active_ring_slot(client: &impl GenericClient) -> Result<i16, StagingError> {
    let ring_slot: Option<i16> = client
        .query_one(
            "select case when pg_current_xact_id() is not null \
                 then pg_sequence_last_value('ring_slot_mirror')::smallint end",
            &[],
        )
        .await?
        .get(0);
    // The staging-ring migration initialises the sequence with
    // `is_called = true`, so `pg_sequence_last_value` is never NULL; a NULL
    // here means that invariant broke, not "slot 0".
    ring_slot.ok_or(StagingError::InvalidRingSlot(-1))
}

/// Appends `changes` into the active ring segment as a single blind
/// multi-row `INSERT` — no `ON CONFLICT`, no merge, one row per change.
/// `row_txid`, `appended_at`, and `route` come from the tables' own
/// defaults/generated expression (see the staging-ring migration), so every
/// producer computes them identically.
///
/// Takes an already-open `Transaction` so the pointer read and the insert
/// share one transaction, and so callers can compose this atomically with
/// something else (stage 01's watermark advance) rather than have a
/// transaction opened and committed underneath them.
///
/// The active slot comes from [`active_ring_slot`] — the unlocked,
/// snapshot-independent mirror read the fence (stage 03) accounts for.
pub async fn append(txn: &Transaction<'_>, changes: &[StagedChange]) -> Result<(), StagingError> {
    if changes.is_empty() {
        return Ok(());
    }

    let ring_slot = active_ring_slot(txn).await?;
    let table = ring_table_name(ring_slot)?;

    const COLUMNS: &str =
        "src_table, key, op, lsn, old_image, new_image, origin_lsn, src_changed, hop_gen";
    const COLS_PER_ROW: usize = 9;
    // Postgres's wire protocol caps a Bind message's parameter count at
    // i16::MAX (65535); 5000 rows keeps every chunk's param count
    // (45000) safely under that regardless of column count.
    const MAX_ROWS_PER_STATEMENT: usize = 5000;

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
                "(${}, ${}, ${}, ${}, ${}::text::jsonb, ${}::text::jsonb, ${}, ${}, ${})",
                base + 1,
                base + 2,
                base + 3,
                base + 4,
                base + 5,
                base + 6,
                base + 7,
                base + 8,
                base + 9,
            ));
            params.push(&row.src_table);
            params.push(&row.key);
            params.push(&row.op);
            params.push(&row.lsn);
            params.push(&row.old_image);
            params.push(&row.new_image);
            params.push(&row.origin_lsn);
            params.push(&row.src_changed);
            params.push(&row.hop_gen);
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
