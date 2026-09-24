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
/// `intake::extract_key`/`defs::ddl::join_pk_key` themselves join
/// composite-key parts on, and this sentinel — unlike a real key — never
/// passes through that encoding at all: it is one fixed literal, compared
/// (and grouped by the fold) as an opaque whole string, not split back into
/// parts.
///
/// This is a *different* question from the one issue #200 closed in
/// `defs::ddl` (a real, in-value U+001F no longer misparses as a field
/// boundary, because a multi-column key now escapes one and a single-column
/// key is never split at all). Neither half of that fix narrows this
/// sentinel, which collides with any key whose text happens to *equal* this
/// exact literal — two ways, both pre-existing and both unchanged by #200:
/// a single-column key whose own value literally spells
/// `"\u{1f}trellis-truncate-sentinel"` (arity-1 keys are deliberately
/// verbatim, see `defs::ddl::push_escaped_composite_key_part`'s "why arity 1
/// is exempt"), and a composite key whose first declared-order component is
/// a genuine empty string and whose remaining components spell
/// `"trellis-truncate-sentinel"` (the U+001F between them is a real,
/// correctly-unescaped field boundary). Both are accepted as the same class
/// of vanishingly unlikely risk every fixed reserved-token sentinel in this
/// crate carries (`relationship_reverse_deferred_src_table` below included)
/// — narrowing them further would mean this sentinel stops being a short,
/// fixed, greppable literal, which is judged not worth it here.
pub const TRUNCATE_SENTINEL_KEY: &str = "\u{1f}trellis-truncate-sentinel";

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
    /// CDC intake's shape: an image-bearing decoded change. The
    /// target-mutation seam stages this shape too, for a relationship-endpoint
    /// target it feeds (issue #402, `staging::target_mutations`): `lsn` is
    /// then the writer's pre-commit write token, not a commit `end_lsn`, and
    /// `origin_lsn` is the origin of the change that produced the write
    /// (issue #469; see [`StagedChange::Recompute::origin_lsn`]). Intake
    /// stamps `origin_lsn` with the commit's own position.
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
        /// Issue #133: the union of every join-key value this row's own
        /// change touched — every column that is some relationship's
        /// `from_col`, read from `old_image` (if present) and `new_image`
        /// (if present). Populated by intake (`intake::mod::Intake`'s
        /// `handle_xlog_data`, using a cached outbound-relationship column
        /// map), backfill's replay of a spilled/parked change, or the
        /// target-mutation seam for a from-side endpoint target; `None`
        /// when this row's `src_table` has no outbound relationship at all,
        /// or (rare, transient) the catalog cache hasn't observed one yet.
        /// See `staging::fold`'s doc comment for the union merge rule this
        /// feeds, and `staging::apply::RelationshipGenBump` for why this
        /// (not the folded old/new image endpoints alone) is what guard
        /// (b) needs to catch a parent the fold erases entirely within one
        /// batch.
        group_key: Option<Vec<String>>,
    },
    /// The shape all three non-CDC producers (reverse propagation,
    /// definition re-derive, backfill) use: a bare, image-less recompute
    /// trigger. It asserts nothing about the row's state — only "recompute
    /// this key."
    ///
    /// `src_changed` (added for issues #51/#52's multi-hop gap) is the
    /// timestamp of the real source commit this recompute traces back to,
    /// when one is known — `Some` for forward/reverse propagation and
    /// quarantine replay (all of which inherit it from the triggering
    /// change), `None` for backfill's cursor-enumerated pre-existing rows
    /// (there is no meaningful origin for a row nothing ever "changed" —
    /// see `intake::publication`'s backfill enumeration). Without this, an
    /// automatically-propagated hop-to-hop chain (transform A's output
    /// feeding transform B as B's input, entirely through `Recompute` rows)
    /// would carry no origin at all past the first hop, leaving both the
    /// per-transform latency histogram (#51) and the end-to-end latency
    /// histogram (#52) unable to observe anything for it — exactly the gap
    /// this field closes.
    Recompute {
        src_table: String,
        key: String,
        hop_gen: i32,
        /// Always `None` in practice: a `Recompute` carries no image at all
        /// (see this variant's own doc comment), so there is no `old_image`/
        /// `new_image` to read a touched join-key value from — see
        /// [`StagedChange::Cdc::group_key`] for the field this mirrors and
        /// why it's meaningless here rather than merely unpopulated.
        group_key: Option<Vec<String>>,
        src_changed: Option<SystemTime>,
        /// Issue #315: the key's row image as it stood before the write that
        /// staged this recompute, when the producer knows it — only
        /// `staging::target_mutations` sets it, for a key it changed in a
        /// target table. It is a hint, not a delta: the recompute still
        /// re-reads the key live. A downstream aggregate also re-derives the
        /// group this image names, so a row that moved groups (or was
        /// deleted) leaves its old group correct too. Stored in the ring's
        /// `old_image` column under `op = 'recompute'`, which the fold keeps
        /// apart from real images (`FoldedChange::prior_image`).
        prior_image: Option<String>,
        /// The source commit this recompute traces back to (issue #469),
        /// carried forward from the triggering change exactly as
        /// `src_changed` is, so `converged_through` gates it only for tokens
        /// at or past that commit. `None` means unknown, which gates every
        /// token: a backfill-enumerated row, or a write with no source
        /// commit behind it.
        origin_lsn: Option<PgLsn>,
    },
    /// A source `TRUNCATE` of `src_table` (issue #60): one row per truncated
    /// relation, key-less (see [`TRUNCATE_SENTINEL_KEY`]) and image-less —
    /// it asserts nothing about any one row's state, only "every row this
    /// source ever produced is gone as of this position." `lsn`/`src_changed`
    /// are stamped at commit exactly like [`StagedChange::Cdc`]'s, by
    /// `intake::stamp_commit_metadata`, which also stamps `origin_lsn` with the
    /// commit's position, matching [`StagedChange::Cdc`] — a truncate always
    /// originates directly from the source, it is never a re-propagated
    /// downstream change.
    Truncate {
        src_table: String,
        lsn: Option<PgLsn>,
        origin_lsn: Option<PgLsn>,
        src_changed: Option<SystemTime>,
    },
    /// Issue #134, epic #127: a to-one relationship's parent-keyed reverse
    /// record (`staging::apply::RelationshipReverseRecord`) whose Phase 3
    /// apply one of issue #132's four guards rejected — persisted so a
    /// later drain can re-derive fresh guard state and retry it, without
    /// ever touching `hop_gen`/[`crate::staging::apply::MAX_HOP_GEN`] (see
    /// `retry_count`'s own doc comment: retrying is not propagation, and
    /// deferral is measured to be the *common* case for this mechanism, not
    /// the exception, so it must never risk tripping the hop bound).
    ///
    /// Carries the parent's own old/new image (`old_image`/`new_image`) and
    /// its own identity in the settled parent projection's LSN chain
    /// (`lsn`) — everything a retry needs to rebuild the exact same
    /// [`crate::staging::apply::RelationshipReverseRecord`] shape the
    /// original attempt used. Deliberately does **not** carry `prev_lsn`,
    /// `prev_gen`, or the watermark `X`: issue #134's resolved design
    /// fork is that those three guard inputs are re-derived fresh, live,
    /// at retry time (`staging::apply::capture_reverse_guard_state`) rather
    /// than replayed from their stale original capture — replaying them
    /// could wrongly pass a guard that should now fail, or wrongly fail one
    /// that would now legitimately pass, silently reintroducing exactly the
    /// class of bug issues #132/#133 closed.
    RelationshipReverseDeferred {
        /// A synthetic per-relationship identity (see
        /// `staging::apply::relationship_reverse_deferred_src_table`),
        /// never a real source table name — chosen so this op's rows never
        /// fold together, at the SQL fold's `(src_table, key)` grouping,
        /// with genuine CDC on the parent's *real* table. If they did, a
        /// deferred retry sitting alone in a later batch would reach the
        /// ordinary per-source forward-evaluation loop and double-apply
        /// the delta its original CDC row already forward-applied when it
        /// first landed (that forward apply is never itself deferred —
        /// only the reverse-relationship delta this variant stands in for
        /// is). Multiple deferred reverses for the *same* relationship and
        /// parent key, staged across more than one rejection, *do* fold
        /// together via this same synthetic identity — reusing the ring's
        /// existing first-old/last-new-image fold rule verbatim, the same
        /// "N changes to one key fold to one record" property issue #131
        /// already established for raw parent CDC.
        src_table: String,
        /// The parent's to-side key text (old key, preferring the
        /// pre-this-transition identity; the new key when there is no old
        /// one, i.e. a parent INSERT) — this op's own fold identity within
        /// its relationship's synthetic namespace above.
        key: String,
        old_image: Option<String>,
        new_image: Option<String>,
        /// This reverse's own identity in the settled parent projection's
        /// LSN chain (mirrors
        /// [`crate::staging::apply::RelationshipReverseRecord::lsn`]) — the
        /// folded parent change's own `GREATEST` `lsn`, unrelated to this
        /// ring row's own append position.
        lsn: Option<PgLsn>,
        src_changed: Option<SystemTime>,
        /// The parent change's own origin (issue #469), with
        /// [`StagedChange::Recompute::origin_lsn`]'s meaning.
        origin_lsn: Option<PgLsn>,
        /// Which `relationship_definitions.id` this reverse belongs to —
        /// how a later drain knows which
        /// [`crate::staging::apply::ReverseRelationshipShape`] to rebuild,
        /// without trying to parse one back out of `src_table` above.
        relationship_id: i64,
        /// This reverse's retry counter: 1 the first time a guard rejects
        /// it, incremented on every subsequent rejection — read back,
        /// folded (`MAX` across a batch's rows for the same key, mirroring
        /// `hop_gen`'s own fold rule), and threaded onto the reconstructed
        /// [`crate::staging::apply::RelationshipReverseRecord`] so metrics
        /// and (eventually, issue #135) a fairness mechanism can observe
        /// how many times a given reverse has been deferred. There is no
        /// `hop_gen` field on this variant at all — see this op's own
        /// migration/doc comment for why retrying must never touch it.
        retry_count: i32,
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
            StagedChange::RelationshipReverseDeferred { src_table, .. } => src_table,
        }
    }
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
    group_key: Option<&'a [String]>,
    /// Issue #134: meaningful only for `op = 'rel_reverse_deferred'` — `0`
    /// for every other variant, matching the ring's own column default.
    retry_count: i32,
    /// Issue #134: meaningful only for `op = 'rel_reverse_deferred'` —
    /// `None` for every other variant.
    relationship_id: Option<i64>,
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
                group_key,
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
                group_key: group_key.as_deref(),
                retry_count: 0,
                relationship_id: None,
            },
            StagedChange::Recompute {
                src_table,
                key,
                hop_gen,
                group_key,
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
                group_key: group_key.as_deref(),
                retry_count: 0,
                relationship_id: None,
            },
            StagedChange::Truncate {
                src_table,
                lsn,
                origin_lsn,
                src_changed,
            } => ChangeRow {
                src_table,
                key: TRUNCATE_SENTINEL_KEY,
                op: "truncate",
                lsn: *lsn,
                old_image: None,
                new_image: None,
                origin_lsn: *origin_lsn,
                src_changed: *src_changed,
                hop_gen: 0,
                group_key: None,
                retry_count: 0,
                relationship_id: None,
            },
            StagedChange::RelationshipReverseDeferred {
                src_table,
                key,
                old_image,
                new_image,
                lsn,
                src_changed,
                origin_lsn,
                relationship_id,
                retry_count,
            } => ChangeRow {
                src_table,
                key,
                op: "rel_reverse_deferred",
                lsn: *lsn,
                old_image: old_image.as_deref(),
                new_image: new_image.as_deref(),
                origin_lsn: *origin_lsn,
                src_changed: *src_changed,
                hop_gen: 0,
                group_key: None,
                retry_count: *retry_count,
                relationship_id: Some(*relationship_id),
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

    const COLUMNS: &str = "src_table, key, op, lsn, old_image, new_image, origin_lsn, src_changed, \
         hop_gen, group_key, retry_count, relationship_id";
    const COLS_PER_ROW: usize = 12;
    // Postgres's wire protocol caps a Bind message's parameter count at
    // i16::MAX (65535); 5000 rows keeps every chunk's param count
    // (60000) safely under that regardless of column count.
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
                "(${}, ${}, ${}, ${}, ${}::text::jsonb, ${}::text::jsonb, ${}, ${}, ${}, ${}, ${}, ${})",
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
                base + 12,
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
            params.push(&row.group_key);
            params.push(&row.retry_count);
            params.push(&row.relationship_id);
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
