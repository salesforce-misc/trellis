//! Publication and slot lifecycle (issue #8) — see "Adjacent invariants
//! that are easy to miss" and "Failure modes" in
//! docs/staging-and-claiming/01-intake-and-lsn-confirmation.md:
//!
//! - [`reconcile_publication`] changes a publication's table set in place
//!   (`ALTER PUBLICATION ... ADD/DROP TABLE`), never by dropping and
//!   recreating it.
//! - [`run_pending_backfills`] discharges the `pending_backfill` markers
//!   [`reconcile_publication`] leaves behind: a newly-added table's
//!   pre-existing rows, staged by enumeration once the marker's transaction
//!   fence has settled and intake has staged everything the enumeration's
//!   snapshot sees (issue #312).
//! - [`initial_snapshot_handshake`] creates a slot and backfills every
//!   watched table from the exact snapshot the slot's creation exports —
//!   gap-free by construction.
//! - [`require_slot_healthy`] is the loud startup check for slot
//!   invalidation/loss.

use std::collections::BTreeSet;
use std::time::Duration;

use tokio_postgres::types::PgLsn;
use tokio_postgres::{GenericClient, Transaction};

use super::error::IntakeError;
use crate::defs::model::TransformStatus;
use crate::pool::quote_ident;
use crate::staging::append::{self, StagedChange};
use crate::staging::session::ProducerSession;
use crate::staging::watermark::StagedWatermark;

/// Joins `schema` and `table` into the `"schema.table"` shape
/// [`StagedChange::src_table`] uses throughout this crate — the *only* place
/// that string is built. Enforces by construction the invariant
/// [`split_qualified`] depends on: neither component may itself contain a
/// `.`. A schema or table literally named `a.b` is legal Postgres (quoted),
/// but the joined-string representation has no way to tell that dot apart
/// from the separator, so [`split_qualified`] would silently mis-split it —
/// this turns that into a loud error at the point the ambiguous string would
/// otherwise be built.
pub(crate) fn qualify(schema: &str, table: &str) -> Result<String, IntakeError> {
    if schema.contains('.') {
        return Err(IntakeError::DottedIdentifierComponent {
            component: schema.to_string(),
        });
    }
    if table.contains('.') {
        return Err(IntakeError::DottedIdentifierComponent {
            component: table.to_string(),
        });
    }
    Ok(format!("{schema}.{table}"))
}

/// Splits a `"schema.table"` name (the shape [`StagedChange::src_table`]
/// uses throughout this crate) into its parts, for building DDL/enumeration
/// SQL that needs them quoted separately. Unambiguous for every name this
/// crate can actually produce: [`qualify`] is the sole construction site for
/// `src_table`, and it rejects any component containing a `.` before
/// joining — so the first `.` here is always the real separator, never one
/// smuggled in through a dotted identifier.
fn split_qualified(name: &str) -> Result<(&str, &str), IntakeError> {
    name.split_once('.')
        .ok_or_else(|| IntakeError::InvalidTableName(name.to_string()))
}

async fn current_publication_tables(
    client: &impl GenericClient,
    publication: &str,
) -> Result<BTreeSet<String>, IntakeError> {
    let rows = client
        .query(
            "select schemaname, tablename from pg_publication_tables where pubname = $1",
            &[&publication],
        )
        .await?;
    rows.into_iter()
        .map(|r| qualify(&r.get::<_, String>(0), &r.get::<_, String>(1)))
        .collect()
}

/// Reconciles `publication`'s table set to exactly `desired_tables`, in
/// place — **never** by dropping and recreating the publication, which would
/// orphan the slot's retention and lose rows written in the gap. Adding a
/// table commits the `ALTER` and a `pending_backfill` marker in one
/// transaction, so the follow-up enumeration ([`run_pending_backfills`]) is
/// exactly as durable as the schema change that requires it. Idempotent:
/// safe to call on every setup pass, and (issue #14) on every periodic
/// re-reconciliation pass a running client's maintenance loop makes.
///
/// Takes a plain `&mut tokio_postgres::Client` rather than a
/// [`crate::staging::session::ProducerSession`]: nothing here needs that
/// session's guards (`synchronous_commit`, the producer singleton advisory
/// lock) — only its `transaction()`/`client()` shape, which a plain
/// `Client` has too. That matters for a running client's maintenance loop,
/// whose own connection is never a `ProducerSession`: the one already-live
/// `ProducerSession` for the whole client lifetime is intake's own (held for
/// as long as [`super::Intake`] runs), and the singleton lock it holds is
/// session-scoped — a second `ProducerSession::connect` call while intake is
/// running would simply fail to acquire it.
pub async fn reconcile_publication(
    client: &mut tokio_postgres::Client,
    publication: &str,
    desired_tables: &[String],
) -> Result<(), IntakeError> {
    let current = current_publication_tables(client, publication).await?;
    let desired: BTreeSet<&String> = desired_tables.iter().collect();

    let to_add: Vec<&String> = desired_tables
        .iter()
        .filter(|t| !current.contains(*t))
        .collect();
    let to_drop: Vec<&String> = current.iter().filter(|t| !desired.contains(t)).collect();

    if to_add.is_empty() && to_drop.is_empty() {
        return Ok(());
    }

    let txn = client.transaction().await?;
    for table in &to_drop {
        let (schema, name) = split_qualified(table)?;
        txn.execute(
            &format!(
                "alter publication {} drop table {}.{}",
                quote_ident(publication),
                quote_ident(schema),
                quote_ident(name)
            ),
            &[],
        )
        .await?;
    }
    for table in &to_add {
        let (schema, name) = split_qualified(table)?;
        txn.execute(
            &format!(
                "alter publication {} add table {}.{}",
                quote_ident(publication),
                quote_ident(schema),
                quote_ident(name)
            ),
            &[],
        )
        .await?;
        // The fence: captured in the *same* transaction as the ADD, so it
        // names exactly the transactions concurrent with this table joining
        // the stream — see the module doc.
        park_marker(&txn, table).await?;
    }
    txn.commit().await?;
    Ok(())
}

/// Parks a catch-up marker for `definition_id`'s *target* table when some
/// `live` definition reads it (issue #315). Called wherever a definition goes
/// live after a build that wrote its target outside the target-mutation seam
/// (`staging::target_mutations`): a reader already attached (only possible
/// for a rebuilt, resumed upstream, since `defs::catalog` refuses to attach a
/// new one to a non-`live` target) re-derives from the rebuilt state once
/// the marker discharges. A no-op for a target nothing reads.
pub(crate) async fn park_target_catchup_if_read(
    client: &impl GenericClient,
    definition_id: i64,
) -> Result<(), IntakeError> {
    let target: Option<String> = client
        .query_opt(
            "select d.target_table from transform_definitions d \
             where d.id = $1 and exists ( \
                 select 1 from transform_definitions r \
                 where r.source_table = d.target_table and r.status = 'live' \
             )",
            &[&definition_id],
        )
        .await?
        .map(|row| row.get(0));
    if let Some(target) = target {
        park_backfill_catchup(client, &target).await?;
    }
    Ok(())
}

/// Parks a fresh catch-up marker for `qualified_table`, reusing the exact
/// `pending_backfill` mechanism [`reconcile_publication`] already relies on
/// for a table newly joining the publication (docs/decisions/0007's
/// amendment). `defs::chunk_queue::complete_direct_backfill` calls this the
/// moment a direct-build definition flips `backfilling` -> `live`: while it
/// sat non-`live`, [`super::super::defs::dependents_of`]'s status filter kept
/// any live CDC delta for `qualified_table` from being folded into its
/// target, so the definition's target may be missing whatever changed on
/// that table during the build. This marker's later discharge
/// ([`run_pending_backfills`]) re-derives every definition on `qualified_table`
/// (now including the newly-`live` one) from current source state, folding
/// in anything skipped meanwhile — the same `run_pending_backfills`-shaped
/// event the amendment describes, just triggered by "every chunk done"
/// instead of "a table newly joined the publication."
///
/// See [`park_marker`] for what happens when a marker for this table already
/// exists.
pub(crate) async fn park_backfill_catchup(
    client: &impl GenericClient,
    qualified_table: &str,
) -> Result<(), IntakeError> {
    Ok(park_marker(client, qualified_table).await?)
}

/// Parks a `pending_backfill` marker for `qualified_table`, fenced at the
/// calling transaction's current snapshot. Every writer of a marker goes
/// through here.
///
/// A table has at most one marker, so a park that finds one already there
/// merges into it (issues #311/#367). It can't simply leave the existing row
/// alone: that row may be mid-discharge, its enumeration snapshot already
/// taken before this caller's change, and the discharge would then delete the
/// only marker and this caller's catch-up would never run. Instead the park
/// gives the row a fresh `generation`, and [`run_pending_backfills`] deletes
/// only the generation it read. A discharge that raced this park leaves the
/// marker for the next pass, which enumerates again from a snapshot that
/// includes this caller's commit.
///
/// The merged row keeps whichever fence is later. Settlement compares only
/// the fence's `xmax` ([`Snapshot::settled_since`]), so the later fence waits
/// for everything either park had to wait for. The fence this statement read
/// can be the older one when it had to wait on the row lock of a concurrent
/// park.
///
/// Returns the bare Postgres error so [`crate::Trellis::request_backfill`]
/// can report it as the plain database failure it is.
pub(crate) async fn park_marker(
    client: &impl GenericClient,
    qualified_table: &str,
) -> Result<(), tokio_postgres::Error> {
    client
        .execute(
            "insert into pending_backfill as pb (table_name, fence_snapshot) \
             values ($1, pg_current_snapshot()) \
             on conflict (table_name) do update set \
               fence_snapshot = case \
                 when pg_snapshot_xmax(excluded.fence_snapshot) > pg_snapshot_xmax(pb.fence_snapshot) \
                 then excluded.fence_snapshot else pb.fence_snapshot end, \
               generation = default, \
               added_at = now()",
            &[&qualified_table],
        )
        .await?;
    Ok(())
}

/// A parsed `pg_snapshot` text representation (`"xmin:xmax:xip,..."`) — only
/// `xmin`/`xmax` matter for fence settlement, so the in-progress list is
/// parsed for validity and otherwise discarded.
#[derive(Debug)]
struct Snapshot {
    xmin: i64,
    xmax: i64,
}

impl Snapshot {
    fn parse(text: &str) -> Result<Self, IntakeError> {
        let mut parts = text.split(':');
        let xmin = parts
            .next()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| IntakeError::InvalidSnapshot(text.to_string()))?;
        let xmax = parts
            .next()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| IntakeError::InvalidSnapshot(text.to_string()))?;
        Ok(Self { xmin, xmax })
    }

    /// Whether every transaction that could have been in flight when `fence`
    /// was captured has since settled (committed or aborted). `xmin` only
    /// advances past a transaction once it completes — so a transaction
    /// concurrent with the fence, still open, holds `xmin` at its own xid
    /// for as long as it stays open. The comparison must be **strict**:
    /// `pg_current_snapshot()`'s `xmax` is "one past the latest *completed*
    /// txid," which says nothing about a still-running transaction whose xid
    /// happens to equal it (the fence's own ADD transaction is exactly such
    /// a case) — `self.xmin > fence.xmax`, not `>=`, is what actually forces
    /// that transaction to complete first.
    fn settled_since(&self, fence: &Snapshot) -> bool {
        self.xmin > fence.xmax
    }
}

async fn current_snapshot(client: &impl GenericClient) -> Result<Snapshot, IntakeError> {
    let text: String = client
        .query_one("select pg_current_snapshot()::text", &[])
        .await?
        .get(0);
    Snapshot::parse(&text)
}

/// Whether `qualified_table` currently has a durable `pending_backfill`
/// marker whose `xmin` fence has **not** yet settled (issue #55) — i.e.
/// whether enumerating this table's rows right now would race a transaction
/// the marker's fence was captured against.
///
/// `false` covers two different "safe to enumerate now" cases the caller
/// doesn't need to distinguish: no marker at all (nothing pending for this
/// table), and a marker whose fence has already settled but
/// [`run_pending_backfills`] simply hasn't discharged it yet. Both mean a
/// synchronous enumeration started right now would see a state
/// [`run_pending_backfills`]'s later discharge is guaranteed to see too (or
/// a strict superset of it), so there is nothing to defer.
///
/// Used at definition-creation time
/// ([`crate::defs::catalog::create_definition_inner`],
/// [`crate::defs::catalog::install_definition`]) to decide whether a fresh
/// transform's initial backfill should run synchronously (the existing,
/// unconditional behavior) or defer to `waiting_to_backfill` and ride this
/// same marker's own discharge instead — see docs/observability.md's
/// "Backfill status and the `xmin` caveat."
pub(crate) async fn backfill_marker_unsettled(
    client: &impl GenericClient,
    qualified_table: &str,
) -> Result<bool, IntakeError> {
    let Some(row) = client
        .query_opt(
            "select fence_snapshot::text from pending_backfill where table_name = $1",
            &[&qualified_table],
        )
        .await?
    else {
        return Ok(false);
    };
    let fence_text: String = row.get(0);
    let fence = Snapshot::parse(&fence_text)?;
    let now = current_snapshot(client).await?;
    Ok(!now.settled_since(&fence))
}

struct PendingBackfill {
    table: String,
    fence: Snapshot,
    /// Which park of `table` this is ([`park_marker`]). Discharge deletes
    /// only this generation.
    generation: i64,
}

async fn fetch_pending_backfills(
    client: &impl GenericClient,
) -> Result<Vec<PendingBackfill>, IntakeError> {
    let rows = client
        .query(
            "select table_name, fence_snapshot::text, generation from pending_backfill",
            &[],
        )
        .await?;
    rows.into_iter()
        .map(|r| {
            let table: String = r.get(0);
            let fence_text: String = r.get(1);
            let generation: i64 = r.get(2);
            Snapshot::parse(&fence_text).map(|fence| PendingBackfill {
                table,
                fence,
                generation,
            })
        })
        .collect()
}

/// Page size for [`enumerate_and_append`]'s server-side cursor. Bounds one
/// backfill enumeration pass to O(page) memory rather than O(table) — the
/// same class of fix issue #8 gave the CDC stream path via [`super::spill`],
/// applied here to the enumeration path that used to `SELECT` an entire
/// source table into one `Vec`. 10k rows keeps each `FETCH` round-trip cheap
/// while staying a trivial allocation even for a table with wide keys.
const BACKFILL_PAGE_ROWS: i64 = 10_000;

/// The fixed cursor name [`enumerate_and_append`] declares. A literal, not
/// caller input, so it needs no quoting/escaping of its own — only the
/// schema/table it selects from does.
const BACKFILL_CURSOR: &str = "trellis_backfill_cursor";

/// Enumerates `src_table`'s current rows as image-less
/// [`StagedChange::Recompute`] triggers — the shape that asserts nothing
/// about a row's state, which is all a backfill actually knows ("this key
/// exists as of now," not any particular before/after image) — and appends
/// each page directly into the active ring segment, inside `txn`.
///
/// Streams via a server-side `DECLARE ... CURSOR` rather than one `SELECT`
/// of the whole table: the earlier version bought a `Vec<StagedChange>` the
/// same size as the source table, unbounded memory for a backfill of any
/// real size (the same class of bug issue #8 fixed for the stream path via
/// spill). A cursor only lives for the transaction that declares it, which
/// is exactly the scope `txn` already has here — so paging changes nothing
/// about the one-transaction durability guarantee both callers rely on.
///
/// Also reused by definition creation's backfill (issue #23): a definition
/// enumerates its source exactly once here regardless of how many
/// calculated fields it declares, preserving the "N columns, one backfill"
/// property as the definition model becomes first-class.
pub(crate) async fn enumerate_and_append(
    txn: &Transaction<'_>,
    src_table: &str,
) -> Result<(), IntakeError> {
    declare_enumeration(txn, src_table).await?;
    append_enumeration(txn, src_table).await
}

/// The first half of [`enumerate_and_append`]: declares the enumeration
/// cursor over `src_table`'s identity key.
///
/// Split out for issue #312: the cursor's snapshot is fixed here, at
/// `DECLARE`, so [`run_pending_backfills`] can capture the WAL position that
/// bounds everything that snapshot sees, and wait for intake to stage up to
/// it, *before* [`append_enumeration`] writes a single `Recompute` row.
async fn declare_enumeration(txn: &Transaction<'_>, src_table: &str) -> Result<(), IntakeError> {
    let (schema, table) = split_qualified(src_table)?;
    let from = format!("{}.{}", quote_ident(schema), quote_ident(table));
    // Issue #308: the table's row-identity key as `ddl` defines it — its
    // `PRIMARY KEY`, or, for an aggregate target (which has none), the
    // `UNIQUE NULLS NOT DISTINCT` grouping columns. Looking only for a
    // primary key used to fail every catch-up marker parked on an aggregate
    // target with `MissingKeyValue`, which left the marker in place to fail
    // every later pass the same way.
    let key_cols = crate::defs::ddl::identity_key_columns(txn, &from).await?;
    if key_cols.is_empty() {
        return Err(IntakeError::MissingKeyValue {
            table: src_table.to_string(),
        });
    }
    // Rendered SQL-side by `ddl::pk_key_sql_expr`, the same expression every
    // other producer of this table's key text uses, so the staged key is
    // byte-identical to what the live path stages for the same row: raw
    // `col::text` for a NOT NULL column (every real primary key; see
    // `ddl::encode_key_part`'s "`PrimaryKeyColumn::nullable` selects the
    // encoding" for why that must stay raw), the NULL-sentinel encoding for
    // a nullable grouping column (matching
    // `staging::apply_aggregate::derive_group_key`, issue #110), and the
    // composite separator escape at arity > 1 (issue #200), all in declared
    // key order (issue #163).
    let key_expr = crate::defs::ddl::pk_key_sql_expr(&key_cols, None);
    txn.batch_execute(&format!(
        "declare {BACKFILL_CURSOR} cursor for select {key_expr} from {from}"
    ))
    .await?;
    Ok(())
}

/// The second half of [`enumerate_and_append`]: pages the cursor
/// [`declare_enumeration`] opened into the active ring segment as
/// image-less `Recompute` rows, then closes it.
async fn append_enumeration(txn: &Transaction<'_>, src_table: &str) -> Result<(), IntakeError> {
    loop {
        let rows = txn
            .query(
                &format!("fetch forward {BACKFILL_PAGE_ROWS} from {BACKFILL_CURSOR}"),
                &[],
            )
            .await?;
        if rows.is_empty() {
            break;
        }
        let mut page = Vec::with_capacity(rows.len());
        for row in &rows {
            // Already fully encoded by `key_expr` above.
            let key: String = row.get(0);
            page.push(StagedChange::Recompute {
                src_table: src_table.to_string(),
                key,
                hop_gen: 0,
                // Issue #133: intentionally `None`, not merely unpopulated
                // — a `Recompute` carries no `old_image`/`new_image` (see
                // `StagedChange::Recompute`'s own doc comment), and
                // `group_key`'s value is specifically "which join-key
                // values this row's own image-bearing history touched," so
                // there is no image here to read one from. This is the
                // "backfill-enumeration producer" the issue's own text
                // flags as needing "the equivalent treatment" — the
                // equivalent treatment for an image-less producer is
                // leaving this `None`, matching every other `Recompute`
                // producer (reverse propagation, definition re-derive).
                group_key: None,
                // No meaningful origin for a backfill's cursor-enumerated
                // pre-existing row — nothing "changed" it; see
                // `StagedChange::Recompute`'s doc comment.
                src_changed: None,
                prior_image: None,
            });
        }
        append::append(txn, &page).await?;
    }
    txn.batch_execute(&format!("close {BACKFILL_CURSOR}"))
        .await?;
    Ok(())
}

/// A fence snapshot plus the row count captured at that same instant, for a
/// table a direct backfill is about to fold into a target (issue #79, bug B).
/// Produced by [`capture_backfill_coverage_fence`] *before* the build reads the
/// table and later persisted by [`write_backfill_coverage`].
#[derive(Clone, Debug)]
pub struct CoverageFence {
    pub fence: String,
    pub count: i64,
}

/// Captures the fence snapshot and row count for `qualified_table` in one
/// statement (issue #79, bug B) — so both describe the same MVCC instant.
///
/// **This must be called before the direct build reads the table**, not after.
/// The build reads each relationship's to-side table once (into a staging
/// table) and the source in progressive per-chunk statements, each under its
/// own autocommit snapshot — there is no single build snapshot. A row that is
/// inserted or updated *after* the build reads it but *before* a fence captured
/// post-build would be visible in that post-build fence yet absent from the
/// target, so [`coverage_covers`] would wrongly skip its catch-up and drop the
/// change (the table joins the publication later still, so CDC never carries
/// it either). A fence captured before any build read cannot vouch for such a
/// row: its `xmin` is invisible in the earlier fence, so `coverage_covers`
/// falls back to enumeration — the safe default the fix demands.
pub async fn capture_backfill_coverage_fence(
    client: &impl GenericClient,
    qualified_table: &str,
) -> Result<CoverageFence, IntakeError> {
    let (schema, table) = split_qualified(qualified_table)?;
    let row = client
        .query_one(
            &format!(
                "select count(*)::bigint, pg_current_snapshot()::text from {}.{}",
                quote_ident(schema),
                quote_ident(table)
            ),
            &[],
        )
        .await?;
    Ok(CoverageFence {
        count: row.get(0),
        fence: row.get(1),
    })
}

/// Persists a coverage record: as of `fence.fence`, `qualified_table` held
/// `fence.count` rows, all of which a direct backfill folded into an
/// already-built target (issue #79, bug B). [`coverage_covers`] later consults
/// this to skip a redundant catch-up enumeration.
///
/// A plain upsert (last write wins) is correct because the caller
/// ([`crate::defs::catalog::install_definition`]) only ever records coverage
/// for a table with exactly one reader, and *clears* it (see
/// [`clear_backfill_coverage`]) the moment a second reader appears — so two
/// live recordings for one table never coexist to be reconciled.
pub async fn write_backfill_coverage(
    client: &impl GenericClient,
    qualified_table: &str,
    fence: &CoverageFence,
) -> Result<(), IntakeError> {
    client
        .execute(
            "insert into backfill_coverage (table_name, fence_snapshot, covered_row_count) \
             values ($1, $2::text::pg_snapshot, $3) \
             on conflict (table_name) do update set \
               fence_snapshot = excluded.fence_snapshot, \
               covered_row_count = excluded.covered_row_count",
            &[&qualified_table, &fence.fence, &fence.count],
        )
        .await?;
    Ok(())
}

/// Captures a fence for `qualified_table` and immediately persists it — the
/// capture-and-write-at-once convenience used by tests that record coverage for
/// a table nothing is concurrently building. Real installs
/// ([`crate::defs::catalog::install_definition`]) must instead
/// [`capture_backfill_coverage_fence`] *before* the build and
/// [`write_backfill_coverage`] after it (see the former's doc comment).
#[cfg(any(test, feature = "internals"))]
pub async fn record_backfill_coverage(
    client: &impl GenericClient,
    qualified_table: &str,
) -> Result<(), IntakeError> {
    let fence = capture_backfill_coverage_fence(client, qualified_table).await?;
    write_backfill_coverage(client, qualified_table, &fence).await
}

/// Drops any coverage record for `qualified_table` — called when a table gains
/// a second reader (so the single-reader assumption [`write_backfill_coverage`]
/// relies on no longer holds). Removing the record forces [`coverage_covers`]
/// back to full enumeration — the safe default.
pub async fn clear_backfill_coverage(
    client: &impl GenericClient,
    qualified_table: &str,
) -> Result<(), IntakeError> {
    client
        .execute(
            "delete from backfill_coverage where table_name = $1",
            &[&qualified_table],
        )
        .await?;
    Ok(())
}

/// Whether `table`'s pre-existing rows are already fully reflected in a
/// directly-built target and its catch-up enumeration can therefore be
/// skipped (issue #79, bug B). Returns `false` — meaning "enumerate, the safe
/// default" — whenever anything is uncertain.
///
/// # Why not a plain fence comparison
///
/// The obvious design — "skip if the build's fence is at least as recent as
/// the table's publication-join fence" — cannot work, because the direct
/// build *always* precedes the join in time: a definition is built (reading
/// the current source + relationship tables) and only *afterward* does the
/// periodic reconcile loop notice those tables and add them to the
/// publication. So the build snapshot's xids are always *older* than the join
/// fence's, and any whole-snapshot `settled_since`-style test would either
/// never skip (useless) or always skip (unsafe) — the global xid clock
/// advances between build and join regardless of whether *this* table saw any
/// write, so it cannot answer the only question that matters: did **this
/// table** change in the gap between the build and the join?
///
/// # What we actually check
///
/// The build's coverage record (`backfill_coverage`) names the exact snapshot
/// `fence` at which the table was fully folded into a target, plus the row
/// count at that instant. The table's contribution is unchanged since the
/// fence — and the build therefore still fully covers it — iff **all three**
/// hold:
///
/// 1. No surviving row was inserted or updated after the fence: every live
///    row's `xmin` is visible in `fence` (`pg_visible_in_snapshot`). An insert
///    or in-place update stamps a fresh, fence-invisible `xmin`, so this
///    catches both.
/// 2. The current row count equals the recorded count. This is what catches a
///    *delete*, whose tuple simply vanishes and so leaves no invisible `xmin`
///    behind. (Insert-then-delete churn that nets to the same count is still
///    caught by rule 1 via the inserted row's `xmin` — unless that row was
///    also deleted, in which case the table's contribution genuinely did not
///    change and skipping is correct.)
/// 3. Both the fence and the current snapshot are in xid epoch 0 (their
///    `pg_snapshot` xmax is below 2^32). The `xmin::text::xid8` cast in rule 1
///    reads a 32-bit tuple xid as an epoch-0 `xid8`; once the xid counter has
///    wrapped (epoch > 0) that reconstruction is wrong, so past the first
///    wraparound we conservatively refuse to skip rather than risk comparing
///    across epochs. Every case short of ~2^32 lifetime transactions — all
///    tests, and any realistic build→join window — is epoch 0.
///
/// A concurrent writer is a non-issue: the caller only reaches here once the
/// marker's own fence has settled, and an as-yet-uncommitted row is simply not
/// visible in `fence`, so it lands on the safe side (rule 1 fails → enumerate).
async fn coverage_covers(txn: &Transaction<'_>, table: &str) -> Result<bool, IntakeError> {
    let (schema, name) = split_qualified(table)?;
    let Some(row) = txn
        .query_opt(
            "select fence_snapshot::text, covered_row_count \
             from backfill_coverage where table_name = $1",
            &[&table],
        )
        .await?
    else {
        return Ok(false);
    };
    let fence: String = row.get(0);
    let count: i64 = row.get(1);
    let covered: bool = txn
        .query_one(
            &format!(
                "select \
                   pg_snapshot_xmax($1::text::pg_snapshot) < '4294967296'::xid8 \
                   and pg_snapshot_xmax(pg_current_snapshot()) < '4294967296'::xid8 \
                   and (select count(*)::bigint from {sch}.{tbl}) = $2 \
                   and not exists ( \
                     select 1 from {sch}.{tbl} \
                     where not pg_visible_in_snapshot(xmin::text::xid8, $1::text::pg_snapshot) \
                   )",
                sch = quote_ident(schema),
                tbl = quote_ident(name),
            ),
            &[&fence, &count],
        )
        .await?
        .get(0);
    Ok(covered)
}

/// Runs every pending backfill whose fence has settled, staging its
/// pre-existing rows and deleting the marker in the *same* transaction as
/// that staging commit. A crash between the `ALTER` and this point leaves
/// the marker durable; a fence that hasn't settled yet is left alone for the
/// next setup pass — this function is meant to be retried on every one.
///
/// Issue #79 (bug B): before enumerating a marker's table, consult
/// [`coverage_covers`]. When a direct backfill has already folded the table's
/// current contents into a target and the table provably hasn't changed since,
/// the enumeration would stage millions of `Recompute` markers that only
/// re-derive already-correct values — so the marker is discharged with nothing
/// staged. Any uncertainty falls back to the full enumeration this has always
/// done, so the skip can never drop work.
///
/// Issue #55: a marker whose fence hasn't settled is where a transform sits
/// in [`TransformStatus::WaitingToBackfill`] — but that status is written
/// once, at creation time ([`backfill_marker_unsettled`]'s callers), not
/// here on every unsettled pass; there is nothing left for this function to
/// *set* for an unsettled marker; `continue` is unchanged. Once a marker
/// *does* settle, [`advance_deferred_definitions`] promotes exactly the
/// definitions that were deferred because of *this* marker —
/// `waiting_to_backfill` -> `backfilling` before the enumeration below, then
/// -> `live` after it commits — never a sibling definition on the same
/// table that's `backfilling`/`live` for an unrelated reason (a live
/// definition's own catch-up marker, or a concurrently-running
/// `backfill_chunks` build), since those were never `waiting_to_backfill` in
/// the first place.
///
/// # Waiting for intake before staging (issue #312)
///
/// A marker's table is already in the publication when its enumeration runs,
/// so a change committed between the `ALTER` and the enumeration reaches the
/// ring twice: once as its own CDC delta, and once inside the enumeration's
/// image-less `Recompute`, which an aggregate turns into a full re-derive of
/// the group from live state. That is harmless only if the delta lands in
/// the same batch as the recompute, or an earlier one. If the recompute's
/// batch seals first, the aggregate re-derives a value that already includes
/// the change, and the delta arriving in a later batch adds it a second time.
/// Intake lags the source, so without a gate the recompute rows routinely
/// reach the ring ahead of the CDC for changes the enumeration already saw.
///
/// So the enumeration does not append until intake has staged everything
/// its cursor can see. The cursor's snapshot is fixed at `DECLARE`; every
/// commit visible to it ends before `pg_current_wal_insert_lsn()` read right
/// after. Once `watermark` (intake's in-process staged-through position)
/// reaches that LSN, those commits' CDC rows are committed in the ring, so
/// the `Recompute` rows appended afterward resolve the ring pointer later
/// and commit later: they land in the same batch as that CDC or a later one.
///
/// If intake doesn't get there within `catch_up_timeout`, the enumeration
/// rolls back, the marker stays, and any definition this pass promoted to
/// `backfilling` returns to `waiting_to_backfill`, so the next pass retries
/// cleanly. The pass then stops rather than waiting again on the remaining
/// markers: each later horizon is at least as far ahead, so every one would
/// most likely time out too, and the maintenance loop seals nothing while
/// this waits. A caller with no intake running yet must not call this at all
/// (see `client::setup_staging`); tests with no CDC stream pass
/// [`crate::staging::StagedWatermark::saturated`].
// The maintenance loop calls [`run_pending_backfills_until`] so it can stop
// the wait on shutdown. This no-stop form is the tests' entry point, so
// nothing in the crate calls it unless `internals` exposes it.
#[allow(dead_code)]
pub async fn run_pending_backfills(
    client: &mut tokio_postgres::Client,
    wake_channel: &str,
    watermark: &StagedWatermark,
    catch_up_timeout: Duration,
) -> Result<(), IntakeError> {
    run_pending_backfills_until(client, wake_channel, watermark, catch_up_timeout, &|| false).await
}

/// [`run_pending_backfills`], with `stop` checked while an enumeration waits
/// for intake. When `stop` returns `true` the wait gives up early, through
/// the same rollback-and-revert path as a timeout.
///
/// This is how the maintenance loop shuts down promptly. It must not drop
/// the future mid-wait instead: the `waiting_to_backfill -> backfilling`
/// promotion is already committed by then, so dropping would skip the revert
/// and leave the definition in `backfilling` for good, since a later pass
/// only promotes `waiting_to_backfill` definitions and so never flips it to
/// `live`.
#[tracing::instrument(
    name = "intake.run_pending_backfills",
    skip(client, wake_channel, watermark, stop),
    fields(pending = tracing::field::Empty, settled = tracing::field::Empty)
)]
pub(crate) async fn run_pending_backfills_until(
    client: &mut tokio_postgres::Client,
    wake_channel: &str,
    watermark: &StagedWatermark,
    catch_up_timeout: Duration,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<(), IntakeError> {
    let pending = fetch_pending_backfills(client).await?;
    tracing::Span::current().record("pending", pending.len());
    if pending.is_empty() {
        return Ok(());
    }
    let now = current_snapshot(client).await?;

    let mut settled = 0usize;
    for marker in pending {
        if !now.settled_since(&marker.fence) {
            // Issue #56/`docs/observability.md`'s "Backfill status and the
            // `xmin` caveat": deliberately *not* a warning — sitting here is
            // safe, not a fault, per that section's explicit "we do not
            // emit a stall metric, a periodic warning log" decision. `debug`
            // only, for someone tracing this loop's own behavior.
            tracing::debug!(
                table = %marker.table,
                "backfill marker not yet settled; still waiting on the xmin fence"
            );
            continue;
        }
        settled += 1;
        let advancing = advance_deferred_definitions(
            client,
            &marker.table,
            TransformStatus::WaitingToBackfill,
            TransformStatus::Backfilling,
        )
        .await?;

        let txn = client.transaction().await?;
        let staged = if coverage_covers(&txn, &marker.table).await? {
            false
        } else {
            declare_enumeration(&txn, &marker.table).await?;
            let horizon: PgLsn = txn
                .query_one("select pg_current_wal_insert_lsn()", &[])
                .await?
                .get(0);
            if !intake_caught_up(watermark, horizon, catch_up_timeout, stop).await {
                txn.rollback().await?;
                revert_to_waiting(client, &advancing).await?;
                tracing::debug!(
                    table = %marker.table,
                    horizon = %horizon,
                    staged_through = %watermark.get(),
                    "backfill enumeration deferred: intake has not staged through its snapshot yet"
                );
                break;
            }
            append_enumeration(&txn, &marker.table).await?;
            true
        };
        // Delete only the marker this pass read (issues #311/#367). A park
        // since then gave the row a new generation, so it stays for the next
        // pass: this enumeration's snapshot may predate that park's change.
        // `skip locked` covers a park that is still in flight: the row stays
        // either way. If that park commits, its generation is new anyway; if
        // it rolls back, the next pass re-runs this marker, which is
        // redundant but safe. Waiting for it instead would hold the
        // maintenance loop on a caller's transaction.
        txn.execute(
            "delete from pending_backfill where table_name in ( \
                 select table_name from pending_backfill \
                 where table_name = $1 and generation = $2 \
                 for update skip locked)",
            &[&marker.table, &marker.generation],
        )
        .await?;
        if staged {
            txn.execute("select pg_notify($1, '')", &[&wake_channel])
                .await?;
        }
        txn.commit().await?;

        mark_definitions_live(client, &advancing).await?;
    }
    tracing::Span::current().record("settled", settled);
    Ok(())
}

/// How often [`intake_caught_up`] re-reads the in-process watermark. The read
/// is a bare atomic load, so this only bounds how late the wait notices.
const CATCH_UP_POLL: Duration = Duration::from_millis(5);

/// Waits until `watermark` reaches `horizon`, `timeout` elapses, or `stop`
/// returns `true`. Returns whether intake got there — see
/// [`run_pending_backfills`]'s "Waiting for intake before staging".
async fn intake_caught_up(
    watermark: &StagedWatermark,
    horizon: PgLsn,
    timeout: Duration,
    stop: &(dyn Fn() -> bool + Sync),
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if watermark.get() >= horizon {
            return true;
        }
        if stop() || tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(CATCH_UP_POLL).await;
    }
}

/// Undoes [`advance_deferred_definitions`]'s `waiting_to_backfill` ->
/// `backfilling` promotion for exactly `ids`, when the enumeration that
/// promotion announced was deferred instead of run. Scoped to `ids` and to
/// the `backfilling` status for the same reason that function is.
async fn revert_to_waiting(client: &impl GenericClient, ids: &[i64]) -> Result<(), IntakeError> {
    if ids.is_empty() {
        return Ok(());
    }
    client
        .execute(
            "update transform_definitions set status = $1 where id = any($2) and status = $3",
            &[
                &TransformStatus::WaitingToBackfill.as_str(),
                &ids,
                &TransformStatus::Backfilling.as_str(),
            ],
        )
        .await?;
    Ok(())
}

/// Promotes every `transform_definitions` row sourced from `table` (matched
/// against `source_table`'s own persisted, fully-qualified shape — issue
/// #72; `table` itself is always already qualified here, since every caller
/// derives it from `pending_backfill.table_name`) currently in `from` to
/// `to`, returning the ids actually moved (issue #55).
///
/// Scoped by both `source_table` and current `status`, so a second,
/// unrelated definition on the same table sitting in some other status for
/// an unrelated reason (e.g. still `backfilling` behind its own independent
/// `backfill_chunks` queue, or already `live`) is never touched. Returning
/// the moved ids — rather than the caller re-deriving "which ones did I just
/// touch" from a second, separately-timed query — is what lets
/// [`run_pending_backfills`] flip *exactly* these rows to `live` afterward
/// without also catching a sibling definition that happened to already be
/// `backfilling` when this marker's discharge ran.
async fn advance_deferred_definitions(
    client: &impl GenericClient,
    table: &str,
    from: TransformStatus,
    to: TransformStatus,
) -> Result<Vec<i64>, IntakeError> {
    let rows = client
        .query(
            "update transform_definitions set status = $1 \
             where source_table = $2 and status = $3 \
             returning id",
            &[&to.as_str(), &table, &from.as_str()],
        )
        .await?;
    let ids: Vec<i64> = rows.into_iter().map(|r| r.get(0)).collect();
    if !ids.is_empty() {
        // Issue #56: a backfill status transition (#55's lifecycle,
        // `docs/observability.md`'s "Transform status lifecycle" diagram) —
        // operationally meaningful, since a transform sitting in
        // `waiting_to_backfill` can mean either "about to advance" or "the
        // xmin fence is pinned by an unrelated long-running transaction
        // cluster-wide" (see that diagram's caveat), and this event is the
        // moment that ambiguity resolves.
        tracing::info!(
            table = %table,
            ids = ?ids,
            from = %from.as_str(),
            to = %to.as_str(),
            "transform status transition"
        );
    }
    Ok(ids)
}

/// Flips exactly `ids` to [`TransformStatus::Live`] (issue #55) — the second
/// half of [`advance_deferred_definitions`]'s `waiting_to_backfill` ->
/// `backfilling` promotion, run once the marker's enumeration has committed.
/// A no-op for an empty `ids` (the common case: no definition was deferred
/// against this particular marker).
///
/// Scoped `status = 'backfilling'` (issue #331): the promotion and this flip
/// are separate transactions, and an operator pause (or a quarantine) can land
/// on one of `ids` in between. That definition stays frozen — its resume
/// re-parks a marker of its own and rebuilds by a fresh backfill — rather than
/// being forced `live` behind the operator's back. Returns the ids actually
/// flipped.
async fn mark_definitions_live(
    client: &impl GenericClient,
    ids: &[i64],
) -> Result<Vec<i64>, IntakeError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let flipped: Vec<i64> = client
        .query(
            "update transform_definitions set status = $1 \
             where id = any($2) and status = $3 \
             returning id",
            &[
                &TransformStatus::Live.as_str(),
                &ids,
                &TransformStatus::Backfilling.as_str(),
            ],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    if !flipped.is_empty() {
        tracing::info!(
            ids = ?flipped,
            to = %TransformStatus::Live.as_str(),
            "transform status transition: backfill enumeration committed"
        );
    }
    for id in &flipped {
        park_target_catchup_if_read(client, *id).await?;
    }
    // Issue #315: a flipped definition whose source is another definition's
    // target was enumerated while it was still `backfilling`, which the
    // target-mutation seam skips, and that target is never in the
    // publication. A write to it between the enumeration and this flip
    // reached nobody, so park a fresh catch-up for the source now that the
    // flip has committed (see `defs::catalog::create_definition_inner`'s
    // matching step). The next discharge flips nothing, so this doesn't loop.
    let sources: Vec<String> = client
        .query(
            "select distinct d.source_table from transform_definitions d \
             where d.id = any($1) and exists ( \
                 select 1 from transform_definitions u where u.target_table = d.source_table \
             )",
            &[&flipped],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    for source in sources {
        park_backfill_catchup(client, &source).await?;
    }
    if flipped.len() < ids.len() {
        let skipped: Vec<i64> = ids
            .iter()
            .filter(|id| !flipped.contains(id))
            .copied()
            .collect();
        tracing::info!(
            ids = ?skipped,
            "backfill enumeration committed, but these definitions left `backfilling` \
             meanwhile; leaving their status as it is"
        );
    }
    Ok(flipped)
}

/// The initial snapshot handshake: creates `slot` and backfills every one of
/// `tables` from the exact snapshot `pg_create_logical_replication_slot`
/// exports — valid for the rest of this transaction — then commits. Intake
/// then streams from the slot's own consistent point. Gap-free by
/// construction, not by overlap-and-dedup: nothing written after the slot's
/// consistent point is enumerated here, and nothing before it is skipped by
/// the stream.
///
/// **Not one atomic unit.** `pg_create_logical_replication_slot` persists the
/// slot to disk the moment it returns, independent of the surrounding
/// transaction — only the backfill and the `replication_progress` INSERT
/// that follow are undone by a rollback or a crash before `commit()`. A
/// crash in that window leaves the slot on disk with no progress row: the
/// orphaned state [`slot_is_orphaned`] below detects and
/// [`IntakeError::OrphanedSlot`] names, rather than dying on "slot already
/// exists" if this function were just retried.
///
/// Callers on a fresh install run this once, before ever calling
/// [`super::Intake::connect`]; an existing install with a `replication_progress`
/// row for `slot` never calls this again. Recovery from losing that slot is
/// not a re-run of this handshake (which would re-enumerate every table at
/// once): [`super::slot_loss::pause_if_slot_lost`] pauses every transform the
/// slot fed and recreates the slot, and each transform is rebuilt by its own
/// fresh backfill when an operator resumes it (issue #310).
///
/// Also seeds `replication_progress` for `slot` at its own consistent point —
/// this is "whatever first uses the slot's name" that `V4__replication_progress.sql`
/// says is responsible for the row's one INSERT. The linchpin
/// (`trellis::intake::stage_and_advance`) only ever UPDATEs it; without this
/// seed the row never exists, so [`super::Intake::connect`]'s precondition
/// check (issue #31, finding 2) would reject every fresh slot.
pub async fn initial_snapshot_handshake(
    session: &mut ProducerSession,
    slot: &str,
    tables: &[String],
) -> Result<(), IntakeError> {
    if slot_is_orphaned(session.client(), slot).await? {
        return Err(IntakeError::OrphanedSlot {
            slot: slot.to_string(),
        });
    }
    let txn = session.transaction().await?;
    // Must be the transaction's first statement (Postgres requires `SET
    // TRANSACTION` before any other command), and matters here: only a
    // REPEATABLE READ transaction holds its snapshot steady across the
    // multiple enumeration queries below.
    txn.execute("set transaction isolation level repeatable read", &[])
        .await?;
    let slot_row = txn
        .query_one(
            "select lsn from pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await?;
    let consistent_point: PgLsn = slot_row.get(0);
    // One append call per table-page rather than collecting every table's
    // changes into one giant `Vec` first: still all one transaction, so this
    // is equivalent to the old "collect all then append once" for atomicity
    // — `append::append` has no state that requires being called exactly
    // once (see its own doc comment: it just reads the ring pointer and
    // inserts).
    //
    // Issue #79 (bug B): a table a direct backfill already folded into a
    // target — and that provably hasn't changed since ([`coverage_covers`]) —
    // is skipped here exactly as `run_pending_backfills` skips it, so the
    // fresh-install path floods the ring no worse than a restart does. This
    // preserves gap-free-by-construction: `coverage_covers` is evaluated in
    // this same repeatable-read snapshot (≈ the slot's consistent point), so
    // any write between the build's coverage fence and that point leaves a
    // fence-invisible `xmin` or changes the row count → `coverage_covers`
    // returns false → the table is enumerated, the safe default. Only a table
    // byte-for-byte identical to its covered state is skipped, and everything
    // after the consistent point streams via CDC as usual.
    for table in tables {
        if coverage_covers(&txn, table).await? {
            continue;
        }
        enumerate_and_append(&txn, table).await?;
    }
    txn.execute(
        "insert into replication_progress (slot_name, confirmed_lsn) values ($1, $2)",
        &[&slot, &consistent_point],
    )
    .await?;
    txn.commit().await?;
    Ok(())
}

/// Whether `slot` exists in `pg_replication_slots` **and belongs to the
/// current database** but has no `replication_progress` row — the orphaned
/// state a crash between `pg_create_logical_replication_slot` (which
/// persists immediately, see [`initial_snapshot_handshake`]'s doc comment)
/// and that same handshake's commit leaves behind. A slot that exists *with*
/// a progress row is a different situation (re-running setup against an
/// already-initialized slot) and is left to the existing "slot already
/// exists" error path.
///
/// `pg_replication_slots` is a cluster-wide system view, not scoped to the
/// connected database, but a logical slot only ever belongs to the database
/// it was created against — so this must filter on `database =
/// current_database()` (issue #188). Without that filter, a slot-name
/// collision with a *different* database on the same Postgres cluster (e.g.
/// another Trellis install sharing the cluster, or — as
/// `generative/tests/convergence.rs`'s shared-cluster harness discovered — a
/// still-live prior test case's database) is indistinguishable from this
/// database's own orphan: `slot_exists` comes back true (the name exists
/// somewhere) and `has_progress_row` comes back false (this database never
/// wrote that row, since the slot was never really this database's own), so
/// an unfiltered check calls it "orphaned" — a misdiagnosis that hides the
/// real condition (a slot name that isn't actually free) behind a
/// plausible-looking crash-recovery story. Scoping the check surfaces that
/// case correctly instead: `slot_is_orphaned` returns `false` (this
/// database has no slot by that name), and the
/// `pg_create_logical_replication_slot` call just below fails loudly with
/// Postgres's own "replication slot already exists" error, naming the
/// actual condition.
async fn slot_is_orphaned(client: &impl GenericClient, slot: &str) -> Result<bool, IntakeError> {
    let slot_exists: bool = client
        .query_one(
            "select exists(select 1 from pg_replication_slots where slot_name = $1 and \
             database = current_database())",
            &[&slot],
        )
        .await?
        .get(0);
    if !slot_exists {
        return Ok(false);
    }
    let has_progress_row: bool = client
        .query_one(
            "select exists(select 1 from replication_progress where slot_name = $1)",
            &[&slot],
        )
        .await?
        .get(0);
    Ok(!has_progress_row)
}

/// The slot's observed health, per `pg_replication_slots.wal_status`.
enum SlotHealth {
    Healthy,
    Missing,
    Invalidated,
}

/// Scoped to `database = current_database()` for the same reason
/// [`slot_is_orphaned`] is (issue #188): `pg_replication_slots` is a
/// cluster-wide view, but a logical slot only ever belongs to the database it
/// was created against. Here the unscoped version fails in the *unsafe*
/// direction — a same-named slot owned by a different database on the same
/// cluster would make this database's own missing-or-invalidated slot look
/// `Healthy`, defeating [`require_slot_healthy`]'s whole purpose (refusing to
/// resume into an unrecoverable WAL gap) on the strength of a slot that isn't
/// ours and that intake could never actually stream from.
async fn slot_health(client: &impl GenericClient, slot: &str) -> Result<SlotHealth, IntakeError> {
    let row = client
        .query_opt(
            "select wal_status from pg_replication_slots where slot_name = $1 and \
             database = current_database()",
            &[&slot],
        )
        .await?;
    Ok(match row {
        None => SlotHealth::Missing,
        Some(r) => {
            let wal_status: String = r.get(0);
            if wal_status == "lost" {
                SlotHealth::Invalidated
            } else {
                SlotHealth::Healthy
            }
        }
    })
}

/// Checked at [`super::Intake::connect`] whenever `last_confirmed_lsn` shows
/// this instance has confirmed work against `slot` before: the slot must
/// still exist and not be marked `lost`. Invalidation (retention cap
/// exceeded) and loss on failover (pre-PG17 doesn't preserve slots across a
/// promotion) both mean everything between `last_confirmed_lsn` and any new
/// slot's start position is unrecoverable by streaming — so this errors
/// rather than let intake silently resume into a gap. It is also the detector
/// [`super::slot_loss::pause_if_slot_lost`] runs during a staging worker's
/// setup, which turns the error into pausing every transform the slot fed
/// (issue #310).
pub async fn require_slot_healthy(
    client: &impl GenericClient,
    slot: &str,
    last_confirmed_lsn: tokio_postgres::types::PgLsn,
) -> Result<(), IntakeError> {
    match slot_health(client, slot).await? {
        SlotHealth::Healthy => Ok(()),
        SlotHealth::Missing | SlotHealth::Invalidated => Err(IntakeError::SlotLost {
            slot: slot.to_string(),
            last_confirmed_lsn: u64::from(last_confirmed_lsn),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_parses_xmin_and_xmax() {
        let s = Snapshot::parse("10:20:11,15").unwrap();
        assert_eq!(s.xmin, 10);
        assert_eq!(s.xmax, 20);
    }

    /// Issue #315: a definition reading another definition's target was
    /// enumerated while `backfilling`, when the target-mutation seam skips
    /// it, and the target is never published. Going live must park a fresh
    /// catch-up on that target, or a write landing between the enumeration
    /// and the flip reaches nobody.
    #[tokio::test]
    async fn going_live_over_a_target_parks_a_catchup_on_that_target() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = db.pool.get().await.expect("acquire connection");

        client
            .batch_execute(
                "insert into source_table_versions (source_table, version) \
                 values ('public.orders', 1), ('public.t', 1); \
                 insert into transform_definitions \
                 (target_table, source_table, source_version, definition_text, status) \
                 values ('public.t', 'public.orders', 1, '', 'live')",
            )
            .await
            .expect("seed the upstream definition");
        let reader: i64 = client
            .query_one(
                "insert into transform_definitions \
                 (target_table, source_table, source_version, definition_text, status) \
                 values ('public.d', 'public.t', 1, '', 'backfilling') returning id",
                &[],
            )
            .await
            .expect("seed the chained definition")
            .get(0);

        let flipped = mark_definitions_live(&**client, &[reader])
            .await
            .expect("mark_definitions_live");
        assert_eq!(flipped, [reader]);

        let markers: Vec<String> = client
            .query("select table_name from pending_backfill", &[])
            .await
            .expect("read pending_backfill")
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(
            markers,
            ["public.t"],
            "a catch-up on the upstream target, and none on the unread public.d"
        );
    }

    #[test]
    fn snapshot_parse_rejects_malformed_text() {
        match Snapshot::parse("not-a-snapshot") {
            Err(IntakeError::InvalidSnapshot(text)) => assert_eq!(text, "not-a-snapshot"),
            other => panic!("expected InvalidSnapshot, got {other:?}"),
        }
    }

    #[test]
    fn fence_settles_only_once_xmin_strictly_passes_the_old_xmax() {
        let fence = Snapshot::parse("10:20:").unwrap();
        assert!(!Snapshot::parse("15:25:").unwrap().settled_since(&fence));
        // Exactly at the fence's xmax must not count as settled — that is
        // precisely the boundary a transaction concurrent with the fence
        // (e.g. the ADD's own transaction) can land on.
        assert!(!Snapshot::parse("20:30:").unwrap().settled_since(&fence));
        assert!(Snapshot::parse("21:31:").unwrap().settled_since(&fence));
    }

    /// Issue #331: `run_pending_backfills` promotes deferred definitions to
    /// `backfilling` and flips them `live` in separate transactions, so an
    /// operator pause (or a quarantine) can land on one in between. The flip
    /// must only move definitions still `backfilling`, and report exactly
    /// those — never force a frozen definition `live` behind the operator.
    #[tokio::test]
    async fn mark_definitions_live_leaves_a_definition_that_left_backfilling_alone() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = db.pool.get().await.expect("acquire connection");

        client
            .execute(
                "insert into source_table_versions (source_table, version) \
                 values ('public.orders', 1)",
                &[],
            )
            .await
            .expect("seed source_table_versions");
        let mut ids = Vec::new();
        for (target, status) in [
            ("public.still_backfilling", "backfilling"),
            ("public.paused_meanwhile", "paused"),
            ("public.quarantined_meanwhile", "quarantined"),
        ] {
            let id: i64 = client
                .query_one(
                    "insert into transform_definitions \
                     (target_table, source_table, source_version, definition_text, status) \
                     values ($1, 'public.orders', 1, '', $2) returning id",
                    &[&target, &status],
                )
                .await
                .expect("seed a definition")
                .get(0);
            ids.push(id);
        }

        let flipped = mark_definitions_live(&**client, &ids)
            .await
            .expect("mark_definitions_live");

        let statuses: Vec<String> = client
            .query(
                "select status from transform_definitions where id = any($1) order by id",
                &[&ids],
            )
            .await
            .expect("read statuses")
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(
            statuses,
            ["live", "paused", "quarantined"],
            "only the definition still backfilling goes live; a frozen one stays frozen"
        );
        assert_eq!(flipped, [ids[0]], "reports exactly the ids it flipped");
    }

    #[test]
    fn split_qualified_rejects_an_unqualified_name() {
        match split_qualified("widgets") {
            Err(IntakeError::InvalidTableName(name)) => assert_eq!(name, "widgets"),
            other => panic!("expected InvalidTableName, got {other:?}"),
        }
    }

    #[test]
    fn qualify_rejects_a_dotted_schema_or_table_component() {
        match qualify("a.b", "widgets") {
            Err(IntakeError::DottedIdentifierComponent { component }) => {
                assert_eq!(component, "a.b")
            }
            other => panic!("expected DottedIdentifierComponent, got {other:?}"),
        }
        match qualify("public", "a.b") {
            Err(IntakeError::DottedIdentifierComponent { component }) => {
                assert_eq!(component, "a.b")
            }
            other => panic!("expected DottedIdentifierComponent, got {other:?}"),
        }
    }

    #[test]
    fn qualify_round_trips_through_split_qualified() {
        let joined = qualify("public", "widgets").unwrap();
        assert_eq!(joined, "public.widgets");
        assert_eq!(split_qualified(&joined).unwrap(), ("public", "widgets"));
    }
}

#[cfg(test)]
mod catch_up_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::oneshot;

    use super::*;

    /// Issue #312 review: once one enumeration gives up waiting for intake,
    /// the pass ends instead of waiting again on each remaining marker. The
    /// maintenance loop seals nothing while a pass waits, so waiting per
    /// marker would multiply that stall by the number of pending markers.
    /// Counting `stop` calls pins it without timing: a `stop` that always
    /// says "give up" is consulted once per wait.
    #[tokio::test]
    async fn a_deferred_enumeration_ends_the_pass() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!(
                "set search_path to {}, public; \
                 create table public.a (id bigint primary key); \
                 create table public.b (id bigint primary key); \
                 insert into public.a values (1); \
                 insert into public.b values (1); \
                 create publication test_pub;",
                crate::config::DEFAULT_SCHEMA
            ))
            .await
            .expect("seed two source tables");
        reconcile_publication(
            &mut client,
            "test_pub",
            &["public.a".to_string(), "public.b".to_string()],
        )
        .await
        .expect("reconcile leaves two settled markers");

        let waits = AtomicUsize::new(0);
        let give_up = || {
            waits.fetch_add(1, Ordering::Relaxed);
            true
        };
        run_pending_backfills_until(
            &mut client,
            "wake",
            &StagedWatermark::new(),
            Duration::from_secs(600),
            &give_up,
        )
        .await
        .expect("run_pending_backfills_until");

        assert_eq!(
            waits.load(Ordering::Relaxed),
            1,
            "only the first marker's enumeration should wait"
        );
        let markers: i64 = client
            .query_one("select count(*) from pending_backfill", &[])
            .await
            .expect("count markers")
            .get(0);
        assert_eq!(markers, 2, "both markers must survive for the next pass");
    }

    async fn connect(db: &testkit::TestDatabase) -> tokio_postgres::Client {
        let (client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!(
                "set search_path to {}, public",
                crate::config::DEFAULT_SCHEMA
            ))
            .await
            .expect("set search_path");
        client
    }

    /// One source table, `public.t`, in the publication with a settled
    /// marker waiting to be discharged. Returns the discharging client and a
    /// second connection for the racing park.
    async fn one_settled_marker(
        db: &testkit::TestDatabase,
    ) -> (tokio_postgres::Client, tokio_postgres::Client) {
        let mut discharger = connect(db).await;
        discharger
            .batch_execute(
                "create table public.t (id bigint primary key); \
                 insert into public.t values (1); \
                 create publication test_pub;",
            )
            .await
            .expect("seed source table");
        reconcile_publication(&mut discharger, "test_pub", &["public.t".to_string()])
            .await
            .expect("reconcile parks a marker");
        (discharger, connect(db).await)
    }

    async fn marker_generation(client: &tokio_postgres::Client) -> Option<i64> {
        client
            .query_opt(
                "select generation from pending_backfill where table_name = 'public.t'",
                &[],
            )
            .await
            .expect("read marker")
            .map(|r| r.get(0))
    }

    /// Runs one discharge pass on `discharger`, holding it at the wait for
    /// intake (after its enumeration snapshot is fixed, before it deletes the
    /// marker) while `race` runs. This forces the #311 window by hand instead
    /// of timing it. `race` sends on `go` to let the discharge finish, and
    /// `done` fires once the discharge has committed.
    async fn discharge_racing<F, Fut>(discharger: &mut tokio_postgres::Client, race: F)
    where
        F: FnOnce(oneshot::Sender<()>, oneshot::Receiver<()>) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let watermark = StagedWatermark::new();
        let waiting = tokio::sync::Notify::new();
        let (go_tx, go_rx) = oneshot::channel::<()>();
        let (done_tx, done_rx) = oneshot::channel::<()>();
        // `stop` is polled only while the enumeration waits for intake.
        let stop = || {
            waiting.notify_one();
            false
        };
        let discharge = async {
            run_pending_backfills_until(
                discharger,
                "wake",
                &watermark,
                Duration::from_secs(600),
                &stop,
            )
            .await
            .expect("run_pending_backfills_until");
            let _ = done_tx.send(());
        };
        let drive = async {
            waiting.notified().await;
            let release = async {
                go_rx.await.expect("race releases the discharge");
                watermark.advance(PgLsn::from(u64::MAX));
            };
            tokio::join!(race(go_tx, done_rx), release);
        };
        tokio::time::timeout(Duration::from_secs(60), async {
            tokio::join!(discharge, drive);
        })
        .await
        .expect("discharge must not block on a racing park");
    }

    /// Issues #311/#367: a catch-up parked for a table whose marker is
    /// mid-discharge must survive that discharge. The discharge's enumeration
    /// snapshot predates the second park, so it can't stand in for it.
    #[tokio::test]
    async fn a_park_during_discharge_survives_it() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut discharger, parker) = one_settled_marker(&db).await;
        let before = marker_generation(&parker).await.expect("marker parked");

        let parker_ref = &parker;
        discharge_racing(&mut discharger, |go, _done| async move {
            park_backfill_catchup(parker_ref, "public.t")
                .await
                .expect("park during discharge");
            go.send(()).expect("release discharge");
        })
        .await;

        let after = marker_generation(&parker)
            .await
            .expect("the racing park's marker must survive the older discharge");
        assert_ne!(after, before, "the surviving marker is the racing park's");
    }

    /// The same race with the park's transaction still open when the
    /// discharge deletes: the discharge must neither block on it nor delete
    /// the row out from under it.
    #[tokio::test]
    async fn a_park_in_flight_during_discharge_survives_it() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut discharger, mut parker) = one_settled_marker(&db).await;
        let before = marker_generation(&parker).await.expect("marker parked");

        let parker_ref = &mut parker;
        discharge_racing(&mut discharger, |go, done| async move {
            let txn = parker_ref.transaction().await.expect("begin park");
            park_backfill_catchup(&txn, "public.t")
                .await
                .expect("park during discharge");
            go.send(()).expect("release discharge");
            done.await.expect("discharge finished");
            txn.commit().await.expect("commit park");
        })
        .await;

        let after = marker_generation(&parker)
            .await
            .expect("the in-flight park's marker must survive the older discharge");
        assert_ne!(after, before, "the surviving marker is the racing park's");
    }

    /// A second park keeps the later of the two fences, whichever order they
    /// arrive in, so the merged marker waits for everything either needed.
    #[tokio::test]
    async fn a_repeat_park_keeps_the_later_fence() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = connect(&db).await;
        async fn xmax(client: &tokio_postgres::Client) -> i64 {
            client
                .query_one(
                    "select pg_snapshot_xmax(fence_snapshot)::text::bigint \
                     from pending_backfill where table_name = 'public.t'",
                    &[],
                )
                .await
                .expect("read fence")
                .get(0)
        }
        client
            .execute(
                "insert into pending_backfill (table_name, fence_snapshot) \
                 values ('public.t', '1000000:1000000:'::pg_snapshot)",
                &[],
            )
            .await
            .expect("plant a fence ahead of the cluster");
        park_backfill_catchup(&client, "public.t")
            .await
            .expect("park behind it");
        assert_eq!(xmax(&client).await, 1_000_000, "an older fence never wins");

        client
            .execute(
                "update pending_backfill set fence_snapshot = '3:3:'::pg_snapshot",
                &[],
            )
            .await
            .expect("plant a fence behind the cluster");
        park_backfill_catchup(&client, "public.t")
            .await
            .expect("park ahead of it");
        assert!(
            xmax(&client).await > 3,
            "a later fence replaces an older one"
        );
    }
}
