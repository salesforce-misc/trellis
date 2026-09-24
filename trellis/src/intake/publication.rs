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
//! - [`create_slot_and_park_markers`] creates a fresh install's slot and
//!   parks a marker on every watched table, so the discharge captures each
//!   one after the slot's consistent point. It reads no rows itself.
//! - [`require_slot_healthy`] is the loud startup check for slot
//!   invalidation/loss, including a slot recreated under the same name.

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
/// amendment). `defs::catalog::complete_direct_backfill` (and, for a
/// synchronous build, `install_definition`, once per table the build read)
/// calls this the moment a direct-build definition flips `backfilling` ->
/// `live`: while it
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
/// this waits. A discharge that fails outright (a failed `DECLARE`, append,
/// marker delete or commit) takes the same rollback-and-revert path before
/// returning its error (issue #387). A caller with no intake running yet must
/// not call this at all (see `client::setup_staging`); tests with no CDC
/// stream pass [`crate::staging::StagedWatermark::saturated`].
///
/// # Dropping what the source no longer backs (issue #330)
///
/// The enumeration only reaches keys the source still has. A definition this
/// pass rebuilds (a resumed one, above all) can hold target rows whose source
/// rows went away while it was frozen, and nothing would ever enumerate them.
/// So, first thing in the transaction, `resume_orphans::delete_orphaned_target_rows`
/// deletes every row of each promoted definition's target that no source row
/// backs, reporting each through the target-mutation seam. It runs before
/// `DECLARE`, and never after the intake wait, which would leave an aggregate
/// group repopulated during the wait at its stale pre-pause value. The
/// maintenance loop that runs this pass is also the only sealer, so any
/// source change committed after the pass starts is drained only once the
/// definition is `live`. The anti-join and the cursor still read different
/// snapshots, which leaves a short race for aggregates in either order. That
/// module's doc comment has the full argument and its limits.
///
/// # A table nothing reads (issue #417)
///
/// A marker on a table no registered definition reads, directly or through a
/// relationship ([`crate::defs::catalog::table_has_reader`]), is discharged
/// without enumerating: no apply would consume its `Recompute` rows. A fresh
/// install parks a marker on every configured source table
/// ([`create_slot_and_park_markers`]), including ones no transform uses yet.
/// A definition registered on such a table later captures it itself: today
/// through registration's own read, or, if an unsettled marker made it defer,
/// through that marker's discharge, which finds it as a reader. The check
/// runs after [`advance_deferred_definitions`], so a deferred definition it
/// can't see yet is one that pass wouldn't have promoted anyway.
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

        // Issue #387: `advancing` is already committed as `backfilling`, and a
        // later pass only promotes `waiting_to_backfill` definitions. So every
        // way out of the discharge short of its commit (a deferral *or* an
        // error) must hand them back, or they never go live.
        match discharge_marker(
            client,
            &marker,
            &advancing,
            wake_channel,
            watermark,
            catch_up_timeout,
            stop,
        )
        .await
        {
            Ok(Discharge::Committed) => {
                // Issue #444: the flip and the catch-ups it parks commit
                // together. A crash or a failed park between them would
                // otherwise leave a definition `live` with its catch-up never
                // parked, silently losing whatever that catch-up re-derives.
                if let Err(error) = go_live(client, &advancing).await {
                    tracing::warn!(
                        table = %marker.table,
                        ids = ?advancing,
                        error = %error,
                        "could not take backfilled definitions live; re-parking their marker"
                    );
                    // The discharge already deleted the marker, so handing
                    // the definitions back to `waiting_to_backfill` alone
                    // would strand them: only a marker's discharge promotes
                    // them. Re-park it with the revert so the next pass
                    // retries the whole discharge.
                    if let Err(hand_back_error) =
                        hand_back_for_retry(client, &marker.table, &advancing).await
                    {
                        tracing::warn!(
                            table = %marker.table,
                            ids = ?advancing,
                            error = %hand_back_error,
                            "could not hand backfilled definitions back for a retry"
                        );
                    }
                    return Err(error);
                }
            }
            Ok(Discharge::Deferred { horizon }) => {
                revert_to_waiting(client, &advancing).await?;
                tracing::debug!(
                    table = %marker.table,
                    horizon = %horizon,
                    staged_through = %watermark.get(),
                    "backfill enumeration deferred: intake has not staged through its snapshot yet"
                );
                break;
            }
            Err(error) => {
                // The maintenance loop only asks `is_err()` of this pass, so
                // this is the one place the failure is reported.
                tracing::warn!(
                    table = %marker.table,
                    error = %error,
                    "backfill discharge failed; its marker stays for the next pass"
                );
                if let Err(revert_error) = revert_to_waiting(client, &advancing).await {
                    tracing::warn!(
                        table = %marker.table,
                        ids = ?advancing,
                        error = %revert_error,
                        "could not return definitions to waiting_to_backfill after a failed discharge"
                    );
                }
                return Err(error);
            }
        }
    }
    tracing::Span::current().record("settled", settled);
    Ok(())
}

/// How [`discharge_marker`] ended short of an error.
enum Discharge {
    /// The enumeration (if the table needed one) and the marker's delete
    /// committed together.
    Committed,
    /// Intake had not staged through `horizon` in time, so the transaction
    /// rolled back and the marker stays.
    Deferred { horizon: PgLsn },
}

/// Runs `marker`'s discharge in one transaction: the enumeration, unless
/// coverage already stands in for it, then the marker's delete. An error
/// drops the transaction, which rolls it back, so the marker survives either
/// way the discharge falls short.
async fn discharge_marker(
    client: &mut tokio_postgres::Client,
    marker: &PendingBackfill,
    advancing: &[i64],
    wake_channel: &str,
    watermark: &StagedWatermark,
    catch_up_timeout: Duration,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<Discharge, IntakeError> {
    let txn = client.transaction().await?;
    // Issue #330: before the enumeration's `DECLARE` and its intake wait; see
    // "Dropping what the source no longer backs" above. On the rollback
    // below (including the `Deferred` and error paths, since both drop
    // `txn` without committing), the deletes roll back with everything else.
    super::resume_orphans::delete_orphaned_target_rows(&txn, &marker.table, advancing).await?;
    // Issue #417: see "A table nothing reads" above.
    let read = crate::defs::catalog::table_has_reader(&txn, &marker.table).await?;
    let staged = if !read || coverage_covers(&txn, &marker.table).await? {
        false
    } else {
        declare_enumeration(&txn, &marker.table).await?;
        let horizon: PgLsn = txn
            .query_one("select pg_current_wal_insert_lsn()", &[])
            .await?
            .get(0);
        if !intake_caught_up(watermark, horizon, catch_up_timeout, stop).await {
            txn.rollback().await?;
            return Ok(Discharge::Deferred { horizon });
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
    Ok(Discharge::Committed)
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
/// `backfilling` promotion for exactly `ids`, when the discharge that
/// promotion announced was deferred or failed instead of committing. Scoped to
/// `ids` and to the `backfilling` status for the same reason that function
/// is, so a definition an operator paused or quarantined meanwhile stays
/// frozen.
///
/// After a failed discharge this runs on the same connection as the
/// transaction that just failed. That transaction may be aborted, and it was
/// dropped rather than rolled back explicitly. It is still safe: dropping a
/// `tokio_postgres::Transaction` queues its `ROLLBACK` on the connection's
/// request channel synchronously, so the server runs it before this update.
/// Runs [`mark_definitions_live`] in its own transaction, so the flip and
/// every catch-up it parks commit together or not at all (issue #444).
async fn go_live(client: &mut tokio_postgres::Client, ids: &[i64]) -> Result<(), IntakeError> {
    if ids.is_empty() {
        return Ok(());
    }
    let txn = client.transaction().await?;
    mark_definitions_live(&txn, ids).await?;
    txn.commit().await?;
    Ok(())
}

/// After a discharge committed but [`go_live`] failed: returns `ids` to
/// `waiting_to_backfill` and re-parks `table`'s marker in one transaction, so
/// the next pass promotes and discharges them again. Either half alone would
/// strand them: a `backfilling` definition is never promoted again, and a
/// `waiting_to_backfill` one only is by a marker's discharge.
async fn hand_back_for_retry(
    client: &mut tokio_postgres::Client,
    table: &str,
    ids: &[i64],
) -> Result<(), IntakeError> {
    if ids.is_empty() {
        return Ok(());
    }
    let txn = client.transaction().await?;
    revert_to_waiting(&txn, ids).await?;
    park_backfill_catchup(&txn, table).await?;
    txn.commit().await?;
    Ok(())
}

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
///
/// Parks the catch-ups the flip calls for on `client` too, so `client` must
/// be a transaction ([`go_live`]'s) for the two to be atomic (issue #444).
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
    // Two kinds of catch-up, both issue #315:
    //
    // - A flipped definition's target that some `live` definition reads (see
    //   [`park_target_catchup_if_read`]): the enumeration wrote it outside
    //   the target-mutation seam, so its readers re-derive from the result.
    // - A flipped definition's source that is another definition's target:
    //   it was enumerated while still `backfilling`, which the seam skips,
    //   and that target is never in the publication. A write to it between
    //   the enumeration and this flip reached nobody (see
    //   `defs::catalog::create_definition_inner`'s matching step). The next
    //   discharge flips nothing, so this doesn't loop.
    //
    // Parked in name order, as `defs::catalog::install_definition` does, so
    // two transactions parking overlapping tables can't deadlock on each
    // other's marker rows.
    let mut tables: Vec<String> = client
        .query(
            "select d.target_table from transform_definitions d \
             where d.id = any($1) and exists ( \
                 select 1 from transform_definitions r \
                 where r.source_table = d.target_table and r.status = 'live' \
             ) \
             union \
             select d.source_table from transform_definitions d \
             where d.id = any($1) and exists ( \
                 select 1 from transform_definitions u where u.target_table = d.source_table \
             )",
            &[&flipped],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    tables.sort_unstable();
    tables.dedup();
    for table in tables {
        park_backfill_catchup(client, &table).await?;
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

/// Fresh-install slot setup: creates `slot`, seeds its `replication_progress`
/// row at the slot's consistent point, and parks a `pending_backfill` marker
/// on every one of `tables`, all in one transaction. It reads no source rows.
/// Each table's existing rows are captured by the first discharge of its
/// marker ([`run_pending_backfills`]), the one capture path every definition
/// backfill goes through (ADR-0016).
///
/// # Why it doesn't read the tables itself (issue #393)
///
/// `pg_create_logical_replication_slot` exports no snapshot. A read in this
/// transaction would use a snapshot taken before slot creation waits out the
/// transactions in flight and reaches its consistent point, so a transaction
/// committing in that wait would be neither read nor streamed. The markers
/// are parked after slot creation returns, so each fence is a snapshot taken
/// after the consistent point, and the discharge's read comes later still:
/// every commit that read misses is after the consistent point, and the slot
/// streams it. A table `reconcile_publication` just added already has a join
/// marker with an earlier fence. [`park_marker`] keeps the later fence, and
/// the discharge reads it once either way. That later fence postdates the
/// join's commit, so it also covers a writer the join's own fence misses (one
/// that starts between that fence and the `ALTER`'s commit; see ADR-0016's
/// open item on the join fence). No discharge runs before this re-park: the
/// maintenance loop starts only after setup, and a crash in between re-runs
/// setup, which parks again.
///
/// Every table in `tables` gets a marker, not only the newly published ones:
/// a table already in the publication has no marker of its own, and without
/// one its rows would never be captured. The discharge skips a table no
/// definition reads yet ([`run_pending_backfills`]'s "A table nothing
/// reads").
///
/// **Not one atomic unit.** `pg_create_logical_replication_slot` persists the
/// slot to disk the moment it returns, independent of the surrounding
/// transaction. Only the markers and the `replication_progress` INSERT that
/// follow are undone by a rollback or a crash before `commit()`. A crash in
/// that window leaves the slot on disk with no progress row: the orphaned
/// state [`slot_is_orphaned`] below detects and [`IntakeError::OrphanedSlot`]
/// names, rather than dying on "slot already exists" if this function were
/// just retried.
///
/// Callers on a fresh install run this once, before ever calling
/// [`super::Intake::connect`]; an existing install with a `replication_progress`
/// row for `slot` never calls this again. Recovery from losing that slot is
/// not a re-run of this function:
/// [`super::slot_loss::pause_if_slot_lost`] pauses every transform the slot
/// fed and recreates the slot, and each transform is rebuilt by its own
/// fresh backfill when an operator resumes it (issue #310).
///
/// The progress row is "whatever first uses the slot's name" that
/// `V4__replication_progress.sql` says is responsible for the row's one
/// INSERT. The linchpin (`trellis::intake::stage_and_advance`) only ever
/// UPDATEs it; without this seed the row never exists, so
/// [`super::Intake::connect`]'s precondition check (issue #31, finding 2)
/// would reject every fresh slot.
pub async fn create_slot_and_park_markers(
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
    // Slot creation must come before any write in this transaction (Postgres
    // refuses to create a logical slot in a transaction that has written).
    // Read committed, so each marker's fence below is a fresh snapshot taken
    // after slot creation returned, not one taken before it waited.
    let slot_row = txn
        .query_one(
            "select lsn from pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await?;
    let consistent_point: PgLsn = slot_row.get(0);
    for table in tables {
        park_marker(&txn, table).await?;
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
/// persists immediately, see [`create_slot_and_park_markers`]'s doc comment)
/// and that same function's commit leaves behind. A slot that exists *with*
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

/// The slot's observed health, per `pg_replication_slots.wal_status` and
/// its `confirmed_flush_lsn` against this instance's last confirmed position.
enum SlotHealth {
    Healthy,
    Missing,
    Invalidated,
    /// Present and not `lost`, but its `confirmed_flush_lsn` is past the
    /// position this instance last confirmed, or not set yet because another
    /// session is still creating it (issue #406). See [`slot_health`] for why
    /// either means the slot isn't the one this instance was acknowledging.
    Recreated,
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
///
/// **A slot recreated under the same name (issue #406).** Existence and
/// `wal_status` can't tell a slot that was dropped and recreated while
/// Trellis was down from the one it was streaming from, and every change
/// committed between the drop and the recreate is gone. Postgres gives a slot
/// no creation identity, but its position is enough: the slot's
/// `confirmed_flush_lsn` moves only when a consumer acknowledges a position,
/// and intake acknowledges only positions it has already persisted to
/// `replication_progress` (the linchpin commits first, and
/// [`super::Intake::connect`] relies on the same invariant to resume from
/// `last_confirmed`). So the slot this instance has been feeding never sits
/// past `last_confirmed`. It can sit behind it, in the window between
/// persisting a position and the acknowledgment reaching the server. A
/// freshly created slot starts at the WAL position of its creation, which is
/// past anything this instance could have confirmed before the drop. The
/// check is conservative. A slot dropped and recreated with nothing
/// committed in between is still reported, as is a slot some other consumer
/// advanced (`pg_replication_slot_advance`, `pg_logical_slot_get_changes`),
/// but that consumer took changes this instance never saw, so it's a real
/// gap too.
async fn slot_health(
    client: &impl GenericClient,
    slot: &str,
    last_confirmed_lsn: PgLsn,
) -> Result<SlotHealth, IntakeError> {
    let row = client
        .query_opt(
            "select wal_status, confirmed_flush_lsn from pg_replication_slots \
             where slot_name = $1 and database = current_database()",
            &[&slot],
        )
        .await?;
    let Some(row) = row else {
        return Ok(SlotHealth::Missing);
    };
    let wal_status: Option<String> = row.get(0);
    if wal_status.as_deref() == Some("lost") {
        return Ok(SlotHealth::Invalidated);
    }
    // NULL only for a physical slot (filtered out by the `database` scope) or
    // a logical slot another session is still creating, which Postgres gives
    // a position once it reaches its consistent point. The slot this instance
    // acknowledged had one from the moment its creation returned, so a NULL
    // one is a recreate in progress; called healthy, intake could start
    // streaming from it, past the gap, as soon as the creation finished.
    let confirmed_flush: Option<PgLsn> = row.get(1);
    Ok(match confirmed_flush {
        Some(position) if position <= last_confirmed_lsn => SlotHealth::Healthy,
        _ => SlotHealth::Recreated,
    })
}

/// Checked at [`super::Intake::connect`] whenever `last_confirmed_lsn` shows
/// this instance has confirmed work against `slot` before: the slot must
/// still exist, not be marked `lost`, and not have been recreated under the
/// same name, which shows up as a position past `last_confirmed_lsn` (issue
/// #406, see [`slot_health`]). Invalidation (retention cap exceeded), loss on
/// failover (pre-PG17 doesn't preserve slots across a promotion) and a
/// drop-and-recreate all mean everything between `last_confirmed_lsn` and the
/// new slot's start position is unrecoverable by streaming — so this errors
/// rather than let intake silently resume into a gap. It is also the detector
/// [`super::slot_loss::pause_if_slot_lost`] runs during a staging worker's
/// setup, which turns the error into pausing every transform the slot fed
/// (issue #310).
pub async fn require_slot_healthy(
    client: &impl GenericClient,
    slot: &str,
    last_confirmed_lsn: PgLsn,
) -> Result<(), IntakeError> {
    match slot_health(client, slot, last_confirmed_lsn).await? {
        SlotHealth::Healthy => Ok(()),
        SlotHealth::Missing | SlotHealth::Invalidated | SlotHealth::Recreated => {
            Err(IntakeError::SlotLost {
                slot: slot.to_string(),
                last_confirmed_lsn: u64::from(last_confirmed_lsn),
            })
        }
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
        register_reader(&db, "public.a", "a_reader").await;
        register_reader(&db, "public.b", "b_reader").await;
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

    /// Registers a definition reading `source`, so the discharge has a
    /// reader to enumerate for: it skips a table nothing reads (issue #417).
    async fn register_reader(db: &testkit::TestDatabase, source: &str, target: &str) {
        let config = crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
        let pool = crate::pool::Pool::new(&config).expect("build a same-crate pool");
        crate::defs::catalog::create_definition(
            &pool,
            &format!("TRANSFORM {target} FROM {source} SELECT id AS total"),
            &std::collections::HashMap::from([("id".to_string(), crate::defs::ValueType::Numeric)]),
        )
        .await
        .expect("register a definition reading the table");
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
        register_reader(db, "public.t", "t_reader").await;
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

    /// Issue #387: a discharge that fails partway (here its `DECLARE`, since
    /// `public.nokey` has no identity key) must hand back the definitions it
    /// promoted to `backfilling`. A later pass only promotes definitions still
    /// in `waiting_to_backfill`, so one left in `backfilling` would never go
    /// live. The marker must survive too, so the retry has something to run.
    #[tokio::test]
    async fn a_failed_discharge_returns_its_definitions_to_waiting() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(&db).await;
        client
            .batch_execute(
                "create table public.nokey (id bigint not null); \
                 insert into public.nokey values (1); \
                 create publication test_pub; \
                 insert into source_table_versions (source_table, version) \
                 values ('public.nokey', 1);",
            )
            .await
            .expect("seed a source table with no identity key");
        reconcile_publication(&mut client, "test_pub", &["public.nokey".to_string()])
            .await
            .expect("reconcile parks a marker");
        let id: i64 = client
            .query_one(
                "insert into transform_definitions \
                 (target_table, source_table, source_version, definition_text, status) \
                 values ('public.d', 'public.nokey', 1, $1, 'waiting_to_backfill') \
                 returning id",
                &[&"TRANSFORM d FROM nokey SELECT id AS x"],
            )
            .await
            .expect("seed a deferred definition")
            .get(0);
        async fn status(client: &tokio_postgres::Client, id: i64) -> String {
            client
                .query_one(
                    "select status from transform_definitions where id = $1",
                    &[&id],
                )
                .await
                .expect("read status")
                .get(0)
        }

        match run_pending_backfills(
            &mut client,
            "wake",
            &StagedWatermark::saturated(),
            Duration::from_secs(600),
        )
        .await
        {
            Err(IntakeError::MissingKeyValue { table }) => assert_eq!(table, "public.nokey"),
            other => panic!("expected the enumeration to fail, got {other:?}"),
        }
        assert_eq!(status(&client, id).await, "waiting_to_backfill");
        let markers: i64 = client
            .query_one("select count(*) from pending_backfill", &[])
            .await
            .expect("count markers")
            .get(0);
        assert_eq!(markers, 1, "the marker must survive for the next pass");

        // Once the cause is fixed, the next pass picks the definition back up.
        client
            .batch_execute("alter table public.nokey add primary key (id)")
            .await
            .expect("give the source a primary key");
        run_pending_backfills(
            &mut client,
            "wake",
            &StagedWatermark::saturated(),
            Duration::from_secs(600),
        )
        .await
        .expect("the retry discharges");
        assert_eq!(status(&client, id).await, "live");
    }

    /// Issue #444: going live after a committed discharge flips the
    /// definition and parks the catch-ups the flip calls for, and the two
    /// commit together. When a park fails (injected here with a trigger
    /// rejecting the catch-up on `public.d`, which a live definition reads),
    /// the definition must not be left `live` with that catch-up lost. It is
    /// handed back to `waiting_to_backfill` with its marker re-parked, and the
    /// next pass takes it live.
    #[tokio::test]
    async fn a_failed_catchup_park_does_not_leave_the_definition_live() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(&db).await;
        client
            .batch_execute(
                "create table public.orders (id bigint primary key); \
                 insert into public.orders values (1); \
                 create publication test_pub; \
                 insert into source_table_versions (source_table, version) \
                 values ('public.orders', 1), ('public.d', 1);",
            )
            .await
            .expect("seed the source table");
        reconcile_publication(&mut client, "test_pub", &["public.orders".to_string()])
            .await
            .expect("reconcile parks a marker");
        let id: i64 = client
            .query_one(
                "insert into transform_definitions \
                 (target_table, source_table, source_version, definition_text, status) \
                 values ('public.d', 'public.orders', 1, $1, 'waiting_to_backfill') \
                 returning id",
                &[&"TRANSFORM d FROM orders SELECT id AS x"],
            )
            .await
            .expect("seed a deferred definition")
            .get(0);
        client
            .batch_execute(
                "insert into transform_definitions \
                 (target_table, source_table, source_version, definition_text, status) \
                 values ('public.r', 'public.d', 1, '', 'live'); \
                 create function reject_d_marker() returns trigger \
                 language plpgsql as $$ \
                 begin raise exception 'injected: cannot park a marker on %', new.table_name; end $$; \
                 create trigger reject_d_marker before insert on pending_backfill \
                   for each row when (new.table_name = 'public.d') \
                   execute function reject_d_marker()",
            )
            .await
            .expect("seed a live reader of public.d and the park-failure trigger");
        async fn status(client: &tokio_postgres::Client, id: i64) -> String {
            client
                .query_one(
                    "select status from transform_definitions where id = $1",
                    &[&id],
                )
                .await
                .expect("read status")
                .get(0)
        }
        async fn markers(client: &tokio_postgres::Client) -> Vec<String> {
            client
                .query(
                    "select table_name from pending_backfill order by table_name",
                    &[],
                )
                .await
                .expect("read markers")
                .into_iter()
                .map(|row| row.get(0))
                .collect()
        }

        let outcome = run_pending_backfills(
            &mut client,
            "wake",
            &StagedWatermark::saturated(),
            Duration::from_secs(600),
        )
        .await;
        assert!(outcome.is_err(), "the injected park failure surfaces");
        assert_eq!(
            status(&client, id).await,
            "waiting_to_backfill",
            "a definition whose catch-up didn't park must not be live"
        );
        assert_eq!(
            markers(&client).await,
            ["public.orders"],
            "the discharged marker is re-parked for the retry, and no catch-up is half-parked"
        );

        client
            .batch_execute("drop trigger reject_d_marker on pending_backfill")
            .await
            .expect("drop the park-failure trigger");
        run_pending_backfills(
            &mut client,
            "wake",
            &StagedWatermark::saturated(),
            Duration::from_secs(600),
        )
        .await
        .expect("the retry discharges");
        assert_eq!(status(&client, id).await, "live");
        assert_eq!(
            markers(&client).await,
            ["public.d"],
            "going live parks the catch-up on the target its reader reads"
        );
    }

    /// Issue #387, the server-side case: the marker delete fails inside the
    /// discharge transaction (forced here by a trigger), which leaves that
    /// transaction aborted. Dropping it must roll it back *before* the revert
    /// runs on the same connection, or the revert fails with "current
    /// transaction is aborted". Meanwhile an operator pauses one promoted
    /// definition and quarantines another while the discharge waits for
    /// intake. The revert must hand back only the one still `backfilling`.
    #[tokio::test]
    async fn a_failed_discharge_after_an_operator_freeze_reverts_only_backfilling() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut discharger, operator) = one_settled_marker(&db).await;
        // `one_settled_marker` registers a reader on `public.t`, which
        // upserts its `source_table_versions` row to version 1 already
        // (issue #417) — no separate seed needed here.
        discharger
            .batch_execute(
                "create function fail_marker_delete() returns trigger \
                 language plpgsql as $$ begin raise exception 'marker delete fails'; end $$; \
                 create trigger fail_marker_delete before delete on pending_backfill \
                 for each row execute function fail_marker_delete();",
            )
            .await
            .expect("make the marker delete fail");
        let mut ids = Vec::new();
        for target in ["public.d1", "public.d2", "public.d3"] {
            let id: i64 = discharger
                .query_one(
                    "insert into transform_definitions \
                     (target_table, source_table, source_version, definition_text, status) \
                     values ($1, 'public.t', 1, $2, 'waiting_to_backfill') \
                     returning id",
                    &[&target, &"TRANSFORM d FROM t SELECT id AS x"],
                )
                .await
                .expect("seed a deferred definition")
                .get(0);
            ids.push(id);
        }
        let (kept, paused, quarantined) = (ids[0], ids[1], ids[2]);

        let watermark = StagedWatermark::new();
        let waiting = tokio::sync::Notify::new();
        // `stop` is polled only while the enumeration waits for intake, which
        // is after the promotion to `backfilling` has committed.
        let stop = || {
            waiting.notify_one();
            false
        };
        let discharge = run_pending_backfills_until(
            &mut discharger,
            "wake",
            &watermark,
            Duration::from_secs(600),
            &stop,
        );
        let freeze = async {
            waiting.notified().await;
            operator
                .execute(
                    "update transform_definitions set status = 'paused' where id = $1",
                    &[&paused],
                )
                .await
                .expect("pause mid-discharge");
            operator
                .execute(
                    "update transform_definitions set status = 'quarantined' where id = $1",
                    &[&quarantined],
                )
                .await
                .expect("quarantine mid-discharge");
            watermark.advance(PgLsn::from(u64::MAX));
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(60), async {
            tokio::join!(discharge, freeze)
        })
        .await
        .expect("the discharge finishes");
        match result {
            Err(IntakeError::Db(error)) => assert_eq!(
                error.code(),
                Some(&tokio_postgres::error::SqlState::RAISE_EXCEPTION)
            ),
            other => panic!("expected the marker delete to fail, got {other:?}"),
        }

        let statuses: Vec<(i64, String)> = operator
            .query(
                "select id, status from transform_definitions where id = any($1) order by id",
                &[&ids],
            )
            .await
            .expect("read statuses")
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        assert_eq!(
            statuses,
            vec![
                (kept, "waiting_to_backfill".to_string()),
                (paused, "paused".to_string()),
                (quarantined, "quarantined".to_string()),
            ]
        );
        assert!(
            marker_generation(&operator).await.is_some(),
            "the marker must survive for the next pass"
        );
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

    /// Seals and drains until nothing is pending: a bounded loop, not a
    /// convergence wait.
    async fn drain_all(pool: &crate::pool::Pool, client: &mut tokio_postgres::Client) {
        let watermark = StagedWatermark::saturated();
        for _ in 0..16 {
            let outcome = crate::staging::seal::seal_phase1(client)
                .await
                .expect("seal phase 1");
            crate::staging::seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
                .await
                .expect("seal phase 2");
            while crate::staging::apply::drain_once(
                pool,
                outcome.sealed_seg_seq,
                "catch_up_tests",
                1,
                "wake",
                &watermark,
            )
            .await
            .expect("drain_once")
            .is_some()
            {}
            crate::staging::retire_drained_segments(client)
                .await
                .expect("retire drained segments");
            if !crate::staging::has_pending(client)
                .await
                .expect("has_pending")
            {
                return;
            }
        }
        panic!("pipeline did not reach quiescence within 16 seal/drain rounds");
    }

    /// `public.orders` (group `g = 0` holds ids 2, 4, 6 and group 1 holds 1,
    /// 3, 5, with `a = id`) summed by `order_rollup`, built, paused, changed
    /// by `gap`, and resumed, with the resume's marker settled and ready to
    /// discharge. Returns a same-crate pool and the discharging connection.
    async fn resumed_rollup(
        db: &testkit::TestDatabase,
        gap: &str,
    ) -> (crate::pool::Pool, tokio_postgres::Client) {
        let config = crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
        let pool = crate::pool::Pool::new(&config).expect("build a same-crate pool");
        let mut client = connect(db).await;
        client
            .batch_execute(
                "create table public.orders (id bigint primary key, g bigint, a numeric); \
                 alter table public.orders replica identity full; \
                 insert into public.orders select s, s % 2, s from generate_series(1, 6) s;",
            )
            .await
            .expect("seed orders");
        let columns = [
            ("id", crate::defs::ValueType::Numeric),
            ("g", crate::defs::ValueType::Numeric),
            ("a", crate::defs::ValueType::Numeric),
        ]
        .into_iter()
        .map(|(name, ty)| (name.to_string(), ty))
        .collect();
        crate::defs::install_definition(
            &pool,
            "TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total",
            &columns,
            "public",
        )
        .await
        .expect("install order_rollup");
        drain_all(&pool, &mut client).await;
        crate::defs::lifecycle::pause_transform(&pool, "order_rollup")
            .await
            .expect("pause");
        client.batch_execute(gap).await.expect("write while paused");
        crate::staging::quarantine::resume_transform(&pool, "order_rollup")
            .await
            .expect("resume");
        // Settles the fence the resume captured.
        client
            .batch_execute("select txid_current()")
            .await
            .expect("consume an xid");
        (pool, client)
    }

    /// Stages one CDC change on `public.orders` the way intake would.
    async fn stage_order_cdc(
        client: &mut tokio_postgres::Client,
        key: &str,
        op: crate::staging::CdcOp,
        old_image: Option<&str>,
        new_image: Option<&str>,
    ) {
        let txn = client.transaction().await.expect("begin");
        append::append(
            &txn,
            &[StagedChange::Cdc {
                src_table: "public.orders".to_string(),
                key: key.to_string(),
                op,
                lsn: Some(PgLsn::from(1)),
                old_image: old_image.map(str::to_string),
                new_image: new_image.map(str::to_string),
                origin_lsn: None,
                src_changed: None,
                hop_gen: 0,
                group_key: None,
            }],
        )
        .await
        .expect("stage cdc");
        txn.commit().await.expect("commit cdc");
    }

    async fn rollup_rows(client: &tokio_postgres::Client) -> Vec<(i64, String)> {
        client
            .query(
                "select g::bigint, total::text from public.order_rollup order by g",
                &[],
            )
            .await
            .expect("read order_rollup")
            .into_iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect()
    }

    /// Issue #330's ordering, the half that fixes where the orphan delete
    /// runs. Group 0 is empty when the discharge starts and is repopulated
    /// after the enumeration's snapshot is taken, so the cursor never sees
    /// the new row and only its CDC carries it. The group must come out as
    /// that row alone. Had the anti-join run after the intake wait (as
    /// #330's spike did), it would have seen the new row, kept the group's
    /// pre-pause total of 12, and the CDC insert would have folded into that
    /// as a delta: 112. The repopulation lands during the wait, so this test
    /// can't tell "before `DECLARE`" from "right after `DECLARE`": both pass.
    #[tokio::test]
    async fn a_group_repopulated_after_the_enumeration_snapshot_holds_only_its_new_rows() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, mut discharger) =
            resumed_rollup(&db, "delete from public.orders where g = 0").await;
        let mut writer = connect(&db).await;

        let writer_ref = &mut writer;
        discharge_racing(&mut discharger, |go, _done| async move {
            writer_ref
                .batch_execute("insert into public.orders values (10, 0, 100)")
                .await
                .expect("repopulate group 0");
            stage_order_cdc(
                writer_ref,
                "10",
                crate::staging::CdcOp::Insert,
                None,
                Some(r#"{"id":"10","g":"0","a":"100"}"#),
            )
            .await;
            go.send(()).expect("release discharge");
        })
        .await;
        drain_all(&pool, &mut discharger).await;

        assert_eq!(
            rollup_rows(&discharger).await,
            vec![(0, "100".to_string()), (1, "9".to_string())],
            "group 0 is rebuilt from its new row alone, not on top of its pre-pause total"
        );
    }

    /// The other half: group 1's last row is deleted after the enumeration's
    /// snapshot, so the orphan delete (which ran before it) kept the group
    /// and the enumeration still names that row. The row's CDC delete is
    /// what removes the group, applied once the definition is `live`.
    #[tokio::test]
    async fn a_group_emptied_after_the_orphan_delete_is_dropped_by_its_cdc() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, mut discharger) =
            resumed_rollup(&db, "delete from public.orders where id in (3, 5)").await;
        let mut writer = connect(&db).await;

        let writer_ref = &mut writer;
        discharge_racing(&mut discharger, |go, _done| async move {
            writer_ref
                .batch_execute("delete from public.orders where id = 1")
                .await
                .expect("empty group 1");
            stage_order_cdc(
                writer_ref,
                "1",
                crate::staging::CdcOp::Delete,
                Some(r#"{"id":"1","g":"1","a":"1"}"#),
                None,
            )
            .await;
            go.send(()).expect("release discharge");
        })
        .await;
        drain_all(&pool, &mut discharger).await;

        assert_eq!(
            rollup_rows(&discharger).await,
            vec![(0, "12".to_string())],
            "group 1 went extinct after the orphan delete ran; its CDC delete drops it"
        );
    }
}
