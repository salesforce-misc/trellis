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
/// **Parks every registration's marker (ADR-0016, issue #418).** Registration
/// reads no source rows and parks nothing: a `waiting_to_backfill` definition
/// is captured by its source's marker's discharge, and this is where that
/// marker comes from. In the same transaction as the `ALTER`s,
/// [`park_registration_markers`] parks one on every source of a
/// `waiting_to_backfill` definition that has none, provided the source needs
/// no further publication change: it is in `desired_tables` (so published
/// once this commits) or is another definition's target (never published,
/// fed by the target-mutation seam). A source this pass just added already
/// has its join marker.
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
    park_registration_markers(&txn, desired_tables).await?;
    txn.commit().await?;
    Ok(())
}

/// Parks a `pending_backfill` marker on every source of a
/// `waiting_to_backfill` definition that has no marker yet and needs no
/// publication change: it is in `published`, or it is another definition's
/// target (ADR-0016's "Who parks the marker"). See [`reconcile_publication`].
///
/// A source that is in neither is left for the pass that publishes it, whose
/// join marker is fenced inside the `ALTER`'s transaction: a marker parked on
/// it any earlier could discharge before the table joined the stream, and a
/// commit between that read and the join would be neither read nor streamed.
///
/// A source that already has a marker is skipped: that marker's discharge
/// dispatches every `waiting_to_backfill` definition on the table. If the
/// marker is mid-discharge and that discharge read the table's definitions
/// before this one registered, it deletes the marker without dispatching
/// this one. The definition is then left without a marker only until the
/// staging worker's next pass, which parks one here. The staging worker
/// runs this every reconcile pass, right before its discharge. The process
/// that applies a `DROP` also runs it, through
/// `Trellis::reconcile_publication_after_drop`, until #427 moves that
/// reconcile to the staging worker.
pub(crate) async fn park_registration_markers(
    client: &impl GenericClient,
    published: &[String],
) -> Result<(), IntakeError> {
    let tables: Vec<String> = client
        .query(
            "select distinct d.source_table from transform_definitions d \
             where d.status = $1 \
               and (d.source_table = any($2) or exists ( \
                   select 1 from transform_definitions u where u.target_table = d.source_table \
               )) \
               and not exists ( \
                   select 1 from pending_backfill pb where pb.table_name = d.source_table \
               ) \
             order by 1",
            &[&TransformStatus::WaitingToBackfill.as_str(), &published],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    for table in tables {
        park_marker(client, &table).await?;
    }
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
/// amendment). `defs::catalog::complete_direct_backfill` calls this, once
/// per table the build read, the moment a chunk- or job-built definition
/// flips `backfilling` -> `live`: while it
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
/// A park also clears the marker's retry state (issue #407, ADR-0016): the
/// new generation is due at once, whatever the last discharge's failures had
/// backed it off to.
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
               added_at = now(), \
               attempts = 0, \
               last_error = null, \
               next_attempt_at = null",
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

struct PendingBackfill {
    table: String,
    fence: Snapshot,
    /// Which park of `table` this is ([`park_marker`]). Discharge deletes
    /// only this generation.
    generation: i64,
    /// How many discharges of this generation have failed so far.
    attempts: i32,
    /// Whether the backoff after the last failure has run out
    /// (`next_attempt_at`, read against the database's clock).
    due: bool,
}

async fn fetch_pending_backfills(
    client: &impl GenericClient,
) -> Result<Vec<PendingBackfill>, IntakeError> {
    let rows = client
        .query(
            "select table_name, fence_snapshot::text, generation, attempts, \
                    coalesce(next_attempt_at <= now(), true) \
             from pending_backfill",
            &[],
        )
        .await?;
    rows.into_iter()
        .map(|r| {
            let table: String = r.get(0);
            let fence_text: String = r.get(1);
            Snapshot::parse(&fence_text).map(|fence| PendingBackfill {
                table,
                fence,
                generation: r.get(2),
                attempts: r.get(3),
                due: r.get(4),
            })
        })
        .collect()
}

/// The backoff before the first retry of a failed discharge (issue #407).
/// Twice the maintenance loop's default reconcile interval, so a marker that
/// just failed sits out at least one pass.
const DISCHARGE_RETRY_BASE: Duration = Duration::from_secs(10);

/// The longest a failing marker waits between attempts. There is no
/// quarantine (ADR-0016): a marker that fails forever is retried at this
/// interval forever, so a fixed cause is picked up within this long.
const DISCHARGE_RETRY_CAP: Duration = Duration::from_secs(300);

/// How long a marker waits after its `attempts`th failed discharge before the
/// next one: [`DISCHARGE_RETRY_BASE`], doubled per further failure, capped at
/// [`DISCHARGE_RETRY_CAP`].
fn discharge_retry_delay(attempts: u32) -> Duration {
    let doublings = attempts.saturating_sub(1).min(31);
    DISCHARGE_RETRY_BASE
        .saturating_mul(1 << doublings)
        .min(DISCHARGE_RETRY_CAP)
}

/// Records a failed discharge of `marker` on its row (issue #407): one more
/// attempt, the error's text, and `delay` from now as when the next pass may
/// try it again. Scoped to the generation the pass read, so a park that raced
/// the failed discharge keeps the fresh, immediately due state
/// [`park_marker`] gave it.
async fn record_discharge_failure(
    client: &impl GenericClient,
    marker: &PendingBackfill,
    error: &IntakeError,
    delay: Duration,
) -> Result<(), IntakeError> {
    client
        .execute(
            "update pending_backfill set \
               attempts = attempts + 1, \
               last_error = $3, \
               next_attempt_at = now() + make_interval(secs => $4) \
             where table_name = $1 and generation = $2",
            &[
                &marker.table,
                &marker.generation,
                &error.to_string(),
                &delay.as_secs_f64(),
            ],
        )
        .await?;
    Ok(())
}

/// Parks a marker on `qualified_table` for a direct-build job that failed
/// (issue #419, `defs::chunk_queue::fail_chunk`), carrying the failure as the
/// marker's retry state: `attempts` failures so far, `error`'s text, and the
/// same backoff a failed discharge gets ([`discharge_retry_delay`]). The
/// discharge retries the build when the backoff runs out, and
/// `Trellis::status` reports the error meanwhile, exactly as for a marker
/// whose discharge failed (issue #407). Parked in the caller's transaction,
/// which hands the build back.
///
/// Merges into a marker already parked on the table like any park
/// ([`park_marker`]), which then takes this retry state too: the table's
/// catch-ups wait out the same backoff, just as they would behind a failing
/// discharge of the table.
pub(crate) async fn park_failed_build(
    client: &impl GenericClient,
    qualified_table: &str,
    attempts: i32,
    error: &str,
) -> Result<(), tokio_postgres::Error> {
    park_marker(client, qualified_table).await?;
    let delay = discharge_retry_delay(u32::try_from(attempts).unwrap_or(1));
    client
        .execute(
            "update pending_backfill set \
               attempts = $2, \
               last_error = $3, \
               next_attempt_at = now() + make_interval(secs => $4) \
             where table_name = $1",
            &[&qualified_table, &attempts, &error, &delay.as_secs_f64()],
        )
        .await?;
    Ok(())
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
                "select count(*)::bigint, pg_current_snapshot()::text \
                 from {}.{}",
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

/// Runs every pending backfill whose fence has settled: dispatches the build
/// of every `waiting_to_backfill` definition on the marker's table, stages the
/// table's pre-existing rows by ring enumeration where something needs them,
/// and deletes the marker, all in one transaction. A crash anywhere short of
/// that commit leaves the marker durable and the definitions
/// `waiting_to_backfill`; a fence that hasn't settled yet is left alone for
/// the next pass — this function is meant to be retried on every one.
///
/// # Dispatch by shape (ADR-0016, issues #418, #419)
///
/// The discharge is the one capture path every definition's build goes
/// through: a fresh registration, a resumed definition's rebuild, and a
/// direct build handed back after a failure alike. Each
/// `waiting_to_backfill` definition on the marker's table gets the build its
/// shape has:
///
/// - **A plain (non-relationship) 1-1 definition**: its primary-key chunk
///   boundaries are planned before the transaction opens
///   ([`crate::defs::backfill::plan_one_to_one_chunks`]), and the transaction
///   persists them as `backfill_chunks` rows and moves the definition to
///   `backfilling` together (`defs::chunk_queue::dispatch_one_to_one`). Drain
///   threads execute the chunks, and the last one flips it `live` and parks
///   its go-live catch-up.
/// - **An aggregate or relationship-enriched 1-1 definition**: the
///   transaction enqueues one direct-build job as a `backfill_chunks` row and
///   moves the definition to `backfilling` together
///   (`defs::chunk_queue::dispatch_direct_build`), after a catalog-only check
///   that the direct build can render it
///   ([`crate::defs::backfill::check_direct_build`]). A drain thread runs the
///   whole ADR-0007 build, and finishing it flips the definition `live` and
///   parks its go-live catch-ups, as for the last chunk.
/// - **The `Unsupported` fallback** goes through the ring enumeration below,
///   and flips `waiting_to_backfill` -> `live` as the last statement of the
///   transaction, together with its go-live catch-up parks. There is no
///   intermediate `backfilling` for it to be stranded in (issue #404).
///
/// A definition an operator paused after this pass read it keeps its status:
/// both status updates are scoped to `waiting_to_backfill`.
///
/// # When the table is enumerated
///
/// The enumeration stages one image-less `Recompute` row per source key, for
/// every `live` reader of the table to re-derive. It runs when a ring-built
/// definition needs it, or when anything other than this pass's
/// background-built definitions (chunks or a direct-build job, which read the
/// table themselves) reads the table (a catch-up for `live` readers) — unless
/// [`coverage_covers`] shows the table unchanged since a direct build folded
/// it in (issue #79, bug B). A marker on a table only background-built
/// definitions read, or nothing reads at all (issue #417: a fresh install parks a marker
/// on every configured source table, [`create_slot_and_park_markers`]), is
/// discharged without enumerating.
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
/// If intake doesn't get there within `catch_up_timeout`, the whole
/// discharge rolls back — no chunk is enqueued and no status moves — and the
/// marker stays, so the next pass retries cleanly. The pass then stops rather
/// than waiting again on the remaining markers: each later horizon is at
/// least as far ahead, so every one would most likely time out too, and the
/// maintenance loop seals nothing while this waits.
///
/// # A failing marker (issues #387, #407)
///
/// A discharge that fails outright rolls back the same way, so the marker
/// and its definitions stay as they were. The failure then ends only that
/// marker's turn, not the pass: it is logged, recorded on the marker (one
/// more `attempts`, the `last_error` text, and a `next_attempt_at` backed off
/// by [`discharge_retry_delay`]), and the pass goes on to the next marker. A
/// marker that isn't due yet is skipped, so a broken one is neither retried
/// every pass nor able to starve the healthy ones behind it, whatever order
/// the markers are read in. There is no quarantine: it is retried at the
/// capped interval until the cause is fixed or its definitions are dropped,
/// and a new park of the table resets the state ([`park_marker`]). The
/// recorded error is what `Trellis::status` reports for every definition
/// reading the table.
///
/// This form returns the first such failure once the pass has run, for tests
/// that expect one; [`run_pending_backfills_until`] returns them all without
/// failing the pass.
///
/// A caller with no intake running yet must not call this at all (see
/// `client::setup_staging`); tests with no CDC stream pass
/// [`crate::staging::StagedWatermark::saturated`].
///
/// # Dropping what the source no longer backs (issue #330)
///
/// The enumeration and the chunks only reach keys the source still has. A
/// definition this pass rebuilds (a resumed one, above all) can hold target
/// rows whose source rows went away while it was frozen, and nothing would
/// ever enumerate them. So, first thing in the transaction,
/// `resume_orphans::delete_orphaned_target_rows` deletes every row of each
/// dispatched definition's target that no source row backs, reporting each
/// through the target-mutation seam. It runs before `DECLARE`, and never
/// after the intake wait, which would leave an aggregate group repopulated
/// during the wait at its stale pre-pause value. The maintenance loop that
/// runs this pass is also the only sealer, so any source change committed
/// after the pass starts is drained only once a ring-built definition is
/// `live`. The anti-join and the cursor still read different snapshots, which
/// leaves a short race for aggregates in either order (issue #436). That
/// module's doc comment has the full argument and its limits.
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
    let failures =
        run_pending_backfills_until(client, wake_channel, watermark, catch_up_timeout, &|| false)
            .await?;
    match failures.into_iter().next() {
        Some(failure) => Err(failure.error),
        None => Ok(()),
    }
}

/// One marker whose discharge failed in a pass of
/// [`run_pending_backfills_until`]. The failure is already logged and
/// recorded on the marker, which stays for a later pass.
#[derive(Debug)]
pub(crate) struct FailedDischarge {
    /// The marker's table.
    #[allow(dead_code)]
    pub(crate) table: String,
    pub(crate) error: IntakeError,
}

/// [`run_pending_backfills`], with `stop` checked while an enumeration waits
/// for intake. When `stop` returns `true` the wait gives up early, through
/// the same rollback as a timeout, so the maintenance loop shuts down
/// promptly with nothing half-dispatched.
///
/// Returns every marker whose discharge failed this pass (see "A failing
/// marker" above). An `Err` means the pass itself couldn't run: reading the
/// markers or recording a failure on one failed, which points at the
/// connection rather than any one marker.
#[tracing::instrument(
    name = "intake.run_pending_backfills",
    skip(client, wake_channel, watermark, stop),
    fields(
        pending = tracing::field::Empty,
        settled = tracing::field::Empty,
        failed = tracing::field::Empty
    )
)]
pub(crate) async fn run_pending_backfills_until(
    client: &mut tokio_postgres::Client,
    wake_channel: &str,
    watermark: &StagedWatermark,
    catch_up_timeout: Duration,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<Vec<FailedDischarge>, IntakeError> {
    let pending = fetch_pending_backfills(client).await?;
    tracing::Span::current().record("pending", pending.len());
    if pending.is_empty() {
        return Ok(Vec::new());
    }
    let now = current_snapshot(client).await?;

    let mut settled = 0usize;
    let mut failures = Vec::new();
    for marker in pending {
        if !marker.due {
            tracing::debug!(
                table = %marker.table,
                attempts = marker.attempts,
                "backfill marker backing off after a failed discharge; not due yet"
            );
            continue;
        }
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
        match discharge_marker(
            client,
            &marker,
            wake_channel,
            watermark,
            catch_up_timeout,
            stop,
        )
        .await
        {
            Ok(Discharge::Committed) => {}
            Ok(Discharge::Deferred { horizon }) => {
                tracing::debug!(
                    table = %marker.table,
                    horizon = %horizon,
                    staged_through = %watermark.get(),
                    "backfill enumeration deferred: intake has not staged through its snapshot yet"
                );
                break;
            }
            Err(error) => {
                // The maintenance loop doesn't report the failures this pass
                // returns, so this is the one place the log shows it; the
                // marker row carries it to `Trellis::status`.
                let attempts = u32::try_from(marker.attempts).unwrap_or(0) + 1;
                let retry_in = discharge_retry_delay(attempts);
                tracing::warn!(
                    table = %marker.table,
                    error = %error,
                    attempts,
                    retry_in_secs = retry_in.as_secs(),
                    "backfill discharge failed; its marker stays and is retried after a backoff"
                );
                record_discharge_failure(&*client, &marker, &error, retry_in).await?;
                failures.push(FailedDischarge {
                    table: marker.table,
                    error,
                });
            }
        }
    }
    let span = tracing::Span::current();
    span.record("settled", settled);
    span.record("failed", failures.len());
    Ok(failures)
}

/// How [`discharge_marker`] ended short of an error.
enum Discharge {
    /// The dispatches, the enumeration (if the table needed one) and the
    /// marker's delete committed together.
    Committed,
    /// Intake had not staged through `horizon` in time, so the transaction
    /// rolled back and the marker stays.
    Deferred { horizon: PgLsn },
}

/// The build [`discharge_marker`] dispatches for one `waiting_to_backfill`
/// definition (ADR-0016's dispatch by shape; see [`run_pending_backfills`]).
enum Build {
    /// A plain 1-1 definition: these `(lo, hi]` primary-key chunk
    /// boundaries, enqueued as `backfill_chunks` rows.
    Chunks(Vec<(Option<String>, String)>),
    /// An aggregate or relationship-enriched 1-1 definition: one direct-build
    /// job, enqueued as a `backfill_chunks` row (issue #419).
    Direct,
    /// The ring enumeration of the marker's table, flipped `live` in the
    /// discharge's own transaction.
    Ring,
}

/// Reads every `waiting_to_backfill` definition sourced from `table` and plans
/// the build its shape gets. Runs on `client` before the discharge's
/// transaction opens, so planning chunk boundaries (a walk of the source's
/// primary-key index) doesn't hold that transaction open.
async fn plan_waiting_builds(
    client: &tokio_postgres::Client,
    table: &str,
) -> Result<Vec<(i64, Build)>, IntakeError> {
    use crate::defs::ast::KeySpace;
    use crate::defs::backfill::{self, BackfillError};
    use crate::defs::catalog::CatalogError;

    let rows = client
        .query(
            "select id, definition_text from transform_definitions \
             where source_table = $1 and status = $2 order by id",
            &[&table, &TransformStatus::WaitingToBackfill.as_str()],
        )
        .await?;
    let mut builds = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i64 = row.get(0);
        let text: String = row.get(1);
        let def = crate::defs::parse(&text).map_err(CatalogError::from)?;
        let chunked =
            matches!(def.key_space, KeySpace::OneToOne) && !backfill::uses_relationships(&def);
        let planned = if chunked {
            backfill::plan_one_to_one_chunks(client, &def, table)
                .await
                .map(Build::Chunks)
        } else {
            backfill::check_direct_build(client, &def, table)
                .await
                .map(|()| Build::Direct)
        };
        let build = match planned {
            Ok(build) => build,
            Err(BackfillError::Unsupported(what)) => {
                tracing::debug!(
                    definition_id = id,
                    table = %table,
                    unsupported = %what,
                    "direct build unsupported; falling back to the ring enumeration"
                );
                Build::Ring
            }
            Err(err) => return Err(CatalogError::DirectBackfill(err).into()),
        };
        builds.push((id, build));
    }
    Ok(builds)
}

/// Runs `marker`'s discharge in one transaction: the orphan delete, the
/// enumeration (see [`run_pending_backfills`]'s "When the table is
/// enumerated"), each waiting definition's dispatch, the marker's delete and
/// the ring-built definitions' flip to `live`. An error drops the
/// transaction, which rolls it back, so the marker survives either way the
/// discharge falls short.
async fn discharge_marker(
    client: &mut tokio_postgres::Client,
    marker: &PendingBackfill,
    wake_channel: &str,
    watermark: &StagedWatermark,
    catch_up_timeout: Duration,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<Discharge, IntakeError> {
    let builds = plan_waiting_builds(client, &marker.table).await?;
    let waiting: Vec<i64> = builds.iter().map(|(id, _)| *id).collect();
    // Definitions whose build reads the table itself, in the background.
    let background: Vec<i64> = builds
        .iter()
        .filter(|(_, build)| !matches!(build, Build::Ring))
        .map(|(id, _)| *id)
        .collect();
    let ring: Vec<i64> = builds
        .iter()
        .filter(|(_, build)| matches!(build, Build::Ring))
        .map(|(id, _)| *id)
        .collect();

    let txn = client.transaction().await?;
    // Issue #330: before the enumeration's `DECLARE` and its intake wait; see
    // "Dropping what the source no longer backs" above. On the rollback
    // below (including the `Deferred` and error paths, since both drop
    // `txn` without committing), the deletes roll back with everything else.
    super::resume_orphans::delete_orphaned_target_rows(&txn, &marker.table, &waiting).await?;
    // A ring-built definition has never read the table, so coverage can't
    // stand in for its enumeration.
    let enumerate = !ring.is_empty()
        || (crate::defs::catalog::table_has_reader(&txn, &marker.table, &background).await?
            && !coverage_covers(&txn, &marker.table).await?);
    if enumerate {
        declare_enumeration(&txn, &marker.table).await?;
        let horizon: PgLsn = txn
            .query_one("select pg_current_wal_insert_lsn()", &[])
            .await?
            .get(0);
        // On a quiet stream nothing else would carry intake past `horizon`
        // until its next keepalive (issue #452).
        crate::staging::converge::request_intake_confirm(&txn).await?;
        if !intake_caught_up(watermark, horizon, catch_up_timeout, stop).await {
            txn.rollback().await?;
            return Ok(Discharge::Deferred { horizon });
        }
        append_enumeration(&txn, &marker.table).await?;
    }
    for (id, build) in &builds {
        match build {
            Build::Chunks(ranges) => {
                crate::defs::chunk_queue::dispatch_one_to_one(&txn, *id, ranges).await?;
            }
            Build::Direct => {
                crate::defs::chunk_queue::dispatch_direct_build(&txn, *id, marker.attempts).await?;
            }
            Build::Ring => {}
        }
    }
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
    let chained_sources = go_live(&txn, &ring).await?;
    if enumerate {
        txn.execute("select pg_notify($1, '')", &[&wake_channel])
            .await?;
    }
    txn.commit().await?;
    // Issue #315: `go_live` parked each chained source's catch-up inside the
    // transaction, so it survives a crash here. Its fence, though, predates
    // the flip's commit, and a seam writer that started after that fence
    // and checked for `live` readers before the commit reached nobody. A
    // re-park now keeps the later fence ([`park_marker`]), which waits that
    // writer out.
    for source in chained_sources {
        park_backfill_catchup(&*client, &source).await?;
    }
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

/// Flips exactly `ids` (the discharge's ring-built definitions)
/// `waiting_to_backfill` -> [`TransformStatus::Live`] inside the discharge's
/// transaction, and parks the go-live catch-ups the flip calls for in that
/// same transaction, so the enumeration, the flip and the catch-ups commit
/// together or not at all (issues #404/#444).
///
/// Scoped `status = 'waiting_to_backfill'` (issue #331): an operator pause
/// (or a quarantine) that landed on one of `ids` since the discharge read it
/// leaves it frozen — its resume re-parks a marker of its own — rather than
/// forcing it `live` behind the operator's back.
///
/// Two kinds of catch-up, both issue #315, parked in name order so two
/// transactions parking overlapping tables can't deadlock on each other's
/// marker rows:
///
/// - A flipped definition's target that some `live` definition reads (see
///   [`park_target_catchup_if_read`]): its readers re-derive from the
///   rebuilt target.
/// - A flipped definition's source that is another definition's target: it
///   was enumerated while the definition wasn't `live`, which the
///   target-mutation seam skips, and that target is never in the
///   publication. A write to it between the enumeration and the flip reached
///   nobody. The next discharge flips nothing, so this doesn't loop.
///
/// Returns the second kind, for the caller to re-park after commit (see
/// [`discharge_marker`]).
async fn go_live(txn: &Transaction<'_>, ids: &[i64]) -> Result<Vec<String>, IntakeError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let flipped: Vec<i64> = txn
        .query(
            "update transform_definitions set status = $1 \
             where id = any($2) and status = $3 \
             returning id",
            &[
                &TransformStatus::Live.as_str(),
                &ids,
                &TransformStatus::WaitingToBackfill.as_str(),
            ],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    if !flipped.is_empty() {
        tracing::info!(
            ids = ?flipped,
            from = %TransformStatus::WaitingToBackfill.as_str(),
            to = %TransformStatus::Live.as_str(),
            "transform status transition: backfill enumeration committed"
        );
    }
    if flipped.len() < ids.len() {
        let skipped: Vec<i64> = ids
            .iter()
            .filter(|id| !flipped.contains(id))
            .copied()
            .collect();
        tracing::info!(
            ids = ?skipped,
            "backfill enumeration staged, but these definitions left `waiting_to_backfill` \
             meanwhile; leaving their status as it is"
        );
    }
    let read_targets: Vec<String> = txn
        .query(
            "select d.target_table from transform_definitions d \
             where d.id = any($1) and exists ( \
                 select 1 from transform_definitions r \
                 where r.source_table = d.target_table and r.status = 'live' \
             )",
            &[&flipped],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    let chained_sources: Vec<String> = txn
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
    let mut tables: Vec<&String> = read_targets.iter().chain(&chained_sources).collect();
    tables.sort_unstable();
    tables.dedup();
    for table in tables {
        park_backfill_catchup(txn, table).await?;
    }
    Ok(chained_sources)
}

/// Test stand-in for the staging worker's maintenance pass over newly
/// registered definitions (ADR-0016), for a test with no staging worker and
/// no publication: parks the marker [`reconcile_publication`] would on every
/// `waiting_to_backfill` definition's source (treating every source as
/// published), then discharges every settled marker with intake taken as
/// caught up. Retries for a few seconds while a fence is still pinned by some
/// other transaction in the cluster (other tests share it), and returns once
/// no definition is left `waiting_to_backfill` or the retries run out. Chunks
/// it enqueues still need a drain worker (or a test's own chunk loop).
/// [`discharge_registrations`], then claims and runs every backfill chunk it
/// (or anything before it) enqueued until none is left: a synchronous
/// stand-in for the staging worker and a drain thread together, for a test
/// that needs its plain 1-1 definitions `live` before it goes on (to chain a
/// definition off one, say). Ring enumerations it stages are left in the
/// ring for the test to drain. Panics on any failure: it is test harness.
#[cfg(any(test, feature = "internals"))]
pub async fn settle_registrations(pool: &crate::pool::Pool) {
    use crate::defs::chunk_queue;
    const CLAIMED_BY: &str = "settle_registrations";
    discharge_registrations(pool)
        .await
        .expect("dispatch registered definitions' builds");
    loop {
        let client = pool.get().await.expect("acquire a connection");
        let claimed = chunk_queue::claim_chunks(&**client, CLAIMED_BY, 1000)
            .await
            .expect("claim backfill chunks");
        drop(client);
        if claimed.is_empty() {
            return;
        }
        for chunk in &claimed {
            chunk_queue::run_claimed_chunk(pool, chunk, CLAIMED_BY, Duration::from_secs(5))
                .await
                .expect("run a backfill chunk");
            chunk_queue::finish_chunk(pool, chunk, CLAIMED_BY)
                .await
                .expect("finish a backfill chunk");
        }
    }
}

#[cfg(any(test, feature = "internals"))]
pub async fn discharge_registrations(pool: &crate::pool::Pool) -> Result<(), IntakeError> {
    let mut client = pool
        .get()
        .await
        .map_err(crate::defs::catalog::CatalogError::from)?;
    for _ in 0..100 {
        let sources: Vec<String> = client
            .query(
                "select distinct source_table from transform_definitions where status = $1",
                &[&TransformStatus::WaitingToBackfill.as_str()],
            )
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        if sources.is_empty() {
            return Ok(());
        }
        park_registration_markers(&**client, &sources).await?;
        // Consume an xid so a fence parked just now has settled.
        client.batch_execute("select txid_current()").await?;
        run_pending_backfills(
            &mut client,
            "trellis_wake",
            &StagedWatermark::saturated(),
            Duration::ZERO,
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
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

/// The slot's observed health, per `pg_replication_slots`' invalidation
/// columns and its `confirmed_flush_lsn` against this instance's last
/// confirmed position.
#[derive(Debug, PartialEq, Eq)]
enum SlotHealth {
    Healthy,
    Missing,
    /// Postgres invalidated the slot; see [`SlotRow::is_invalidated`].
    Invalidated,
    /// Present and not invalidated, but its `confirmed_flush_lsn` is past the
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
    // `invalidation_reason` (PG17+) and `conflicting` (PG16+) are read
    // through `to_jsonb` so one query runs on every supported server: a
    // column the server's view doesn't have reads as NULL instead of failing
    // the query.
    let row = client
        .query_opt(
            "select s.wal_status, s.confirmed_flush_lsn, \
                    to_jsonb(s) ->> $2::text, \
                    (to_jsonb(s) ->> $3::text)::boolean \
             from pg_replication_slots s \
             where s.slot_name = $1 and s.database = current_database()",
            &[&slot, &INVALIDATION_REASON_COLUMN, &CONFLICTING_COLUMN],
        )
        .await?;
    Ok(classify_slot(
        row.map(|row| SlotRow {
            wal_status: row.get(0),
            confirmed_flush: row.get(1),
            invalidation_reason: row.get(2),
            conflicting: row.get(3),
        }),
        last_confirmed_lsn,
    ))
}

/// The `pg_replication_slots` columns [`slot_health`] reads by name through
/// `to_jsonb`, where a misspelt name reads as NULL just like a server
/// without the column; `slot_health_reads_a_real_slot` pins both names.
const INVALIDATION_REASON_COLUMN: &str = "invalidation_reason";
const CONFLICTING_COLUMN: &str = "conflicting";

/// The `pg_replication_slots` columns [`slot_health`] reads for one slot.
struct SlotRow {
    wal_status: Option<String>,
    confirmed_flush: Option<PgLsn>,
    /// PG17+; NULL on older servers and for a slot that is still valid.
    invalidation_reason: Option<String>,
    /// PG16+; NULL on older servers.
    conflicting: Option<bool>,
}

impl SlotRow {
    /// Issue #413: `wal_status = 'lost'` covers only invalidation by the
    /// retention cap (`max_slot_wal_keep_size`). A logical slot can also be
    /// invalidated on a standby by a recovery conflict (`rows_removed`,
    /// `wal_level_insufficient`, PG16+) or, on PG18, by
    /// `idle_replication_slot_timeout` (`idle_timeout`), and none of those
    /// set `wal_status` to `lost`. PG17+ names every reason in
    /// `invalidation_reason`; PG16 has only `conflicting`, which is true for
    /// exactly the two recovery-conflict reasons. Streaming from any
    /// invalidated slot fails, so each of these is as unrecoverable as `lost`.
    fn is_invalidated(&self) -> bool {
        self.wal_status.as_deref() == Some("lost")
            || self.invalidation_reason.is_some()
            || self.conflicting == Some(true)
    }
}

/// [`slot_health`]'s decision over the row it read, if any.
fn classify_slot(row: Option<SlotRow>, last_confirmed_lsn: PgLsn) -> SlotHealth {
    let Some(row) = row else {
        return SlotHealth::Missing;
    };
    if row.is_invalidated() {
        return SlotHealth::Invalidated;
    }
    // NULL only for a physical slot (filtered out by the `database` scope) or
    // a logical slot another session is still creating, which Postgres gives
    // a position once it reaches its consistent point. The slot this instance
    // acknowledged had one from the moment its creation returned, so a NULL
    // one is a recreate in progress; called healthy, intake could start
    // streaming from it, past the gap, as soon as the creation finished.
    match row.confirmed_flush {
        Some(position) if position <= last_confirmed_lsn => SlotHealth::Healthy,
        _ => SlotHealth::Recreated,
    }
}

/// Checked at [`super::Intake::connect`] whenever `last_confirmed_lsn` shows
/// this instance has confirmed work against `slot` before: the slot must
/// still exist, not be invalidated, and not have been recreated under the
/// same name, which shows up as a position past `last_confirmed_lsn` (issue
/// #406, see [`slot_health`]). Invalidation (retention cap exceeded, a
/// standby's recovery conflict, PG18's idle timeout), loss on
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
mod slot_health_tests {
    use super::*;

    fn last_confirmed() -> PgLsn {
        PgLsn::from(0x1000)
    }

    fn valid_slot() -> SlotRow {
        SlotRow {
            wal_status: Some("reserved".to_string()),
            confirmed_flush: Some(PgLsn::from(0x0800)),
            invalidation_reason: None,
            conflicting: Some(false),
        }
    }

    #[test]
    fn a_valid_slot_behind_the_last_confirmed_position_is_healthy() {
        assert_eq!(
            classify_slot(Some(valid_slot()), last_confirmed()),
            SlotHealth::Healthy
        );
    }

    #[test]
    fn a_missing_slot_is_missing() {
        assert_eq!(classify_slot(None, last_confirmed()), SlotHealth::Missing);
    }

    #[test]
    fn a_lost_slot_is_invalidated() {
        let row = SlotRow {
            wal_status: Some("lost".to_string()),
            invalidation_reason: Some("wal_removed".to_string()),
            ..valid_slot()
        };
        assert_eq!(
            classify_slot(Some(row), last_confirmed()),
            SlotHealth::Invalidated
        );
    }

    /// Issue #413: PG17+ names the reason even when `wal_status` isn't
    /// `lost` — a standby's recovery conflicts, and PG18's idle timeout.
    #[test]
    fn every_invalidation_reason_is_invalidated_whatever_wal_status_says() {
        for reason in ["rows_removed", "wal_level_insufficient", "idle_timeout"] {
            let row = SlotRow {
                invalidation_reason: Some(reason.to_string()),
                ..valid_slot()
            };
            assert_eq!(
                classify_slot(Some(row), last_confirmed()),
                SlotHealth::Invalidated,
                "invalidation_reason = {reason}"
            );
        }
    }

    /// Issue #413: PG16 has no `invalidation_reason`, only `conflicting`.
    #[test]
    fn a_conflicting_slot_without_a_reason_column_is_invalidated() {
        let row = SlotRow {
            conflicting: Some(true),
            ..valid_slot()
        };
        assert_eq!(
            classify_slot(Some(row), last_confirmed()),
            SlotHealth::Invalidated
        );
    }

    /// Pre-PG16 servers report neither column: `wal_status` alone decides.
    #[test]
    fn a_server_without_either_column_still_reads_healthy() {
        let row = SlotRow {
            invalidation_reason: None,
            conflicting: None,
            ..valid_slot()
        };
        assert_eq!(
            classify_slot(Some(row), last_confirmed()),
            SlotHealth::Healthy
        );
    }

    #[test]
    fn a_slot_past_the_last_confirmed_position_is_recreated() {
        let row = SlotRow {
            confirmed_flush: Some(PgLsn::from(0x2000)),
            ..valid_slot()
        };
        assert_eq!(
            classify_slot(Some(row), last_confirmed()),
            SlotHealth::Recreated
        );
    }

    /// The query itself, against this box's real server: the `to_jsonb`
    /// reads must parse and return a valid slot as healthy.
    #[tokio::test]
    async fn slot_health_reads_a_real_slot() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = db.pool.get().await.expect("acquire connection");
        let lsn: PgLsn = client
            .query_one(
                "select lsn from pg_create_logical_replication_slot('slot_413', 'pgoutput')",
                &[],
            )
            .await
            .expect("create a slot")
            .get(0);
        let health = slot_health(&**client, "slot_413", lsn).await;
        // A misspelt column name would read as NULL, the same as a server
        // without the column, and every other check here would still pass;
        // so pin the names `slot_health` reads to the versions that have them.
        let columns = client
            .query_one(
                "select current_setting('server_version_num')::int, \
                        to_jsonb(s) ? $1, to_jsonb(s) ? $2 \
                 from pg_replication_slots s where s.slot_name = 'slot_413'",
                &[&INVALIDATION_REASON_COLUMN, &CONFLICTING_COLUMN],
            )
            .await;
        client
            .execute("select pg_drop_replication_slot('slot_413')", &[])
            .await
            .expect("drop the slot");
        let columns = columns.expect("read the slot's columns");
        let version: i32 = columns.get(0);
        assert_eq!(
            columns.get::<_, bool>(1),
            version >= 170000,
            "invalidation_reason on {version}"
        );
        assert_eq!(
            columns.get::<_, bool>(2),
            version >= 160000,
            "conflicting on {version}"
        );
        assert_eq!(health.expect("slot_health"), SlotHealth::Healthy);
        assert_eq!(
            slot_health(&**client, "slot_413", lsn)
                .await
                .expect("slot_health"),
            SlotHealth::Missing
        );
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
    /// enumerated while it wasn't `live`, when the target-mutation seam skips
    /// it, and the target is never published. Going live must park a fresh
    /// catch-up on that target, or a write landing between the enumeration
    /// and the flip reaches nobody.
    #[tokio::test]
    async fn going_live_over_a_target_parks_a_catchup_on_that_target() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = db.pool.get().await.expect("acquire connection");

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
                 values ('public.d', 'public.t', 1, '', 'waiting_to_backfill') returning id",
                &[],
            )
            .await
            .expect("seed the chained definition")
            .get(0);

        let txn = client.transaction().await.expect("begin");
        let chained = go_live(&txn, &[reader]).await.expect("go_live");
        txn.commit().await.expect("commit");
        assert_eq!(
            chained,
            ["public.t"],
            "reported for the post-commit re-park"
        );

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

    /// Issue #331: an operator pause (or a quarantine) can land on a
    /// definition after the discharge read it and before its flip. The flip
    /// must only move definitions still `waiting_to_backfill` — never force a
    /// frozen definition `live` behind the operator.
    #[tokio::test]
    async fn going_live_leaves_a_definition_that_left_waiting_alone() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = db.pool.get().await.expect("acquire connection");

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
            ("public.still_waiting", "waiting_to_backfill"),
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

        let txn = client.transaction().await.expect("begin");
        go_live(&txn, &ids).await.expect("go_live");
        txn.commit().await.expect("commit");

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
            "only the definition still waiting goes live; a frozen one stays frozen"
        );
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

    /// Issue #407: the backoff after a failed discharge starts at the base,
    /// doubles per further failure, and stops growing at the cap, however
    /// many failures pile up.
    #[test]
    fn discharge_retry_delay_doubles_up_to_the_cap() {
        let secs = |attempts| discharge_retry_delay(attempts).as_secs();
        assert_eq!(DISCHARGE_RETRY_BASE.as_secs(), 10);
        assert_eq!(DISCHARGE_RETRY_CAP.as_secs(), 300);
        assert_eq!(secs(0), 10, "no failure yet reads as the first");
        assert_eq!(
            (1..=6).map(secs).collect::<Vec<_>>(),
            vec![10, 20, 40, 80, 160, 300]
        );
        assert_eq!(secs(40), 300);
        assert_eq!(
            secs(u32::MAX),
            300,
            "no overflow on a marker failing forever"
        );
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

    /// Issue #387: a discharge that fails partway (here planning the plain
    /// 1-1 definition's chunks, since `public.nokey` has no identity key)
    /// must leave its definitions `waiting_to_backfill`. A later pass only
    /// dispatches definitions still waiting, so one left in `backfilling`
    /// would never go live. The marker must survive too, so the retry has
    /// something to run.
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
            Err(IntakeError::Catalog(error)) => assert!(
                matches!(
                    *error,
                    crate::defs::catalog::CatalogError::DirectBackfill(
                        crate::defs::backfill::BackfillError::Ddl(
                            crate::defs::ddl::DdlError::NoPrimaryKey { .. }
                        )
                    )
                ),
                "expected the chunk planning to fail on the missing key, got {error:?}"
            ),
            other => panic!("expected the chunk planning to fail, got {other:?}"),
        }
        assert_eq!(status(&client, id).await, "waiting_to_backfill");
        let markers: i64 = client
            .query_one("select count(*) from pending_backfill", &[])
            .await
            .expect("count markers")
            .get(0);
        assert_eq!(markers, 1, "the marker must survive for the next pass");

        // Once the cause is fixed, the next pass after the marker's backoff
        // (issue #407; run out by hand here) picks the definition back up.
        client
            .batch_execute(
                "alter table public.nokey add primary key (id); \
                 update pending_backfill set next_attempt_at = now()",
            )
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
        assert_eq!(
            status(&client, id).await,
            "backfilling",
            "the retry dispatches the definition's chunks"
        );
    }

    /// A marker's retry state, as `(attempts, last_error, seconds until
    /// next_attempt_at)`, or `None` if `table` has no marker.
    async fn retry_state(
        client: &tokio_postgres::Client,
        table: &str,
    ) -> Option<(i32, Option<String>, Option<f64>)> {
        client
            .query_opt(
                "select attempts, last_error, \
                        extract(epoch from next_attempt_at - now())::float8 \
                 from pending_backfill where table_name = $1",
                &[&table],
            )
            .await
            .expect("read retry state")
            .map(|row| (row.get(0), row.get(1), row.get(2)))
    }

    /// Issue #407 (ADR-0016): a marker whose discharge always fails doesn't
    /// stop the healthy marker behind it in the same pass. The failure is
    /// recorded on the marker with a backoff, the marker isn't retried
    /// before its next-attempt time, each retry that fails again backs off
    /// further, and a new park of the table resets the state.
    ///
    /// `public.nokey` has no identity key, so planning its plain 1-1
    /// definition's chunks fails on every attempt. Its marker is parked first
    /// so an unordered read meets it first too.
    #[tokio::test]
    async fn a_failing_marker_backs_off_without_starving_the_marker_behind_it() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(&db).await;
        client
            .batch_execute(
                "create table public.nokey (id bigint not null); \
                 insert into public.nokey values (1); \
                 create table public.t (id bigint primary key); \
                 insert into public.t values (1); \
                 create publication test_pub; \
                 insert into source_table_versions (source_table, version) \
                 values ('public.nokey', 1);",
            )
            .await
            .expect("seed a broken and a healthy source table");
        reconcile_publication(&mut client, "test_pub", &["public.nokey".to_string()])
            .await
            .expect("reconcile parks the broken table's marker");
        client
            .execute(
                "insert into transform_definitions \
                 (target_table, source_table, source_version, definition_text, status) \
                 values ('public.d', 'public.nokey', 1, $1, 'waiting_to_backfill')",
                &[&"TRANSFORM d FROM nokey SELECT id AS x"],
            )
            .await
            .expect("seed the broken table's deferred definition");
        register_reader(&db, "public.t", "t_reader").await;
        reconcile_publication(
            &mut client,
            "test_pub",
            &["public.nokey".to_string(), "public.t".to_string()],
        )
        .await
        .expect("reconcile parks the healthy table's marker");

        async fn pass(client: &mut tokio_postgres::Client) -> Vec<FailedDischarge> {
            run_pending_backfills_until(
                client,
                "wake",
                &StagedWatermark::saturated(),
                Duration::from_secs(600),
                &|| false,
            )
            .await
            .expect("a failing marker must not fail the pass")
        }

        let failures = pass(&mut client).await;
        assert_eq!(
            retry_state(&client, "public.t").await,
            None,
            "the healthy marker behind the failing one must discharge in the same pass"
        );
        assert_eq!(
            failures
                .iter()
                .map(|failure| failure.table.as_str())
                .collect::<Vec<_>>(),
            vec!["public.nokey"]
        );
        let (attempts, last_error, retry_in) = retry_state(&client, "public.nokey")
            .await
            .expect("the failing marker stays");
        assert_eq!(attempts, 1);
        let last_error = last_error.expect("the failure's error is recorded");
        assert!(
            last_error.contains("no primary key"),
            "the recorded error names the cause, got {last_error:?}"
        );
        let retry_in = retry_in.expect("a failed marker has a next-attempt time");
        assert!(
            (5.0..=10.0).contains(&retry_in),
            "the first retry waits out the base backoff, got {retry_in}s"
        );

        // Not due yet: the next pass leaves it alone.
        assert!(pass(&mut client).await.is_empty());
        assert_eq!(
            retry_state(&client, "public.nokey").await.map(|s| s.0),
            Some(1),
            "a marker is not retried before its next-attempt time"
        );

        // Due again (run out by hand), it fails again and backs off further.
        client
            .execute(
                "update pending_backfill set next_attempt_at = now() - interval '1 second'",
                &[],
            )
            .await
            .expect("run the backoff out");
        assert_eq!(pass(&mut client).await.len(), 1);
        let (attempts, _, retry_in) = retry_state(&client, "public.nokey")
            .await
            .expect("the failing marker stays");
        assert_eq!(attempts, 2);
        let retry_in = retry_in.expect("a failed marker has a next-attempt time");
        assert!(
            (15.0..=20.0).contains(&retry_in),
            "the second retry waits twice as long, got {retry_in}s"
        );

        // A new park of the table starts it over, due at once.
        park_backfill_catchup(&client, "public.nokey")
            .await
            .expect("re-park the broken table");
        assert_eq!(
            retry_state(&client, "public.nokey").await,
            Some((0, None, None)),
            "a new park resets the retry state"
        );
    }

    /// Issue #407: a park that lands while a discharge of the same table is
    /// failing keeps its fresh state. The failure is recorded against the
    /// generation the pass read, which the park replaced.
    #[tokio::test]
    async fn a_park_racing_a_failed_discharge_stays_due() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut discharger, parker) = one_settled_marker(&db).await;
        // `one_settled_marker`'s `live` reader makes the discharge enumerate
        // (and so wait, which the race runs in). A waiting definition whose
        // dispatch then fails to move its status fails the discharge.
        discharger
            .batch_execute(
                "insert into transform_definitions \
                 (target_table, source_table, source_version, definition_text, status) \
                 values ('public.d', 'public.t', 1, 'TRANSFORM d FROM t SELECT id AS x', \
                         'waiting_to_backfill'); \
                 create function fail_status_move() returns trigger \
                 language plpgsql as $$ begin raise exception 'status move fails'; end $$; \
                 create trigger fail_status_move before update on transform_definitions \
                 for each row execute function fail_status_move();",
            )
            .await
            .expect("make the discharge fail");

        let parker_ref = &parker;
        discharge_racing(&mut discharger, |go, _done| async move {
            park_backfill_catchup(parker_ref, "public.t")
                .await
                .expect("park during discharge");
            go.send(()).expect("release discharge");
        })
        .await;

        let status: String = parker
            .query_one(
                "select status from transform_definitions where target_table = 'public.d'",
                &[],
            )
            .await
            .expect("read status")
            .get(0);
        assert_eq!(status, "waiting_to_backfill", "the discharge failed");
        assert_eq!(
            retry_state(&parker, "public.t").await,
            Some((0, None, None)),
            "the racing park's generation carries no failure"
        );
    }

    /// Issue #387, the server-side case: the marker delete fails inside the
    /// discharge transaction (forced here by a trigger), which leaves that
    /// transaction aborted, and rolling it back must leave every definition
    /// where it was. Meanwhile an operator pauses one waiting definition and
    /// quarantines another while the discharge waits for intake: their
    /// freezes stand, and the other stays `waiting_to_backfill`.
    #[tokio::test]
    async fn a_failed_discharge_after_an_operator_freeze_leaves_every_definition_where_it_was() {
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
        // `stop` is polled only while the enumeration waits for intake.
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
        match result.as_deref() {
            Ok(
                [
                    FailedDischarge {
                        error: IntakeError::Db(error),
                        ..
                    },
                ],
            ) => assert_eq!(
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
        settle_registrations(&pool).await;
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

    /// Issue #330's orphan delete, on a resumed aggregate's direct-build
    /// rebuild (issue #419). The discharge deletes the target rows no source
    /// row backs and dispatches the job; `race` then writes the source before
    /// the job reads it, staging that write's CDC the way intake would. The
    /// job runs and finishes, and the CDC drains once the definition is
    /// `live`.
    async fn rebuild_racing(
        pool: &crate::pool::Pool,
        discharger: &mut tokio_postgres::Client,
        race: &str,
        key: &str,
        op: crate::staging::CdcOp,
        old_image: Option<&str>,
        new_image: Option<&str>,
    ) {
        run_pending_backfills(
            discharger,
            "wake",
            &StagedWatermark::saturated(),
            Duration::ZERO,
        )
        .await
        .expect("discharge the resume's marker");
        let status: String = discharger
            .query_one(
                "select status from transform_definitions \
                 where target_table = 'public.order_rollup'",
                &[],
            )
            .await
            .expect("read status")
            .get(0);
        assert_eq!(status, "backfilling", "the rebuild is a direct-build job");
        discharger.batch_execute(race).await.expect("race the job");
        stage_order_cdc(discharger, key, op, old_image, new_image).await;
        settle_registrations(pool).await;
        drain_all(pool, discharger).await;
    }

    /// Group 0 is empty when the discharge's orphan delete runs, so its
    /// pre-pause row goes, and it is repopulated before the job reads the
    /// source. The job builds it from the new row, and that row's CDC,
    /// draining after the flip, must not count it again: its LSN is at or
    /// below the recompute horizon the build stamped on the group, so it
    /// re-derives the group instead of adding 100 to it (200).
    #[tokio::test]
    async fn a_group_repopulated_before_the_rebuild_reads_holds_only_its_new_rows() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, mut discharger) =
            resumed_rollup(&db, "delete from public.orders where g = 0").await;

        rebuild_racing(
            &pool,
            &mut discharger,
            "insert into public.orders values (10, 0, 100)",
            "10",
            crate::staging::CdcOp::Insert,
            None,
            Some(r#"{"id":"10","g":"0","a":"100"}"#),
        )
        .await;

        assert_eq!(
            rollup_rows(&discharger).await,
            vec![(0, "100".to_string()), (1, "9".to_string())],
            "group 0 is rebuilt from its new row alone, counted once"
        );
    }

    /// Group 1's last row is deleted after the orphan delete kept the group
    /// and before the job reads the source, so the job writes nothing for it
    /// and its pre-pause row survives the rebuild (#436's race). The row's
    /// CDC delete is what removes the group, applied once the definition is
    /// `live`.
    #[tokio::test]
    async fn a_group_emptied_after_the_orphan_delete_is_dropped_by_its_cdc() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, mut discharger) =
            resumed_rollup(&db, "delete from public.orders where id in (3, 5)").await;

        rebuild_racing(
            &pool,
            &mut discharger,
            "delete from public.orders where id = 1",
            "1",
            crate::staging::CdcOp::Delete,
            Some(r#"{"id":"1","g":"1","a":"1"}"#),
            None,
        )
        .await;

        assert_eq!(
            rollup_rows(&discharger).await,
            vec![(0, "12".to_string())],
            "group 1 went extinct after the orphan delete ran; its CDC delete drops it"
        );
    }

    /// The extinct-horizon half of the build's horizon. Group 0 is emptied
    /// while the definition is paused, with those deletes' CDC still staged
    /// when it goes live again, and the discharge's orphan delete drops the
    /// group's row, so the job writes none. A new row lands in the group after
    /// the job read, and its CDC drains in one batch with the deletes, against
    /// a group with no row. The deletes are at or below the extinct horizon
    /// the build raised, so the group is re-derived: 100. Folded as deltas
    /// instead, the deletes would subtract rows the build never counted: 88.
    #[tokio::test]
    async fn a_group_emptied_before_the_rebuild_reads_holds_only_its_new_rows() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, mut discharger) =
            resumed_rollup(&db, "delete from public.orders where g = 0").await;
        for id in [2, 4, 6] {
            stage_order_cdc(
                &mut discharger,
                &id.to_string(),
                crate::staging::CdcOp::Delete,
                Some(&format!(r#"{{"id":"{id}","g":"0","a":"{id}"}}"#)),
                None,
            )
            .await;
        }

        run_pending_backfills(
            &mut discharger,
            "wake",
            &StagedWatermark::saturated(),
            Duration::ZERO,
        )
        .await
        .expect("discharge the resume's marker");
        settle_registrations(&pool).await;
        discharger
            .batch_execute("insert into public.orders values (10, 0, 100)")
            .await
            .expect("repopulate group 0 after the build read");
        stage_order_cdc(
            &mut discharger,
            "10",
            crate::staging::CdcOp::Insert,
            None,
            Some(r#"{"id":"10","g":"0","a":"100"}"#),
        )
        .await;
        drain_all(&pool, &mut discharger).await;

        assert_eq!(
            rollup_rows(&discharger).await,
            vec![(0, "100".to_string()), (1, "9".to_string())],
            "group 0 holds only its new row: the deletes the build already read don't reach it"
        );
    }
}

/// Issue #418 (ADR-0016): registration's marker comes from the reconcile
/// pass, and the discharge dispatches each waiting definition's build by
/// shape. These drive the pieces directly rather than waiting on a running
/// staging worker (#297).
#[cfg(test)]
mod dispatch_tests {
    use super::*;

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

    /// `public.orders` with three rows (in two groups of `g`), a publication
    /// that doesn't hold it yet, and its version row.
    async fn seed(client: &tokio_postgres::Client) {
        client
            .batch_execute(
                "create table public.orders (id bigint primary key, g int, a numeric); \
                 alter table public.orders replica identity full; \
                 insert into public.orders values (1, 1, 10), (2, 1, 20), (3, 2, 30); \
                 create publication test_pub; \
                 insert into source_table_versions (source_table, version) \
                 values ('public.orders', 1)",
            )
            .await
            .expect("seed the source table");
    }

    /// A definition row sourced from `source`, as registration leaves one.
    async fn define(
        client: &tokio_postgres::Client,
        source: &str,
        target: &str,
        text: &str,
        status: &str,
    ) -> i64 {
        client
            .query_one(
                "insert into transform_definitions \
                 (target_table, source_table, source_version, definition_text, status) \
                 values ($1, $2, 1, $3, $4) returning id",
                &[&target, &source, &text, &status],
            )
            .await
            .expect("seed a definition")
            .get(0)
    }

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

    async fn chunks(client: &tokio_postgres::Client, id: i64) -> i64 {
        client
            .query_one(
                "select count(*) from backfill_chunks where definition_id = $1",
                &[&id],
            )
            .await
            .expect("count chunks")
            .get(0)
    }

    async fn staged(client: &tokio_postgres::Client) -> bool {
        crate::staging::has_pending(client)
            .await
            .expect("read the ring")
    }

    async fn reconcile(client: &mut tokio_postgres::Client, desired: &[&str]) {
        let desired: Vec<String> = desired.iter().map(|t| t.to_string()).collect();
        reconcile_publication(client, "test_pub", &desired)
            .await
            .expect("reconcile");
    }

    /// Consumes an xid, so a fence parked just before has settled, then runs
    /// one discharge pass with intake taken as caught up.
    async fn discharge(client: &mut tokio_postgres::Client) {
        client
            .batch_execute("select txid_current()")
            .await
            .expect("consume an xid");
        run_pending_backfills(
            client,
            "wake",
            &StagedWatermark::saturated(),
            Duration::ZERO,
        )
        .await
        .expect("discharge");
    }

    /// ADR-0016's "Who parks the marker": a waiting definition's source gets
    /// its marker from the reconcile pass that publishes it (the join
    /// marker), from a later pass once it is already published, or at once
    /// when it is another definition's target. A source this pass doesn't
    /// publish gets none, since a marker could then discharge before the
    /// table joined the stream.
    #[tokio::test]
    async fn the_reconcile_pass_parks_every_registrations_marker() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(&db).await;
        seed(&client).await;
        let text = "TRANSFORM d FROM orders SELECT a + a AS x";
        define(
            &client,
            "public.orders",
            "public.d",
            text,
            "waiting_to_backfill",
        )
        .await;

        reconcile(&mut client, &[]).await;
        assert!(
            markers(&client).await.is_empty(),
            "an unpublished source waits for the pass that publishes it"
        );

        reconcile(&mut client, &["public.orders"]).await;
        assert_eq!(markers(&client).await, ["public.orders"], "the join marker");
        discharge(&mut client).await;
        assert!(
            markers(&client).await.is_empty(),
            "precondition: discharged"
        );

        // A second registration on the now-published source, and one chained
        // off `public.d` (another definition's target, never published).
        client
            .batch_execute(
                "insert into source_table_versions (source_table, version) \
                 values ('public.d', 1)",
            )
            .await
            .expect("seed public.d's version row");
        define(
            &client,
            "public.orders",
            "public.e",
            text,
            "waiting_to_backfill",
        )
        .await;
        define(
            &client,
            "public.d",
            "public.f",
            "TRANSFORM f FROM d SELECT x AS y",
            "waiting_to_backfill",
        )
        .await;
        reconcile(&mut client, &["public.orders"]).await;
        assert_eq!(
            markers(&client).await,
            ["public.d", "public.orders"],
            "an already-published source and a definition's target both get a marker"
        );
    }

    /// A plain 1-1 definition is built by `backfill_chunks`: the discharge
    /// enqueues them and moves it to `backfilling` in the same transaction,
    /// and stages no ring enumeration when nothing else reads the table.
    #[tokio::test]
    async fn a_plain_one_to_one_definition_is_dispatched_as_chunks() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(&db).await;
        seed(&client).await;
        let id = define(
            &client,
            "public.orders",
            "public.d",
            "TRANSFORM d FROM orders SELECT a + a AS x",
            "waiting_to_backfill",
        )
        .await;
        reconcile(&mut client, &["public.orders"]).await;

        discharge(&mut client).await;
        assert_eq!(status(&client, id).await, "backfilling");
        assert_eq!(chunks(&client, id).await, 1);
        assert!(!staged(&client).await, "no ring enumeration of the source");
        assert!(
            markers(&client).await.is_empty(),
            "the marker is discharged"
        );
    }

    /// A shape the direct build can't render (here a cyclic field-alias
    /// chain) falls back to the ring: the discharge enumerates the source and
    /// flips the definition straight from `waiting_to_backfill` to `live` in
    /// the same transaction, with no `backfilling` in between (#404).
    #[tokio::test]
    async fn an_unsupported_shape_goes_live_by_ring_in_the_discharge() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(&db).await;
        seed(&client).await;
        let id = define(
            &client,
            "public.orders",
            "public.d",
            "TRANSFORM d FROM orders SELECT b + 1 AS a, a + 1 AS b",
            "waiting_to_backfill",
        )
        .await;
        reconcile(&mut client, &["public.orders"]).await;

        discharge(&mut client).await;
        assert_eq!(status(&client, id).await, "live");
        assert_eq!(chunks(&client, id).await, 0);
        assert!(
            staged(&client).await,
            "the source is enumerated into the ring"
        );
        assert!(markers(&client).await.is_empty());
    }

    /// An aggregate waiting on the discharge gets one direct-build job
    /// (issue #419), committed with its move to `backfilling`, and nothing is
    /// enumerated into the ring: the job reads the source itself.
    #[tokio::test]
    async fn an_aggregate_waiting_on_the_discharge_gets_a_direct_build_job() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(&db).await;
        seed(&client).await;
        let id = define(
            &client,
            "public.orders",
            "public.rollup",
            "TRANSFORM rollup FROM orders GROUP BY g SELECT sum(a) AS total",
            "waiting_to_backfill",
        )
        .await;
        reconcile(&mut client, &["public.orders"]).await;

        discharge(&mut client).await;
        assert_eq!(status(&client, id).await, "backfilling");
        assert_eq!(chunks(&client, id).await, 1);
        let unbounded: bool = client
            .query_one(
                "select hi is null and lo is null from backfill_chunks where definition_id = $1",
                &[&id],
            )
            .await
            .expect("read the job")
            .get(0);
        assert!(unbounded, "the job builds the whole definition");
        assert!(
            !staged(&client).await,
            "the source is not enumerated into the ring"
        );
        assert!(markers(&client).await.is_empty());
    }

    /// A ring-built definition has never read its table, so a coverage
    /// record an earlier direct build left behind can't stand in for its
    /// enumeration: the discharge enumerates even though the coverage vouches
    /// that the table hasn't changed since. Skipping it would flip the
    /// definition `live` over an empty target.
    #[tokio::test]
    async fn a_ring_built_definition_is_enumerated_even_where_coverage_covers_the_table() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(&db).await;
        seed(&client).await;
        define(
            &client,
            "public.orders",
            "public.rollup",
            "TRANSFORM rollup FROM orders GROUP BY g SELECT sum(a) AS total",
            "live",
        )
        .await;
        record_backfill_coverage(&client, "public.orders")
            .await
            .expect("record the live aggregate's coverage");
        let id = define(
            &client,
            "public.orders",
            "public.d",
            "TRANSFORM d FROM orders SELECT b + 1 AS a, a + 1 AS b",
            "waiting_to_backfill",
        )
        .await;
        reconcile(&mut client, &["public.orders"]).await;

        discharge(&mut client).await;
        assert_eq!(status(&client, id).await, "live");
        assert!(
            staged(&client).await,
            "the ring-built definition's enumeration isn't skipped for coverage"
        );
    }

    /// Issue #444, closed by construction: a ring-built definition's flip to
    /// `live` and the catch-ups it calls for commit in the discharge's own
    /// transaction. A park that fails (a trigger rejecting the catch-up on
    /// `public.d`, which a `live` definition reads) rolls the whole discharge
    /// back: the definition stays `waiting_to_backfill`, the marker and the
    /// ring are as they were, and the next pass takes it `live`.
    #[tokio::test]
    async fn a_failed_catchup_park_rolls_the_whole_discharge_back() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(&db).await;
        seed(&client).await;
        let id = define(
            &client,
            "public.orders",
            "public.d",
            "TRANSFORM d FROM orders SELECT b + 1 AS a, a + 1 AS b",
            "waiting_to_backfill",
        )
        .await;
        client
            .batch_execute(
                "insert into source_table_versions (source_table, version) \
                 values ('public.d', 1); \
                 create function reject_d_marker() returns trigger \
                 language plpgsql as $$ \
                 begin raise exception 'injected: cannot park a marker on %', new.table_name; end $$; \
                 create trigger reject_d_marker before insert on pending_backfill \
                   for each row when (new.table_name = 'public.d') \
                   execute function reject_d_marker()",
            )
            .await
            .expect("seed public.d's version row and the park-failure trigger");
        define(
            &client,
            "public.d",
            "public.r",
            "TRANSFORM r FROM d SELECT a AS y",
            "live",
        )
        .await;
        reconcile(&mut client, &["public.orders"]).await;
        client
            .batch_execute("select txid_current()")
            .await
            .expect("consume an xid");

        let outcome = run_pending_backfills(
            &mut client,
            "wake",
            &StagedWatermark::saturated(),
            Duration::ZERO,
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
            "the marker survives, and no catch-up is half-parked"
        );
        assert!(!staged(&client).await, "the enumeration rolled back too");

        // Run the failed marker's backoff (issue #407) out by hand.
        client
            .batch_execute(
                "drop trigger reject_d_marker on pending_backfill; \
                 update pending_backfill set next_attempt_at = now()",
            )
            .await
            .expect("drop the park-failure trigger");
        discharge(&mut client).await;
        assert_eq!(status(&client, id).await, "live");
        assert_eq!(
            markers(&client).await,
            ["public.d"],
            "going live parks the catch-up on the target its reader reads"
        );
    }

    /// A chunked definition on a table another definition already reads
    /// still gets the ring enumeration: the marker may carry a catch-up the
    /// `live` reader needs, and the discharge can't tell.
    #[tokio::test]
    async fn a_live_reader_still_gets_the_enumeration_beside_a_chunked_build() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(&db).await;
        seed(&client).await;
        define(
            &client,
            "public.orders",
            "public.old",
            "TRANSFORM old FROM orders SELECT a AS x",
            "live",
        )
        .await;
        let id = define(
            &client,
            "public.orders",
            "public.d",
            "TRANSFORM d FROM orders SELECT a + a AS x",
            "waiting_to_backfill",
        )
        .await;
        reconcile(&mut client, &["public.orders"]).await;

        discharge(&mut client).await;
        assert_eq!(status(&client, id).await, "backfilling");
        assert_eq!(chunks(&client, id).await, 1);
        assert!(
            staged(&client).await,
            "the live reader's enumeration is staged"
        );
    }
}
