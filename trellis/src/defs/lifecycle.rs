//! The other half of a definition's lifecycle (issue #142, ADR-0014):
//! **pause** and **drop**.
//!
//! ```text
//! [*] --define--> Backfilling --> Live --pause--> Paused --drop--> [*]
//!                                  ^                 |
//!                                  +---- resume -----+   (a fresh backfill,
//!                                                         not a catch-up)
//! ```
//!
//! Three things this module deliberately does *not* do, each straight out of
//! the ADR:
//!
//! 1. **It does not invent a freezing mechanism.** Pausing writes
//!    [`TransformStatus::Paused`] into the same `transform_definitions.status`
//!    column the poison fuse already writes
//!    [`TransformStatus::Quarantined`] into, and the claim-time fold stops
//!    dispatching to the target for exactly the reason it already stops for a
//!    quarantined one: [`super::catalog::dependents_of`]'s `t.status = 'live'`
//!    predicate, the single gate every target resolution goes through. The
//!    one place that gate did not previously reach is the durable
//!    backfill-chunk queue, which is why
//!    [`super::chunk_queue::claim_chunks`] now consults the same column —
//!    same gate, extended to the one dispatch path that was outside it, not a
//!    second gate.
//!
//! 2. **It does not delete shared state.** A drop removes the target's data —
//!    always, there is no keep-the-rows option — and otherwise only what the
//!    *target* owns: its `transform_definitions` row (whose `on delete
//!    cascade` takes `backfill_chunks` with it,
//!    `V20__backfill_chunks.sql`), the edges leading *into* its schema node,
//!    and its per-column quarantine bookkeeping (`column_status`,
//!    `column_deaths`, `column_failures`, `column_pause_cascades`). Everything
//!    keyed to the *source* table and co-owned with sibling definitions —
//!    ring segments, claims, `poison`/`poison_held`/`key_deaths`, the
//!    `transform_fuse_gate` row — is untouched, because a sibling reading the
//!    same source is still using it and a drain worker may be mid-batch over
//!    it right now. A chunk a worker holds at drop time is released on its own
//!    heartbeat/TTL, never force-cleared.
//!
//! 3. **It does not cascade.** A live definition chaining off the target
//!    being dropped refuses the drop and is named in the error
//!    ([`CatalogError::DependentsBlockDrop`]); the operator retires
//!    dependents first, in reverse dependency order.
//!
//! Both verbs are **idempotent and non-transactional with any host
//! migration** (ADR-0014, "Pause and drop are idempotent"): they run on
//! Trellis's own pooled connections, so a framework migration that is
//! replayed or rolled back has to be safe in both directions. Pausing an
//! already-frozen definition succeeds as a no-op and dropping an absent one
//! succeeds as a no-op, so the two questions a replayed migration cannot
//! answer for itself — "did the pause land?", "is it already gone?" — both
//! resolve to success.
//!
//! **Publication shrinkage is the caller's last step, not this module's.**
//! [`crate::Trellis::drop_transform`] calls
//! [`crate::intake::publication::reconcile_publication`] inline once the drop
//! commits (ADR-0014, "The publication shrinks by reconciliation"). It lives
//! at the facade rather than here because reconciling needs a concrete
//! `tokio_postgres::Client` and the configured publication name, neither of
//! which this layer has — see that method's own doc comment.

use tokio_postgres::Transaction;

use super::catalog::{CatalogError, definition_by_target, dependents_of, relationship_by_name};
use super::model::{EdgeKind, TransformStatus};
use crate::pool::{Pool, quote_ident};

/// What [`pause_transform`] actually did — returned so a caller (and a test)
/// can tell a real transition from the idempotent no-op, even though both are
/// successes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PauseOutcome {
    /// The definition was live (or backfilling, or waiting to backfill) and
    /// is now [`TransformStatus::Paused`].
    Paused,
    /// The definition was already frozen — by an earlier pause, or by the
    /// poison fuse — and was left exactly as it was. ADR-0014's idempotency
    /// clause: this is a success, not a conflict.
    AlreadyFrozen(TransformStatus),
}

/// What [`drop_transform`] actually did — same "distinguish the no-op from
/// the real thing without failing either" contract as [`PauseOutcome`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DropOutcome {
    /// The definition existed, was paused, and is gone.
    Dropped,
    /// No definition by that name was registered. ADR-0014's idempotency
    /// clause: dropping an absent definition is a success.
    Absent,
}

/// Freezes `target` — ADR-0014's deliberate, operator-driven half of the
/// pause state whose other half is the poison fuse's
/// [`crate::staging::quarantine::trip_transform_fuse_if_crossed`].
///
/// `target` is the **bare** transform name, matching every other
/// operator-facing entry point in this crate
/// ([`crate::staging::quarantine::resume_transform`],
/// [`crate::Trellis::status`]) — see [`definition_by_target`]'s doc comment
/// for why that, not the qualified spelling, is the addressing scheme.
///
/// Idempotent: a definition already [`TransformStatus::Paused`] *or*
/// [`TransformStatus::Quarantined`] is left untouched and reported as
/// [`PauseOutcome::AlreadyFrozen`]. A quarantined definition is deliberately
/// **not** overwritten to `paused`: it is already frozen (which is all a
/// pause asks for), and flattening the poison trigger into an operator one
/// would erase the reason from [`crate::Trellis::quarantined`]'s report while
/// changing nothing about the definition's actual behaviour.
///
/// A definition still `backfilling` or `waiting_to_backfill` pauses fine —
/// the pause is a freeze on *dispatch*, and both of those states dispatch
/// (the chunk queue, and the pending-backfill discharge respectively). Not
/// restricting the pause to `live` is what lets an operator stop a runaway
/// initial build, which is exactly the situation a pause exists for.
///
/// A target with no `transform_definitions` row at all is
/// [`CatalogError::TransformNotFound`], not a no-op: you cannot freeze
/// something that was never defined, and unlike a drop there is no
/// replayed-migration reading under which "it isn't there" is the outcome the
/// caller wanted.
#[tracing::instrument(name = "lifecycle.pause_transform", skip(pool), fields(transform = %target))]
pub(crate) async fn pause_transform(
    pool: &Pool,
    target: &str,
) -> Result<PauseOutcome, CatalogError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    let Some((id, status)) = locked_status(&txn, target).await? else {
        return Err(CatalogError::TransformNotFound {
            transform: target.to_string(),
        });
    };

    if is_frozen(status) {
        // Commit rather than roll back: nothing was written, and committing
        // releases the `for update` lock the same way the mutating arm does.
        txn.commit().await?;
        tracing::info!(
            transform = %target,
            status = %status.as_str(),
            "pause is a no-op; transform was already frozen"
        );
        return Ok(PauseOutcome::AlreadyFrozen(status));
    }

    txn.execute(
        "update transform_definitions set status = $1 where id = $2",
        &[&TransformStatus::Paused.as_str(), &id],
    )
    .await?;
    txn.commit().await?;

    tracing::info!(
        transform = %target,
        from = %status.as_str(),
        to = %TransformStatus::Paused.as_str(),
        "transform paused"
    );
    Ok(PauseOutcome::Paused)
}

/// Removes `target`'s definition — ADR-0014's terminal reap.
///
/// **Only ever acts on a frozen definition.** There is no direct live-to-gone
/// edge in the lifecycle: quiescing first is a precondition, so the removal
/// never has to reason about a fold still dispatching to the target. A
/// definition that is not [`TransformStatus::Paused`] or
/// [`TransformStatus::Quarantined`] is refused with
/// [`CatalogError::TransformNotPaused`]; pause it first.
///
/// **Always removes the associated data.** The target table is dropped, full
/// stop — there is no option to retire the definition while keeping its rows.
/// Keeping derived rows after removing the definition that explains them has
/// no use worth naming, and the paused state already serves the caller who
/// wants the data to stick around unmaintained: leave it paused rather than
/// dropping it. Because the target table is Trellis-owned, dropping it is
/// Trellis's to do; the source table is the user's and is untouched.
///
/// **Refuses rather than cascades** when a live definition still chains off
/// this target ([`CatalogError::DependentsBlockDrop`], naming them). The
/// dependency check runs before anything is written, and it is deliberately
/// live-only: a dependent that is itself frozen is not deriving from this
/// target right now, so it does not block — and the operator retiring a chain
/// works from the leaves inward, which is exactly reverse dependency order.
///
/// Idempotent: an unregistered `target` is [`DropOutcome::Absent`], a
/// success.
#[tracing::instrument(name = "lifecycle.drop_transform", skip(pool), fields(transform = %target))]
pub(crate) async fn drop_transform(pool: &Pool, target: &str) -> Result<DropOutcome, CatalogError> {
    let Some(def) = definition_by_target(pool, target).await? else {
        tracing::info!(transform = %target, "drop is a no-op; no such definition");
        return Ok(DropOutcome::Absent);
    };

    if !is_frozen(def.status) {
        return Err(CatalogError::TransformNotPaused {
            transform: target.to_string(),
            status: def.status,
        });
    }

    // Before any write. `dependents_of` is keyed on the qualified node name
    // (issue #74) and filters to `status = 'live'` internally, which is
    // precisely ADR-0014's refusal condition — so this is the existing
    // dependency query, not a new one.
    let mut blockers: Vec<String> = dependents_of(pool, &def.target_table, EdgeKind::Source)
        .await?
        .into_iter()
        .map(|d| d.def.target)
        .collect();

    // ADR-0014's "chains off the target" is not only the `FROM <target>`
    // spelling. A `RELATIONSHIP <name> FROM <t>.<col> TO <target>.<col>`
    // makes every live transform over `<t>` that reads `<name>.<column>` a
    // reader of this target's rows — and that path leaves no `source` edge
    // behind ([`EdgeKind::Join`] has no writer yet), so `dependents_of`
    // above cannot see it. Left unchecked the drop succeeds and that reader
    // silently derives from a table that no longer exists; worse, the
    // `relationship` edge this drop deliberately does *not* remove keeps the
    // target's `schema_nodes` row alive, so [`super::all_source_tables`]
    // keeps naming the vanished table and every later
    // `reconcile_publication` — including the running client's own periodic
    // one — fails `42P01`, wedging intake fleet-wide.
    for (from_table, name) in relationships_pointing_at(pool, target).await? {
        blockers.extend(live_relationship_readers(pool, &from_table, &name).await?);
    }

    if !blockers.is_empty() {
        blockers.sort();
        blockers.dedup();
        return Err(CatalogError::DependentsBlockDrop {
            subject: target.to_string(),
            dependents: blockers,
        });
    }

    let qualified = def.target_table.clone();
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    // Re-read under `for update` and re-check: the dependency and status
    // checks above ran outside this transaction, and pause/drop are
    // explicitly not transactional with a host migration, so a concurrent
    // resume could have unfrozen the row in between. Losing that race must
    // not silently drop a live definition.
    let Some((id, status)) = locked_status(&txn, target).await? else {
        return Ok(DropOutcome::Absent);
    };
    if !is_frozen(status) {
        return Err(CatalogError::TransformNotPaused {
            transform: target.to_string(),
            status,
        });
    }

    // ADR-0014, "Quarantine state follows its owner": the target-keyed
    // per-column quarantine bookkeeping goes with the data it describes —
    // its forensic value is about rows that are about to stop existing (or
    // stop being maintained). These four tables are bare-transform-keyed
    // (`V22__column_status_drops_target_table_fkey.sql` explains why they
    // are not FK'd, and therefore why this has to be an explicit delete
    // rather than a cascade).
    //
    // What is *not* here is the point: `poison`, `poison_held`, `key_deaths`
    // and `transform_fuse_gate` are keyed to the **source** table and shared
    // with every sibling definition reading it, so a drop leaves the whole-key
    // poison band exactly as it found it.
    for table in ["column_status", "column_deaths", "column_failures"] {
        txn.execute(
            &format!("delete from {table} where transform_table = $1"),
            &[&target],
        )
        .await?;
    }
    txn.execute(
        "delete from column_pause_cascades \
         where downstream_transform = $1 or upstream_transform = $1",
        &[&target],
    )
    .await?;

    // Edges *into* the target node are the ones this definition owns: its
    // `source` edge from its own source table, plus any `join` edges its
    // relationship-enriched fields added. Edges *out of* the target node
    // belong to whatever chains off it and are left alone — as is a
    // `relationship` edge, which a `RELATIONSHIP` declaration owns and
    // `drop_relationship` reaps.
    if let Some(node_id) = node_id_for(&txn, &qualified).await? {
        txn.execute(
            "delete from schema_edges where to_node_id = $1 and kind in ('source', 'join')",
            &[&node_id],
        )
        .await?;
        // The node itself only goes once nothing at all still names it —
        // it may well still be some other definition's source.
        txn.execute(
            "delete from schema_nodes n \
             where n.id = $1 \
               and not exists ( \
                   select 1 from schema_edges e \
                   where e.from_node_id = n.id or e.to_node_id = n.id \
               ) \
               and not exists ( \
                   select 1 from transform_definitions t \
                   where t.target_table = n.table_name or t.source_table = n.table_name \
               )",
            &[&node_id],
        )
        .await?;
    }

    // `backfill_chunks` rides this out on its `on delete cascade`
    // (`V20__backfill_chunks.sql`) — per-definition work, keyed to the
    // definition, removed with it. A chunk another worker holds right now is
    // deleted here too, but that is safe by construction: `finish_chunk`
    // locks its `transform_definitions` row and raises
    // `ChunkQueueError::DefinitionNotFound` when it is gone, and the worker's
    // write targets a table this same transaction is about to drop.
    txn.execute("delete from transform_definitions where id = $1", &[&id])
        .await?;

    // The data goes with the definition, unconditionally (ADR-0014, "Drop
    // always removes the associated data"). The target table is Trellis-owned,
    // so this is Trellis's to drop; a caller who wants the derived rows to
    // survive unmaintained leaves the definition paused instead of dropping it.
    txn.batch_execute(&format!(
        "drop table if exists {}",
        quote_qualified(&qualified)
    ))
    .await?;

    // Both of these key a *qualified table name* to pending intake work, and
    // an enumeration marker naming a table that no longer exists would fail
    // the next `run_pending_backfills` discharge with a live `42P01`.
    txn.execute(
        "delete from pending_backfill where table_name = $1",
        &[&qualified],
    )
    .await?;
    txn.execute(
        "delete from backfill_coverage where table_name = $1",
        &[&qualified],
    )
    .await?;

    txn.commit().await?;
    tracing::info!(
        transform = %target,
        target_table = %qualified,
        "transform dropped"
    );
    Ok(DropOutcome::Dropped)
}

/// Removes the relationship named `name` on `from_table` — a relationship is
/// a definition too, and ADR-0014's inverse applies to it uniformly.
///
/// `(from_table, name)` because a relationship name is unique *per from-table*,
/// not globally (`V16__relationship_definitions.sql`) — the same pair
/// [`relationship_by_name`] takes and the same pair a calculated field's
/// `<rel>.<column>` head resolves against.
///
/// **Refuses rather than cascades**, exactly like [`drop_transform`]: any
/// *live* transform whose text still references this relationship blocks the
/// drop and is named in [`CatalogError::DependentsBlockDrop`]. Dependents are
/// found by re-parsing each live definition's persisted text and asking
/// [`super::eval::relationship_references`] what it reads — the same
/// resolution path [`super::catalog::resolve_relationships`] uses to enrich a
/// field, rather than a second, driftable notion of "uses this relationship".
///
/// Drops the relationship's Trellis-owned parent projection table along with
/// it (`relationship_projections`' own row rides the `on delete cascade`
/// `V26__relationship_projections.sql` put there against exactly this day).
///
/// Idempotent: an unregistered relationship is [`DropOutcome::Absent`].
///
/// **No pause for a relationship.** A relationship has no `status` column and
/// nothing in the fold gates on one — freezing it would mean building the
/// second freezing mechanism ADR-0014 rules out, so the verb is deliberately
/// absent rather than faked. A relationship is dropped outright, once nothing
/// live still reads it.
#[tracing::instrument(
    name = "lifecycle.drop_relationship",
    skip(pool),
    fields(relationship = %name, from_table = %from_table)
)]
pub(crate) async fn drop_relationship(
    pool: &Pool,
    from_table: &str,
    name: &str,
) -> Result<DropOutcome, CatalogError> {
    let Some(reldef) = relationship_by_name(pool, from_table, name).await? else {
        tracing::info!(
            relationship = %name,
            from_table = %from_table,
            "drop is a no-op; no such relationship"
        );
        return Ok(DropOutcome::Absent);
    };

    let dependents = live_relationship_readers(pool, from_table, name).await?;
    if !dependents.is_empty() {
        return Err(CatalogError::DependentsBlockDrop {
            subject: format!("{from_table}.{name}"),
            dependents,
        });
    }

    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    let projection: Option<String> = txn
        .query_opt(
            "select projection_table from relationship_projections where relationship_id = $1",
            &[&reldef.id],
        )
        .await?
        .map(|row| row.get(0));

    // The `relationship` edge is persisted parent -> child (`to_table ->
    // from_table`; see `create_relationship`'s own comment for why that
    // direction). Removed only when no *other* relationship still connects
    // the same pair — two relationships between the same two tables share
    // the one edge, since `schema_edges` is unique on
    // `(from_node_id, to_node_id, kind)`.
    txn.execute(
        "delete from schema_edges e \
         using schema_nodes parent, schema_nodes child \
         where e.kind = 'relationship' \
           and e.from_node_id = parent.id and e.to_node_id = child.id \
           and split_part(parent.table_name, '.', 2) = $1 \
           and split_part(child.table_name, '.', 2) = $2 \
           and not exists ( \
               select 1 from relationship_definitions r \
               where r.to_table = $1 and r.from_table = $2 and r.id <> $3 \
           )",
        &[&reldef.def.to_table, &reldef.def.from_table, &reldef.id],
    )
    .await?;

    // Cascades `relationship_projections`' row.
    txn.execute(
        "delete from relationship_definitions where id = $1",
        &[&reldef.id],
    )
    .await?;

    if let Some(projection) = projection {
        txn.batch_execute(&format!(
            "drop table if exists {}.{}",
            quote_ident(pool.target_schema()),
            quote_ident(&projection)
        ))
        .await?;
    }

    txn.commit().await?;
    tracing::info!(
        relationship = %name,
        from_table = %from_table,
        "relationship dropped"
    );
    Ok(DropOutcome::Dropped)
}

/// Every `(from_table, name)` relationship whose **to**-side is the bare
/// table `target` — the relationships through which a transform over some
/// other source can be reading `target`'s rows.
///
/// `relationship_definitions.from_table`/`to_table` are persisted bare
/// (`create_relationship` resolves them to qualified names only for its own
/// `pg_catalog` checks and the shared `schema_nodes`/`schema_edges`; the
/// declaration's own columns stay as written), so this matches bare — the
/// same spelling `drop_relationship` addresses them by.
async fn relationships_pointing_at(
    pool: &Pool,
    target: &str,
) -> Result<Vec<(String, String)>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select from_table, name from relationship_definitions \
             where to_table = $1 order by id",
            &[&target],
        )
        .await?;
    Ok(rows.into_iter().map(|r| (r.get(0), r.get(1))).collect())
}

/// The bare target names of every **live** transform that still reads the
/// relationship `name` declared on `from_table`.
///
/// Scoped to definitions whose own source *is* `from_table`: a relationship
/// name is only resolvable from a definition over its from-table (that is
/// what makes the name per-from-table unique in the first place), so a
/// same-named relationship on a different from-table is a different
/// relationship and must not be counted as a dependent here.
async fn live_relationship_readers(
    pool: &Pool,
    from_table: &str,
    name: &str,
) -> Result<Vec<String>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select definition_text from transform_definitions \
             where status = 'live' order by id",
            &[],
        )
        .await?;

    let mut readers = Vec::new();
    for row in rows {
        let text: String = row.get(0);
        let def = super::parse(&text)?;
        if def.source != from_table {
            continue;
        }
        if super::eval::relationship_references(&def)
            .iter()
            .any(|(rel, _)| rel == name)
        {
            readers.push(def.target);
        }
    }
    Ok(readers)
}

/// ADR-0014's single frozen state, both triggers. The one predicate every
/// pause/resume/drop precondition in this crate asks.
fn is_frozen(status: TransformStatus) -> bool {
    matches!(
        status,
        TransformStatus::Paused | TransformStatus::Quarantined
    )
}

/// `(id, status)` for the bare-named `target`, row-locked for the rest of the
/// transaction — the same `for update` + `split_part(target_table, '.', 2)`
/// shape [`crate::staging::quarantine::resume_transform`] uses, so a pause, a
/// resume and a drop racing each other on one definition serialize on its row
/// rather than interleaving.
async fn locked_status(
    txn: &Transaction<'_>,
    target: &str,
) -> Result<Option<(i64, TransformStatus)>, CatalogError> {
    let row = txn
        .query_opt(
            "select id, status from transform_definitions \
             where split_part(target_table, '.', 2) = $1 for update",
            &[&target],
        )
        .await?;
    Ok(row.map(|row| {
        let id: i64 = row.get(0);
        let status_text: String = row.get(1);
        let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
            panic!("transform_definitions.status held unrecognized value '{status_text}'")
        });
        (id, status)
    }))
}

async fn node_id_for(txn: &Transaction<'_>, qualified: &str) -> Result<Option<i64>, CatalogError> {
    let row = txn
        .query_opt(
            "select id from schema_nodes where table_name = $1",
            &[&qualified],
        )
        .await?;
    Ok(row.map(|row| row.get(0)))
}

/// A persisted `"schema.table"` rendered DDL-ready, component-independently
/// quoted — the same shape [`super::ddl::qualified_target_table`] produces,
/// but from the already-qualified string this module reads back rather than
/// from a `TransformDef` plus a configured schema. Reading it back is the
/// point: a definition persists the schema it *was* created under (issue
/// #73), and dropping its table must address that one, not whatever
/// `Config::target_schema` happens to say today.
///
/// `transform_definitions.target_table` has been fully qualified since issue
/// #73 and neither component can itself contain a `.` (the grammar rejects a
/// dotted identifier component), so the first `.` is the separator. A value
/// somehow missing one is quoted as a bare name and left to the connection's
/// `search_path`, rather than panicking over a row this crate's own writers
/// cannot produce.
fn quote_qualified(qualified: &str) -> String {
    match qualified.split_once('.') {
        Some((schema, table)) => format!("{}.{}", quote_ident(schema), quote_ident(table)),
        None => quote_ident(qualified),
    }
}
