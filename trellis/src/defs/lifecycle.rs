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
//! 3. **It does not cascade.** A definition chaining off the target being
//!    dropped refuses the drop and is named in the error
//!    ([`CatalogError::DependentsBlockDrop`]); the operator retires
//!    dependents first, in reverse dependency order. *Any* registered
//!    dependent blocks, whatever its status — see `drop_transform`'s doc
//!    comment for why a frozen or mid-backfill one is no safer to strand than
//!    a live one (issue #231).
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

use super::catalog::{CatalogError, relationship_by_name};
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
/// [`crate::Trellis::status`]) — see [`super::catalog::definition_by_target`]'s doc comment
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

    let Some(locked) = locked_definition(&txn, target).await? else {
        return Err(CatalogError::TransformNotFound {
            transform: target.to_string(),
        });
    };
    let (id, status) = (locked.id, locked.status);

    if status.is_frozen() {
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
/// **Refuses rather than cascades** when a definition still chains off this
/// target ([`CatalogError::DependentsBlockDrop`], naming them). *Any*
/// registered dependent blocks, not only a live one (issue #231): the
/// refusal's question is "is this definition still registered and could it
/// still need the target?", and every status other than gone answers yes.
/// A `backfilling`/`waiting_to_backfill` dependent is building *from* the
/// target right now, so dropping it out from under the build fails it
/// mid-flight or leaves it holding garbage; a `paused` or `quarantined` one
/// is worse, because ADR-0014's resume rebuilds by a fresh backfill from
/// source — take that source away and the dependent can never be resumed at
/// all. The operator retires a chain from the leaves inward, which is exactly
/// reverse dependency order.
///
/// "Dependent" covers relationships as well as transforms: a `RELATIONSHIP`
/// whose **to**-side is this target blocks the drop on its own, with no
/// transform reading through it, because it is a registered definition naming
/// the target — and because the surviving `relationship` edge would otherwise
/// strand the target's `schema_nodes` row (see [`dependency_blockers`] for
/// what that wedges). `drop_relationship` it first.
///
/// **Everything — the status precondition, the dependency refusal, and every
/// write — happens in one transaction, under the target's own `for update`
/// row lock** (issue #231). The checks used to run on separate pooled
/// connections before the transaction opened, leaving a window in which a
/// concurrent `resume` could unfreeze the row, or a concurrent `define`
/// register a fresh dependent, after the check and before the commit. A
/// refusal is still write-free: it returns before any statement mutates
/// anything, and the transaction is rolled back on the way out.
///
/// Idempotent: an unregistered `target` is [`DropOutcome::Absent`], a
/// success.
#[tracing::instrument(name = "lifecycle.drop_transform", skip(pool), fields(transform = %target))]
pub(crate) async fn drop_transform(pool: &Pool, target: &str) -> Result<DropOutcome, CatalogError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    // Read under `for update` and check *here*, not on a pooled connection
    // before this transaction opened: pause/drop are explicitly not
    // transactional with a host migration, so a concurrent resume could
    // otherwise unfreeze the row between a pre-check and this commit. Losing
    // that race must not silently drop a live definition.
    let Some(locked) = locked_definition(&txn, target).await? else {
        tracing::info!(transform = %target, "drop is a no-op; no such definition");
        return Ok(DropOutcome::Absent);
    };
    let (id, status, qualified) = (locked.id, locked.status, locked.target_table);

    if !status.is_frozen() {
        return Err(CatalogError::TransformNotPaused {
            transform: target.to_string(),
            status,
        });
    }

    // Before any write, and inside the same transaction as the writes, so a
    // `define` that registers a new dependent cannot interleave between the
    // refusal check and the commit (issue #231).
    let blockers = dependency_blockers(&txn, target, &qualified, id).await?;
    if !blockers.is_empty() {
        return Err(CatalogError::DependentsBlockDrop {
            subject: target.to_string(),
            dependents: blockers,
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

    // `backfill_chunks` rides this out on its `on delete cascade`
    // (`V20__backfill_chunks.sql`) — per-definition work, keyed to the
    // definition, removed with it. A chunk another worker holds right now is
    // deleted here too, but that is safe by construction: `finish_chunk`
    // locks its `transform_definitions` row and raises
    // `ChunkQueueError::DefinitionNotFound` when it is gone, and the worker's
    // write targets a table this same transaction is about to drop.
    //
    // Deleted *before* the node cleanup below (issue #232): the node reap's
    // own `not exists (... transform_definitions ...)` guard has to run once
    // this row is actually gone, or it always finds itself still there and
    // never reaps the node — every drop would leave an orphaned
    // `schema_nodes` row behind. Purely an ordering fix; both deletes commit
    // together in this same transaction either way.
    txn.execute("delete from transform_definitions where id = $1", &[&id])
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
        // The node itself only goes once nothing at all still names it — it
        // may well still be some other definition's source. This has to run
        // after the `transform_definitions` delete above, or the `not
        // exists` below always sees the row being dropped and never reaps
        // the node (issue #232).
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
/// registered transform whose text still references this relationship blocks
/// the drop and is named in [`CatalogError::DependentsBlockDrop`] — whatever
/// its status, for the reasons [`drop_transform`]'s own doc comment gives
/// (issue #231), and checked inside the same transaction as the writes for
/// the same reason. Dependents are found by re-parsing each definition's
/// persisted text and asking
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

    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    // Inside the transaction that does the removal, so a `define` of a fresh
    // reader cannot interleave between the check and the commit (issue #231).
    let dependents = relationship_readers(&txn, from_table, name, None).await?;
    if !dependents.is_empty() {
        return Err(CatalogError::DependentsBlockDrop {
            subject: format!("{from_table}.{name}"),
            dependents,
        });
    }

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

/// Every still-registered definition that chains off the bare-named `target`
/// — ADR-0014's refusal set, gathered on `txn` so [`drop_transform`] can
/// run it inside the very transaction that does the removal (issue #231).
///
/// `qualified_target` is the target's `"schema.table"` identity (the key
/// `schema_nodes` is on since issue #74); `definition_id` is the target's own
/// `transform_definitions.id`, excluded from the result so a definition whose
/// source node and target node happen to be the same row cannot block its own
/// drop — previously impossible to hit only because the check was live-only
/// and a drop's subject is always frozen.
///
/// Returns blocker names sorted and deduplicated, in the same two spellings
/// [`CatalogError::DependentsBlockDrop`]'s `subject` uses: a bare transform
/// name, or `from_table.relationship_name` for a relationship. A definition
/// reachable both by a `source` edge and through a relationship is one
/// blocker, named once.
async fn dependency_blockers(
    txn: &Transaction<'_>,
    target: &str,
    qualified_target: &str,
    definition_id: i64,
) -> Result<Vec<String>, CatalogError> {
    let mut blockers = source_edge_dependents(txn, qualified_target, definition_id).await?;

    // ADR-0014's "chains off the target" is not only the `FROM <target>`
    // spelling. A `RELATIONSHIP <name> FROM <t>.<col> TO <target>.<col>`
    // names this target in its own declaration, and there are *two* distinct
    // dependents hiding behind that one edge.
    //
    // 1. **The relationship itself**, whether or not anything reads through
    //    it. It is a registered definition naming the target, so the same
    //    rule the rest of this function applies — any dependent still
    //    defined, in any status, blocks — makes it a blocker in its own
    //    right. This is not a nicety: the drop deliberately leaves
    //    `relationship` edges alone (they belong to the declaration, and
    //    `drop_relationship` reaps them), so a relationship surviving its
    //    to-side keeps the target's `schema_nodes` row alive with nothing
    //    left to explain it. [`super::all_source_tables`] then keeps naming
    //    a table that no longer exists, and every later
    //    `reconcile_publication` — including the running client's own
    //    periodic one — fails `42P01`, wedging intake fleet-wide. Refusing
    //    is what keeps that unreachable; the reader check below never did,
    //    because the damage is edge-scoped and that check is reader-scoped.
    //
    // 2. **Every transform reading `<name>.<column>`**, which is a reader of
    //    this target's rows by a path that leaves no `source` edge behind
    //    ([`EdgeKind::Join`] has no writer yet), so `source_edge_dependents`
    //    above cannot see it. Named in addition to the relationship rather
    //    than instead of it, so the refusal shows the operator the whole
    //    subgraph still standing on this target rather than one layer of it
    //    at a time.
    for (from_table, name) in relationships_pointing_at(txn, target).await? {
        blockers.push(format!("{from_table}.{name}"));
        blockers.extend(relationship_readers(txn, &from_table, &name, Some(target)).await?);
    }

    blockers.sort();
    blockers.dedup();
    Ok(blockers)
}

/// The bare target names of every definition reached by a `source` edge out
/// of `qualified_target`'s schema node — i.e. everything defined
/// `FROM <target>`.
///
/// The same graph walk [`super::catalog::dependents_of`] does, deliberately
/// *without* its `t.status = 'live'` filter (issue #231): that filter is
/// right for the claim-time fold, which must not write a CDC delta into a
/// target whose baseline isn't settled, and wrong for a drop, which is asking
/// the different question of whether anything still *needs* the target. It is
/// a separate query rather than a parameter on `dependents_of` because the
/// two want different answers and only this one wants names alone — no
/// parse, no `source_columns` fan-out.
async fn source_edge_dependents(
    txn: &Transaction<'_>,
    qualified_target: &str,
    definition_id: i64,
) -> Result<Vec<String>, CatalogError> {
    let rows = txn
        .query(
            "select split_part(t.target_table, '.', 2) \
             from schema_nodes from_node \
             join schema_edges se on se.from_node_id = from_node.id and se.kind = $2 \
             join schema_nodes to_node on to_node.id = se.to_node_id \
             join transform_definitions t on t.target_table = to_node.table_name \
             where from_node.table_name = $1 and t.id <> $3 \
             order by t.id",
            &[
                &qualified_target,
                &EdgeKind::Source.as_str(),
                &definition_id,
            ],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
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
    txn: &Transaction<'_>,
    target: &str,
) -> Result<Vec<(String, String)>, CatalogError> {
    let rows = txn
        .query(
            "select from_table, name from relationship_definitions \
             where to_table = $1 order by id",
            &[&target],
        )
        .await?;
    Ok(rows.into_iter().map(|r| (r.get(0), r.get(1))).collect())
}

/// The bare target names of every registered transform that still reads the
/// relationship `name` declared on `from_table` — every status, not only
/// `live` (issue #231: a frozen reader still needs the relationship the day
/// it resumes, and resuming rebuilds from source).
///
/// Scoped to definitions whose own source *is* `from_table`: a relationship
/// name is only resolvable from a definition over its from-table (that is
/// what makes the name per-from-table unique in the first place), so a
/// same-named relationship on a different from-table is a different
/// relationship and must not be counted as a dependent here.
///
/// `exclude` drops one bare target name from the result — the definition
/// being dropped itself, when [`drop_transform`] asks this about a
/// relationship pointing at its own target. Without it a definition that
/// reads a relationship whose to-side is its own target would block its own
/// drop, which the previous live-only filter hid (a drop's subject is always
/// frozen, so it never matched).
async fn relationship_readers(
    txn: &Transaction<'_>,
    from_table: &str,
    name: &str,
    exclude: Option<&str>,
) -> Result<Vec<String>, CatalogError> {
    let rows = txn
        .query(
            "select definition_text from transform_definitions order by id",
            &[],
        )
        .await?;

    let mut readers = Vec::new();
    for row in rows {
        let text: String = row.get(0);
        let def = super::parse(&text)?;
        if def.source != from_table || exclude == Some(def.target.as_str()) {
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

/// The row-locked identity of the bare-named `target`: its id, its status,
/// and its persisted qualified `"schema.table"`.
///
/// `for update` + `split_part(target_table, '.', 2)` is the same shape
/// [`crate::staging::quarantine::resume_transform`] uses, so a pause, a
/// resume and a drop racing each other on one definition serialize on its row
/// rather than interleaving. The qualified name comes back from *this* read
/// rather than an earlier unlocked one so every fact a drop acts on is read
/// under the lock it holds.
struct LockedDefinition {
    id: i64,
    status: TransformStatus,
    target_table: String,
}

async fn locked_definition(
    txn: &Transaction<'_>,
    target: &str,
) -> Result<Option<LockedDefinition>, CatalogError> {
    let row = txn
        .query_opt(
            "select id, status, target_table from transform_definitions \
             where split_part(target_table, '.', 2) = $1 for update",
            &[&target],
        )
        .await?;
    Ok(row.map(|row| {
        let status_text: String = row.get(1);
        let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
            panic!("transform_definitions.status held unrecognized value '{status_text}'")
        });
        LockedDefinition {
            id: row.get(0),
            status,
            target_table: row.get(2),
        }
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
