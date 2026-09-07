//! Quarantine (issue #16, stage 06's other half): isolate, evict, park,
//! release. See docs/staging-and-claiming/06-cleanup-and-reclaim.md's
//! "Quarantine" section (the design this module implements) and
//! docs/decisions/0003-quarantine-storage-and-api.md (storage shape,
//! settled) plus docs/open-questions.md's "Quarantine policy details" (the
//! knobs that ADR left open — [`DEFAULT_DEATH_THRESHOLD`] is this module's
//! answer for the fuse threshold, documented there rather than left silent).
//!
//! **Failure classification** ([`classify`]) is the entry point [`super::apply::drain_once`]
//! consults on every Phase-3 failure — see
//! docs/staging-and-claiming/05-apply-and-exactly-once-deltas.md's
//! "Failure classification" table, which this module's [`FailureClass`]
//! mirrors one-for-one except for the "ordering artefact" row: see
//! [`classify`]'s doc comment for why that row folds into [`FailureClass::Transient`]
//! here rather than getting its own variant.
//!
//! **Isolation and eviction** ([`isolate_and_evict`]) is what turns "a
//! non-transient failure happened" into "this specific key is the cause" —
//! never the other way around. **Parking** ([`park_batch_contribution`]) is
//! what keeps a poisoned key's *later* healthy changes from vanishing while
//! it's excluded; it is called from [`super::apply::compute`]/
//! [`super::apply::apply_and_mark_drained`], not from this module's own
//! callers, because it must run inside the same Phase-3 transaction as the
//! rest of the batch's apply. **Release** ([`release_key`]) is the
//! operator-driven undo.
//!
//! **The one sanctioned exception to immutability** ([`purge_dropped_table`])
//! lives here too: a staged row naming a table Postgres no longer has can
//! never apply, and unlike every other failure class, retrying or
//! quarantining it individually cannot help — the fix is schema-shaped, not
//! key-shaped.

use std::collections::{HashMap, HashSet};
use std::time::SystemTime;

use tokio_postgres::types::PgLsn;
use tokio_postgres::{GenericClient, Transaction};

use crate::defs::catalog::CatalogError;
use crate::defs::ddl::DdlError;
use crate::pool::Pool;

use super::append::{self, CdcOp, RING_SIZE, StagedChange, ring_table_name};
use super::apply::{self, ApplyError};
use super::fold::FoldedChange;

/// The fuse threshold ADR-0003 left open, decided here: a key evicts once
/// [`record_key_death`] returns a count at or past this many. `0` disables
/// eviction entirely (see [`isolate_and_evict`]) — an operator who has set
/// this to `0` has chosen to let a poisoning key stop the instance via the
/// same instance-wide-stop mechanism a halting schema error uses (doc 06,
/// condition 4: an undrainable batch below the retirement boundary wedges
/// every candidate), rather than have this module evict silently. Not yet
/// wired to any per-instance or per-transform config surface — this crate
/// has none today — so every call site uses this constant directly; see
/// docs/open-questions.md's "Quarantine policy details" for the follow-up.
pub const DEFAULT_DEATH_THRESHOLD: i32 = 5;

// ---------------------------------------------------------------------
// Failure classification
// ---------------------------------------------------------------------

/// Which of doc 05's failure-classification rows an [`ApplyError`] falls
/// into, decided by [`classify`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    /// Retry; charge nothing to any key.
    Transient,
    /// Reload the schema and retry; back off on consecutive misses only.
    VersionFenceMiss,
    /// Propagate loudly; never quarantine. The instance stops.
    Halting,
    /// Everything else: isolate before blaming (see [`isolate_and_evict`]).
    Isolate,
}

/// Classifies `err` per doc 05's failure-classification table.
///
/// **Design decision on "ordering artefact"** (doc 05: "a delta guard
/// tripped while a lower-numbered batch is still outstanding" — self-heals,
/// charge only once every predecessor has drained): no concrete mechanism in
/// this codebase produces that shape today. [`super::apply::apply_target`]'s
/// ordered pre-lock takes every lock this batch needs inside one statement,
/// so it cannot itself observe "a predecessor is still outstanding" as a
/// distinct error — the only failure a lock conflict *can* surface here is a
/// plain serialization failure or deadlock, already [`FailureClass::Transient`].
/// Rather than invent a fifth class with no real trigger, this row folds
/// into [`FailureClass::Transient`]: both share the exact treatment doc 05
/// specifies for ordering artefacts ("charge nothing"), and both are, by
/// construction, retried by [`super::apply::drain_once`]'s reload-recompute
/// loop — which is itself what "once every predecessor has drained" reduces
/// to when there is no separate signal to wait on.
pub fn classify(err: &ApplyError) -> FailureClass {
    match err {
        ApplyError::VersionFenceMiss { .. } => FailureClass::VersionFenceMiss,
        ApplyError::HopBoundExceeded { .. } => FailureClass::Halting,
        // Both of these mean "this definition can never work against this
        // source's real schema" — a structural, schema-shape diagnosis
        // exactly like the hop bound, not a per-row data problem. By the
        // time either reaches here, `compute()` has already ruled out "the
        // table is simply gone" (`drain_once` special-cases
        // `ApplyError::SourceTableDropped` before classification ever runs)
        // — what's left is a real primary key shape `ddl::source_primary_key`
        // cannot use, which every key touching that source reproduces
        // identically alone. Isolating it would charge, and eventually
        // evict, every such key one at a time for a failure none of them
        // individually caused.
        ApplyError::Ddl(DdlError::NoPrimaryKey { .. })
        | ApplyError::Ddl(DdlError::CompositePrimaryKeyUnsupported { .. }) => FailureClass::Halting,
        ApplyError::Catalog(
            CatalogError::BoundSourceRelationMissing { .. }
            | CatalogError::BoundSourceColumnMissing { .. }
            | CatalogError::BoundSourceColumnIncompatible { .. }
            | CatalogError::TargetRelationNotFound { .. }
            | CatalogError::TargetRelationMismatch { .. },
        ) => FailureClass::Halting,
        _ if is_transient(err) => FailureClass::Transient,
        _ => FailureClass::Isolate,
    }
}

/// The transient SQLSTATEs doc 05 names (`40001`/`40P01`, lock-not-available,
/// statement timeout) plus a dropped connection — [`tokio_postgres::Error::code`]
/// is `None` for a connection-level failure (never reached the server to get
/// a SQLSTATE at all), which is exactly the "dropped connection" case doc 05
/// lists alongside the coded ones.
fn is_transient(err: &ApplyError) -> bool {
    let ApplyError::Db(db_err) = err else {
        return false;
    };
    match db_err.code() {
        Some(code) => {
            *code == tokio_postgres::error::SqlState::T_R_SERIALIZATION_FAILURE
                || *code == tokio_postgres::error::SqlState::T_R_DEADLOCK_DETECTED
                || *code == tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE
                || *code == tokio_postgres::error::SqlState::QUERY_CANCELED
        }
        None => true,
    }
}

/// Whether `err` is Postgres's "the table named in this query doesn't
/// exist" (`42P01`) — the live-truth signal [`super::apply::compute`] uses
/// to raise [`ApplyError::SourceTableDropped`] instead of a generic
/// database error, so [`super::apply::drain_once`] can route it to
/// [`purge_dropped_table`] rather than the ordinary isolate/evict path.
pub(super) fn is_undefined_table(err: &tokio_postgres::Error) -> bool {
    err.code() == Some(&tokio_postgres::error::SqlState::UNDEFINED_TABLE)
}

/// Whether `source_table` no longer exists, checked directly via
/// `to_regclass` — the live-truth signal [`super::apply::compute`] uses
/// alongside [`is_undefined_table`]. `pg_catalog.to_regclass` returns `NULL`
/// for a name that resolves to nothing rather than raising `42P01`, which is
/// exactly what makes `ddl::source_primary_key`'s own query come back with
/// zero rows (and thus `DdlError::NoPrimaryKey`) for a dropped table — the
/// same shape a real table that genuinely lacks a primary key produces. This
/// is what tells the two apart before either is trusted.
pub(super) async fn source_table_missing(
    pool: &Pool,
    source_table: &str,
) -> Result<bool, ApplyError> {
    let client = pool.get().await?;
    let row = client
        .query_one(
            "select pg_catalog.to_regclass($1) is null",
            &[&source_table],
        )
        .await?;
    Ok(row.get(0))
}

// ---------------------------------------------------------------------
// The poison marker
// ---------------------------------------------------------------------

/// Which of `candidates` (a folded batch's non-truncate `(src_table, key)`
/// pairs) are already evicted, per the `poison` marker table — the query
/// [`super::apply::compute`] runs before evaluating anything, so a poisoned
/// key never reaches `f()` at all: the fold excludes it **globally**, not
/// just from this one failing batch.
pub(super) async fn poisoned_keys_among(
    pool: &Pool,
    candidates: &[&FoldedChange],
) -> Result<HashSet<(String, Option<u32>, String)>, ApplyError> {
    if candidates.is_empty() {
        return Ok(HashSet::new());
    }
    let client = pool.get().await?;
    let src_tables: Vec<&str> = candidates
        .iter()
        .map(|change| change.src_table.as_str())
        .collect();
    let source_oids: Vec<Option<u32>> = candidates
        .iter()
        .map(|change| change.source_relation_oid)
        .collect();
    let keys: Vec<&str> = candidates
        .iter()
        .map(|change| change.key.as_str())
        .collect();
    let rows = client
        .query(
            "select u.src_table, u.source_relation_oid, u.key from \
             unnest($1::text[], $2::oid[], $3::text[]) as u(src_table, source_relation_oid, key) \
             join poison p on p.key = u.key and ( \
                 p.source_relation_oid = u.source_relation_oid \
                 or (u.source_relation_oid is null and p.source_relation_oid is null \
                     and p.src_table = u.src_table) \
             )",
            &[&src_tables, &source_oids, &keys],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect())
}

/// Parks `changes` — a batch's own folded contribution for keys already in
/// the `poison` marker — into `poison_held`, keyed by `(source_relation_oid,
/// key, seg_seq)` when available and by `(src_table, key, seg_seq)` for
/// legacy rows. Must run **inside the same
/// Phase-3 transaction** as the rest of the batch's apply, before the
/// drained mark — see the module doc comment's "Parked work... is the
/// source of truth" and doc 06's matching section: this is what stops a
/// healthy later change to a poisoned key from vanishing when the batch
/// that excluded it retires.
pub(super) async fn park_batch_contribution(
    txn: &Transaction<'_>,
    seg_seq: i64,
    changes: &[FoldedChange],
) -> Result<(), ApplyError> {
    for change in changes {
        let op = folded_change_op(change);
        txn.execute(
            "insert into poison_held \
                 (src_table, source_relation_oid, key, seg_seq, op, lsn, old_image, new_image, \
                   origin_lsn, src_changed, hop_gen, group_key) \
              values ($1, $2::oid, $3, $4, $5, $6, $7::text::jsonb, $8::text::jsonb, \
                      $9, $10, $11, $12) \
              on conflict do nothing",
            &[
                &change.src_table,
                &change.source_relation_oid,
                &change.key,
                &seg_seq,
                &op,
                &change.lsn,
                &change.old_image,
                &change.new_image,
                &change.origin_lsn,
                &change.src_changed,
                &change.hop_gen,
                &change.group_key,
            ],
        )
        .await?;
    }
    Ok(())
}

/// A [`FoldedChange`]'s shape, rendered to the same three-way-plus-recompute
/// vocabulary `poison_held.op`'s CHECK constraint accepts — derived purely
/// from image presence, matching [`super::apply::compute`]'s own "three
/// shapes" dispatch (its doc comment on the `Some`/`(None, Some)`/`(None,
/// None)` match), so a held row's `op` records exactly the shape
/// [`release_key`] will later need to reconstruct.
fn folded_change_op(change: &FoldedChange) -> &'static str {
    match (&change.old_image, &change.new_image) {
        (Some(_), Some(_)) => "update",
        (None, Some(_)) => "insert",
        (Some(_), None) => "delete",
        (None, None) => "recompute",
    }
}

/// Clears `key_deaths` for every `(src_table, key)` pair in `keys` — called
/// from inside a successful apply's own transaction, per doc 06: "a clean
/// drain clears the counters for the keys it just applied, so a transient
/// death does not accumulate toward a false eviction."
pub(super) async fn clear_key_deaths(
    txn: &Transaction<'_>,
    keys: &[(String, Option<u32>, String)],
) -> Result<(), ApplyError> {
    if keys.is_empty() {
        return Ok(());
    }
    let src_tables: Vec<&str> = keys.iter().map(|(t, _, _)| t.as_str()).collect();
    let source_oids: Vec<Option<u32>> = keys.iter().map(|(_, oid, _)| *oid).collect();
    let ks: Vec<&str> = keys.iter().map(|(_, _, k)| k.as_str()).collect();
    txn.execute(
        "delete from key_deaths \
         using unnest($1::text[], $2::oid[], $3::text[]) as u(src_table, source_relation_oid, key) \
          where key_deaths.key = u.key and ( \
              key_deaths.source_relation_oid = u.source_relation_oid \
              or (u.source_relation_oid is null and key_deaths.source_relation_oid is null \
                  and key_deaths.src_table = u.src_table) \
          )",
        &[&src_tables, &source_oids, &ks],
    )
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------
// Isolate before blaming
// ---------------------------------------------------------------------

/// Increments `key_deaths` for `(src_table, key)` and returns the new
/// count — an upsert, since the counter's row may not exist yet for a key's
/// first attributed failure.
async fn record_key_death(
    client: &impl GenericClient,
    src_table: &str,
    source_relation_oid: Option<u32>,
    key: &str,
    last_error: &str,
) -> Result<i32, ApplyError> {
    let (sql, params): (&str, Vec<&(dyn tokio_postgres::types::ToSql + Sync)>) =
        if source_relation_oid.is_some() {
            (
                "insert into key_deaths (src_table, source_relation_oid, key, deaths, last_error, last_death_at) \
                 values ($1, $2::oid, $3, 1, $4, now()) \
                 on conflict (source_relation_oid, key) where source_relation_oid is not null do update set \
                    deaths = key_deaths.deaths + 1, \
                    last_error = excluded.last_error, \
                    last_death_at = now() \
                 returning deaths",
                vec![&src_table, &source_relation_oid, &key, &last_error],
            )
        } else {
            (
                "insert into key_deaths (src_table, source_relation_oid, key, deaths, last_error, last_death_at) \
                 values ($1, $2::oid, $3, 1, $4, now()) \
                 on conflict (src_table, key) where source_relation_oid is null do update set \
                    deaths = key_deaths.deaths + 1, \
                    last_error = excluded.last_error, \
                    last_death_at = now() \
                 returning deaths",
                vec![&src_table, &source_relation_oid, &key, &last_error],
            )
        };
    let row = client.query_one(sql, &params).await?;
    Ok(row.get(0))
}

/// Marks `(src_table, key)` poisoned (idempotent — a re-eviction after
/// release refreshes the marker rather than erroring) and parks this
/// batch's own folded contribution for it, in one transaction — the
/// eviction act itself. `contribution` is the one [`FoldedChange`] this
/// batch folded for the key (if any); a key can in principle cross the
/// threshold in a batch that folded no record for it at all only via a
/// race with a concurrent isolate elsewhere, which is not a live path here
/// since eviction always runs against the same batch that just isolated the
/// key.
async fn evict_key(
    txn: &Transaction<'_>,
    seg_seq: i64,
    src_table: &str,
    source_relation_oid: Option<u32>,
    key: &str,
    last_error: &str,
    contribution: Option<&FoldedChange>,
) -> Result<(), ApplyError> {
    let (sql, params): (&str, Vec<&(dyn tokio_postgres::types::ToSql + Sync)>) =
        if source_relation_oid.is_some() {
            (
                "insert into poison (src_table, source_relation_oid, key, last_error) \
                 values ($1, $2::oid, $3, $4) \
                 on conflict (source_relation_oid, key) where source_relation_oid is not null do update set \
                    last_error = excluded.last_error, poisoned_at = now()",
                vec![&src_table, &source_relation_oid, &key, &last_error],
            )
        } else {
            (
                "insert into poison (src_table, source_relation_oid, key, last_error) \
                 values ($1, $2::oid, $3, $4) \
                 on conflict (src_table, key) where source_relation_oid is null do update set \
                    last_error = excluded.last_error, poisoned_at = now()",
                vec![&src_table, &source_relation_oid, &key, &last_error],
            )
        };
    txn.execute(sql, &params).await?;
    if let Some(change) = contribution {
        park_batch_contribution(txn, seg_seq, std::slice::from_ref(change)).await?;
    }
    Ok(())
}

/// Runs `folded`'s (non-truncate) records one at a time, each computed and
/// applied *alone* inside a probe transaction that always rolls back
/// (skipping the drained mark), to attribute an isolate-eligible failure to
/// the specific key(s) that fail on their own — doc 06's "Isolate before
/// blaming."
///
/// Returns:
/// - `Ok(None)` if no single key reproduced an isolate-eligible failure (the
///   error is surfaced, not blamed — [`super::apply::drain_once`] returns
///   the *original* failure it already holds, unmodified), or if
///   `threshold == 0` (the fuse is disabled, per [`DEFAULT_DEATH_THRESHOLD`]'s
///   doc comment).
/// - `Ok(Some(retry_folded))` if at least one key crossed the death
///   threshold and was evicted: `retry_folded` is `folded` with every
///   now-evicted key removed, ready for [`super::apply::drain_once`] to
///   recompute and reapply.
/// - `Err(_)` if a probe itself hit a [`FailureClass::Halting`] error: this
///   propagates immediately, unattributed to any key, per doc 06's "What
///   must never be quarantined" — discovered during isolation is no
///   different from discovered on the whole batch.
///
/// **Accepted trade-off, not a bug**: [`super::apply::drain_once`] calls this
/// once per failed attempt, and a key that keeps failing on every retry
/// (not yet evicted — still below `threshold`) is charged again on each
/// call, all before that one `drain_once` cycle either evicts it or gives
/// up. That repeated within-cycle charging *is* the mechanism doc 06
/// describes: a key's death count is exactly "how many real, observed
/// attempts have failed for it", and eviction is meant to trigger partway
/// through a single stubborn batch's retries just as much as across
/// separate batches. Charging at most once per `drain_once` call, instead,
/// would silently slow eviction for a batch that fails on every attempt —
/// worse, not better.
pub async fn isolate_and_evict(
    pool: &Pool,
    seg_seq: i64,
    claimed_by: &str,
    wake_channel: &str,
    folded: &[FoldedChange],
    threshold: i32,
) -> Result<Option<Vec<FoldedChange>>, ApplyError> {
    if threshold == 0 {
        return Ok(None);
    }

    let mut poisoned: Vec<(String, Option<u32>, String, String)> = Vec::new();
    for change in folded {
        if change.is_truncate {
            continue;
        }
        let singleton = std::slice::from_ref(change);
        let plan = match apply::compute(pool, singleton).await {
            Ok(plan) => plan,
            Err(err) => {
                let class = classify(&err);
                if class == FailureClass::Halting {
                    record_halting_stop(pool, &err.to_string()).await?;
                    return Err(err);
                }
                if class == FailureClass::Isolate {
                    poisoned.push((
                        change.src_table.clone(),
                        change.source_relation_oid,
                        change.key.clone(),
                        err.to_string(),
                    ));
                }
                continue;
            }
        };

        let mut client = pool.get().await?;
        let txn = client.transaction().await?;
        let outcome =
            apply::apply_and_mark_drained(&txn, seg_seq, claimed_by, &plan, wake_channel).await;
        let _ = txn.rollback().await;
        if let Err(err) = outcome {
            let class = classify(&err);
            if class == FailureClass::Halting {
                record_halting_stop(pool, &err.to_string()).await?;
                return Err(err);
            }
            if class == FailureClass::Isolate {
                poisoned.push((
                    change.src_table.clone(),
                    change.source_relation_oid,
                    change.key.clone(),
                    err.to_string(),
                ));
            }
        }
    }

    if poisoned.is_empty() {
        return Ok(None);
    }

    let mut evict_now: Vec<(String, Option<u32>, String, String)> = Vec::new();
    {
        let client = pool.get().await?;
        for (src_table, source_relation_oid, key, last_error) in &poisoned {
            let deaths =
                record_key_death(&**client, src_table, *source_relation_oid, key, last_error)
                    .await?;
            if deaths >= threshold {
                evict_now.push((
                    src_table.clone(),
                    *source_relation_oid,
                    key.clone(),
                    last_error.clone(),
                ));
            }
        }
    }

    if evict_now.is_empty() {
        return Ok(None);
    }

    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    for (src_table, source_relation_oid, key, last_error) in &evict_now {
        let contribution = folded.iter().find(|change| {
            !change.is_truncate
                && &change.key == key
                && match source_relation_oid {
                    Some(oid) => change.source_relation_oid == Some(*oid),
                    None => change.source_relation_oid.is_none() && &change.src_table == src_table,
                }
        });
        evict_key(
            &txn,
            seg_seq,
            src_table,
            *source_relation_oid,
            key,
            last_error,
            contribution,
        )
        .await?;
    }
    txn.commit().await?;

    let evicted: HashSet<(&str, Option<u32>, &str)> = evict_now
        .iter()
        .map(|(table, source_relation_oid, key, _)| {
            (table.as_str(), *source_relation_oid, key.as_str())
        })
        .collect();
    let retry_folded: Vec<FoldedChange> = folded
        .iter()
        .filter(|change| {
            change.is_truncate
                || !evicted.iter().any(|(table, source_relation_oid, key)| {
                    key == &change.key.as_str()
                        && match source_relation_oid {
                            Some(oid) => change.source_relation_oid == Some(*oid),
                            None => {
                                change.source_relation_oid.is_none()
                                    && table == &change.src_table.as_str()
                            }
                        }
                })
        })
        .cloned()
        .collect();
    Ok(Some(retry_folded))
}

// ---------------------------------------------------------------------
// Release
// ---------------------------------------------------------------------

/// Operator-driven release, one transaction: replays every `poison_held` row
/// for the current OID resolved from `(src_table, key)` (or legacy rows when
/// no OID is present), in batch order (`seg_seq` ascending) then position
/// order (`held_seq` ascending), into the
/// active batch; then deletes the held rows, the marker, and the death
/// counter. Doc 06: "Each replayed row keeps its **original** origin
/// position" — [`StagedChange::Cdc`]'s `lsn`/`origin_lsn` fields are set
/// from the held row's own columns, never reset, which is what keeps the
/// key's band blocked until the release actually drains rather than
/// silently un-gating the read-your-writes predicate.
///
/// Returns how many held rows were replayed.
pub async fn release_key(pool: &Pool, src_table: &str, key: &str) -> Result<usize, ApplyError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    let source_relation_oid: Option<u32> = txn
        .query_one("select pg_catalog.to_regclass($1)::oid", &[&src_table])
        .await?
        .get(0);

    let held = txn
        .query(
            "select id, seg_seq, source_relation_oid, op, lsn, old_image::text, new_image::text, origin_lsn, \
                     src_changed, hop_gen, group_key \
              from poison_held \
              where key = $2 \
                and (source_relation_oid = $3::oid \
                     or (source_relation_oid is null and src_table = $1)) \
              order by seg_seq asc, held_seq asc",
            &[&src_table, &key, &source_relation_oid],
        )
        .await?;

    let mut held_ids_by_oid: HashMap<Option<u32>, Vec<i64>> = HashMap::new();
    let changes: Vec<StagedChange> = held
        .iter()
        .map(|row| {
            let held_id: i64 = row.get(0);
            let source_relation_oid: Option<u32> = row.get(2);
            held_ids_by_oid
                .entry(source_relation_oid)
                .or_default()
                .push(held_id);
            let op: String = row.get(3);
            let lsn: Option<PgLsn> = row.get(4);
            let old_image: Option<String> = row.get(5);
            let new_image: Option<String> = row.get(6);
            let origin_lsn: Option<PgLsn> = row.get(7);
            let src_changed: Option<SystemTime> = row.get(8);
            let hop_gen: i32 = row.get(9);
            let group_key: Option<String> = row.get(10);
            if op == "recompute" {
                StagedChange::Recompute {
                    src_table: src_table.to_string(),
                    source_relation_oid,
                    key: key.to_string(),
                    hop_gen,
                    group_key,
                }
            } else {
                let cdc_op = match op.as_str() {
                    "insert" => CdcOp::Insert,
                    "delete" => CdcOp::Delete,
                    _ => CdcOp::Update,
                };
                StagedChange::Cdc {
                    src_table: src_table.to_string(),
                    source_relation_oid,
                    key: key.to_string(),
                    op: cdc_op,
                    lsn,
                    old_image,
                    new_image,
                    origin_lsn,
                    src_changed,
                    hop_gen,
                    group_key,
                }
            }
        })
        .collect();

    append::append(&txn, &changes).await?;

    for (source_relation_oid, held_ids) in held_ids_by_oid {
        match source_relation_oid {
            Some(source_relation_oid) => {
                txn.execute(
                    "delete from poison_held \
                     where id = any($1::bigint[]) \
                       and source_relation_oid = $2::oid \
                       and key = $3",
                    &[&held_ids, &source_relation_oid, &key],
                )
                .await?;
            }
            None => {
                txn.execute(
                    "delete from poison_held \
                     where id = any($1::bigint[]) \
                       and source_relation_oid is null \
                       and src_table = $2 and key = $3",
                    &[&held_ids, &src_table, &key],
                )
                .await?;
            }
        }
    }
    txn.execute(
        "delete from poison where key = $2 and ( \
             source_relation_oid = $3::oid \
             or (source_relation_oid is null and src_table = $1) \
         )",
        &[&src_table, &key, &source_relation_oid],
    )
    .await?;
    txn.execute(
        "delete from key_deaths where key = $2 and ( \
             source_relation_oid = $3::oid \
             or (source_relation_oid is null and src_table = $1) \
         )",
        &[&src_table, &key, &source_relation_oid],
    )
    .await?;

    txn.commit().await?;
    Ok(changes.len())
}

// ---------------------------------------------------------------------
// The one sanctioned exception to immutability
// ---------------------------------------------------------------------

/// **The only sanctioned write to a sealed batch's rows** (doc 06). A staged
/// row naming a table Postgres no longer has can never apply — the apply
/// raises [`ApplyError::SourceTableDropped`] before writing anything, so the
/// batch can never drain and, per doc 06's condition 4, wedges the whole
/// ring below it. The escape hatch: delete `src_table`'s rows from every
/// ring table and from the quarantine track (a poisoned/held/dying key
/// naming a table that no longer exists is equally unresolvable by retry or
/// release). Invoked only from [`super::apply::drain_once`], in response to
/// a *live* `42P01` from a query against `src_table` itself — not a cached
/// or stale signal, so there is no separate "reload and check again" step
/// here: the error that triggers this call already reflects current
/// database state. OID-bearing rows are removed by OID so a recreated
/// relation with the same presentation name remains intact; the legacy
/// marker/counter tables remain name-keyed until their own schema migration.
pub async fn purge_dropped_table(
    pool: &Pool,
    src_table: &str,
    source_relation_oid: Option<u32>,
) -> Result<(), ApplyError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    for slot in 0..RING_SIZE {
        let table = ring_table_name(slot)?;
        match source_relation_oid {
            Some(source_relation_oid) => {
                txn.execute(
                    &format!("delete from {table} where source_relation_oid = $1::oid"),
                    &[&source_relation_oid],
                )
                .await?;
            }
            None => {
                txn.execute(
                    &format!(
                        "delete from {table} where source_relation_oid is null and src_table = $1"
                    ),
                    &[&src_table],
                )
                .await?;
            }
        }
    }
    match source_relation_oid {
        Some(source_relation_oid) => {
            txn.execute(
                "delete from poison where source_relation_oid = $1::oid",
                &[&source_relation_oid],
            )
            .await?;
        }
        None => {
            txn.execute(
                "delete from poison where source_relation_oid is null and src_table = $1",
                &[&src_table],
            )
            .await?;
        }
    }
    match source_relation_oid {
        Some(source_relation_oid) => {
            txn.execute(
                "delete from poison_held where source_relation_oid = $1::oid",
                &[&source_relation_oid],
            )
            .await?;
        }
        None => {
            txn.execute(
                "delete from poison_held where source_relation_oid is null and src_table = $1",
                &[&src_table],
            )
            .await?;
        }
    }
    match source_relation_oid {
        Some(source_relation_oid) => {
            txn.execute(
                "delete from key_deaths where source_relation_oid = $1::oid",
                &[&source_relation_oid],
            )
            .await?;
        }
        None => {
            txn.execute(
                "delete from key_deaths where source_relation_oid is null and src_table = $1",
                &[&src_table],
            )
            .await?;
        }
    }
    txn.commit().await?;
    Ok(())
}

// ---------------------------------------------------------------------
// The halting-stop metric
// ---------------------------------------------------------------------

/// Increments `halting_stops`' single row and records `reason` — the "real
/// metric (counter + last reason)" doc 05's failure-classification table
/// calls for on the halting class, so "stopped" and "slow" are
/// distinguishable from the outside. A plain counter table, matching this
/// crate's existing convention for small operational state (`pause_leases`,
/// `drainers`) rather than a Prometheus-style dependency this crate has
/// none of today.
pub async fn record_halting_stop(pool: &Pool, reason: &str) -> Result<(), ApplyError> {
    let client = pool.get().await?;
    client
        .execute(
            "update halting_stops \
             set stop_count = stop_count + 1, last_reason = $1, last_stopped_at = now() \
             where id",
            &[&reason],
        )
        .await?;
    Ok(())
}

/// [`record_halting_stop`]'s counterpart read: the current stop count and
/// last reason, for an operator dashboard or health check.
#[derive(Debug, Clone)]
pub struct HaltingStopStats {
    pub stop_count: i64,
    pub last_reason: Option<String>,
    pub last_stopped_at: Option<SystemTime>,
}

pub async fn halting_stop_stats(pool: &Pool) -> Result<HaltingStopStats, ApplyError> {
    let client = pool.get().await?;
    let row = client
        .query_one(
            "select stop_count, last_reason, last_stopped_at from halting_stops where id",
            &[],
        )
        .await?;
    Ok(HaltingStopStats {
        stop_count: row.get(0),
        last_reason: row.get(1),
        last_stopped_at: row.get(2),
    })
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn classify_maps_hop_bound_to_halting() {
        let err = ApplyError::HopBoundExceeded {
            hop_gen: 40,
            tables: vec!["t".to_string()],
        };
        assert_eq!(classify(&err), FailureClass::Halting);
    }

    #[test]
    fn classify_maps_version_fence_miss() {
        let err = ApplyError::VersionFenceMiss {
            src_table: "orders".to_string(),
        };
        assert_eq!(classify(&err), FailureClass::VersionFenceMiss);
    }

    #[test]
    fn classify_maps_claim_lost_to_isolate() {
        // ClaimLost is not transient, not a fence miss, not halting — it
        // falls to the catch-all `Isolate` bucket, matching doc 05's
        // "everything else" row. (In practice `drain_once` never reaches
        // isolation for `ClaimLost` today since it isn't attributable to a
        // key, but the classifier itself has no special case for it — see
        // `isolate_and_evict`'s "no single key reproduces" fallback for how
        // an unattributable isolate-classified error still surfaces rather
        // than getting blamed on something.)
        assert_eq!(classify(&ApplyError::ClaimLost), FailureClass::Isolate);
    }
}
