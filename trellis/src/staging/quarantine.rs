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

use std::collections::{HashMap, HashSet, VecDeque};
#[cfg(any(test, feature = "internals"))]
use std::time::SystemTime;

#[cfg(any(test, feature = "internals"))]
use tokio_postgres::types::PgLsn;
use tokio_postgres::types::ToSql;
use tokio_postgres::{GenericClient, Transaction};

use crate::defs::ast::{KeySpace, ValueType};
use crate::defs::catalog;
use crate::defs::ddl::{self, DdlError, PrimaryKeyColumn};
use crate::defs::eval::{self, RelationshipContext, Row};
use crate::defs::model::{Definition, TransformStatus};
use crate::defs::validate;
use crate::pool::{Pool, quote_ident};

#[cfg(any(test, feature = "internals"))]
use super::append::{self, CdcOp, StagedChange};
use super::append::{RING_SIZE, ring_table_name};
use super::apply::{self, ApplyError};
use super::fold::FoldedChange;
use super::target_mutations::TargetMutations;
use super::watermark::StagedWatermark;

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

/// The column-fuse's threshold (`docs/decisions/0003-quarantine-storage-and-api.md`'s
/// amendment, "Undecided" -> now decided): a sibling of
/// [`DEFAULT_DEATH_THRESHOLD`], same value, named separately because the two
/// fuses count different things and are meant to stay independently
/// tunable if either is ever wired to real config — [`DEFAULT_DEATH_THRESHOLD`]
/// counts *repeated attempts against one key*; this counts *distinct
/// poisoned rows for one `(transform, column)` pair* (see
/// `column_failures`' migration comment). Fixed count, not a percentage —
/// same reasoning [`DEFAULT_DEATH_THRESHOLD`] documents, not yet configurable
/// per transform/column.
pub const DEFAULT_COLUMN_DEATH_THRESHOLD: i32 = DEFAULT_DEATH_THRESHOLD;

/// The whole-transform fuse's threshold (`docs/decisions/0003-quarantine-storage-and-api.md`'s
/// "The original transform-wide fuse still exists as a coarser, separate
/// tier"): another sibling of [`DEFAULT_DEATH_THRESHOLD`] and
/// [`DEFAULT_COLUMN_DEATH_THRESHOLD`], same value, named separately for the
/// same independent-tunability reason those two document. This one counts
/// *distinct evicted keys for one `src_table`* — see
/// [`trip_transform_fuse_if_crossed`] — rather than repeated attempts
/// against one key or distinct poisoned rows for one `(transform, column)`
/// pair.
pub const DEFAULT_TRANSFORM_DEATH_THRESHOLD: i32 = DEFAULT_DEATH_THRESHOLD;

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
        // — what's left is a real primary key shape (no primary key at all)
        // or type `ddl::source_primary_key` cannot use (issue #107), which
        // every key touching that source reproduces identically alone.
        // Isolating it would charge, and eventually evict, every such key
        // one at a time for a failure none of them individually caused. A
        // composite (multi-column) primary key used to be a third such
        // structural rejection here too, before issue #121 taught every 1-1
        // consumer to key on the source's full primary key rather than
        // narrowing it to one column.
        ApplyError::Ddl(DdlError::NoPrimaryKey { .. })
        | ApplyError::Ddl(DdlError::UnsupportedPrimaryKeyType { .. }) => FailureClass::Halting,
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
///
/// **`candidates` must already carry the canonical `src_table` identity**
/// (issue #283) — `poison` is keyed on it, so a raw bare spelling matches
/// nothing here and the key it names is re-evaluated (and re-poisoned) despite
/// already being evicted. [`super::apply::compute`], the only caller, resolves
/// each distinct source table through [`CanonicalSrcTables`] before building
/// this list, and compares the returned set against the same canonical pairs.
pub(super) async fn poisoned_keys_among(
    pool: &Pool,
    candidates: &[(&str, &str)],
) -> Result<HashSet<(String, String)>, ApplyError> {
    if candidates.is_empty() {
        return Ok(HashSet::new());
    }
    let client = pool.get().await?;
    let src_tables: Vec<&str> = candidates.iter().map(|(t, _)| *t).collect();
    let keys: Vec<&str> = candidates.iter().map(|(_, k)| *k).collect();
    let rows = client
        .query(
            "select p.src_table, p.key from poison p \
             join unnest($1::text[], $2::text[]) as u(src_table, key) \
               on p.src_table = u.src_table and p.key = u.key",
            &[&src_tables, &keys],
        )
        .await?;
    Ok(rows.into_iter().map(|r| (r.get(0), r.get(1))).collect())
}

/// Parks `changes` — a batch's own folded contribution for keys already in
/// the `poison` marker — into `poison_held`, keyed `(src_table, key,
/// seg_seq)` and idempotent on that triple. Must run **inside the same
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
        // Issue #315: a recompute's prior-image hint rides in `old_image`,
        // exactly as it does in the ring, so `release_key` replays it.
        let old_image = if op == "recompute" {
            &change.prior_image
        } else {
            &change.old_image
        };
        txn.execute(
            "insert into poison_held \
                 (src_table, key, seg_seq, op, lsn, old_image, new_image, \
                  origin_lsn, src_changed, hop_gen, group_key) \
             values ($1, $2, $3, $4, $5, $6::text::jsonb, $7::text::jsonb, \
                      $8, $9, $10, $11) \
             on conflict (src_table, key, seg_seq) do nothing",
            &[
                &change.src_table,
                &change.key,
                &seg_seq,
                &op,
                &change.lsn,
                old_image,
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
///
/// **`keys` must already carry the canonical `src_table` identity** (issue
/// #283), for the same reason [`poisoned_keys_among`]'s candidates must:
/// [`record_key_death`] writes under it, so clearing by a raw bare spelling
/// would delete nothing and leave a transient death accumulating toward a false
/// eviction forever. `ApplyPlan::applied_keys` — this function's only source of
/// `keys` — is built canonically in [`super::apply::compute`] for that reason.
pub(super) async fn clear_key_deaths(
    txn: &Transaction<'_>,
    keys: &[(String, String)],
) -> Result<(), ApplyError> {
    if keys.is_empty() {
        return Ok(());
    }
    let src_tables: Vec<&str> = keys.iter().map(|(t, _)| t.as_str()).collect();
    let ks: Vec<&str> = keys.iter().map(|(_, k)| k.as_str()).collect();
    txn.execute(
        "delete from key_deaths \
         using unnest($1::text[], $2::text[]) as u(src_table, key) \
         where key_deaths.src_table = u.src_table and key_deaths.key = u.key",
        &[&src_tables, &ks],
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
///
/// `src_table` is the canonical identity, not the ring spelling (issue #283):
/// two spellings of one logical source used to maintain two independent
/// row-level counters for the same physical row, so the row-level fuse took up
/// to twice as many real failures to fire.
async fn record_key_death(
    client: &impl GenericClient,
    src_table: &str,
    key: &str,
    last_error: &str,
) -> Result<i32, ApplyError> {
    let row = client
        .query_one(
            "insert into key_deaths (src_table, key, deaths, last_error, last_death_at) \
             values ($1, $2, 1, $3, now()) \
             on conflict (src_table, key) do update set \
                 deaths = key_deaths.deaths + 1, \
                 last_error = excluded.last_error, \
                 last_death_at = now() \
             returning deaths",
            &[&src_table, &key, &last_error],
        )
        .await?;
    Ok(row.get(0))
}

/// One key [`isolate_and_evict`]'s probe loop reproduced an
/// [`FailureClass::Isolate`] failure for, carrying **both** spellings of its
/// source table (issue #283) because the two halves of that function need
/// different ones: quarantine's own counter/marker tables are keyed on
/// `canonical_src_table`, while matching the key back to the ring row it came
/// from (its parked contribution, and the `retry_folded` exclusion) has to use
/// `raw_src_table`, the spelling `folded` actually holds.
#[derive(Clone)]
struct PoisonedProbe {
    /// The ring row's own `src_table`, verbatim.
    raw_src_table: String,
    /// [`qualified_src_table`] of the above — quarantine's canonical key.
    canonical_src_table: String,
    key: String,
    last_error: String,
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
    key: &str,
    last_error: &str,
    contribution: Option<&FoldedChange>,
) -> Result<(), ApplyError> {
    tracing::warn!(
        src_table = %src_table,
        key = %key,
        last_error = %last_error,
        "evicting a key to the poison table; it crossed the row-level death threshold"
    );
    txn.execute(
        "insert into poison (src_table, key, last_error) \
         values ($1, $2, $3) \
         on conflict (src_table, key) do update set \
             last_error = excluded.last_error, poisoned_at = now()",
        &[&src_table, &key, &last_error],
    )
    .await?;
    if let Some(change) = contribution {
        // Issue #283: `poison_held` is keyed on the same canonical identity as
        // the `poison` row just written, so the parked contribution is restaged
        // under `src_table` rather than the ring row's own spelling. Without
        // this, a bare-staged ring row's held work would be invisible to
        // `release_key`'s canonical delete — orphaned parked work that
        // `converge` gates on forever. Replaying it later under the qualified
        // name is also what issue #267 made the ring invariant anyway.
        let mut parked = change.clone();
        parked.src_table = src_table.to_string();
        park_batch_contribution(txn, seg_seq, std::slice::from_ref(&parked)).await?;
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

    // Issue #283: every counter/marker write below lands under the *canonical*
    // (qualified, where resolvable) identity of the ring row's `src_table`,
    // never the raw spelling — resolved once per distinct source table here and
    // threaded through `attribute_column_failure`/`record_key_death`/
    // `evict_key`/`trip_transform_fuse_if_crossed` alike. The raw spelling is
    // still what this function *matches ring rows on* (`contribution` below,
    // and the `retry_folded` filter at the end): those compare against
    // `folded`'s own strings, which are the ring's, not quarantine's.
    let mut canonical_srcs = CanonicalSrcTables::default();
    let mut poisoned: Vec<PoisonedProbe> = Vec::new();
    for change in folded {
        // Issue #134/#135 review follow-up: a `rel_reverse_deferred` row
        // must never be probed/poisoned/parked here, for the same reason
        // `park_batch_contribution`/`poisoned_park` already exclude it at
        // the `compute()` level (that module's own comment) — `poison_held`
        // has no columns for `relationship_id`/`retry_count` and no
        // `rel_reverse_deferred` `op` value in its own CHECK constraint
        // (`V13__quarantine.sql`, deliberately not widened when V28 added
        // the new ring op — see that migration's own doc comment), so
        // parking one would silently derive a *wrong* `op` from image shape
        // alone (`folded_change_op`), drop `relationship_id`/`retry_count`
        // entirely, and — worse — `release_key` would later re-append it as
        // a bogus `StagedChange::Cdc` against this op's synthetic sentinel
        // `src_table` (`apply::relationship_reverse_deferred_src_table`),
        // which is not a real table at all. Skipping it here is the loud,
        // safe failure mode doc 06 asks for: if nothing else in this batch
        // reproduces the error in isolation, `poisoned` stays empty and the
        // caller (`classify_and_retry`'s `Isolate` arm) surfaces the
        // original failure rather than silently corrupting quarantine
        // state. A complete fix — genuinely quarantine-safe deferred
        // reverses, with their own `poison_held` columns/op mirroring this
        // issue's V28 migration — is real and larger than this follow-up;
        // tracked separately rather than attempted here.
        if change.is_truncate || change.relationship_reverse_deferred.is_some() {
            continue;
        }
        let singleton = std::slice::from_ref(change);
        let plan = match apply::compute(pool, singleton).await {
            Ok(plan) => plan,
            Err(err) => {
                let class = classify(&err);
                if class == FailureClass::Halting {
                    tracing::error!(
                        src_table = %change.src_table,
                        key = %change.key,
                        error = %err,
                        "halting failure diagnosing a probed key's compute; propagating, \
                         never quarantined"
                    );
                    record_halting_stop(pool, &err.to_string()).await?;
                    return Err(err);
                }
                if class == FailureClass::Isolate {
                    // ADR-0003's amendment, layered alongside (not instead
                    // of) the row-level charge just below: a probe failure
                    // that's specifically an evaluator error names the
                    // calculated field it broke on
                    // ([`crate::defs::eval::EvalError::field`]), which this
                    // attributes to a `(transform, column)` pair and charges
                    // toward that pair's own, independent fuse. See
                    // [`attribute_column_failure`]'s doc comment for why
                    // this never changes what gets returned from *this*
                    // function — the existing row-level fuse below is
                    // completely unmodified by this call. Also see that same
                    // doc comment's "Ambiguous match -> no attribution,
                    // deliberately": if the failing field name matches more
                    // than one sibling transform on this source, this call
                    // intentionally attributes nothing rather than guess,
                    // and the row-level fuse below is exactly what still
                    // protects against the failure going otherwise unhandled.
                    let canonical = canonical_srcs.get(pool, &change.src_table).await?;
                    attribute_column_failure(pool, &canonical, &change.key, &err).await?;
                    poisoned.push(PoisonedProbe {
                        raw_src_table: change.src_table.clone(),
                        canonical_src_table: canonical,
                        key: change.key.clone(),
                        last_error: err.to_string(),
                    });
                }
                continue;
            }
        };

        let mut client = pool.get().await?;
        let txn = client.transaction().await?;
        // This probe applies-then-rolls-back purely to classify a poisoned
        // key's failure in isolation — it never commits, so a guard (a)
        // rejection here would only ever muddy the diagnosis of an
        // unrelated failure, never protect real state. `saturated()`
        // (issue #132) makes guard (a) a no-op for this probe, matching how
        // every other guard here is unaffected too: a guard rejection is an
        // `Ok` outcome (the fallback-Recompute path), never the
        // `ApplyError` this probe is specifically trying to reproduce.
        let outcome = apply::apply_and_mark_drained(
            &txn,
            seg_seq,
            claimed_by,
            &plan,
            wake_channel,
            &StagedWatermark::saturated(),
        )
        .await;
        let _ = txn.rollback().await;
        if let Err(err) = outcome {
            let class = classify(&err);
            if class == FailureClass::Halting {
                tracing::error!(
                    src_table = %change.src_table,
                    key = %change.key,
                    error = %err,
                    "halting failure diagnosing a probed key's apply; propagating, never \
                     quarantined"
                );
                record_halting_stop(pool, &err.to_string()).await?;
                return Err(err);
            }
            if class == FailureClass::Isolate {
                let canonical = canonical_srcs.get(pool, &change.src_table).await?;
                attribute_column_failure(pool, &canonical, &change.key, &err).await?;
                poisoned.push(PoisonedProbe {
                    raw_src_table: change.src_table.clone(),
                    canonical_src_table: canonical,
                    key: change.key.clone(),
                    last_error: err.to_string(),
                });
            }
        }
    }

    if poisoned.is_empty() {
        return Ok(None);
    }

    let mut evict_now: Vec<PoisonedProbe> = Vec::new();
    {
        let client = pool.get().await?;
        for probe in &poisoned {
            let deaths = record_key_death(
                &**client,
                &probe.canonical_src_table,
                &probe.key,
                &probe.last_error,
            )
            .await?;
            if deaths >= threshold {
                evict_now.push(probe.clone());
            }
        }
    }

    if evict_now.is_empty() {
        return Ok(None);
    }

    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    for probe in &evict_now {
        let contribution = folded
            .iter()
            .find(|c| !c.is_truncate && c.src_table == probe.raw_src_table && c.key == probe.key);
        evict_key(
            &txn,
            seg_seq,
            &probe.canonical_src_table,
            &probe.key,
            &probe.last_error,
            contribution,
        )
        .await?;
    }
    // Sorted (and deduped) — the dedup is what this is for, but the sort now
    // also fixes the order in which this transaction takes the per-source
    // fuse gates (issue #159, `take_fuse_gate`), so two concurrent evictions
    // spanning the same two sources cannot grab them in opposite orders. No
    // new cycle against the `poison` row locks either: the `evict_key` loop
    // above has already taken every one of them before the first gate is
    // touched, so a transaction waiting on a gate is never itself holding one
    // while a gate-holder waits on it.
    //
    // Deduped on the *canonical* identity (issue #283), which is also what
    // makes the gate and both counts below single-keyed for a source staged
    // under two spellings: two raw spellings of one logical table are one
    // entry here, taking one gate and counting one combined budget, instead of
    // two independent half-budgets neither of which ever reached the threshold.
    let mut evicted_src_tables: Vec<&str> = evict_now
        .iter()
        .map(|p| p.canonical_src_table.as_str())
        .collect();
    evicted_src_tables.sort_unstable();
    evicted_src_tables.dedup();
    for src_table in evicted_src_tables {
        trip_transform_fuse_if_crossed(&txn, pool, src_table).await?;
    }
    txn.commit().await?;

    // Raw, not canonical: this set is matched against `folded`'s own ring
    // spellings just below.
    let evicted: HashSet<(&str, &str)> = evict_now
        .iter()
        .map(|p| (p.raw_src_table.as_str(), p.key.as_str()))
        .collect();
    let retry_folded: Vec<FoldedChange> = folded
        .iter()
        .filter(|c| c.is_truncate || !evicted.contains(&(c.src_table.as_str(), c.key.as_str())))
        .cloned()
        .collect();
    Ok(Some(retry_folded))
}

/// Resolves a ring row's raw `src_table` to the fully-qualified identity
/// [`catalog::transforms_for_source`]/[`catalog::dependents_of`] require
/// (issue #74, ADR-0007: `schema_nodes` keys on qualified identity, so a bare
/// lookup there silently finds *nothing* rather than erroring) — this
/// module's counterpart to `super::apply`'s own `qualified_schema_node_key`,
/// which every `apply.rs` call site already goes through.
///
/// Issue #281: both of this module's `transforms_for_source` call sites
/// ([`trip_transform_fuse_if_crossed`] and [`attribute_column_failure`]) used
/// to pass the raw `src_table` straight through, so for any bare spelling the
/// whole-transform fuse crossed its threshold and quarantined nothing, and
/// column failures went unattributed — both entirely silently, since an
/// unqualified argument is an empty result set, not an error. Issue #267
/// stopped `apply.rs` *emitting* bare `src_table` going forward, but durable
/// pre-#267 ring rows and the crate's own integration fixtures that stage
/// bare `src_table` by hand still reach here.
///
/// **Unresolvable names fall through to the raw spelling rather than
/// erroring**, which is the one deliberate difference from
/// `apply::qualified_schema_node_key` (that one propagates
/// [`catalog::CatalogError::SourceTableNotFound`] to its caller; this one
/// instead mirrors `catalog::resolve_relationship_endpoint`, which tolerates
/// exactly that variant and hands back the name unchanged). Every
/// caller here is *diagnosing* an already-failed batch: turning a name this
/// module cannot resolve into a brand-new `ApplyError` would replace the
/// original failure being quarantined with a confusing secondary one, and
/// could abort the eviction transaction that is the whole point of the call.
/// Two real shapes hit that branch — `apply::relationship_reverse_deferred_src_table`'s
/// U+001F-prefixed synthetic sentinel (neither bare nor qualified, and not a
/// physical table at all; [`isolate_and_evict`] already skips those rows
/// before either call site, so this is belt-and-braces), and a durable ring
/// row naming a source table that has since been dropped. Both previously
/// reached `transforms_for_source` and found nothing; both still do, which is
/// exactly the pre-existing behavior for the cases where nothing *can* be
/// found.
///
/// **Issue #283: this is also the canonical key every quarantine counter and
/// marker table is now written and read under** — `poison`, `poison_held`,
/// `key_deaths`, `column_failures` and `transform_fuse_gate`. Before that, all
/// five stored whatever spelling the ring row happened to carry, so one logical
/// source staged both bare and qualified accumulated two independent sets of
/// quarantine state that never combined: two half-threshold fuse budgets that
/// never tripped, two row-level death counters for one physical row, a fold
/// exclusion that missed a key poisoned under the other spelling, and a
/// per-spelling (so non-serializing) `take_fuse_gate` lock row. Every write and
/// every read now goes through this one resolution, and
/// `V33__quarantine_canonical_src_table.sql` folded the pre-existing bare rows
/// into their qualified counterpart. The unresolvable spellings above are the
/// deliberate exception: they keep their raw key, which is self-consistent
/// (nothing else can resolve them either) and exactly the pre-#283 behaviour.
pub(super) async fn qualified_src_table(
    pool: &Pool,
    src_table: &str,
) -> Result<String, ApplyError> {
    if src_table.contains('.') {
        return Ok(src_table.to_string());
    }
    match catalog::resolve_graph_identity(pool, src_table).await {
        Ok(qualified) => Ok(qualified),
        Err(catalog::CatalogError::SourceTableNotFound(_)) => Ok(src_table.to_string()),
        Err(err) => Err(err.into()),
    }
}

/// A memo over [`qualified_src_table`] for the two callers that resolve a whole
/// folded batch's worth of `src_table`s at once ([`isolate_and_evict`] and
/// [`super::apply::compute`], issue #283): a batch routinely carries many
/// changes per source table, and the resolution is a pure function of the
/// catalog for the duration of one batch.
///
/// Cheap by construction in the common case: an already-qualified spelling —
/// which, since issue #267, is every `src_table` `apply.rs` emits — short
/// circuits inside [`qualified_src_table`] without touching the database at
/// all, so this only ever spends a round trip on the durable bare rows and
/// hand-staged fixtures that issue #281's doc comment enumerates, once each.
#[derive(Default)]
pub(super) struct CanonicalSrcTables {
    cache: HashMap<String, String>,
}

impl CanonicalSrcTables {
    /// The canonical (qualified, where resolvable) identity for `src_table`,
    /// resolving and memoizing it on first sight.
    pub(super) async fn get(&mut self, pool: &Pool, src_table: &str) -> Result<String, ApplyError> {
        if let Some(canonical) = self.cache.get(src_table) {
            return Ok(canonical.clone());
        }
        let canonical = qualified_src_table(pool, src_table).await?;
        self.cache.insert(src_table.to_string(), canonical.clone());
        Ok(canonical)
    }

    /// The already-resolved canonical identity for `src_table`, or `None` if
    /// [`Self::get`] was never called for it — the borrow-free lookup
    /// [`super::apply::compute`] uses once it has pre-resolved every source
    /// table in a batch, so its per-change hot loop takes no `&mut self` and no
    /// `.await`.
    pub(super) fn canonical(&self, src_table: &str) -> Option<&str> {
        self.cache.get(src_table).map(String::as_str)
    }
}

// ---------------------------------------------------------------------
// Whole-transform fuse (ADR-0003's original, coarser tier — issue #105)
// ---------------------------------------------------------------------
//
// ADR-0003: "if a failure isn't attributable to one column (e.g. a
// key-shape/DDL failure that dooms every column's write for that row
// alike), it trips the whole transform to quarantined exactly as before."
// Every key [`isolate_and_evict`] evicts is exactly that: a failure that
// survived a probe run *alone* and still wasn't attributable to a single
// calculated field (whether or not [`attribute_column_failure`] separately
// also charged a column fuse alongside it — the two tiers are independent,
// see that function's own doc comment). So the `poison` marker table this
// module already maintains *is* the distinct-evicted-key count this fuse
// needs — no new counter table, unlike [`key_deaths`]/`column_deaths`'s
// incrementally-maintained counters: `poison` is already keyed one row per
// `(src_table, key)`, so `count(*) where src_table = $1` is already exactly
// "how many distinct keys for this source are currently evicted," with no
// write-amplification tradeoff to make. Issue #160 narrowed *which* of those
// rows a given definition is charged for (only those evicted since its last
// resume re-armed the fuse — see `trip_transform_fuse_if_crossed`), but the
// table is still the whole counter.
//
// What `poison` cannot supply on its own is *serialization* between two
// concurrent evictions for the same source (issue #159): each counts inside
// its own transaction and cannot see the other's uncommitted insert, so two
// workers landing the 4th and 5th eviction at once both counted 4 and
// neither tripped. That is what `transform_fuse_gate`/[`take_fuse_gate`]
// adds — one per-`src_table` row lock, in exactly the shape
// [`record_key_death`] already uses for `key_deaths`, taken before any
// counting happens. It still introduces no duplicated count anywhere; see
// `V30__transform_fuse_gate.sql` for why the counts themselves stayed on
// `poison`.

/// Takes `src_table`'s whole-transform-fuse gate on `txn` and holds it until
/// that transaction ends: the serialization point issue #159 was missing.
/// Every eviction transaction that is about to ask "has this source crossed
/// [`DEFAULT_TRANSFORM_DEATH_THRESHOLD`]?" passes through this one row first,
/// so two of them for the same source can never both answer from a snapshot
/// taken before the other's `poison` insert landed.
///
/// An `INSERT ... ON CONFLICT DO UPDATE`, not a `SELECT ... FOR UPDATE`, for
/// the same reason [`record_key_death`]/`charge_column_failure` use that
/// shape for `key_deaths`/`column_deaths`: the gate row may not exist yet
/// (first-ever eviction for this source), and `FOR UPDATE` over zero rows
/// locks nothing at all — two concurrent first evictions would each sail
/// straight through. `ON CONFLICT` covers both halves: Postgres's speculative
/// insertion makes the losing *inserter* wait on the winner's transaction,
/// and `DO UPDATE` (not `DO NOTHING`, which takes no row lock once the row
/// exists) makes every later caller wait on the row lock.
///
/// Deliberately no `RETURNING` and no maintained count: the threshold
/// decision stays a `count(*)` over `poison`, which is the only table that
/// can also answer #160's windowed, per-definition form of the same
/// question. `V30__transform_fuse_gate.sql` has the full rationale.
async fn take_fuse_gate(txn: &Transaction<'_>, src_table: &str) -> Result<(), ApplyError> {
    txn.execute(
        "insert into transform_fuse_gate (src_table, checks, last_checked_at) \
         values ($1, 1, now()) \
         on conflict (src_table) do update set \
             checks = transform_fuse_gate.checks + 1, \
             last_checked_at = now()",
        &[&src_table],
    )
    .await?;
    Ok(())
}

/// Checks whether `src_table`'s whole-transform fuse has crossed
/// [`DEFAULT_TRANSFORM_DEATH_THRESHOLD`] and, if so, quarantines every
/// transform definition [`crate::defs::catalog::transforms_for_source`]
/// resolves for it (a source table can back more than one transform — each
/// one independently pays for its own source's poisoned-key breadth, so
/// every one of them trips together rather than picking just one).
///
/// Must run **inside the same transaction** [`isolate_and_evict`] just used
/// to insert this call's own triggering eviction(s) into `poison` — the
/// `count(*)` below is read against `txn` itself (not a fresh pool
/// connection) specifically so it observes those just-inserted, not-yet-committed
/// rows; a separate connection would undercount until commit and could miss
/// the exact eviction that crosses the threshold.
///
/// **Concurrent evictions for one source are serialized first** (issue
/// #159), by [`take_fuse_gate`], before either count below runs. Reading
/// `poison` from `txn` is what makes this call see *its own* new rows, but it
/// is also exactly what made it blind to a *sibling* transaction's
/// concurrent, not-yet-committed ones: two workers each poisoning a key for
/// the same source (say the source's 4th and 5th) each counted 4, neither
/// crossed the threshold-of-5, and the transform stayed live past its fuse
/// point until some later, unrelated eviction happened to re-run the check.
/// The gate's per-`src_table` row lock forces those two into an order; the
/// second one through does not begin counting until the first has committed,
/// and — READ COMMITTED, one fresh snapshot per statement — both counts below
/// then include the sibling's rows. This never changes *whether* a genuine
/// threshold crossing trips, only how promptly: the fuse could always
/// undercount, never overcount, so no previously-correct non-trip becomes a
/// false trip.
///
/// **The count is windowed by the resuming operator's re-arm point** (issue
/// #160): only `poison` rows whose `poisoned_at` is *after* the definition's
/// `transform_definitions.fuse_rearmed_at` count toward the threshold. Before
/// this, [`resume_transform`] left `poison` untouched, so a `src_table` that
/// had ever reached five evicted keys kept that count forever and the next
/// single new eviction — for any key, for any reason — re-tripped the fuse
/// immediately; the intended "five fresh failures re-trips" degraded into
/// "one failure re-trips, forever." See `V29__transform_fuse_rearm.sql` for
/// why re-arming (this option) beats deleting the `src_table`'s `poison` rows
/// on resume: those rows are the fold's global exclusion *marker* and may own
/// parked `poison_held` work, and they are shared by every transform on the
/// same source (only one of which is being resumed). `fuse_rearmed_at` is
/// `null` for a never-resumed definition, read here as `-infinity` — i.e.
/// identical to the pre-#160 unwindowed count.
///
/// The count is therefore per *definition* rather than one shared count for
/// `src_table`: sibling transforms on the same source are resumed
/// independently, so each one carries its own budget. That is a strictly
/// finer-grained version of the previous behaviour (with no resume anywhere,
/// every sibling sees the same number the single old query returned).
///
/// The two mechanisms compose without either knowing about the other: the
/// gate decides *when* a transaction is allowed to count, the re-arm window
/// decides *which* `poison` rows that count includes. Both windowed and
/// unwindowed counts still read `poison` directly, so a per-definition window
/// needs no gate of its own — once the gate has been passed, every committed
/// sibling row is visible to both, whatever their `poisoned_at`.
///
/// `src_table` is routed through [`qualified_src_table`] before the catalog
/// lookup (issue #281): a bare spelling handed straight to
/// [`catalog::transforms_for_source`] silently resolves to *no* definitions,
/// so the fuse would cross its threshold and then quarantine nothing at all.
///
/// The old unwindowed `count(*)` survives as a cheap guard in front of the
/// loop: a window can only ever *remove* `poison` rows from the count, so a
/// source below threshold in total is below it for every definition, and the
/// per-definition work (plus the `catalog` lookup's second pool connection,
/// taken while `txn` is open) is skipped entirely — the same fast path every
/// eviction below threshold took before #160.
///
/// Idempotent: a definition already [`TransformStatus::Quarantined`] is left
/// alone (no redundant write, no repeated log line) on every later eviction
/// that keeps `src_table` above threshold.
///
/// `pub` rather than module-private only so the issue-#159 regression test
/// (`tests/quarantine.rs`) can drive two genuinely overlapping eviction
/// transactions through it directly: the race lives in the window between one
/// transaction's `poison` insert and its commit, which two full
/// `drain_once`/`isolate_and_evict` pipelines cannot be made to interleave
/// deterministically from the outside.
pub async fn trip_transform_fuse_if_crossed(
    txn: &Transaction<'_>,
    pool: &Pool,
    src_table: &str,
) -> Result<(), ApplyError> {
    // Issue #283: resolve the canonical identity *first*, before the gate and
    // both counts — all three key on it, and so does every write
    // `isolate_and_evict` made on the way here. Deliberately ahead of the
    // pre-threshold fast path below, which used to be the reason this
    // resolution sat further down (it takes a pool connection while `txn` is
    // open — see `pool.rs`'s note on that hazard): the fast path's own count is
    // one of the queries that has to be canonically keyed, so it can't run
    // first. The cost is bounded to the case that needs it — an
    // already-qualified `src_table`, which since issue #267 is every spelling
    // `apply.rs` emits, short-circuits inside `qualified_src_table` with no
    // database access at all, and the gate this eviction is about to take is
    // not yet held when the connection is acquired.
    let src_table = &qualified_src_table(pool, src_table).await?;

    // Issue #159's serialization point, before either count below. Must come
    // first: a count taken ahead of the gate could be stale by the time the
    // gate is granted, which is the whole bug. Keyed canonically (issue #283),
    // so two workers evicting one logical source under two different spellings
    // queue on one gate row instead of taking two independent locks and
    // serializing against nothing.
    take_fuse_gate(txn, src_table).await?;

    // Unwindowed guard, kept from the pre-#160 shape: every definition's
    // windowed count is a *subset* of this one (the window only ever removes
    // `poison` rows, never adds them), so a source table below threshold in
    // total cannot have any single definition at or above it, and the whole
    // per-definition loop below — including the `catalog` call's second pool
    // connection, acquired while this eviction's own `txn` is still open —
    // can be skipped outright. Every eviction pays this one indexed
    // `count(*)`; only a genuinely threshold-deep source pays for the rest.
    let poisoned_total: i64 = txn
        .query_one(
            "select count(*) from poison where src_table = $1",
            &[&src_table],
        )
        .await?
        .get(0);
    if poisoned_total < DEFAULT_TRANSFORM_DEATH_THRESHOLD as i64 {
        return Ok(());
    }

    // Already canonical (resolved at the top of this function, issue #283) —
    // which is exactly the form `transforms_for_source` requires (issue #281).
    let definitions = catalog::transforms_for_source(pool, src_table).await?;
    // Defense in depth for the issue-#281 class: "threshold crossed, nothing
    // to quarantine" is never a normal outcome — `poison` rows only exist for
    // a source some definition was evaluating — so say so out loud rather than
    // returning silently, which is exactly what made the original bug
    // invisible. Not a `debug_assert!`: a source table legitimately dropped
    // (or every definition on it dropped) between the eviction and this lookup
    // reaches here too, and that is not a programming error.
    if definitions.is_empty() {
        tracing::warn!(
            src_table = %src_table,
            poisoned_total,
            "whole-transform fuse threshold crossed but no definitions resolved for this \
             source; nothing quarantined"
        );
    }
    for def in definitions {
        if def.status == TransformStatus::Quarantined {
            continue;
        }
        // `fuse_rearmed_at` is read inside `txn` (from the definition's own
        // committed row, by id) rather than carried on `Definition`: it is
        // pure fuse bookkeeping with no other reader, so widening the
        // catalog's public model — and every construction site of it — for
        // one call site isn't worth it.
        let poisoned_count: i64 = txn
            .query_one(
                "select count(*) from poison \
                 where src_table = $1 \
                   and poisoned_at > coalesce( \
                         (select fuse_rearmed_at from transform_definitions where id = $2), \
                         '-infinity'::timestamptz)",
                &[&src_table, &def.id],
            )
            .await?
            .get(0);
        if poisoned_count < DEFAULT_TRANSFORM_DEATH_THRESHOLD as i64 {
            continue;
        }
        tracing::warn!(
            transform = %def.def.target,
            src_table = %src_table,
            poisoned_count,
            "whole-transform fuse tripped; quarantining"
        );
        txn.execute(
            "update transform_definitions set status = $1 where id = $2",
            &[&TransformStatus::Quarantined.as_str(), &def.id],
        )
        .await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Column-level fuse (docs/decisions/0003-quarantine-storage-and-api.md's
// 2026-09-12 amendment)
// ---------------------------------------------------------------------
//
// Everything below is a second, independent fuse tier, finer-grained than
// the row-level one above: it trips per `(transform, column)` instead of
// per key, so one broken calculated-field formula doesn't force every other
// healthy column on the same transform into quarantine. It never changes
// [`isolate_and_evict`]'s own return value or the row-level fuse's
// behavior — [`attribute_column_failure`] is pure side-effecting bookkeeping
// called *alongside* the existing per-key charge, and a definition with
// nothing currently paused pays only one extra small indexed lookup per
// batch (`paused_columns_for`, called from [`super::apply::compute`]).
//
// **Scope, stated up front**: this tier only ever activates for
// [`crate::defs::ast::KeySpace::OneToOne`] definitions (plain or
// relationship-enriched). [`crate::defs::ast::KeySpace::Aggregate`] fields
// are never attributed here and can never be paused by the automatic fuse —
// `staging::apply_aggregate`'s incremental delta model (recently the subject
// of its own delicate bug fixes) has no notion of "skip this one column and
// keep accumulating the others," and inventing one is a materially bigger
// project than this amendment. An aggregate transform's overall lifecycle
// status (the existing whole-transform fuse) is completely unaffected by
// this scope cut.

/// Attributes `err` to a `(transform, column)` pair and charges it toward
/// that pair's fuse, if `err` is specifically
/// [`crate::defs::eval::EvalError`] (a calculated-field failure — the only
/// kind [`crate::defs::eval::EvalError::field`] can name) coming from a
/// [`crate::defs::ast::KeySpace::OneToOne`] definition. Anything else (a DDL/
/// key-shape failure, a plain database error) is left entirely to the
/// existing row-level fuse — this function is a no-op for it.
///
/// Attribution is by field-name match against every definition
/// [`crate::defs::catalog::transforms_for_source`] returns for `src_table`
/// — [`super::apply::compute`] evaluates one definition's fields inside one
/// `eval::evaluate*` call and doesn't itself thread transform identity
/// through [`crate::defs::eval::EvalError`] (which would mean widening that
/// error type's public shape, and every existing construction site/test of
/// it, just for this one caller).
///
/// **Ambiguous match -> no attribution, deliberately.** If more than one
/// sibling `KeySpace::OneToOne` definition on `src_table` declares a field
/// named [`crate::defs::eval::EvalError::field`], there is no reliable way
/// to tell here which one actually produced `err` (only one may even be the
/// one that's broken). Guessing — e.g. "lowest id wins" — would be a
/// *deterministic misattribution*: every failure would land on the same
/// (possibly perfectly healthy) column every time, potentially freezing it
/// while the actually-broken sibling never accumulates a `column_status`
/// entry at all. That's worse than doing nothing, so this falls through to
/// the existing whole-row/transform-wide fuse (the row-level charge
/// [`isolate_and_evict`] already runs alongside this call, unaffected by
/// this function's return value either way) instead of attempting a fancier
/// disambiguation heuristic.
async fn attribute_column_failure(
    pool: &Pool,
    src_table: &str,
    key: &str,
    err: &ApplyError,
) -> Result<(), ApplyError> {
    let ApplyError::Eval(eval_err) = err else {
        return Ok(());
    };
    let field = eval_err.field();

    // Issue #281: qualify first — a bare `src_table` handed straight to
    // `transforms_for_source` returns an empty candidate set, so every column
    // failure on it fell through unattributed. See [`qualified_src_table`].
    let qualified = qualified_src_table(pool, src_table).await?;
    let candidates = catalog::transforms_for_source(pool, &qualified).await?;
    let mut matches = candidates.into_iter().filter(|def| {
        matches!(def.def.key_space, KeySpace::OneToOne)
            && def.def.fields.iter().any(|f| f.name == field)
    });
    let Some(def) = matches.next() else {
        return Ok(());
    };
    // See this function's doc comment ("Ambiguous match -> no attribution,
    // deliberately"): a second sibling definition matching the same field
    // name means attribution would be a guess, not a fact, so fall back to
    // the pre-existing row-level fuse rather than risk freezing the wrong
    // column.
    if matches.next().is_some() {
        return Ok(());
    }
    let transform = def.def.target;

    // Issue #283: `column_failures` is keyed on the canonical identity, not the
    // raw ring spelling — its primary key `(transform_table, column_name,
    // src_table, key)` is what makes `column_deaths` count *distinct* failing
    // rows, and a split key let one stubborn row charge that counter twice
    // (the one place the dual spelling over-counted rather than under-counted).
    charge_column_failure(pool, &transform, field, &qualified, key, &err.to_string()).await
}

/// Every column [`super::apply::compute`] must currently exclude from
/// evaluation/writes for `transform_table` — the read
/// [`super::apply::compute`] does once per definition per batch to implement
/// "freeze at the last successfully computed value" (decision: never null a
/// paused column out, never keep reattempting its already-fused formula).
/// Empty (the overwhelmingly common case) for a transform with nothing
/// paused.
///
/// `defs::backfill`'s durable chunk-queue write path
/// (`write_one_to_one_range`/`backfill_relationship_one_to_one`) needs this
/// same exclusion for the same reason (a (re-)executed backfill chunk must
/// not overwrite a column live CDC has since paused) but runs its own copy
/// of the identical query (`defs::backfill::paused_columns_for`) rather than
/// calling this function — `defs` sits below `staging` in this crate's
/// layering (`staging::apply` already depends on `defs::backfill`, so the
/// reverse dependency would be circular) and that call site already holds a
/// plain `&Client` rather than a `&Pool`.
pub(super) async fn paused_columns_for(
    pool: &Pool,
    transform_table: &str,
) -> Result<HashSet<String>, ApplyError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select column_name from column_status where transform_table = $1",
            &[&transform_table],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

/// Records one distinct poisoned row's failure for `(transform, column)` and
/// charges its counter — but only if `(src_table, key)` hasn't already been
/// recorded for this exact pair (`column_failures`' primary key makes the
/// insert idempotent). This is what makes the column fuse count *distinct
/// poisoned rows*, not raw retry attempts: a single stubborn key retried
/// across several `drain_once` attempts (the row-level fuse's own bread and
/// butter — see `quarantine.rs`'s existing
/// `repeated_real_failures_cross_the_eviction_threshold_and_the_batch_still_drains`
/// test) charges this counter exactly once, no matter how many times it's
/// probed, so it can never cross this fuse's threshold alone — only a real
/// *breadth* of distinct failing rows can. Trips the fuse
/// ([`trip_column_fuse`]) once the count reaches
/// [`DEFAULT_COLUMN_DEATH_THRESHOLD`].
async fn charge_column_failure(
    pool: &Pool,
    transform: &str,
    column: &str,
    src_table: &str,
    key: &str,
    error: &str,
) -> Result<(), ApplyError> {
    let client = pool.get().await?;
    let inserted = client
        .execute(
            "insert into column_failures (transform_table, column_name, src_table, key, error) \
             values ($1, $2, $3, $4, $5) \
             on conflict (transform_table, column_name, src_table, key) do nothing",
            &[&transform, &column, &src_table, &key, &error],
        )
        .await?;
    if inserted == 0 {
        return Ok(());
    }

    let row = client
        .query_one(
            "insert into column_deaths (transform_table, column_name, deaths, last_error, last_death_at) \
             values ($1, $2, 1, $3, now()) \
             on conflict (transform_table, column_name) do update set \
                 deaths = column_deaths.deaths + 1, \
                 last_error = excluded.last_error, \
                 last_death_at = now() \
             returning deaths",
            &[&transform, &column, &error],
        )
        .await?;
    let deaths: i32 = row.get(0);
    if deaths >= DEFAULT_COLUMN_DEATH_THRESHOLD {
        trip_column_fuse(pool, transform, column, error).await?;
    }
    Ok(())
}

/// Trips the column fuse for `(transform, column)`: pauses it
/// (`column_status`, `local_fuse = true` — this pair's *own* fuse tripped,
/// as opposed to a pause it only inherited via [`cascade_pause`]), resets
/// its counter, and cascades the pause to every dependent reader
/// (decision #5).
///
/// Deliberately does **not** clear `column_failures` here (unlike the
/// row-level fuse's `key_deaths`, which the counter reset above otherwise
/// mirrors): those rows are [`Trellis::sample_quarantined`]'s data source
/// for a `transform.column` target, and clearing them at the exact moment
/// the fuse trips would erase the evidence right when a caller most wants to
/// see it (diagnosing *why* it paused). Nothing needs them cleared to stay
/// correct either — once paused, [`super::apply::compute`] excludes this
/// column from evaluation entirely, so no *new* failure can be attributed to
/// it while it stays paused. [`resume_column`] is what actually clears
/// `column_failures`, once the column is live again and eligible to start
/// accumulating a fresh set.
async fn trip_column_fuse(
    pool: &Pool,
    transform: &str,
    column: &str,
    last_error: &str,
) -> Result<(), ApplyError> {
    tracing::warn!(
        transform = %transform,
        column = %column,
        last_error = %last_error,
        "column fuse tripped; pausing it and cascading the pause to its dependents"
    );
    let client = pool.get().await?;
    client
        .execute(
            "insert into column_status (transform_table, column_name, paused_at, last_error, local_fuse) \
             values ($1, $2, now(), $3, true) \
             on conflict (transform_table, column_name) do update set \
                 local_fuse = true, last_error = excluded.last_error",
            &[&transform, &column, &last_error],
        )
        .await?;
    client
        .execute(
            "delete from column_deaths where transform_table = $1 and column_name = $2",
            &[&transform, &column],
        )
        .await?;

    cascade_pause(pool, transform, column).await
}

/// Pauses every direct and transitive dependent of `(transform, column)`
/// (decision #5: a transform reading a paused column's output must also
/// pause, rather than silently consume a frozen/stale value with no
/// signal) — a breadth-first walk over
/// [`crate::defs::catalog::column_dependents`], recorded into
/// `column_pause_cascades` so [`resume_column`] can later tell a purely
/// cascaded pause apart from one with its own independent (`local_fuse`)
/// reason to stay paused. Iterative, not recursive: `docs/transforms.md`'s
/// "Chaining and cycle detection" guarantees the underlying dependency graph
/// is acyclic, so a queue-based walk always terminates, without needing
/// `async fn` self-recursion's `Box::pin` boilerplate.
///
/// A dependent already paused (by an earlier cascade, or its own
/// `local_fuse`) still gets this new cascade edge recorded (so un-cascading
/// `transform`/`column` later can't wrongly resume it out from under a
/// *different* still-live reason), but its own further dependents are not
/// re-walked — they were already reached when this dependent first paused.
async fn cascade_pause(pool: &Pool, transform: &str, column: &str) -> Result<(), ApplyError> {
    let mut queue: VecDeque<(String, String)> = VecDeque::new();
    queue.push_back((transform.to_string(), column.to_string()));

    while let Some((upstream_transform, upstream_column)) = queue.pop_front() {
        let deps = catalog::column_dependents(pool, &upstream_transform, &upstream_column).await?;
        for (downstream_transform, downstream_column) in deps {
            let client = pool.get().await?;
            client
                .execute(
                    "insert into column_pause_cascades \
                         (downstream_transform, downstream_column, upstream_transform, upstream_column) \
                     values ($1, $2, $3, $4) \
                     on conflict do nothing",
                    &[
                        &downstream_transform,
                        &downstream_column,
                        &upstream_transform,
                        &upstream_column,
                    ],
                )
                .await?;

            let newly_paused = client
                .execute(
                    "insert into column_status (transform_table, column_name, paused_at, last_error, local_fuse) \
                     values ($1, $2, now(), $3, false) \
                     on conflict (transform_table, column_name) do nothing",
                    &[
                        &downstream_transform,
                        &downstream_column,
                        &format!(
                            "paused because upstream column '{upstream_transform}.{upstream_column}' \
                             is paused"
                        ),
                    ],
                )
                .await?;
            if newly_paused > 0 {
                tracing::warn!(
                    transform = %downstream_transform,
                    column = %downstream_column,
                    upstream_transform = %upstream_transform,
                    upstream_column = %upstream_column,
                    "column paused via cascade from an upstream pause"
                );
                queue.push_back((downstream_transform, downstream_column));
            }
        }
    }
    Ok(())
}

/// Pauses one calculated column deliberately — the operator-driven trigger
/// for the same column-pause state [`trip_column_fuse`] reaches automatically,
/// exposed through the grammar as `PAUSE TRANSFORM <target>.<column>` (issue
/// #227; issue #228, decision 2).
///
/// ADR-0014's pause is "one state, two triggers", and that is as true at
/// column granularity as at whole-transform granularity: this writes the same
/// `column_status` row the fuse writes and runs the same [`cascade_pause`]
/// over dependent readers, so a paused column freezes at its current value, is
/// skipped by [`super::apply::compute`]'s evaluation, and is recovered by the
/// same [`resume_column`] (which re-derives it across every existing row).
/// Nothing here is a second freezing mechanism.
///
/// **`local_fuse` is set even though no fuse tripped.** That column records
/// "this pair has a reason of its own to stay paused", as opposed to a pause
/// merely inherited via [`cascade_pause`] — which is exactly true of an
/// operator pause, and is what stops [`resume_column`] on some *upstream*
/// column from un-pausing this one out from under the operator who paused it.
/// The trade-off: `column_status.last_error` is left null, so a reader
/// ([`crate::Trellis::quarantine_status`]) sees this the same way it sees a
/// cascaded pause — paused, with no error of its own. That matches the
/// whole-transform pause, which likewise records the frozen state without
/// recording who asked for it.
///
/// Idempotent, per ADR-0014: pausing an already-paused column (by an earlier
/// pause, by its own fuse, or purely by cascade) succeeds, upgrading a
/// cascade-only pause to one with its own reason and re-walking the cascade
/// (itself idempotent). Pause runs on Trellis's own connections rather than
/// inside a caller's migration transaction, so a replayed migration has to be
/// safe to re-run.
///
/// Whether `transform` and `column` actually exist is checked by the caller
/// ([`crate::Trellis::apply`]), which has the catalog reads and the
/// [`crate::TrellisError`] variants for it — `column_status` has no foreign
/// key onto either (see `V22__column_status_drops_target_table_fkey.sql`), so
/// this function would otherwise happily park a row naming nothing.
#[tracing::instrument(
    name = "quarantine.pause_column",
    skip(pool),
    fields(transform = %transform, column = %column)
)]
pub async fn pause_column(pool: &Pool, transform: &str, column: &str) -> Result<(), ApplyError> {
    {
        let client = pool.get().await?;
        client
            .execute(
                "insert into column_status (transform_table, column_name, paused_at, local_fuse) \
                 values ($1, $2, now(), true) \
                 on conflict (transform_table, column_name) do update set local_fuse = true",
                &[&transform, &column],
            )
            .await?;
    }
    tracing::info!(
        transform = %transform,
        column = %column,
        "column paused by request; cascading the pause to its dependents"
    );
    cascade_pause(pool, transform, column).await
}

/// Resumes a paused column: clears its counter, recomputes its value across
/// every existing row ([`recompute_column`]), clears its `column_status` row
/// and outgoing cascade edges, then un-cascades every dependent this pause
/// reached — but only a dependent with *no other* remaining reason to stay
/// paused (no `local_fuse` of its own, and no other live
/// `column_pause_cascades` edge into it — decision: careful not to un-pause
/// a dependent that has its own independent reason to stay paused). Returns
/// every `(transform, column)` pair actually resumed, `transform`/`column`
/// itself first, in the order resumed.
///
/// Each pair's unpause also parks a `pending_backfill` catch-up marker for
/// its definition's source table, in the same transaction (issue #305): the
/// recompute reads the source once, so a row changed after that read — its
/// delta applied while the column was still paused — would otherwise keep a
/// stale value forever. The marker's discharge re-derives every row with the
/// column unpaused, the same closed loop
/// `defs::catalog::complete_direct_backfill` ends a first `define`'s chunked
/// build with.
///
/// Errors with [`ApplyError::ColumnNotPaused`] if `(transform, column)`
/// isn't currently paused — resuming a live column is caller error, not a
/// silent no-op.
///
/// Errors with [`ApplyError::DefinitionNotLive`] — with no side effects at
/// all, checked before any of this function's deletes run — if `transform`
/// isn't currently applying ([`TransformStatus::is_applying`]: `live`, or
/// `catching_up` once its build has finished; most concretely, still
/// `Backfilling` behind an in-flight `backfill_chunks` queue). See that
/// variant's doc comment for why: [`recompute_column`] takes one snapshot of
/// the source table, and a row a still-running backfill chunk inserts into
/// the target during that window would never be revisited once
/// `column_status` is cleared, permanently stranding it. The same check
/// applies per-pair inside the cascade queue below; a downstream pair
/// blocked on its own definition not being live yet is simply left paused
/// (not resumed, not an abort of the whole call) rather than risking the
/// same bug one hop down — by the time a downstream pair is reached, any
/// upstream pairs earlier in the queue have already been fully resumed and
/// committed, so there is nothing left to roll back.
#[tracing::instrument(
    name = "quarantine.resume_column",
    skip(pool),
    fields(transform = %transform, column = %column, resumed = tracing::field::Empty)
)]
pub async fn resume_column(
    pool: &Pool,
    transform: &str,
    column: &str,
) -> Result<Vec<(String, String)>, ApplyError> {
    {
        let client = pool.get().await?;
        let exists = client
            .query_opt(
                "select 1 from column_status where transform_table = $1 and column_name = $2",
                &[&transform, &column],
            )
            .await?
            .is_some();
        if !exists {
            return Err(ApplyError::ColumnNotPaused {
                transform: transform.to_string(),
                column: column.to_string(),
            });
        }

        // Gate before any mutation: resuming is all-or-nothing, so a
        // blocked resume must leave `column_deaths`/`column_failures`/
        // `column_status` untouched, not partially cleaned up. A missing
        // definition (a dangling `column_status` row with nothing left in
        // the catalog) isn't this function's problem to police — fall
        // through and let the loop below's own lookup handle it the way it
        // already does.
        if let Some(def) = catalog::definition_by_target(pool, transform).await?
            && !def.status.is_applying()
        {
            return Err(ApplyError::DefinitionNotLive {
                transform: transform.to_string(),
            });
        }

        client
            .execute(
                "delete from column_deaths where transform_table = $1 and column_name = $2",
                &[&transform, &column],
            )
            .await?;
        client
            .execute(
                "delete from column_failures where transform_table = $1 and column_name = $2",
                &[&transform, &column],
            )
            .await?;
    }

    let mut resumed = Vec::new();
    let mut queue: VecDeque<(String, String)> = VecDeque::new();
    queue.push_back((transform.to_string(), column.to_string()));

    while let Some((t, c)) = queue.pop_front() {
        let Some(def) = catalog::definition_by_target(pool, &t).await? else {
            continue;
        };
        if !def.status.is_applying() {
            // Reached via cascade (the initial pair was already gated above
            // before any side effects ran): a downstream dependent this
            // pause cascaded onto (`column_dependents`, unlike the
            // `status = 'live'`-filtered lookups CDC apply uses, does not
            // require the dependent to be live) can still be mid-backfill.
            // Aborting the whole call here would misrepresent what already
            // happened, since earlier pairs in this queue may already be
            // fully resumed and committed — instead this pair alone is left
            // exactly as it was, still paused, to be resumed on a later
            // call once its own definition reaches live.
            continue;
        }
        recompute_column(pool, &def, &c).await?;

        // Unpause and park a catch-up marker in one transaction (issue
        // #305). `recompute_column` read the source under one snapshot, and
        // until `column_status` is cleared live CDC apply skips `c`: a row
        // changed after that read had its delta applied to every *other*
        // column, leaving `c` at the recompute's pre-change value, and
        // nothing would ever revisit it once unpaused. The marker is the same
        // closed loop a first `define`'s chunked build ends with
        // (`defs::catalog::complete_direct_backfill`): once its fence
        // settles, `run_pending_backfills` re-enumerates the source and
        // re-derives every row with `c` unpaused. It must become visible
        // atomically with the unpause — a marker discharged while `c` is
        // still paused would re-open exactly this gap.
        let mut client = pool.get().await?;
        let txn = client.transaction().await?;
        txn.execute(
            "delete from column_status where transform_table = $1 and column_name = $2",
            &[&t, &c],
        )
        .await?;
        // Issue #476: until that discharge, the definition reports
        // `catching_up` (it keeps applying).
        crate::intake::publication::park_catch_up(
            &*txn,
            &[def.id],
            std::slice::from_ref(&def.source_table),
        )
        .await?;
        let affected = txn
            .query(
                "delete from column_pause_cascades \
                 where upstream_transform = $1 and upstream_column = $2 \
                 returning downstream_transform, downstream_column",
                &[&t, &c],
            )
            .await?;
        for row in affected {
            let downstream_transform: String = row.get(0);
            let downstream_column: String = row.get(1);
            let status = txn
                .query_opt(
                    "select local_fuse from column_status \
                     where transform_table = $1 and column_name = $2",
                    &[&downstream_transform, &downstream_column],
                )
                .await?;
            let Some(status) = status else {
                // Already resumed by some other path (shouldn't happen
                // within one resume walk, but tolerate it rather than panic).
                continue;
            };
            let local_fuse: bool = status.get(0);
            if local_fuse {
                continue;
            }
            let remaining: i64 = txn
                .query_one(
                    "select count(*) from column_pause_cascades \
                     where downstream_transform = $1 and downstream_column = $2",
                    &[&downstream_transform, &downstream_column],
                )
                .await?
                .get(0);
            if remaining == 0 {
                queue.push_back((downstream_transform, downstream_column));
            }
        }
        txn.commit().await?;
        resumed.push((t.clone(), c.clone()));
    }

    tracing::Span::current().record("resumed", resumed.len());
    tracing::info!(resumed = ?resumed, "resumed paused column(s)");
    Ok(resumed)
}

/// Resumes a whole-transform-frozen definition — ADR-0003's coarser,
/// transform-wide fuse tier, distinct from [`resume_column`]'s per-column
/// tier (which additionally requires the owning definition to already be
/// `live`; a frozen definition is, by construction, never that).
///
/// **One resume for both of ADR-0014's pause triggers** (issue #142).
/// `target`'s current status must be either [`TransformStatus::Quarantined`]
/// (the poison fuse tripped it) or [`TransformStatus::Paused`] (an operator
/// froze it deliberately via `PAUSE TRANSFORM` — see [`crate::Trellis::apply`]) —
/// [`ApplyError::TransformNotPaused`] otherwise, checked before any
/// mutation, because resuming a transform that isn't frozen at all is caller
/// error, not a silent no-op, matching [`resume_column`]'s
/// [`ApplyError::ColumnNotPaused`] discipline. The recovery is identical for
/// both triggers and deliberately so (ADR-0014's "Resume reconciles with
/// source, not by catch-up"): a frozen definition's share of the change
/// stream is drained for its siblings while it's frozen and is not
/// recoverable by replay.
///
/// This function itself only schedules that reconciliation. It drops the
/// definition to [`TransformStatus::WaitingToBackfill`] and re-parks a fresh
/// `pending_backfill` marker for its source table (reusing
/// [`crate::intake::publication::park_backfill_catchup`], the mechanism a
/// chunked build's own post-completion catch-up uses, see
/// `defs::catalog::complete_direct_backfill`). The target is left exactly as
/// the freeze left it until that marker's discharge
/// ([`crate::intake::publication::run_pending_backfills`]) runs, which in one
/// transaction deletes every target row no current source row backs (issue
/// #330, `intake::resume_orphans`) and dispatches the rebuild by shape
/// (ADR-0016): chunks or a direct-build job that drain threads run, or, for
/// a shape the direct build can't render, an enumeration of every current
/// source row for the drain to re-derive. The discharge respects the marker's `xmin`
/// fence, never re-deriving the target without waiting out a concurrent
/// transaction that might still be pinning it (issue #55;
/// docs/observability.md's "Backfill status and the `xmin` caveat" applies
/// here exactly as it does to a fresh transform's own initial backfill:
/// resuming can sit in `waiting_to_backfill` for as long as some unrelated
/// transaction pins the cluster's `xmin`, and that is correct, not a fault).
///
/// Clears any stale `backfill_coverage` record for the source table first
/// (issue #79, bug B's multi-reader contract): a quarantined definition's
/// target may be broken or only partially written, so a coverage record
/// that would otherwise let the discharge skip enumeration cannot be
/// trusted here — full re-enumeration is the safe default.
///
/// Deletes the definition's unclaimed, undone `backfill_chunks` rows too
/// (issue #332): the fresh backfill makes them redundant, and a pause only
/// withheld them from dispatch. Chunks a worker still holds are left for that
/// worker; however it gives one up, it is discarded rather than handed out
/// again or used to complete the definition (issues #360/#397,
/// `defs::chunk_queue::finish_chunk`/`release_chunk`/`reclaim_stale_chunks`).
///
/// **The trip half of this contract** lives in
/// [`trip_transform_fuse_if_crossed`], called from [`isolate_and_evict`]
/// once a batch of evictions lands: when `src_table`'s distinct-evicted-key
/// count (the `poison` marker table) crosses
/// [`DEFAULT_TRANSFORM_DEATH_THRESHOLD`], every transform definition
/// subscribed to that source is quarantined (issue #105 — this used to have
/// no writer at all; `resume_transform` predates it and was written first so
/// the trip mechanism would have somewhere correct to land).
///
/// **Re-arms that fuse** (issue #160) by stamping
/// `transform_definitions.fuse_rearmed_at`, so the resumed transform gets a
/// fresh [`DEFAULT_TRANSFORM_DEATH_THRESHOLD`] budget of *new* evictions
/// rather than re-tripping on the very next one (the `poison` rows that
/// tripped it are still there — see
/// [`trip_transform_fuse_if_crossed`]'s doc comment and
/// `V29__transform_fuse_rearm.sql` for why they're windowed out rather than
/// deleted). This deliberately does **not** touch `key_deaths`: that's the
/// independent per-key fuse tier, cleared by a clean drain of the key itself
/// (`clear_key_deaths`), and a resume makes no claim about any individual
/// key's health — only about the transform's.
#[tracing::instrument(name = "quarantine.resume_transform", skip(pool), fields(transform = %target))]
pub async fn resume_transform(pool: &Pool, target: &str) -> Result<(), ApplyError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    // `target` is the bare transform name (matching `resume_column`'s own
    // `transform` parameter convention), but `transform_definitions.target_table`
    // is persisted fully-qualified (issue #73) — match on its bare suffix,
    // the same `split_part(target_table, '.', 2)` pattern
    // `TargetTableSuffixCollision`'s own check already uses, rather than
    // requiring every caller to know and pass the qualified identity.
    let row = txn
        .query_opt(
            "select id, source_table, status from transform_definitions \
             where split_part(target_table, '.', 2) = $1 for update",
            &[&target],
        )
        .await?;
    let Some(row) = row else {
        return Err(ApplyError::TransformNotFound {
            transform: target.to_string(),
        });
    };
    let id: i64 = row.get(0);
    let source_table: String = row.get(1);
    let status_text: String = row.get(2);
    let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
        panic!("transform_definitions.status held unrecognized value '{status_text}'")
    });
    // [`TransformStatus::is_frozen`] rather than a local `matches!`: ADR-0014's
    // frozen state is defined once, so a status folded into it later reaches
    // every precondition and dispatch gate that asks (issue #231).
    if !status.is_frozen() {
        return Err(ApplyError::TransformNotPaused {
            transform: target.to_string(),
        });
    }

    // `fuse_rearmed_at = now()` in the same statement, not a separate one:
    // re-arming the whole-transform fuse is part of the same atomic
    // "this transform starts over" transition as the status drop (issue
    // #160). `now()` is the transaction timestamp, and every later eviction
    // stamps `poison.poisoned_at` from its own, strictly later transaction,
    // so `trip_transform_fuse_if_crossed`'s strict `>` comparison gives this
    // transform a full, fresh `DEFAULT_TRANSFORM_DEATH_THRESHOLD` budget of
    // post-resume evictions. Keys still sitting in `poison` from before the
    // resume are deliberately left there — they remain globally excluded
    // from folding (and their parked `poison_held` work remains releasable
    // via `release_key`), they just no longer count toward this transform's
    // fuse.
    txn.execute(
        "update transform_definitions set status = $1, fuse_rearmed_at = now() where id = $2",
        &[&TransformStatus::WaitingToBackfill.as_str(), &id],
    )
    .await?;

    // Discard the definition's leftover unclaimed backfill chunks (issue
    // #332). The pause only withheld them from `claim_chunks`; unfrozen, they
    // would be handed out again and re-run work the fresh backfill parked
    // below already does. In the same transaction as the status drop, so no
    // claim can see the definition dispatchable with them still present, and
    // `claim_chunks`' `for update skip locked` never waits on these rows.
    //
    // A chunk a worker still holds is left alone, as a pause leaves it: that
    // worker may still be writing its range. The `fuse_rearmed_at` stamp
    // above marks it stale (issues #360/#397): however that worker gives it
    // up (finishing, failing or dying), the chunk queue deletes it rather
    // than completing the definition or freeing it for a rerun, and parks
    // the catch-up marker repairing the target should its write land after
    // the rebuild has already gone live (#331).
    txn.execute(
        "delete from backfill_chunks \
         where definition_id = $1 and not done and claimed_by is null",
        &[&id],
    )
    .await?;

    // `transform_definitions.source_table` is already the fully-qualified
    // `"schema.table"` form (issue #72) — re-resolving it via
    // `resolve_source_schema_in_txn` (bare names only) or re-`qualify`-ing it
    // would reject it outright (`DottedIdentifierComponent`).
    crate::intake::publication::clear_backfill_coverage(&*txn, &source_table).await?;
    crate::intake::publication::park_backfill_catchup(&*txn, &source_table).await?;
    // Issue #310: a transform paused by a lost replication slot stops being
    // reported as such the moment it is resumed.
    txn.execute(
        "delete from slot_loss_pauses where transform_id = $1",
        &[&id],
    )
    .await?;

    txn.commit().await?;
    tracing::info!(
        transform = %target,
        from = %status.as_str(),
        to = %TransformStatus::WaitingToBackfill.as_str(),
        "transform resumed; re-parked for a fresh backfill"
    );
    Ok(())
}

/// Re-derives `column`'s value across every current row of `def.def.source`
/// and writes it into `def.def.target`, freshly evaluated against live
/// source data — [`resume_column`]'s "re-run the backfill for just this
/// column's formula against already-built rows" (ADR-0003's amendment).
///
/// **Judgment call, flagged rather than silently made**: this deliberately
/// does *not* reuse the chunked/durable `backfill_chunks` queue
/// (ADR-0007's amendment) or `defs::oracle::render_expr_sql`'s pure-SQL
/// rendering. A resume is a rare, operator-driven action (not a hot path,
/// and not something correctness elsewhere depends on completing
/// quickly), so a single straightforward pass — read every row, evaluate
/// this one definition's fields in Rust (reusing the exact evaluator the
/// live apply path already trusts, relationships included), write back only
/// `column` — is a much smaller, lower-risk surface than either
/// alternative: the chunk queue is built for *initial* backfill's
/// crash-resumability at billion-row scale, over-built for a single-column
/// recompute; and `render_expr_sql`'s bare-identifier rendering is only
/// safe in a plain `select ... from source` (its one real caller,
/// `defs::backfill::write_one_to_one_range`) — reusing it inside an `update
/// target ... from source` would risk an ambiguous-column error whenever a
/// passthrough field shares its name with a source column that also exists
/// on the target. Trade-off: no durable chunking, so a resume against a very
/// large source table runs as one pass rather than resumable steps —
/// acceptable for a rare, bounded, operator-invoked action; flagged here as
/// a follow-up if that ever stops being true. (The write-back itself is
/// split into short transactions of [`RECOMPUTE_COLUMN_CHUNK`] keys, so the
/// pass never holds row locks across the whole target.)
///
/// A row that still fails to evaluate (the underlying data problem isn't
/// actually fixed for it) is skipped rather than aborting the whole resume —
/// its column stays at whatever it was frozen to, and it remains eligible to
/// re-trip the fuse later via ordinary live CDC if it keeps failing.
async fn recompute_column(pool: &Pool, def: &Definition, column: &str) -> Result<(), ApplyError> {
    if !def.def.fields.iter().any(|f| f.name == column) {
        return Err(ApplyError::ColumnNotPaused {
            transform: def.def.target.clone(),
            column: column.to_string(),
        });
    }
    // Issue #315: an aggregate column has nothing to recompute here. The
    // aggregate apply path never honors a column pause (only a 1-1 plan
    // drops paused columns — see `apply::compute`), so the column was never
    // frozen, and the catch-up marker `resume_column` parks re-derives every
    // group through the ordinary drain path anyway. The per-row write-back
    // below keys on the *source* primary key, which an aggregate target
    // doesn't have: it used to fail the whole resume.
    if matches!(def.def.key_space, KeySpace::Aggregate { .. }) {
        return Ok(());
    }

    // Issue #121: this resume path's write-back below now keys on the
    // *target*'s full (possibly composite) primary key, through the shared,
    // arity-generic key-contract text ([`ddl::pk_key_sql_expr`]) rather than
    // a single named column — the same generalization every other 1-1
    // consumer (`staging::apply`, `defs::backfill`, `staging::self_check`)
    // makes. This is still only meaningful for a 1-1 definition regardless
    // of arity: an Aggregate target's primary key is its `GROUP BY` columns,
    // unrelated to the source's primary key.
    let pk = ddl::source_primary_key(pool, &def.source_table).await?;
    let source_ident = ddl::qualified_source_table(&def.source_table);
    let pk_key_expr = ddl::pk_key_sql_expr(&pk, Some("t"));
    // A row belonging to a NULL-keyed group (only reachable when `source` is
    // itself an aggregate target with a nullable `GROUP BY` column — see the
    // loop below's own comment) can't be told apart from a real value by
    // `pk_key_expr` alone: issue #110's NULL-component encoding folds a real
    // SQL `NULL` into a sentinel *string*, precisely so it round-trips
    // through this crate's shared key contract, which means it is never
    // itself a SQL `NULL` for `db_row.get`'s `Option` check to catch. Select
    // the "any component NULL" test directly instead.
    let null_check = pk
        .iter()
        .map(|c| format!("t.{} is null", quote_ident(&c.name)))
        .collect::<Vec<_>>()
        .join(" or ");

    let mut client = pool.get().await?;
    // Issue #248: an explicit per-column `jsonb_build_object`, not
    // `to_jsonb(t.*)` — see `apply::row_as_text_jsonb_sql`'s doc comment for
    // why: `to_jsonb`'s own ISO-8601 writer renders `timestamp`/`timestamptz`
    // differently than the `::text` cast this same recompute's evaluator
    // uses everywhere else, which this rare, operator-driven path is not
    // exempt from just because it isn't the hot CDC path.
    let row_columns = apply::live_row_columns(&**client, &def.source_table).await?;
    let doc_expr = apply::row_as_text_jsonb_sql("t", &row_columns);
    let db_rows = client
        .query(
            &format!(
                "select {pk_key_expr} as pk_text, ({null_check}) as pk_has_null, \
                 e.key, e.value \
                 from {source_ident} t \
                 cross join lateral jsonb_each_text({doc_expr}) e"
            ),
            &[],
        )
        .await?;

    let mut order: Vec<String> = Vec::new();
    let mut rows_by_pk: HashMap<String, Row> = HashMap::new();
    for db_row in db_rows {
        // Issue #211/#121: `pk_text` here is built from this crate's shared
        // key-contract text (`ddl::pk_key_sql_expr`), which routes a nullable
        // column's part through issue #110's `NULL_KEY_SENTINEL`/escape
        // treatment before it's ever bound anywhere — so, unlike a raw
        // `{col}::text` cast, it is never itself a SQL `NULL`, even for a
        // source row belonging to a NULL-keyed group. `pk_has_null` (selected
        // separately, above) is what actually detects that case. A NULL
        // component is only reachable when `source` is itself an aggregate
        // target: `pk` (`ddl::source_primary_key`) is that aggregate's
        // `GROUP BY` grouping-column PK, which `create_aggregate_target_table`
        // declares `UNIQUE NULLS NOT DISTINCT` rather than a real `PRIMARY
        // KEY` specifically so it *can* hold NULL (issue #110's whole
        // subject) — a genuine, never-NULL source primary key can never
        // produce this.
        //
        // `def` (this recompute's own target — always a 1-1, regardless of
        // its primary key's arity) can never hold a row for that NULL-keyed
        // group either way: `ddl::create_target_table` always
        // declares its own primary key column a real `primary key`, which
        // Postgres makes NOT NULL unconditionally regardless of whether the
        // *source* column this target's key was narrowed from is itself
        // nullable. That's exactly the reasoning issue #205 used to drop a
        // NULL-keyed group from `apply::apply_target`'s write path instead
        // of storing a literal sentinel/NULL as a target PK value, and the
        // same outcome `defs::backfill::discover_pk_ranges`'s ordered
        // `(lo, hi]` PK-range walk already produces structurally (a
        // NULL-keyed row is `unknown` against every `<=`/`>` chunk bound, so
        // it's never selected by any chunk, and a from-scratch backfill of
        // this same target never attempts to insert it either).
        //
        // So a NULL-keyed group's row has nothing to recompute *and* nothing
        // to write back — there is no target row `column`'s new value could
        // ever land in — and is dropped here before it ever reaches
        // `rows_by_pk`/`order`: it never gets an entry in the relationship
        // context this function builds below (`build_relationship_context`,
        // for any definition that reads relationships), and the row loop
        // further down (which walks `order` and issues one `UPDATE ...
        // where {pk_key_expr} = $2` per entry) never attempts a write for
        // it — the same "no representable row" answer a from-scratch
        // backfill already gives this group, kept consistent here via the
        // explicit `pk_has_null` test above (see the query's own comment for
        // why this can't simply check `pk_text` for a SQL `NULL`).
        let pk_has_null: bool = db_row.get(1);
        if pk_has_null {
            continue;
        }
        let pk_text: String = db_row.get(0);
        let key: String = db_row.get(2);
        let value: Option<String> = db_row.get(3);
        rows_by_pk
            .entry(pk_text.clone())
            .or_insert_with(|| {
                order.push(pk_text.clone());
                Row::new()
            })
            .insert(key, value);
    }

    let rel_refs = eval::relationship_references(&def.def);
    let rel_ctx = if rel_refs.is_empty() {
        RelationshipContext::default()
    } else {
        let rows: Vec<Option<Row>> = order
            .iter()
            .map(|pk_text| Some(rows_by_pk[pk_text].clone()))
            .collect();
        // This resume path is an ad hoc, non-transactional, per-row pass
        // over every current source row — not part of the staging ring's
        // claim/fold/compute/apply pipeline `build_relationship_context`'s
        // gen-bump signal exists to guard (issue #130, epic #127), so
        // `old_rows: None`/`changes: None` here: there is no folded change
        // (with an old image, or a #133 `group_key`) to widen the
        // touched-key set from, and the returned gen-bump map is discarded
        // rather than applied in any transaction (there isn't one spanning
        // this whole function to apply it in).
        let (ctx, _gen_bumps) =
            apply::build_relationship_context(pool, &def.source_table, &def.def, &rows, None, None)
                .await?;
        ctx
    };

    let field_type = if rel_refs.is_empty() {
        let inferred = validate::infer_field_types(&def.def, &def.source_columns, &HashMap::new())?;
        inferred.get(column).copied().unwrap_or(ValueType::Numeric)
    } else {
        // Broader sweep, reviewer follow-up to issue #74 (epic #78's own
        // whole-branch review): `def.def.target` is always bare, even for an
        // explicitly `schema.target`-qualified `TRANSFORM` (issue #76) — see
        // `staging::apply::compute`'s identical fix, right above this
        // function's own `apply::to_column_types` call, for the full
        // reasoning. `def.target_table` (already the persisted, qualified
        // identity) is available here the same way.
        let col_names = vec![column.to_string()];
        let types = apply::to_column_types(pool, &def.target_table, &col_names).await?;
        types.get(column).copied().unwrap_or(ValueType::Numeric)
    };
    let pg_type = ddl::pg_type_name(field_type);

    // `def.target_table` (issue #73's persisted, qualified identity), not a
    // bare `quote_ident(&def.def.target)` — reviewer follow-up to issue #74
    // (epic #78's own whole-branch review): a target explicitly qualified
    // into a non-default schema (issue #76) isn't necessarily on this
    // connection's pinned `search_path`. See
    // `staging::apply::apply_target`'s identical fix for the live CDC-apply
    // write path this recompute UPDATE shares the same bug class with.
    let target_ident = ddl::qualified_target_table_ident(&def.target_table);
    let col_ident = quote_ident(column);
    let mut regex_cache = eval::RegexCache::new();

    // Exclude every *other* column of this same definition that's still
    // paused (a sibling with its own independent, still-broken formula) from
    // this evaluation — `column` itself is still marked paused in
    // `column_status` at this point (`resume_column` only deletes that row
    // *after* this call returns), so a plain `paused_columns_for` read would
    // otherwise exclude `column` too and this recompute would silently
    // evaluate nothing for it. Without this exclusion, a still-broken
    // sibling's formula throwing on some row would fail the whole
    // `evaluate_with_relationships` call for that row (the un-excluding
    // form used to be called here), and the `let Ok(...) else { continue; }`
    // guard below would then skip recomputing `column` for that row too —
    // silently leaving it frozen even though `column`'s own formula is fine.
    let mut excluded = paused_columns_for(pool, &def.def.target).await?;
    excluded.remove(column);

    // Evaluate first (pure, no database), so each write-back transaction
    // below holds its row locks only for its own statements.
    let mut values: Vec<(&String, Option<String>)> = Vec::with_capacity(order.len());
    for pk_text in &order {
        let row = &rows_by_pk[pk_text];
        let Ok(mut evaluated) = eval::evaluate_with_relationships_excluding(
            &def.def,
            row,
            &def.source_columns,
            &rel_ctx,
            &mut regex_cache,
            &excluded,
        ) else {
            continue;
        };
        let value: Option<String> = evaluated.remove(column).flatten().map(|v| v.to_string());
        values.push((pk_text, value));
    }

    // Issue #315: the write-back goes through the target-mutation seam, so a
    // definition chained off this target hears about every row whose value
    // actually changed (the `is distinct from` guard leaves an unchanged row
    // untouched and unreported).
    //
    // One bounded transaction per chunk of keys, not one for the whole
    // target: a transaction holding row locks has an xid, and a long-lived
    // xid holds the ring's seal gate (docs/staging-and-claiming/03) shut for
    // every definition until it commits, besides blocking this target's own
    // drains the whole time. Each chunk row-locks its existing target rows in
    // ascending key order, the order a drain's own pre-lock takes
    // (`apply::apply_target`), so it can't deadlock against one; the same
    // statement captures each row's prior image when something reads the
    // target, and its `ctid`, which the lock keeps stable until commit and
    // which lets each update find its row without re-scanning the table.
    let update_sql = format!(
        "update {target_ident} t set {col_ident} = $1::text::{pg_type} \
         where t.ctid = $2::text::tid and t.{col_ident} is distinct from $1::text::{pg_type}",
    );
    for chunk in values.chunks(RECOMPUTE_COLUMN_CHUNK) {
        // Every key here decodes to real values: a NULL-keyed row was already
        // dropped (`pk_has_null`, above) before it reached `values`.
        let mut key_parts: Vec<Vec<String>> = Vec::with_capacity(chunk.len());
        for (pk_text, _) in chunk {
            if let Some(parts) = apply::decode_target_pk_parts(&pk, &def.def.target, pk_text)? {
                key_parts.push(parts);
            }
        }
        let key_refs: Vec<&Vec<String>> = key_parts.iter().collect();
        let arrays = apply::transpose_pk_parts(pk.len(), &key_refs);
        let params: Vec<&(dyn ToSql + Sync)> = arrays.iter().map(|a| a as _).collect();

        let txn = client.transaction().await?;
        let mut mutations = TargetMutations::new();
        let image_expr = mutations.image_sql(&txn, &def.target_table, "t").await?;
        let image_select = match &image_expr {
            Some(expr) => format!("({expr})::text"),
            None => "null::text".to_string(),
        };
        let locked = txn
            .query(
                &recompute_lock_sql(&target_ident, &pk, &image_select),
                &params,
            )
            .await?;
        let mut locked_rows: HashMap<String, (String, Option<String>)> = locked
            .into_iter()
            .map(|row| (row.get(0), (row.get(1), row.get(2))))
            .collect();
        for (pk_text, value) in chunk {
            // No target row for this key (nothing to update, as before).
            let Some((ctid, prior)) = locked_rows.remove(pk_text.as_str()) else {
                continue;
            };
            if txn.execute(&update_sql, &[value, &ctid]).await? > 0 {
                mutations.record(&def.target_table, (*pk_text).clone(), prior, 0, None, None);
            }
        }
        mutations.flush(&txn).await?;
        txn.commit().await?;
    }

    Ok(())
}

/// How many keys [`recompute_column`] writes back per transaction.
const RECOMPUTE_COLUMN_CHUNK: usize = 1000;

/// The statement each [`recompute_column`] write-back chunk locks its target
/// rows with, in ascending key order: every row's key-contract text, `ctid`
/// and `image_select`, bound to `pk.len()` per-column key arrays
/// ([`apply::transpose_pk_parts`]).
///
/// Issue #377: the chunk's keys are matched through a bound keyset relation
/// joined on the target's own primary-key columns, so each key is an index
/// probe. The earlier `pk_key_sql_expr(...) = any($1::text[])` compared a
/// computed text expression that no index covers, which made every chunk a
/// full scan of the target.
fn recompute_lock_sql(target_ident: &str, pk: &[PrimaryKeyColumn], image_select: &str) -> String {
    let target_pk_expr = ddl::pk_key_sql_expr(pk, Some("t"));
    let lock_order = pk
        .iter()
        .map(|c| format!("t.{}", quote_ident(&c.name)))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "select {target_pk_expr}, t.ctid::text, {image_select} from {target_ident} t \
         join {} on ({}) order by {lock_order} for update of t",
        apply::pk_keyset_unnest(pk, 1),
        apply::pk_keyset_match(pk, "t"),
    )
}

// ---------------------------------------------------------------------
// Release
// ---------------------------------------------------------------------

/// Operator-driven release, one transaction: replays every `poison_held` row
/// for `(src_table, key)`, in batch order (`seg_seq` ascending) then
/// position order (`held_seq` ascending — a tie-breaker only, since this
/// table holds at most one row per `(src_table, key, seg_seq)`), into the
/// active batch; then deletes the held rows, the marker, and the death
/// counter. Doc 06: "Each replayed row keeps its **original** origin
/// position" — [`StagedChange::Cdc`]'s `lsn`/`origin_lsn` fields are set
/// from the held row's own columns, never reset, which is what keeps the
/// key's band blocked until the release actually drains rather than
/// silently un-gating the read-your-writes predicate.
///
/// Returns how many held rows were replayed.
#[cfg(any(test, feature = "internals"))]
pub async fn release_key(pool: &Pool, src_table: &str, key: &str) -> Result<usize, ApplyError> {
    // Issue #283: quarantine's tables are keyed canonically, but this is an
    // operator entry point that may be handed either spelling of a source, and
    // rows written before the V33 fold (or under a spelling V33 could not
    // resolve) can still be keyed raw. Matching the set of both is what keeps a
    // release total either way — a partial release would leave orphaned parked
    // work `converge` gates on forever, which is strictly worse than the extra
    // array element. Each replayed row is restaged under *its own* stored
    // `src_table` rather than one chosen spelling, so a legacy bare held row
    // goes back onto the ring exactly as it left it.
    let names = canonical_and_raw(pool, src_table).await?;
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    let held = txn
        .query(
            "select seg_seq, op, lsn, old_image::text, new_image::text, origin_lsn, \
                    src_changed, hop_gen, group_key, src_table \
             from poison_held \
             where src_table = any($1::text[]) and key = $2 \
             order by seg_seq asc, held_seq asc",
            &[&names, &key],
        )
        .await?;

    let changes: Vec<StagedChange> = held
        .iter()
        .map(|row| {
            let op: String = row.get(1);
            let lsn: Option<PgLsn> = row.get(2);
            let old_image: Option<String> = row.get(3);
            let new_image: Option<String> = row.get(4);
            let origin_lsn: Option<PgLsn> = row.get(5);
            let src_changed: Option<SystemTime> = row.get(6);
            let hop_gen: i32 = row.get(7);
            let group_key: Option<Vec<String>> = row.get(8);
            let src_table: String = row.get(9);
            if op == "recompute" {
                StagedChange::Recompute {
                    src_table: src_table.to_string(),
                    key: key.to_string(),
                    hop_gen,
                    group_key,
                    src_changed,
                    // `park_batch_contribution` parks a hinted recompute's
                    // prior image in `old_image` (issue #315).
                    prior_image: old_image,
                    origin_lsn,
                }
            } else {
                let cdc_op = match op.as_str() {
                    "insert" => CdcOp::Insert,
                    "delete" => CdcOp::Delete,
                    _ => CdcOp::Update,
                };
                StagedChange::Cdc {
                    src_table: src_table.to_string(),
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

    txn.execute(
        "delete from poison_held where src_table = any($1::text[]) and key = $2",
        &[&names, &key],
    )
    .await?;
    txn.execute(
        "delete from poison where src_table = any($1::text[]) and key = $2",
        &[&names, &key],
    )
    .await?;
    txn.execute(
        "delete from key_deaths where src_table = any($1::text[]) and key = $2",
        &[&names, &key],
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
/// database state.
///
/// The quarantine deletes match both the canonical identity and the raw ring
/// spelling (issue #283), for the same reason [`release_key`]'s do: a leftover
/// `poison_held` row for a table that no longer exists can never be released or
/// drained, so a partial purge re-wedges exactly what this call exists to
/// unwedge. The *ring* deletes stay on the raw spelling alone — `src_table`
/// there is the string the wedged rows literally hold, which is what
/// `SourceTableDropped` reported (issue #267's final fix) and the only spelling
/// that can be in the ring for this call to have happened at all. Note that a
/// dropped table is precisely the case [`qualified_src_table`] cannot resolve,
/// so for a bare wedged row the canonical name usually *is* the raw one here,
/// and the array is what covers the case where it *is* still resolvable (the
/// table is gone but `schema_nodes`/`transform_definitions` still name it) and
/// the quarantine rows are bare while the ring rows are not.
///
/// **Known residual, deliberately not widened here:** [`canonical_and_raw`]
/// derives the canonical name *from* the given one, so it never adds the bare
/// suffix when handed an already-qualified spelling. A legacy bare quarantine
/// row for a table `V33__quarantine_canonical_src_table.sql` could not fold
/// (its bare suffix ambiguous across two schemas, or unresolvable at migration
/// time) therefore still survives a purge keyed on the qualified ring spelling —
/// exactly the pre-#283 behavior, since that delete was single-spelling too.
/// Stripping to the bare suffix here would fix that only by reintroducing the
/// ambiguity #74/ADR-0007 exists to prevent: a purge of `archive.orders` would
/// delete a bare `orders` row that may belong to `public.orders`. Tracked as its
/// own question rather than guessed at.
pub async fn purge_dropped_table(pool: &Pool, src_table: &str) -> Result<(), ApplyError> {
    let names = canonical_and_raw(pool, src_table).await?;
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    for slot in 0..RING_SIZE {
        let table = ring_table_name(slot)?;
        txn.execute(
            &format!("delete from {table} where src_table = $1"),
            &[&src_table],
        )
        .await?;
    }
    txn.execute(
        "delete from poison where src_table = any($1::text[])",
        &[&names],
    )
    .await?;
    txn.execute(
        "delete from poison_held where src_table = any($1::text[])",
        &[&names],
    )
    .await?;
    txn.execute(
        "delete from key_deaths where src_table = any($1::text[])",
        &[&names],
    )
    .await?;
    txn.commit().await?;
    Ok(())
}

/// `src_table` plus its canonical identity, deduped — the match set the two
/// whole-key/whole-source *deletes* above use instead of a single spelling
/// (issue #283). Only for delete paths that must be total: counting and
/// charging still key strictly on the canonical identity, which is the entire
/// point of that issue (two spellings, one budget), and would be re-split by
/// matching a set here.
async fn canonical_and_raw(pool: &Pool, src_table: &str) -> Result<Vec<String>, ApplyError> {
    let canonical = qualified_src_table(pool, src_table).await?;
    if canonical == src_table {
        return Ok(vec![canonical]);
    }
    Ok(vec![canonical, src_table.to_string()])
}

// ---------------------------------------------------------------------
// The halting-stop metric
// ---------------------------------------------------------------------

/// Increments `halting_stops`' single row and records `reason` — the "real
/// metric (counter + last reason)" doc 05's failure-classification table
/// calls for on the halting class, so "stopped" and "slow" are
/// distinguishable from the outside. A plain counter table, matching this
/// crate's existing convention for small operational state (`drainers`)
/// rather than a Prometheus-style dependency this crate has none of today.
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
#[cfg(any(test, feature = "internals"))]
pub struct HaltingStopStats {
    pub stop_count: i64,
    pub last_reason: Option<String>,
    pub last_stopped_at: Option<SystemTime>,
}

#[cfg(any(test, feature = "internals"))]
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
    fn classify_maps_unsupported_primary_key_type_to_halting() {
        // Issue #107: a non-text-stable single-column primary key is exactly
        // as structural as the composite-key and no-key cases above, so it
        // must halt too rather than isolate-and-evict every key touching
        // that source one at a time.
        let err = ApplyError::Ddl(DdlError::UnsupportedPrimaryKeyType {
            source_table: "events".to_string(),
            column: "occurred_at".to_string(),
            pg_type: "timestamp with time zone".to_string(),
        });
        assert_eq!(classify(&err), FailureClass::Halting);
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
    /// `EXPLAIN`'s plan text for `sql` with `params` bound, one line per row.
    async fn explain_plan(
        client: &tokio_postgres::Client,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> String {
        client
            .query(&format!("explain {sql}"), params)
            .await
            .expect("explain")
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Issue #377: a `recompute_column` write-back chunk locks its target rows
    /// through an index, at both a single-column and a composite primary key,
    /// for a full chunk of keys. The pre-#377 shape, matching the computed
    /// key-contract text, run against the same tables and keys, can only plan
    /// a sequential scan (proving the plan difference is real, not just that
    /// both shapes happen to allow an index).
    #[tokio::test]
    async fn recompute_lock_matches_a_chunk_of_keys_through_the_primary_key_index() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(
                "create table single_pk (id bigint primary key, v integer); \
                 insert into single_pk select g, g from generate_series(1, 1000000) g; \
                 analyze single_pk; \
                 create table composite_pk (a bigint, b text, v integer, primary key (a, b)); \
                 insert into composite_pk select g, 'k' || g, g from generate_series(1, 1000000) g; \
                 analyze composite_pk;",
            )
            .await
            .expect("seed large indexed targets");

        let col = |name: &str, data_type: &str| PrimaryKeyColumn {
            name: name.to_string(),
            data_type: data_type.to_string(),
            nullable: false,
        };
        // A full chunk of keys spread across the table, as a resume would bind.
        let ids: Vec<i64> = (0..RECOMPUTE_COLUMN_CHUNK as i64)
            .map(|i| i * 997 + 1)
            .collect();
        let cases = [
            (
                "single_pk",
                vec![col("id", "bigint")],
                ids.iter().map(|i| vec![i.to_string()]).collect::<Vec<_>>(),
            ),
            (
                "composite_pk",
                vec![col("a", "bigint"), col("b", "text")],
                ids.iter()
                    .map(|i| vec![i.to_string(), format!("k{i}")])
                    .collect::<Vec<_>>(),
            ),
        ];
        for (table, pk, keys) in cases {
            let target_ident = quote_ident(table);
            let key_refs: Vec<&Vec<String>> = keys.iter().collect();
            let arrays = apply::transpose_pk_parts(pk.len(), &key_refs);
            let params: Vec<&(dyn ToSql + Sync)> = arrays.iter().map(|a| a as _).collect();
            let plan = explain_plan(
                &client,
                &recompute_lock_sql(&target_ident, &pk, "null::text"),
                &params,
            )
            .await;
            assert!(
                plan.contains("Index") && !plan.contains("Seq Scan"),
                "{table}: the keyset match should probe the primary-key index, got:\n{plan}"
            );

            let key_texts: Vec<String> = keys
                .iter()
                .map(|parts| ddl::join_pk_key(parts.iter().map(|s| s.as_str())))
                .collect();
            let old_plan = explain_plan(
                &client,
                &format!(
                    "select 1 from {target_ident} t where {} = any($1::text[])",
                    ddl::pk_key_sql_expr(&pk, Some("t"))
                ),
                &[&key_texts],
            )
            .await;
            assert!(
                old_plan.contains("Seq Scan"),
                "{table}: sanity check, the pre-#377 key-text match can't use an index, \
                 got:\n{old_plan}"
            );
        }
    }
}
