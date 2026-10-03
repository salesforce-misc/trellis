//! The Re-derive build of a 1-1 target (#625 F8a; ADR-0002 "A build is
//! Re-derive over chunks, and applies from its first chunk").
//!
//! A plain 1-1 target (no relationship path, on a captured source) is built
//! by the same scheduled build as a ledger aggregate (see the parent
//! module): the staging worker's start moves it `waiting_to_backfill ->
//! backfilling` with `build = 'rederive'`, it applies from that commit, a
//! plan job enqueues `(lo, hi]` chunks of its source's primary key, a
//! rebuild also gets a sweep, and the transaction that finishes the last
//! chunk moves it `live` (B7). Nothing parks a catch-up for it.
//!
//! What differs is the chunk. A 1-1 target's rows are absolute: a target row
//! is the evaluation of one source row, so a chunk writes the target rows
//! themselves, with no group deltas and so no merger. What orders a chunk
//! against a page is the key's entry on the 1-1 slim ledger (#623 D6,
//! `super::super::one_to_one_ledger`), as it orders a page against a page:
//!
//! 1. read the keys in the range, leaving out the quarantined ones
//!    (`poison`) and any with a `NULL` part, as the old 1-1 range build did;
//! 2. lock their entries in key order (`one_to_one_ledger::lock_entries`:
//!    an entry for each key with none, then the others `for update`), under
//!    the build's short [`super::CHUNK_LOCK_TIMEOUT`];
//! 3. one statement ([`rederive_statement`]) reads `pg_current_snapshot()`,
//!    the active segment and the locked keys' source rows, rewrites their
//!    entries (`basis` := the snapshot, `applied_seg` raised to the segment,
//!    `applied_lsn` left alone, a key with no row a tombstone), upserts the
//!    target row of every key that has a source row (only when its values
//!    change) and deletes the target row of every key that hasn't. Every
//!    field is the SQL `defs::oracle` renders, as the old range build
//!    computed it. A changed key is reported to the target-mutation seam
//!    with its prior image when the target has a reader. It runs with
//!    sequential scans off (`CHUNK_PLAN_SETTINGS`), so it reaches the
//!    ledger, the target and the source through their keys' indexes and
//!    costs the same however large the ledger is.
//!
//! Why a chunk and a page agree: every writer of a 1-1 target row holds the
//! key's entry while it reads and writes. The chunk's snapshot is taken
//! after its lock, so it sees every change a page applied before. A page
//! after the chunk applies a change only if ADR-0002's I2 holds: one the
//! snapshot saw is refused, and one it didn't is newer than the row the
//! chunk wrote. A key a page inserted after step 1 isn't locked, and its own
//! change applies it.
//!
//! A paused column (`column_status`) is left out of the chunk's insert and
//! update, as the page and the old range build leave it.

use std::collections::HashSet;
use std::time::Instant;

use tokio_postgres::Transaction;

use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::model::Definition;
use crate::metrics::{self, BuildStatement};
use crate::pool::{Pool, quote_ident};

use super::super::apply::ApplyError;
use super::super::one_to_one_ledger::{self, EntryChange};
use super::super::target_mutations::TargetMutations;
use super::{CHUNK_LOCK_TIMEOUT, SweepOutcome};

/// What a 1-1 Re-derive build needs to know about its target.
#[derive(Debug, Clone)]
pub struct OneToOnePlan {
    /// The target's qualified identity (`transform_definitions.target_table`).
    target: String,
    /// The bare target name, which `column_status` keys on.
    transform: String,
    source_table: String,
    pk: Vec<PrimaryKeyColumn>,
    /// Each field's name and its self-contained SQL over the source's
    /// columns, in definition order.
    fields: Vec<(String, String)>,
}

/// Whether `definition` is a 1-1 target the Re-derive build serves (#625
/// F8a): a 1-1 key space, no relationship path in any field (those wait for
/// milestone E), and fields whose aliases substitute to self-contained SQL
/// (a cyclic alias chain is the ring enumeration's). The caller checks the
/// source is captured, not seam-fed (#625 F6).
pub(crate) fn buildable(definition: &Definition) -> bool {
    matches!(
        definition.def.key_space,
        crate::defs::ast::KeySpace::OneToOne
    ) && !crate::defs::backfill::uses_relationships(&definition.def)
        && crate::defs::backfill::substitute_all_fields(&definition.def).is_ok()
}

impl OneToOnePlan {
    /// The plan for the definition whose target is `target` (its bare name),
    /// or `None` when there is no such definition or it isn't a 1-1 target
    /// the Re-derive build serves ([`buildable`]).
    #[cfg(any(test, feature = "internals"))]
    pub async fn load(pool: &Pool, target: &str) -> Result<Option<Self>, ApplyError> {
        let Some(definition) = crate::defs::catalog::definition_by_target(pool, target).await?
        else {
            return Ok(None);
        };
        Self::for_definition(pool, &definition).await
    }

    /// [`Self::load`] for a definition already read.
    pub(crate) async fn for_definition(
        pool: &Pool,
        definition: &Definition,
    ) -> Result<Option<Self>, ApplyError> {
        if !buildable(definition) {
            return Ok(None);
        }
        let substituted = crate::defs::backfill::substitute_all_fields(&definition.def)?;
        let fields = definition
            .def
            .fields
            .iter()
            .zip(&substituted)
            .map(|(field, expr)| {
                (
                    field.name.clone(),
                    crate::defs::oracle::render_expr_sql(expr),
                )
            })
            .collect();
        let pk = ddl::source_primary_key(pool, &definition.source_table).await?;
        Ok(Some(Self {
            target: definition.target_table.clone(),
            transform: definition.def.target.clone(),
            source_table: definition.source_table.clone(),
            pk,
            fields,
        }))
    }
}

/// What one 1-1 [`run_chunk`] or sweep batch's Re-derive did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OneToOneOutcome {
    /// The source keys it locked and re-derived.
    pub keys: usize,
    /// The target rows it inserted or changed.
    pub written: i64,
    /// The target rows it deleted (a locked key with no source row).
    pub deleted: i64,
}

/// Runs one 1-1 build chunk, `(lo, hi]` of the target's source primary key,
/// in `txn` (see the module doc). `lo` and `hi` are encoded keys, as
/// `backfill_chunks` stores a range's bounds; `lo` is `None` for the first
/// chunk. The caller commits.
///
/// `txn` must be `read committed`, as for an aggregate's chunk
/// ([`super::run_chunk`]): step 3's snapshot must be its own statement's,
/// taken after the entry lock (ADR-0002 I1). A lock wait past
/// [`CHUNK_LOCK_TIMEOUT`] fails it with `55P03`, and an entry the tombstone
/// GC collects under its lock with [`ApplyError::LedgerEntryCollected`];
/// both are transient.
pub async fn run_chunk(
    txn: &Transaction<'_>,
    plan: &OneToOnePlan,
    lo: Option<&str>,
    hi: &str,
) -> Result<OneToOneOutcome, ApplyError> {
    run_range(txn, plan, None, lo, hi).await
}

/// Runs one chunk of a field build (#625 F8b; see the parent module's
/// "Field builds"), `(lo, hi]` of the target's source primary key, in `txn`,
/// which must be `read committed`. The caller commits.
///
/// It reads and locks the range's keys as [`run_chunk`] does, and then one
/// statement ([`field_statement`]) rewrites `fields` (but for any paused
/// now) of each locked key's existing target row from the source row the
/// statement's snapshot reads, where they differ. It inserts no row, deletes
/// none and leaves the keys' entries alone: a row's existence and its other
/// columns are Apply's and the whole build's.
///
/// Why it must leave the entries alone: a chunk that moved a key's `basis`
/// to its snapshot would have every later page refuse a change that
/// snapshot saw (ADR-0002 I2), and the change's other columns would be lost
/// if no page had applied it yet, since this chunk writes only `fields`.
///
/// Why the entry lock is still needed: the statement's writes are absolute
/// values from its snapshot. Without the lock, a page could apply a newer
/// change between the snapshot and the write, and the chunk would put the
/// field back behind it. Under the lock, a page that holds the key commits
/// first and the snapshot sees its change. One that comes later writes the
/// field from its own change's image, since the field applies from the
/// build's start. A change newer than the snapshot leaves the field newer
/// too. An older one that I2 still lets through leaves it behind only until
/// the newer change the snapshot saw applies, which I2 lets through as
/// well, since the chunk left the entry as it was. A page that read the
/// field as paused can't come later: the edit, the column resume and the
/// capture release bump the source's version fence
/// (`super::bump_version_fence`).
pub async fn run_field_chunk(
    txn: &Transaction<'_>,
    plan: &OneToOnePlan,
    fields: &[String],
    lo: Option<&str>,
    hi: &str,
) -> Result<OneToOneOutcome, ApplyError> {
    run_range(txn, plan, Some(fields), lo, hi).await
}

/// [`run_chunk`] (`scope` `None`) or [`run_field_chunk`] (`scope` the
/// fields).
async fn run_range(
    txn: &Transaction<'_>,
    plan: &OneToOnePlan,
    scope: Option<&[String]>,
    lo: Option<&str>,
    hi: &str,
) -> Result<OneToOneOutcome, ApplyError> {
    let (lo, hi) = decode_bounds(plan, lo, hi)?;
    let pk_idents: Vec<String> = plan.pk.iter().map(|c| quote_ident(&c.name)).collect();
    let range_where = crate::defs::backfill::pk_range_where(&pk_idents, &plan.pk, &lo);
    let mut params = crate::defs::backfill::range_params(&lo, &hi);

    // 1. The keys in the range, but for the quarantined ones (#616, #625
    // F-A5) and any with a `NULL` part (no target row can hold one), as the
    // old range build's write and `fail_chunk`'s narrowing count them.
    let started = Instant::now();
    let source_param = format!("${}", params.len() + 1);
    let mut key_params = params.clone();
    key_params.push(&plan.source_table);
    let k_expr = ddl::pk_key_sql_expr(&plan.pk, Some("s"));
    let not_null: Vec<String> = pk_idents
        .iter()
        .map(|c| format!("s.{c} is not null"))
        .collect();
    let keys: Vec<String> = txn
        .query(
            &format!(
                "select {k_expr} from {} s where {range_where} and {} \
                 and not exists (select 1 from poison p \
                                 where p.src_table = {source_param} and p.key = {k_expr})",
                ddl::qualified_source_table(&plan.source_table),
                not_null.join(" and "),
            ),
            &key_params,
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    metrics::record_build_statement(BuildStatement::ChunkKeys, started.elapsed());
    if keys.is_empty() {
        return Ok(OneToOneOutcome::default());
    }
    let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();

    // 2. The entry lock, under the chunk's own short lock timeout.
    lock_chunk_entries(txn, plan, &key_refs).await?;

    // 3. The read, the entries and the target rows, in one statement.
    let keys_param = format!("${}", params.len() + 1);
    params.push(&key_refs);
    let pick = chunk_pick(plan, &lo, &keys_param);
    let outcome = rederive(txn, plan, scope, &pick, &params, keys.len()).await?;
    // Test-only pause point (#623 D1), directly after the chunk's one
    // read-and-write statement. See `super::super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::super::interleave::pause_at(
        txn,
        super::super::interleave::PausePoint::AfterRederiveRead,
        &plan.target,
    )
    .await?;
    tracing::debug!(
        target_table = %plan.target,
        keys = outcome.keys,
        written = outcome.written,
        deleted = outcome.deleted,
        fields = ?scope,
        "1-1 build chunk re-derived its range"
    );
    Ok(outcome)
}

/// A chunk's encoded `(lo, hi]` bounds as their key columns' text.
fn decode_bounds(
    plan: &OneToOnePlan,
    lo: Option<&str>,
    hi: &str,
) -> Result<(Option<Vec<String>>, Vec<String>), ApplyError> {
    let decode = |text: &str| -> Result<Vec<String>, ApplyError> {
        Ok(ddl::split_pk_key(&plan.pk, &plan.source_table, text)?
            .into_iter()
            .map(|part| part.map(|c| c.into_owned()).unwrap_or_default())
            .collect())
    };
    Ok((lo.map(decode).transpose()?, decode(hi)?))
}

/// How a chunk over `(lo, hi]` picks its locked keys' rows, the keys being
/// the `text[]` parameter `keys_param`: the source and the target are each
/// read by their key range, and filtered to the locked keys by their
/// encoded key (a hashed `= any`, as the array is a constant in the
/// statement's plan). A key the chunk didn't lock (inserted after its key
/// read) is left to its own change.
fn chunk_pick(plan: &OneToOnePlan, lo: &Option<Vec<String>>, keys_param: &str) -> Pick {
    let pk_idents: Vec<String> = plan.pk.iter().map(|c| quote_ident(&c.name)).collect();
    let k_src = ddl::pk_key_sql_expr(&plan.pk, Some("s"));
    let k_tgt = ddl::pk_key_sql_expr(&plan.pk, Some("t"));
    let qualify = |alias: &str| {
        let idents: Vec<String> = pk_idents.iter().map(|c| format!("{alias}.{c}")).collect();
        crate::defs::backfill::pk_range_where(&idents, &plan.pk, lo)
    };
    Pick {
        src_from: format!(
            "where {} and {k_src} = any({keys_param}::text[])",
            qualify("s")
        ),
        target_where: format!("{} and {k_tgt} = any({keys_param}::text[])", qualify("t")),
        keys: keys_param.to_string(),
    }
}

/// The plan of a 1-1 chunk's read-and-write statement over `(lo, hi]`, for
/// `keys` (encoded, as the chunk's key read returns them), as `explain`'s
/// text, under the settings the chunk runs it with, in `txn`. For tests of
/// the plan's shape. It locks nothing and writes nothing.
#[cfg(any(test, feature = "internals"))]
pub async fn explain_chunk(
    txn: &Transaction<'_>,
    plan: &OneToOnePlan,
    lo: Option<&str>,
    hi: &str,
    keys: &[&str],
) -> Result<String, ApplyError> {
    explain_range(txn, plan, None, lo, hi, keys).await
}

/// [`explain_chunk`] for a field build's chunk ([`run_field_chunk`]) over
/// `fields`.
#[cfg(any(test, feature = "internals"))]
pub async fn explain_field_chunk(
    txn: &Transaction<'_>,
    plan: &OneToOnePlan,
    fields: &[String],
    lo: Option<&str>,
    hi: &str,
    keys: &[&str],
) -> Result<String, ApplyError> {
    explain_range(txn, plan, Some(fields), lo, hi, keys).await
}

#[cfg(any(test, feature = "internals"))]
async fn explain_range(
    txn: &Transaction<'_>,
    plan: &OneToOnePlan,
    scope: Option<&[String]>,
    lo: Option<&str>,
    hi: &str,
    keys: &[&str],
) -> Result<String, ApplyError> {
    let (lo, hi) = decode_bounds(plan, lo, hi)?;
    let mut params = crate::defs::backfill::range_params(&lo, &hi);
    let keys_param = format!("${}", params.len() + 1);
    let keys = keys.to_vec();
    params.push(&keys);
    let pick = chunk_pick(plan, &lo, &keys_param);
    let (sql, _, _) = prepare_statement(txn, plan, scope, &pick).await?;
    let Some(sql) = sql else {
        return Ok("nothing to write".to_string());
    };
    let rows = query_under_chunk_plan(txn, &format!("explain {sql}"), &params).await?;
    Ok(rows
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n"))
}

/// One batch of a 1-1 rebuild's sweep (#625 F3's sweep, for a 1-1 target;
/// see [`super::sweep_batch`]), in `txn`, which must be `read committed`.
/// The caller commits.
///
/// It reads up to `scan` ledger entries in key order after `cursor`, and
/// picks the live ones (not a tombstone) the build hasn't re-derived: those
/// whose `basis` is null (written by Apply alone) or a snapshot taken before
/// the build's start, `start_xid`. A quarantined key is left as it is. Then
/// it locks and re-derives them as a chunk does, reading the source by each
/// key's typed columns: a key with no row any more (deleted while the
/// definition was frozen) becomes a tombstone and its target row is
/// deleted.
pub async fn sweep_batch(
    txn: &Transaction<'_>,
    plan: &OneToOnePlan,
    start_xid: &str,
    cursor: Option<&str>,
    scan: i64,
) -> Result<SweepOutcome, ApplyError> {
    let key = quote_ident(crate::defs::ledger::KEY_COLUMN);
    let tombstone = quote_ident(crate::defs::ledger::TOMBSTONE_COLUMN);
    let basis = quote_ident(crate::defs::ledger::BASIS_COLUMN);
    let after = if cursor.is_some() {
        format!("where {key} > $3")
    } else {
        "where $3::text is null".to_string()
    };
    let started = Instant::now();
    let row = txn
        .query_one(
            &format!(
                "with w as ( \
                     select {key} as k, not {tombstone} \
                            and ({basis} is null \
                                 or pg_catalog.pg_snapshot_xmax({basis}) <= $1::text::xid8) \
                            as stale \
                     from {ledger} {after} order by {key} limit $2 \
                 ) \
                 select (select count(*) from w), \
                        (select k from w order by k desc limit 1), \
                        array(select k from w where stale \
                                and not exists (select 1 from poison p \
                                                where p.src_table = $4 and p.key = w.k) \
                              order by k)",
                ledger = one_to_one_ledger::ledger_ident(&plan.target),
            ),
            &[&start_xid, &scan, &cursor, &plan.source_table],
        )
        .await?;
    metrics::record_build_statement(BuildStatement::ChunkKeys, started.elapsed());
    let scanned: i64 = row.get(0);
    let next: Option<String> = row.get(1);
    let keys: Vec<String> = row.get(2);
    let finished = scanned < scan;
    if keys.is_empty() {
        return Ok(SweepOutcome {
            scanned,
            rederived: 0,
            delta_rows: 0,
            next,
            finished,
        });
    }
    let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    lock_chunk_entries(txn, plan, &key_refs).await?;

    // The source and the target are read by their typed key columns, so
    // each is one index probe per key: `$1` is the keys' `text[]`, `$2..`
    // one `text[]` per key column.
    let mut parts = vec![Vec::with_capacity(keys.len()); plan.pk.len()];
    for key in &key_refs {
        let split = ddl::split_pk_key(&plan.pk, &plan.source_table, key)?;
        for (column, part) in parts.iter_mut().zip(split) {
            column.push(part.map(|p| p.into_owned()));
        }
    }
    let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&key_refs];
    for part in &parts {
        params.push(part);
    }
    let arrays: Vec<String> = plan
        .pk
        .iter()
        .enumerate()
        .map(|(i, column)| format!("${}::text[]::{}[]", i + 2, column.data_type))
        .collect();
    let columns: Vec<String> = (0..plan.pk.len())
        .map(|i| format!("__trellis_p{i}"))
        .collect();
    let typed = format!("unnest({}) as u({})", arrays.join(", "), columns.join(", "));
    let on = |alias: &str| -> String {
        plan.pk
            .iter()
            .enumerate()
            .map(|(i, column)| format!("{alias}.{} = u.__trellis_p{i}", quote_ident(&column.name)))
            .collect::<Vec<_>>()
            .join(" and ")
    };
    let pick = Pick {
        src_from: format!("join {typed} on {}", on("s")),
        target_where: format!("exists (select 1 from {typed} where {})", on("t")),
        keys: "$1".to_string(),
    };
    let outcome = rederive(txn, plan, None, &pick, &params, keys.len()).await?;
    tracing::debug!(
        target_table = %plan.target,
        scanned,
        rederived = keys.len(),
        written = outcome.written,
        deleted = outcome.deleted,
        "1-1 re-derive build sweep batch re-derived its stale entries"
    );
    Ok(SweepOutcome {
        scanned,
        rederived: keys.len(),
        delta_rows: 0,
        next,
        finished,
    })
}

/// A 1-1 chunk's or sweep batch's entry lock
/// (`one_to_one_ledger::lock_entries`, every key a Re-derive), under
/// [`CHUNK_LOCK_TIMEOUT`], which it sets for the lock alone, as
/// [`super::lock_chunk_entries`] does for an aggregate's.
async fn lock_chunk_entries(
    txn: &Transaction<'_>,
    plan: &OneToOnePlan,
    keys: &[&str],
) -> Result<(), ApplyError> {
    // Planted bug (#625 F3): the chunk's read-and-write runs with no entry
    // lock on the keys that had an entry, so it reads an entry a page is
    // between reading and writing. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let skip_lock = crate::plant::fires(crate::plant::Plant::ChunkWithoutEntryLock, true);
    #[cfg(not(any(test, feature = "test-util")))]
    let skip_lock = false;
    let mut sorted = keys.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let changes: Vec<EntryChange<'_>> = sorted
        .iter()
        .map(|key| EntryChange {
            key,
            apply: None,
            present: true,
        })
        .collect();
    let started = Instant::now();
    let locked = async {
        let previous: String = txn
            .query_one("select current_setting('lock_timeout')", &[])
            .await?
            .get(0);
        crate::locks::set_local_lock_timeout(txn, CHUNK_LOCK_TIMEOUT).await?;
        // The segment stamp is only for an Apply the insert settles; a
        // Re-derive's placeholder takes none.
        one_to_one_ledger::lock_entries(txn, &plan.target, &changes, 0, true, skip_lock).await?;
        txn.execute("select set_config('lock_timeout', $1, true)", &[&previous])
            .await?;
        Ok::<_, ApplyError>(())
    }
    .await;
    metrics::record_build_statement(BuildStatement::ChunkLock, started.elapsed());
    locked
}

/// How a chunk or a sweep batch picks its keys' rows in
/// [`rederive_statement`].
struct Pick {
    /// What follows `from <source> s` in the source read: the join and
    /// filter that pick the locked keys' rows.
    src_from: String,
    /// A predicate over the target aliased `t` that picks the locked keys'
    /// target rows.
    target_where: String,
    /// The locked keys' `text[]` parameter.
    keys: String,
}

/// The plan settings a 1-1 chunk's or sweep batch's statement runs under
/// (#625 F8a), so its plan doesn't hang on row estimates or statistics: no
/// sequential scan, so the ledger, the target and the source are each
/// reached through their key's index. Without it, the estimate of the
/// locked keys joined to their source rows (a join on the encoded key,
/// which has no statistics) ran to millions, and the entry rewrite hashed
/// the whole ledger for each chunk, so a chunk cost grew with the ledger.
/// [`CHUNK_PLAN_RESET`] puts it back after the statement.
const CHUNK_PLAN_SETTINGS: &str = "set local enable_seqscan = off";

/// Undoes [`CHUNK_PLAN_SETTINGS`] for the rest of the transaction.
const CHUNK_PLAN_RESET: &str = "set local enable_seqscan to default";

/// Runs `sql`, the chunk statement or an `explain` of it, under
/// [`CHUNK_PLAN_SETTINGS`], and puts the settings back after it: the one
/// place [`rederive`] and [`explain_chunk`] run it, so the plan a test
/// explains is the plan a chunk runs. An error leaves the setting on, but
/// it also aborts `txn`, and a `set local` ends with the transaction.
async fn query_under_chunk_plan(
    txn: &Transaction<'_>,
    sql: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error> {
    txn.batch_execute(CHUNK_PLAN_SETTINGS).await?;
    let rows = txn.query(sql, params).await?;
    txn.batch_execute(CHUNK_PLAN_RESET).await?;
    Ok(rows)
}

/// [`rederive_statement`] (`scope` `None`) or [`field_statement`] (`scope`
/// the field build's fields) for `plan`'s locked keys as `pick` picks them,
/// with the columns paused now left out and, when the target has a reader,
/// its prior images: the SQL, the image expression, and the seam's buffer.
async fn prepare_statement(
    txn: &Transaction<'_>,
    plan: &OneToOnePlan,
    scope: Option<&[String]>,
    pick: &Pick,
) -> Result<(Option<String>, Option<String>, TargetMutations), ApplyError> {
    let paused: HashSet<String> = txn
        .query(
            "select column_name from column_status where transform_table = $1",
            &[&plan.transform],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    let mut mutations = TargetMutations::new();
    let image = mutations.image_sql(txn, &plan.target, "t").await?;
    let sql = match scope {
        None => Some(rederive_statement(plan, pick, &paused, image.as_deref())),
        Some(fields) => field_statement(plan, pick, fields, &paused, image.as_deref()),
    };
    Ok((sql, image, mutations))
}

/// Runs [`rederive_statement`] (or, for a field build, [`field_statement`])
/// for `plan`'s locked keys, reports every
/// target row it changed to the target-mutation seam, and returns what it
/// did. `keys` is how many keys were locked.
async fn rederive(
    txn: &Transaction<'_>,
    plan: &OneToOnePlan,
    scope: Option<&[String]>,
    pick: &Pick,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    keys: usize,
) -> Result<OneToOneOutcome, ApplyError> {
    let (sql, image, mut mutations) = prepare_statement(txn, plan, scope, pick).await?;
    let Some(sql) = sql else {
        return Ok(OneToOneOutcome {
            keys,
            written: 0,
            deleted: 0,
        });
    };
    let started = Instant::now();
    let rows = query_under_chunk_plan(txn, &sql, params).await?;
    metrics::record_build_statement(BuildStatement::ChunkWrite, started.elapsed());
    // A `select` of scalar subqueries with no `from`: always one row.
    let row = rows.first().expect("the chunk statement returns one row");
    let written: i64 = row.get(1);
    let deleted: i64 = row.get(2);
    if image.is_some() {
        let changed: Option<Vec<String>> = row.get(3);
        let priors: Option<Vec<Option<String>>> = row.get(4);
        for (key, prior) in changed
            .unwrap_or_default()
            .into_iter()
            .zip(priors.unwrap_or_default())
        {
            mutations.record(&plan.target, key, prior, 0, None, None);
        }
        mutations.flush(txn).await?;
    }
    Ok(OneToOneOutcome {
        keys,
        written,
        deleted,
    })
}

/// A 1-1 chunk's or sweep batch's one read-and-write statement (see the
/// module doc, step 3). `paused` are the columns it leaves as they are.
/// With `image` (the target's prior-image expression over `t`, when the
/// target has a reader), it also returns every changed key and its prior
/// image, in key order, for the seam: each target row's writers all hold
/// its entry, so the statement's snapshot of the target is the row as it
/// stood before this transaction.
///
/// Returns one row: the entries rewritten, the target rows written, the
/// target rows deleted, and with `image` the changed keys and their prior
/// images.
fn rederive_statement(
    plan: &OneToOnePlan,
    pick: &Pick,
    paused: &HashSet<String>,
    image: Option<&str>,
) -> String {
    let q = |c: &str| quote_ident(c);
    let ledger = one_to_one_ledger::ledger_ident(&plan.target);
    let target = ddl::qualified_target_table_ident(&plan.target);
    let source = ddl::qualified_source_table(&plan.source_table);
    let k_src = ddl::pk_key_sql_expr(&plan.pk, Some("s"));
    let k_tgt = ddl::pk_key_sql_expr(&plan.pk, Some("t"));
    let pk_idents: Vec<String> = plan.pk.iter().map(|c| q(&c.name)).collect();
    let written: Vec<&(String, String)> = plan
        .fields
        .iter()
        .filter(|(name, _)| !paused.contains(name))
        .collect();
    let field_idents: Vec<String> = written.iter().map(|(name, _)| q(name)).collect();
    let mut src_cols: Vec<String> = pk_idents.iter().map(|c| format!("s.{c} as {c}")).collect();
    src_cols.extend(
        written
            .iter()
            .map(|(name, sql)| format!("{sql} as {}", q(name))),
    );
    let insert_cols: Vec<String> = pk_idents.iter().chain(&field_idents).cloned().collect();
    // Every column can be paused at once; a new key still gets its bare row.
    let on_conflict = if field_idents.is_empty() {
        "do nothing".to_string()
    } else {
        let sets: Vec<String> = field_idents
            .iter()
            .map(|f| format!("{f} = excluded.{f}"))
            .collect();
        let old: Vec<String> = field_idents.iter().map(|f| format!("t.{f}")).collect();
        let new: Vec<String> = field_idents
            .iter()
            .map(|f| format!("excluded.{f}"))
            .collect();
        format!(
            "do update set {} where ({}) is distinct from ({})",
            sets.join(", "),
            old.join(", "),
            new.join(", ")
        )
    };
    let (prior, seam) = match image {
        Some(image) => (
            format!(
                "prior as (select {k_tgt} as __k, {image}::text as __image \
                 from {target} t where {target_where}), ",
                target_where = pick.target_where
            ),
            ", (select array_agg(c.__k order by c.__k) from changed c), \
             (select array_agg(p.__image order by c.__k) \
              from changed c left join prior p on p.__k = c.__k)"
                .to_string(),
        ),
        None => (String::new(), String::new()),
    };
    format!(
        "with snap as ( \
             select pg_catalog.pg_current_snapshot() as __snap, \
                    (select max(seg_seq) from segments) as __seg \
         ), \
         src as ( \
             select {k_src} as __k, {src_cols} from {source} s {src_from} \
         ), \
         v as ( \
             select u.__k, r.__k is not null as __present \
             from unnest({keys}::text[]) as u(__k) left join src r on r.__k = u.__k \
         ), \
         {prior}\
         upd as ( \
             update {ledger} l set {basis} = snap.__snap, \
                 {seg} = greatest(l.{seg}, snap.__seg), {tombstone} = not v.__present \
             from v, snap \
             where l.{key} = v.__k and l.{key} = any({keys}::text[]) \
             returning l.{key} \
         ), \
         ups as ( \
             insert into {target} as t ({insert_cols}) \
             select {insert_cols} from src \
             on conflict ({pk_cols}) {on_conflict} \
             returning {k_tgt} as __k \
         ), \
         del as ( \
             delete from {target} t \
             where {target_where} \
               and {k_tgt} in (select __k from v where not __present) \
             returning {k_tgt} as __k \
         ), \
         changed as (select __k from ups union all select __k from del) \
         select (select count(*) from upd), (select count(*) from ups), \
                (select count(*) from del){seam}",
        src_cols = src_cols.join(", "),
        src_from = pick.src_from,
        keys = pick.keys,
        target_where = pick.target_where,
        basis = q(crate::defs::ledger::BASIS_COLUMN),
        seg = q(crate::defs::ledger::APPLIED_SEG_COLUMN),
        tombstone = q(crate::defs::ledger::TOMBSTONE_COLUMN),
        key = q(crate::defs::ledger::KEY_COLUMN),
        insert_cols = insert_cols.join(", "),
        pk_cols = pk_idents.join(", "),
    )
}

/// A field build's chunk statement ([`run_field_chunk`]): rewrites `fields`,
/// but for the `paused` ones, of each locked key's target row from its
/// source row, where they differ, through the target's key (see the module
/// doc's step 3 for the snapshot and the plan settings). It touches no
/// ledger entry, inserts no target row and deletes none.
///
/// Returns one row shaped as [`rederive_statement`]'s: 0 entries rewritten,
/// the target rows written, 0 deleted, and with `image` the changed keys and
/// their prior images, in key order, for the seam. `None` when every one of
/// `fields` is paused (or gone), so there is nothing to write.
fn field_statement(
    plan: &OneToOnePlan,
    pick: &Pick,
    fields: &[String],
    paused: &HashSet<String>,
    image: Option<&str>,
) -> Option<String> {
    let q = |c: &str| quote_ident(c);
    let target = ddl::qualified_target_table_ident(&plan.target);
    let source = ddl::qualified_source_table(&plan.source_table);
    let k_src = ddl::pk_key_sql_expr(&plan.pk, Some("s"));
    let k_tgt = ddl::pk_key_sql_expr(&plan.pk, Some("t"));
    let written: Vec<&(String, String)> = plan
        .fields
        .iter()
        .filter(|(name, _)| fields.contains(name) && !paused.contains(name))
        .collect();
    if written.is_empty() {
        return None;
    }
    let pk_idents: Vec<String> = plan.pk.iter().map(|c| q(&c.name)).collect();
    let field_idents: Vec<String> = written.iter().map(|(name, _)| q(name)).collect();
    let mut src_cols: Vec<String> = pk_idents.iter().map(|c| format!("s.{c} as {c}")).collect();
    src_cols.extend(
        written
            .iter()
            .map(|(name, sql)| format!("{sql} as {}", q(name))),
    );
    let sets: Vec<String> = field_idents
        .iter()
        .map(|f| format!("{f} = r.{f}"))
        .collect();
    let join: Vec<String> = pk_idents.iter().map(|c| format!("t.{c} = r.{c}")).collect();
    let old: Vec<String> = field_idents.iter().map(|f| format!("t.{f}")).collect();
    let new: Vec<String> = field_idents.iter().map(|f| format!("r.{f}")).collect();
    let (prior, seam) = match image {
        Some(image) => (
            format!(
                "prior as (select {k_tgt} as __k, {image}::text as __image \
                 from {target} t where {target_where}), ",
                target_where = pick.target_where
            ),
            ", (select array_agg(c.__k order by c.__k) from upd c), \
             (select array_agg(p.__image order by c.__k) \
              from upd c left join prior p on p.__k = c.__k)"
                .to_string(),
        ),
        None => (String::new(), String::new()),
    };
    Some(format!(
        "with src as ( \
             select {k_src} as __k, {src_cols} from {source} s {src_from} \
         ), \
         {prior}\
         upd as ( \
             update {target} t set {sets} \
             from src r \
             where {join} and {target_where} \
               and ({old}) is distinct from ({new}) \
             returning {k_tgt} as __k \
         ) \
         select 0::bigint, (select count(*) from upd), 0::bigint{seam}",
        src_cols = src_cols.join(", "),
        src_from = pick.src_from,
        target_where = pick.target_where,
        sets = sets.join(", "),
        join = join.join(" and "),
        old = old.join(", "),
        new = new.join(", "),
    ))
}
