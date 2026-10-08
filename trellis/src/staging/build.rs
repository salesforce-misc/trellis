//! The Re-derive build (#625; epic #556, ADR-0002 "A build is Re-derive over
//! chunks, and applies from its first chunk"): its primitives, a build chunk
//! ([`run_chunk`], and [`one_to_one::run_chunk`] for a 1-1 target) and the
//! group-delta merger ([`merge_deltas`]) (F1), and their scheduling (F2).
//! Since F3 it is the only build of the shapes it serves (see "Shapes"): a
//! fresh definition's and a resumed one's alike.
//!
//! # The scheduled build (F2, F3)
//!
//! - **Start** ([`start_ready_builds`]). The staging worker's reconcile pass
//!   starts each ready definition that [`qualifies`] instead of parking it a
//!   registration marker: once its source's capture gate is clear, one
//!   transaction moves it `waiting_to_backfill -> backfilling` with
//!   `transform_definitions.build = 'rederive'` and enqueues its plan job.
//!   From that commit it applies (`defs::model::APPLYING_SQL`, B1): Apply
//!   folds its source's changes into its ledger and groups, so nothing needs
//!   a catch-up. The start also records `build_seg`, the segment active at
//!   its commit, and a page re-derives rather than applies the keys of any
//!   batch at or below it (#733, see `start`). The backfill discharge never
//!   dispatches a definition that qualifies (`intake::markers`), and a
//!   resume leaves it to this start
//!   (`super::quarantine::resume_transform`), so each definition is on
//!   exactly one build path.
//! - **Rebuild** (F3). A resumed definition keeps its ledger, its groups and
//!   any group deltas still owed to them (B4), so its build is a Re-derive
//!   over its existing entries, with no truncate. Its start also enqueues a
//!   **sweep** job ([`run_sweep`]): once the plan job and every chunk are
//!   done, it walks the ledger and re-derives each live entry the chunks
//!   didn't (its `basis` is null or older than the start), which is a key
//!   deleted while the definition was frozen. A fresh build's ledger is
//!   empty at its start, so it gets no sweep.
//! - **Plan** ([`run_plan`]). A drain worker walks the source's primary key
//!   and enqueues `rederive` chunks of `ClientOptions::build_chunk_rows`
//!   rows, [`PLAN_BATCH`] per transaction (Q13). The walk and every chunk
//!   compare the key under the collations the walk started under, which the
//!   rows record (`backfill_chunks.key_collations`, #769), so re-collating
//!   the key mid-build doesn't reorder it under the ranges.
//! - **Work order** ([`work_once`], B6). A drain worker takes segments first,
//!   then a merge when a building target has deltas, then one chunk, and a
//!   chunk only under the ring's backlog bound ([`chunk_allowed`]). A chunk's
//!   entry lock gives up after [`CHUNK_LOCK_TIMEOUT`], and the chunk is
//!   retried after a short backoff without a charge.
//! - **Live** ([`try_complete`], B7). The flip is strict: the plan job is
//!   done, every chunk is done, and the delta table is empty, checked under
//!   the definition's row lock. No catch-up is parked.
//!
//! A pause freezes the build where it is: claims and merges skip a frozen
//! definition, and its deltas stay. A drop takes the chunk rows (`on delete
//! cascade`) and the delta table with the target.
//!
//! # The chunk
//!
//! A chunk re-derives every source key in a `(lo, hi]` range of the source's
//! primary key, in the caller's one transaction:
//!
//! 1. read the keys in the range, leaving out the ones quarantined for the
//!    definition (`poison`, per transform since #799), as the drain does;
//! 2. one statement ([`super::ledger::chunk_insert_statement`]) reads
//!    `pg_current_snapshot()`, the active segment and the keys' source rows,
//!    inserts the entry of every key that has none from them, in key order
//!    (`basis` := the snapshot, `applied_lsn` left null, a key with no row a
//!    tombstone stamped with the segment), and appends the new entries'
//!    per-group increments to `<target>__deltas`. A key whose entry exists
//!    by the time its insert runs is left to steps 3 and 4;
//! 3. lock the other keys' entries as a page does
//!    ([`super::ledger::lock_entries`]: a sorted `for update`);
//! 4. if there are any, one statement ([`super::ledger::chunk_statement`])
//!    reads the snapshot, the segment and their source rows afresh, rewrites
//!    their entries from them (`basis` := the snapshot, a tombstone's
//!    `applied_seg` raised to the segment, `applied_lsn` left alone, a key
//!    with no row a tombstone), and appends the moves' per-group increments.
//!
//! Steps 2 and 3 run under the short [`CHUNK_LOCK_TIMEOUT`], so a drain page
//! holding one of the keys makes the chunk give up rather than wait (ADR-0002
//! I7, #625 Q4). On a fresh build every key is new, and step 2 is the whole
//! chunk: each entry is written once, with no placeholder to rewrite (#723).
//!
//! The chunk never touches a group row, so two chunks, or a chunk and a
//! page, never wait on each other's groups (#617's failure mode). A key
//! inserted after step 1 isn't read, and its row is ignored: its insert's
//! change applies it. A chunk is idempotent: run again, it finds every entry
//! equal to the live row and appends nothing.
//!
//! Why a chunk and Apply agree: both hold a key's entry lock while they read
//! and write the entry. For a key that had an entry, the chunk's snapshot is
//! taken after its lock, so it sees every change an earlier Apply folded in.
//! A change applied after the chunk is either visible in the chunk's basis
//! (skipped, ADR-0002 I2) or not (applied over the entry the chunk wrote,
//! which is the snapshot's state). A key with no entry has contributed
//! nothing anywhere. The insert writes it from a snapshot taken before its
//! uniqueness check, which is right because no Apply can have written the
//! key since: an entry it wrote would be there for the check to find, and
//! the tombstone GC, the only thing that removes an entry an Apply wrote,
//! skips a ledger under a build (`super::retire::collect_tombstones`, #723).
//! A page deletes only the placeholders it wrote no change to, which hold no
//! applied change (`super::ledger::chunk_insert_statement`). An Apply that
//! wrote the key first counts it from nothing, and the chunk then locks it
//! and moves it by the difference. That needs the change's image to be the
//! key's latest state the definition hasn't seen, which only a change
//! committed after the start is sure to be: an older one, drained after the
//! start, may have had a later change drained before it, and a key deleted
//! that way has no row for a chunk to find. A page re-derives those keys
//! instead (`start`).
//!
//! # The merger
//!
//! [`merge_deltas`] claims up to `limit` delta rows of one merge partition,
//! oldest first through the claim key's index (`for update skip locked`),
//! deletes them, sums them per group and upserts the sums in group order,
//! all in one statement ([`super::ledger::merge_statement`]), with the same
//! upsert Apply uses. Then it deletes the groups whose every accumulator is
//! 0 and hands every written group to the target-mutation seam, as Apply
//! does ([`super::ledger::finish_groups`]). A merger locks only delta rows it
//! claims without waiting and group rows in group order, so it can't
//! deadlock with a page.
//!
//! **One merger per partition (#717).** Each delta row carries its group's
//! merge partition, a hash of the group under its columns' own equality,
//! generated by the delta table (`defs::ledger::DELTA_PART_COLUMN`), so all
//! of a group's rows are in one partition. A merger takes one partition's
//! transaction-scoped advisory lock without waiting, trying the partitions
//! with rows in turn, and skips the target if another transaction holds
//! every one of them. Mergers of different partitions write disjoint groups,
//! so they run side by side; two mergers of one partition would almost
//! always share a group, and the second would wait on the first's group row
//! for the rest of the first's transaction (the F2 profile measured that at
//! ~30% of build worker time on 8 workers, which is why F2b allowed one
//! merger per target). The skipping worker goes on to a chunk. Neither the
//! lock nor the skip changes anything [`try_complete`] reads: a merge in
//! flight holds its claimed rows deleted but uncommitted, so the table still
//! reads non-empty to everyone else until it commits.
//!
//! **The delta table is a queue (F2b).** Every row is deleted soon after
//! it's appended, so its heap and the claim key's index fill with dead rows
//! between vacuums, and its statistics say little about its contents. The
//! claim and the emptiness checks read through the index, the merge
//! statement runs with plan settings that don't depend on the statistics
//! ([`merge_deltas`]), and a merger vacuums the table every
//! [`VACUUM_EVERY`] rows it merges, so the walk past dead index entries
//! stays bounded by that, not by the build's length.
//!
//! **A failing merge (#901).** A merge applies whole groups, so its failure
//! names no key to charge, and it's classified by target instead
//! ([`fail_merge`]). A transient failure (a lost connection, a lock or
//! serialization conflict) is retried at once a few times, then backs the
//! target off, uncharged. A refused write (`42501`) halts like a drain's
//! (`super::halt`). Any other failure, such as an application's check or
//! deferred constraint that a merged group breaks, backs the target off and
//! is charged, and its [`chunk_queue::MAX_CHARGED_ATTEMPTS`]th charge halts
//! the definition's closure with kind `halt`, the target and the error on
//! `capture_failure`. The backoff and the charges are the drain worker's own
//! ([`MergeFailures`]). A target set aside this way doesn't hold up the
//! worker: [`work_once`] goes on to the next target's merge and to the
//! chunk claims.
//!
//! While a target is being built its groups have two channels, Apply and the
//! merger, so a group can be transiently partial or even negative. B5 (a
//! group is deleted only when every accumulator is 0) keeps such a group's
//! owed sums. Delta rows are discarded only with the ledger: a source
//! truncate (`super::ledger::truncate_ledger`), a drop, or the one-pass build
//! (#625 B4).
//!
//! # Shapes
//!
//! Every target [`super::ledger::route`] sends to the ledger: a plain
//! aggregate, its fields maintained by increments (`SUM`/`AVG` over an exact
//! argument, `COUNT`) or recomputed from the group's entries (`MIN`/`MAX`,
//! `BOOL_AND`/`BOOL_OR`, a float `SUM`/`AVG`, a composed field), its
//! arguments plain columns or expressions (#625 F5). An expression argument
//! is the SQL `defs::oracle` renders, which a chunk computes over the source
//! table and a page over its change's image cast to the source's row type.
//! A source that is another definition's target waits for #625 F6, and a
//! relationship-fed aggregate for F9. [`BuildPlan::load`] returns `None` for
//! anything else.
//!
//! **Plain 1-1 targets (F8a, [`one_to_one`]).** A 1-1 target with no
//! relationship path, on a captured source, takes the same scheduled build:
//! the start, the plan job, the sweep on a rebuild and the strict flip. Its
//! chunk writes the target rows themselves, ordered against pages by the
//! keys' entries on the 1-1 slim ledger (#623 D6), so it appends no group
//! deltas, its target has no delta table, and nothing merges for it. A
//! relationship-enriched 1-1 target waits for milestone E.
//!
//! **Field builds (F8b).** `ALTER TRANSFORM ... ADD`/`ALTER` and a column
//! resume rebuild only the fields they change, in the background
//! ([`start_field_build`]). The fields they change take in every field that
//! reads one of them by alias, directly or through others (issue #748,
//! `defs::eval::AliasReaders`), since its value moves with theirs; a column
//! pause holds such a reader out of Apply with the field it reads, so the
//! resume rebuilds both. Each chunk takes the readers in again from the
//! definition as it stands when the chunk is planned, so a reader an edit
//! added after the build was registered (of a field awaiting its capture,
//! which this build releases) is written too. The call's own transaction (#666: `apply` only
//! registers) moves the definition `live -> backfilling` with `build =
//! 'rederive'`, or leaves it `backfilling` when a build is already running,
//! and enqueues a plan job whose `backfill_chunks.fields` names the fields.
//! The field applies from that commit, as a whole build's definition does
//! (B1): no `column_status` row holds it out of Apply, so every page after
//! the commit writes it from its change's image, and nothing needs a
//! catch-up. The plan job enqueues chunks with the same scope, and the same
//! strict flip moves the definition `live` once they are all done, so the
//! definition reads `backfilling` until every field it was building is.
//!
//! - **The chunk** of a plain 1-1 target ([`one_to_one::run_field_chunk`])
//!   takes the keys' entry lock and then rewrites just the fields of their
//!   existing target rows from one snapshot, leaving the entries alone. A
//!   relationship-enriched 1-1 target, which only a column resume reaches
//!   (`ALTER` refuses a relationship path), takes a page's whole-row
//!   Re-derive of the range's keys instead (`apply::DirectRederive`), and
//!   reads their related rows after the entry lock, as it reads the rows
//!   (#832): a parent change that commits after that read stages a
//!   recompute whose page waits on the lock and writes after the chunk. An
//!   aggregate's fields are never held out of Apply, so neither call builds
//!   one.
//! - **The capture gate.** A field that reads a source column the source's
//!   capture may not image yet (#622) can't apply before capture images it:
//!   a page would meet rows staged without the column and quarantine their
//!   keys. `ALTER` holds such a field out of Apply with a `column_status`
//!   row marked `awaiting_capture`, and the plan job starts by waiting
//!   ([`field_build_ready`]): until the installed capture images every
//!   column the definition reads and the source's capture gate is clear, it
//!   gives its claim back for a later try. Then one transaction releases the
//!   field, and the walk begins after it, so every chunk's snapshot is
//!   taken after the field applies.
//!
//! **Recomputed fields (F5).** A chunk writes a delta row for every group an
//! entry it changed moved into or out of, even when the increments net to 0,
//! with whether a changed entry counted in the group before (`__out`) and
//! the changed entries that count in it now (`__keys`). The merger writes
//! every group a row names, and then rewrites the groups' recomputed fields
//! (`ledger::recompute_written`): a group whose rows only added entries,
//! and whose row the upsert found, folds those entries' current values into
//! each `MIN`/`MAX`/`BOOL_AND`/`BOOL_OR`; every other group is recomputed
//! from all of its entries, as a page does. B5 can delete a group's row
//! while entries it counted are still live (a page brought its accumulators
//! to 0 with a delta pending), so a row the merger's upsert creates is
//! always recomputed.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};

use tokio_postgres::{GenericClient, IsolationLevel, Transaction};

use crate::defs::ast::{TransformDef, ValueType};
use crate::defs::chunk_queue::{self, ChunkQueueError, ChunkWork, ClaimFence, ClaimedChunk};
use crate::defs::model::{Definition, TransformStatus};
use crate::defs::{catalog, ddl};
use crate::metrics::{self, BuildStatement};
use crate::pool::{Pool, quote_ident};

use super::apply::ApplyError;
use super::ledger::{self, LedgerTargetPlan, WrittenGroup};
use super::target_mutations::TargetMutations;

pub mod one_to_one;

/// How long a chunk waits for its entry lock before giving up (#625 Q4).
/// A drain page waits up to `locks::LOCK_TIMEOUT` for the same locks, so a
/// page never queues for long behind a chunk, and a chunk never holds up a
/// page that got there first. The caller retries the chunk later.
pub const CHUNK_LOCK_TIMEOUT: Duration = Duration::from_secs(1);

/// What a Re-derive build needs to know about one target.
#[derive(Debug, Clone)]
pub struct BuildPlan {
    ledger: LedgerTargetPlan,
}

impl BuildPlan {
    /// The plan for the definition whose target is `target` (its bare
    /// name, as `TRANSFORM <target>` names it), or `None` when there is no
    /// such definition or its target isn't one a Re-derive build serves yet
    /// (see the module doc's "Shapes").
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
        let Some(shape) = buildable_shape(definition) else {
            return Ok(None);
        };
        let pk = ddl::source_primary_key(pool, &definition.source_table).await?;
        let identity = {
            let client = pool.get().await?;
            ddl::identity_key_columns(&**client, &definition.target_table).await?
        };
        Ok(Some(Self {
            ledger: LedgerTargetPlan::new(
                &definition.target_table,
                &definition.source_table,
                pk,
                identity,
                shape,
            ),
        }))
    }
}

/// What one [`run_chunk`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkOutcome {
    /// The source keys the chunk found in its range, and re-derived.
    pub keys: usize,
    /// The group-delta rows it appended: per statement that wrote entries
    /// (the insert, and the rewrite of keys that had one), one per group
    /// whose increments weren't all 0.
    pub delta_rows: i64,
}

/// Runs one build chunk, `(lo, hi]` of the target's source primary key, in
/// `txn` (see the module doc). `lo` and `hi` are encoded keys, as
/// `backfill_chunks` stores a range's bounds; `lo` is `None` for the first
/// chunk. The caller commits.
///
/// `txn` must be `read committed`, as a page's is: step 4's snapshot has to
/// be its own statement's, taken after the entry lock (ADR-0002 I1), and
/// step 2's has to be taken after step 1's read. Under `repeatable read`
/// both would be the transaction's, from step 1, and the chunk could rewrite
/// an entry over a change a page applied between.
///
/// A lock wait past [`CHUNK_LOCK_TIMEOUT`] fails the chunk with `55P03`
/// (`crate::locks::is_lock_not_available`), and the caller rolls back and
/// retries it later. So does an entry that is gone by the time the chunk
/// locks it ([`ApplyError::LedgerEntryCollected`], #712), which the GC's skip
/// of a building ledger leaves to a chunk run outside a build (a test's).
/// Both are transient (`crate::staging::quarantine::classify`).
///
/// The range predicate is the 1-1 build's row comparison, which admits a key
/// with a `NULL` part when an earlier part decides it. Only a source keyed
/// by a nullable unique index (another aggregate's target) has such keys,
/// and the Re-derive build doesn't take seam-fed sources before #625 F6.
pub async fn run_chunk(
    txn: &Transaction<'_>,
    plan: &BuildPlan,
    lo: Option<&str>,
    hi: &str,
) -> Result<ChunkOutcome, ApplyError> {
    let ledger = &plan.ledger;
    let source_table = ledger.source_table();
    let pk = ledger.source_pk();
    let (range_where, lo, hi) = chunk_range(ledger, lo, hi)?;
    let mut params = crate::defs::backfill::range_params(&lo, &hi);

    // 1. The keys in the range, but for the ones quarantined for this
    // definition (#625 F-A5, per transform since #799): the drain leaves
    // such a key out of the definition's apply, and so does the build, until
    // `release_key` re-derives it.
    let started = Instant::now();
    let source_param = format!("${}", params.len() + 1);
    let target_param = format!("${}", params.len() + 2);
    let mut key_params = params.clone();
    key_params.push(&source_table);
    key_params.push(&ledger.target);
    let keys: Vec<String> = txn
        .query(
            &format!(
                "select {k} from {} s where {range_where} \
                 and not exists (select 1 from poison p \
                                 join transform_definitions d on d.id = p.transform_id \
                                 where p.src_table = {source_param} and p.key = {k} \
                                   and d.target_table = {target_param})",
                ddl::qualified_source_table(source_table),
                k = ddl::pk_key_sql_expr(pk, Some("s")),
            ),
            &key_params,
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    metrics::record_build_statement(BuildStatement::ChunkKeys, started.elapsed());
    if keys.is_empty() {
        return Ok(ChunkOutcome {
            keys: 0,
            delta_rows: 0,
        });
    }
    let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let keys_param = format!("${}", params.len() + 1);

    // 2 and 3, under the chunk's own short lock timeout: the entries of the
    // keys that have none, inserted from the chunk's read, and then the
    // entry lock of the others.
    let previous: String = txn
        .query_one("select current_setting('lock_timeout')", &[])
        .await?
        .get(0);
    crate::locks::set_local_lock_timeout(txn, CHUNK_LOCK_TIMEOUT).await?;
    // Test-only pause points (#723). See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::BeforeChunkInsert,
        &ledger.target,
    )
    .await?;
    let started = Instant::now();
    #[cfg(any(test, feature = "test-util"))]
    let pause = super::interleave::pause_in_statement(
        txn,
        super::interleave::PausePoint::AfterChunkSnapshot,
        &ledger.target,
    )
    .await?;
    #[cfg(not(any(test, feature = "test-util")))]
    let pause = None;
    let mut insert_params = params.clone();
    insert_params.push(&key_refs);
    let row = ledger::query_one_by_entry_key(
        txn,
        &ledger::chunk_insert_statement(ledger, &range_where, &keys_param, pause),
        &insert_params,
    )
    .await?;
    metrics::record_build_statement(BuildStatement::ChunkWrite, started.elapsed());
    if pause.is_some() {
        crate::locks::set_local_lock_timeout(txn, CHUNK_LOCK_TIMEOUT).await?;
    }
    let inserted: std::collections::HashSet<String> =
        row.get::<_, Vec<String>>(0).into_iter().collect();
    let mut delta_rows: i64 = row.get(1);
    let existing: Vec<&str> = key_refs
        .iter()
        .copied()
        .filter(|k| !inserted.contains(*k))
        .collect();
    let started = Instant::now();
    let locked = ledger::lock_entries(
        txn,
        ledger,
        &existing,
        ledger::NewEntries::None,
        skip_lock(),
    )
    .await;
    metrics::record_build_statement(BuildStatement::ChunkLock, started.elapsed());
    locked?;
    txn.execute("select set_config('lock_timeout', $1, true)", &[&previous])
        .await?;

    // 4. The read, the entries and the deltas of the keys that had an
    // entry, in one statement.
    if !existing.is_empty() {
        let started = Instant::now();
        params.push(&existing);
        let sql = ledger::chunk_statement(ledger, &range_where, &keys_param);
        let row = ledger::query_one_by_entry_key(txn, &sql, &params).await?;
        metrics::record_build_statement(BuildStatement::ChunkWrite, started.elapsed());
        delta_rows += row.get::<_, i64>(1);
    }
    // Test-only pause point (#623 D1), directly after the chunk's last
    // read-and-write statement. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterRederiveRead,
        &ledger.target,
    )
    .await?;
    tracing::debug!(
        target_table = %ledger.target,
        keys = keys.len(),
        delta_rows,
        "build chunk re-derived its range"
    );
    Ok(ChunkOutcome {
        keys: keys.len(),
        delta_rows,
    })
}

/// A chunk's `(lo, hi]` over `ledger`'s source key, `lo` and `hi` encoded
/// keys as [`run_chunk`] takes them: the range's predicate over the bare key
/// columns, binding `$1..$n`, and the decoded bounds its parameters
/// (`defs::backfill::range_params`) borrow.
#[allow(clippy::type_complexity)]
fn chunk_range(
    ledger: &LedgerTargetPlan,
    lo: Option<&str>,
    hi: &str,
) -> Result<(String, Option<Vec<String>>, Vec<String>), ApplyError> {
    let source_table = ledger.source_table();
    let pk = ledger.source_pk();
    let decode = |text: &str| -> Result<Vec<String>, ApplyError> {
        Ok(ddl::split_pk_key(pk, source_table, text)?
            .into_iter()
            .map(|part| part.map(|c| c.into_owned()).unwrap_or_default())
            .collect())
    };
    let lo = lo.map(decode).transpose()?;
    let hi = decode(hi)?;
    let pk_idents: Vec<String> = pk.iter().map(|c| quote_ident(&c.name)).collect();
    Ok((
        crate::defs::backfill::pk_range_where(&pk_idents, pk, &lo),
        lo,
        hi,
    ))
}

/// Whether a chunk's or a sweep batch's entry lock is skipped: the
/// `chunk_without_entry_lock` plant (#625 F3), under which the chunk's
/// read-and-write reads an entry a page is between reading and writing.
/// See `crate::plant`.
fn skip_lock() -> bool {
    #[cfg(any(test, feature = "test-util"))]
    return crate::plant::fires(crate::plant::Plant::ChunkWithoutEntryLock, true);
    #[cfg(not(any(test, feature = "test-util")))]
    false
}

/// A sweep batch's entry lock ([`ledger::lock_entries`], a placeholder for
/// a key with no entry), under [`CHUNK_LOCK_TIMEOUT`], which it sets for the
/// lock alone. Its time is recorded whether or not the lock is had: a batch
/// that gives up spent it waiting all the same.
async fn lock_sweep_entries(
    txn: &Transaction<'_>,
    ledger: &LedgerTargetPlan,
    keys: &[&str],
) -> Result<(), ApplyError> {
    let skip_lock = skip_lock();
    let started = Instant::now();
    let locked = async {
        let previous: String = txn
            .query_one("select current_setting('lock_timeout')", &[])
            .await?
            .get(0);
        crate::locks::set_local_lock_timeout(txn, CHUNK_LOCK_TIMEOUT).await?;
        ledger::lock_entries(
            txn,
            ledger,
            keys,
            ledger::NewEntries::Placeholders,
            skip_lock,
        )
        .await?;
        txn.execute("select set_config('lock_timeout', $1, true)", &[&previous])
            .await?;
        Ok::<_, ApplyError>(())
    }
    .await;
    metrics::record_build_statement(BuildStatement::ChunkLock, started.elapsed());
    locked
}

/// What one [`sweep_batch`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepOutcome {
    /// The ledger entries the batch read past, live or not.
    pub scanned: i64,
    /// The live entries among them the build hadn't re-derived, which the
    /// batch re-derived.
    pub rederived: usize,
    /// The group-delta rows it appended.
    pub delta_rows: i64,
    /// The last entry key it read past: the next batch's cursor. `None`
    /// when it read none.
    pub next: Option<String>,
    /// It read fewer than it was allowed: the sweep is done.
    pub finished: bool,
}

/// One batch of a rebuild's sweep (#625 F3; see the module doc), in `txn`,
/// which must be `read committed` as a chunk's is. The caller commits.
///
/// It reads up to `scan` ledger entries in key order after `cursor` (all of
/// them from the first with `None`), and picks the live ones the build
/// hasn't re-derived: those whose `basis` is null (written by Apply alone)
/// or is a snapshot taken before the build's start, `start_xid` (the
/// snapshot's `xmax` is at or before it). A key quarantined for the
/// definition (`poison`) is left as it is, as a chunk leaves it (#625 F-A5).
/// Then it locks them ([`lock_sweep_entries`]) and re-derives them in one
/// statement
/// ([`ledger::sweep_statement`]), which reads the source by each key: a key
/// with no row any more (deleted while the definition was frozen) becomes a
/// tombstone, and its group sheds it through the group deltas. The pick is
/// read before the lock and nothing relies on it after: a picked entry that
/// changed meanwhile is re-derived all the same, which is idempotent.
///
/// Reading a bounded window of entries rather than a bounded number of
/// picks keeps each statement short whatever the ledger holds (F-A8).
pub async fn sweep_batch(
    txn: &Transaction<'_>,
    plan: &BuildPlan,
    start_xid: &str,
    cursor: Option<&str>,
    scan: i64,
) -> Result<SweepOutcome, ApplyError> {
    let ledger = &plan.ledger;
    let key = quote_ident(crate::defs::ledger::KEY_COLUMN);
    let member = quote_ident(crate::defs::ledger::MEMBER_COLUMN);
    let tombstone = quote_ident(crate::defs::ledger::TOMBSTONE_COLUMN);
    let basis = quote_ident(crate::defs::ledger::BASIS_COLUMN);
    let after = if cursor.is_some() {
        format!("where {key} > $3")
    } else {
        "where $3::text is null".to_string()
    };
    let source_table = ledger.source_table();
    let started = Instant::now();
    let row = txn
        .query_one(
            &format!(
                "with w as ( \
                     select {key} as k, {member} and not {tombstone} \
                            and ({basis} is null \
                                 or pg_catalog.pg_snapshot_xmax({basis}) <= $1::text::xid8) \
                            as stale \
                     from {ledger_ident} {after} order by {key} limit $2 \
                 ) \
                 select (select count(*) from w), \
                        (select k from w order by k desc limit 1), \
                        array(select k from w where stale \
                                and not exists (select 1 from poison p \
                                                join transform_definitions d \
                                                  on d.id = p.transform_id \
                                                where p.src_table = $4 and p.key = w.k \
                                                  and d.target_table = $5) \
                              order by k)",
                ledger_ident = ledger.ledger_ident(),
            ),
            &[&start_xid, &scan, &cursor, &source_table, &ledger.target],
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
    lock_sweep_entries(txn, ledger, &key_refs).await?;
    let parts = ledger::sweep_key_params(ledger, &key_refs)?;
    let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&key_refs];
    for part in &parts {
        params.push(part);
    }
    let started = Instant::now();
    let row =
        ledger::query_one_by_entry_key(txn, &ledger::sweep_statement(ledger), &params).await?;
    metrics::record_build_statement(BuildStatement::ChunkWrite, started.elapsed());
    let delta_rows: i64 = row.get(1);
    tracing::debug!(
        target_table = %ledger.target,
        scanned,
        rederived = keys.len(),
        delta_rows,
        "re-derive build sweep batch re-derived its stale entries"
    );
    Ok(SweepOutcome {
        scanned,
        rederived: keys.len(),
        delta_rows,
        next,
        finished,
    })
}

/// What one [`merge_deltas`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergeOutcome {
    /// The merge partition it merged (#717), or `None` when it merged none:
    /// the table was empty, or every partition with rows was another
    /// merger's.
    pub partition: Option<i16>,
    /// The delta rows it claimed and deleted, all from `partition`. Fewer
    /// than the `limit` means none were left in that partition; other
    /// partitions may still have rows.
    pub claimed: i64,
    /// The groups it wrote and kept.
    pub written: usize,
    /// The groups it emptied and deleted.
    pub deleted: usize,
    /// The kept groups whose recomputed fields it folded from the entries
    /// that entered them (#625 F5; `ledger::recompute_written`).
    pub folded: usize,
    /// The kept groups whose recomputed fields it recomputed from all of
    /// their entries.
    pub recomputed: usize,
    /// Every partition with rows was being merged by another transaction,
    /// so this one claimed nothing and touched nothing (#625 F2b, #717).
    pub skipped: bool,
}

impl MergeOutcome {
    /// Nothing merged: `skipped` when other mergers held every partition
    /// with rows.
    fn nothing(skipped: bool) -> Self {
        Self {
            partition: None,
            claimed: 0,
            written: 0,
            deleted: 0,
            folded: 0,
            recomputed: 0,
            skipped,
        }
    }
}

/// The plan settings the merge statement runs under (#625 F2b; see
/// `ledger::merge_statement`), so its plan doesn't hang on the delta
/// table's statistics: no nested loop, so the upsert's result joins the
/// group sums by hash whatever the claim's row estimate, and no sequential
/// scan, so the claim walks the claim key's index. [`MERGE_PLAN_RESET`]
/// puts them back after the statement.
///
/// The claimed count's `left join ... on true` has no join method but a
/// nested loop, so the plan carries the planner's penalty for a disabled
/// node there (before PostgreSQL 18), and its total cost is about `1e10`
/// whatever the batch. The estimate decides only the plan's shape: Trellis's
/// sessions run with JIT off ([`crate::pool::JIT_OFF`]), so it can't make
/// Postgres compile the statement (#794).
const MERGE_PLAN_SETTINGS: &str = "set local enable_nestloop = off; set local enable_seqscan = off";

/// Undoes [`MERGE_PLAN_SETTINGS`] for the rest of the transaction.
const MERGE_PLAN_RESET: &str =
    "set local enable_nestloop to default; set local enable_seqscan to default";

/// Runs `sql`, the merge statement or an `explain` of it, binding `$1` to
/// `limit` and `$2` to the merge `partition`, under [`MERGE_PLAN_SETTINGS`],
/// and puts the settings back after it. The one place both [`merge_deltas`]
/// and [`explain_merge`] run it, so the plan a test explains is the plan a
/// merger runs. An error leaves the settings on, but it also aborts `txn`,
/// and a `set local` ends with the transaction.
async fn query_under_merge_plan(
    txn: &Transaction<'_>,
    sql: &str,
    limit: i64,
    partition: i16,
) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error> {
    txn.batch_execute(MERGE_PLAN_SETTINGS).await?;
    let rows = txn.query(sql, &[&limit, &partition]).await?;
    txn.batch_execute(MERGE_PLAN_RESET).await?;
    Ok(rows)
}

/// The plan [`merge_deltas`] would run for `limit` rows of merge partition
/// `partition`, as `explain`'s text, under the same settings, in `txn`. For
/// tests of the plan's shape.
#[cfg(any(test, feature = "internals"))]
pub async fn explain_merge(
    txn: &Transaction<'_>,
    plan: &BuildPlan,
    limit: i64,
    partition: i16,
) -> Result<String, ApplyError> {
    let sql = ledger::merge_statement(&plan.ledger, None, false);
    let rows = query_under_merge_plan(txn, &format!("explain {sql}"), limit, partition).await?;
    Ok(rows
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n"))
}

/// The plans of a build's two Re-derive statements over `keys` (encoded,
/// as a chunk's key read returns them), as `explain`'s text, each labelled,
/// under the settings a chunk and a sweep batch run them with (#778): the
/// chunk's over `(lo, hi]` ([`run_chunk`]) and the sweep batch's
/// ([`sweep_batch`]). For tests of the plans' shape. It locks and writes
/// nothing.
#[cfg(any(test, feature = "internals"))]
pub async fn explain_rederive(
    txn: &Transaction<'_>,
    plan: &BuildPlan,
    lo: Option<&str>,
    hi: &str,
    keys: &[&str],
) -> Result<Vec<(&'static str, String)>, ApplyError> {
    let ledger = &plan.ledger;
    let explain = |rows: Vec<tokio_postgres::Row>| {
        rows.iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let (range_where, lo, hi) = chunk_range(ledger, lo, hi)?;
    let mut params = crate::defs::backfill::range_params(&lo, &hi);
    let keys_param = format!("${}", params.len() + 1);
    let keys = keys.to_vec();
    params.push(&keys);
    let chunk = explain(
        ledger::query_by_entry_key(
            txn,
            &format!(
                "explain {}",
                ledger::chunk_statement(ledger, &range_where, &keys_param)
            ),
            &params,
        )
        .await?,
    );
    let parts = ledger::sweep_key_params(ledger, &keys)?;
    let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&keys];
    for part in &parts {
        params.push(part);
    }
    let sweep = explain(
        ledger::query_by_entry_key(
            txn,
            &format!("explain {}", ledger::sweep_statement(ledger)),
            &params,
        )
        .await?,
    );
    Ok(vec![("chunk statement", chunk), ("sweep statement", sweep)])
}

/// One statement: the merge partitions of the delta table `deltas` (quoted,
/// qualified) that have a row visible to it, in order (#717). A loose scan
/// of the claim index: one descent per partition with rows, and one more,
/// so it costs the same whatever the backlog, and needs no partition count
/// (`defs::ledger::DELTA_PARTITIONS` is baked into each table when it's
/// created).
fn backlogged_partitions_sql(deltas: &str) -> String {
    let part = quote_ident(crate::defs::ledger::DELTA_PART_COLUMN);
    let seq = quote_ident(crate::defs::ledger::DELTA_SEQ_COLUMN);
    format!(
        "with recursive p(part) as ( \
             (select {part} from {deltas} order by {part}, {seq} limit 1) \
             union all \
             select (select d.{part} from {deltas} d where d.{part} > p.part \
                     order by d.{part}, d.{seq} limit 1) \
             from p where p.part is not null \
         ) \
         select part from p where part is not null"
    )
}

/// Where the next [`merge_deltas`] in this process starts its look for a
/// free partition, so concurrent mergers start at different ones (#717).
static NEXT_PARTITION: AtomicUsize = AtomicUsize::new(0);

/// `partitions` rotated to start at the next merger's turn
/// ([`NEXT_PARTITION`]).
fn in_turn(mut partitions: Vec<i16>) -> Vec<i16> {
    if !partitions.is_empty() {
        let start = NEXT_PARTITION.fetch_add(1, Ordering::Relaxed) % partitions.len();
        partitions.rotate_left(start);
    }
    partitions
}

/// Takes the transaction-scoped advisory lock of merge partition `partition`
/// of the delta table `deltas` (quoted, qualified) without waiting, and
/// returns whether it got it (#717).
///
/// The key is two `int4`s, `(hashtext(deltas), -1 - partition)`. No other
/// production code takes a two-key advisory lock (Apply's key-order stripes
/// went with #623 D6). The only others are the test-only planted bugs'
/// (`crate::plant`), `(hashtext(target), hashtext(group key))` over a
/// target's unquoted name, and like this one they never wait. So a key that
/// two of them share, or two targets whose delta tables' names hash alike,
/// costs only parallelism: a merger skips a partition it can't lock. The
/// single-`bigint` locks (the producer's, `super::session`, and the test
/// pause points') are another key space altogether.
async fn try_lock_partition(
    txn: &Transaction<'_>,
    deltas: &str,
    partition: i16,
) -> Result<bool, tokio_postgres::Error> {
    Ok(txn
        .query_one(
            "select pg_try_advisory_xact_lock(hashtext($1), -1 - $2::int4)",
            &[&deltas, &i32::from(partition)],
        )
        .await?
        .get(0))
}

/// Folds up to `limit` of the target's group-delta rows into its groups, in
/// `txn` (see the module doc), all from one merge partition. The caller
/// commits.
///
/// At most one merger works on a partition at a time (#717): this lists the
/// partitions with rows, starting at a different one each call
/// ([`NEXT_PARTITION`]), and merges the first whose lock it takes without
/// waiting. A partition that another merger emptied between the list and
/// the lock is passed over (holding its lock to the end of `txn` costs
/// nothing: it has no rows to merge). If every partition with rows is
/// another merger's, it returns at once with [`MergeOutcome::skipped`] set
/// and nothing done. Mergers of different partitions write disjoint groups,
/// so they never wait on each other's group rows (the F2 profile measured
/// those waits at ~30% of build worker time when mergers shared groups).
///
/// One partition per transaction, not several: a statement upserts its
/// groups in group order, as a page does, so two statements in one
/// transaction could lock groups out of that order and deadlock with a page.
pub async fn merge_deltas(
    txn: &Transaction<'_>,
    plan: &BuildPlan,
    limit: i64,
) -> Result<MergeOutcome, ApplyError> {
    let deltas = &plan.ledger.deltas_ident;
    let partitions: Vec<i16> = txn
        .query(&backlogged_partitions_sql(deltas), &[])
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();
    let mut skipped = false;
    for partition in in_turn(partitions) {
        if !try_lock_partition(txn, deltas, partition).await? {
            skipped = true;
            continue;
        }
        let outcome = merge_partition(txn, plan, partition, limit).await?;
        if outcome.claimed > 0 {
            return Ok(outcome);
        }
    }
    Ok(MergeOutcome::nothing(skipped))
}

/// [`merge_deltas`]' work on one merge partition whose lock `txn` holds.
async fn merge_partition(
    txn: &Transaction<'_>,
    plan: &BuildPlan,
    partition: i16,
    limit: i64,
) -> Result<MergeOutcome, ApplyError> {
    let ledger = &plan.ledger;
    let mut mutations = TargetMutations::new();
    let image_columns = mutations.image_columns(txn, &ledger.target).await?;
    // Planted bug (#625 F3): the merger keeps the delta rows it applies, so
    // the next merge applies them again. Once per target per process, so
    // the build still ends (kept every time, the rows would be merged
    // forever): the first merge that claims a row. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let planted = if crate::plant::armed() == Some(crate::plant::Plant::MergeWithoutDelete) {
        let database: String = txn
            .query_one("select current_database()::text", &[])
            .await?
            .get(0);
        let name = format!("{database}.{}", ledger.deltas_ident);
        (!planted_merges().contains(&name)).then_some(name)
    } else {
        None
    };
    #[cfg(any(test, feature = "test-util"))]
    let keep_claimed = planted.is_some();
    #[cfg(not(any(test, feature = "test-util")))]
    let keep_claimed = false;
    let sql = ledger::merge_statement(ledger, image_columns.as_deref(), keep_claimed);
    let started = Instant::now();
    let rows = query_under_merge_plan(txn, &sql, limit, partition).await?;
    metrics::record_build_statement(BuildStatement::MergeUpsert, started.elapsed());
    let claimed: i64 = rows.first().map_or(0, |row| row.get(0));
    #[cfg(any(test, feature = "test-util"))]
    if let Some(name) = &planted
        && claimed > 0
    {
        planted_merges().insert(name.clone());
        crate::plant::fires(crate::plant::Plant::MergeWithoutDelete, true);
    }
    // Test-only pause point, after the merger's upsert, with its claimed
    // delta rows and its groups locked. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterGroupUpsert,
        &ledger.target,
    )
    .await?;
    let groups: Vec<WrittenGroup> = rows
        .iter()
        .filter_map(|row| WrittenGroup::from_row(row, 1))
        .collect();
    let started = Instant::now();
    let (folded, recomputed) = ledger::recompute_written(txn, ledger, &groups).await?;
    metrics::record_build_statement(BuildStatement::MergeRecompute, started.elapsed());
    let started = Instant::now();
    let (written, deleted) =
        ledger::finish_groups(txn, ledger, groups, &mut mutations, |_| (0, None, None)).await?;
    mutations.flush(txn).await?;
    metrics::record_build_statement(BuildStatement::MergeFinish, started.elapsed());
    Ok(MergeOutcome {
        partition: Some(partition),
        claimed,
        written,
        deleted,
        folded,
        recomputed,
        skipped: false,
    })
}

/// The targets (their database and delta table) a merge has kept its
/// claimed rows of in this process under the `merge_without_delete` plant
/// ([`merge_partition`]).
#[cfg(any(test, feature = "test-util"))]
fn planted_merges() -> std::sync::MutexGuard<'static, std::collections::HashSet<String>> {
    static MERGED: LazyLock<Mutex<std::collections::HashSet<String>>> =
        LazyLock::new(Mutex::default);
    MERGED.lock().unwrap_or_else(PoisonError::into_inner)
}

// ---------------------------------------------------------------------
// The scheduled build (#625 F2)
// ---------------------------------------------------------------------

/// Source rows per chunk unless the client says otherwise (#625 Q4).
pub const DEFAULT_CHUNK_ROWS: i64 = 10_000;

/// The most group-delta rows one merger pass claims (#625 Q4). A pass writes
/// at most this many groups.
pub const MERGE_BATCH: i64 = 5_000;

/// How many chunk boundaries the plan job commits per transaction (#625 Q13),
/// so the first chunks run while the walk goes on.
pub const PLAN_BATCH: usize = 100;

/// The `application_name` a build chunk's transaction runs under, so
/// `pg_stat_activity` tells its statements from a drain page's, which share
/// their text (`ledger::lock_entries`).
pub const CHUNK_APPLICATION_NAME: &str = "trellis build chunk";

/// The `application_name` a merger pass's transaction runs under.
pub const MERGE_APPLICATION_NAME: &str = "trellis build merge";

/// The `transform_definitions.build` word of a running Re-derive build.
const BUILD_REDERIVE: &str = "rederive";

/// The shape a Re-derive build serves, if `definition` has one (see the
/// module doc's "Shapes").
///
/// A relationship-fed target has none: with no relationships resolved,
/// [`ledger::route`] refuses one, so it keeps the one-pass build until #625
/// F9.
fn buildable_shape(definition: &Definition) -> Option<ledger::LedgerShape> {
    shape_of(&definition.def, &definition.source_columns)
}

/// [`buildable_shape`] of a definition's parts.
fn shape_of(
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
) -> Option<ledger::LedgerShape> {
    ledger::route(def, source_columns, &HashMap::new())
}

/// Whether the ledger of aggregate `def` carries its partial `GROUP BY`
/// index (`defs::ledger::aggregate_ledger_index_ddl`, #723). It does unless
/// the Re-derive build takes `def` ([`qualifies`], whose test this repeats:
/// `source_is_definition_target` is its source check) and its target has
/// no recomputed field. On that path nothing reads the ledger by group:
/// pages, chunks, the sweep, the merger and the tombstone GC read entries
/// by key, and only a recomputed field's statement reads a group's entries
/// (`super::ledger`'s `recompute_statement`). The old build and the resume's
/// orphan sweep read by group, and they run only for a definition this
/// path doesn't take. Leaving the index out spares every entry write its
/// maintenance, which at 100M entries cost most of the build's WAL as
/// full-page images of its leaves.
///
/// Decided once, when the ledger is created (`defs::ddl`), and again by the
/// old build, which drops and rebuilds the ledger's indexes
/// (`defs::backfill`).
///
/// If `ALTER TRANSFORM` ever edits an aggregate (`defs::catalog` refuses
/// that today), an edit that adds a recomputed field to a ledger without
/// this index must build the index as part of its background build, before
/// any recompute reads it.
pub(crate) fn ledger_indexes_groups(
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
    source_is_definition_target: bool,
) -> bool {
    match shape_of(def, source_columns) {
        Some(shape) if !source_is_definition_target => shape.recomputes(),
        _ => true,
    }
}

/// Whether a Re-derive build may take `definition` (#625 F2, F5, F8a): a
/// target the ledger maintains ([`buildable_shape`]) or a plain 1-1 target
/// ([`one_to_one::buildable`]), on a captured source. A
/// source that is another definition's target is fed by the target-mutation
/// seam, whose writer can commit after a chunk's snapshot; it needs a fence
/// first (#625 F6). Taking one also changes what a failing chunk's
/// narrowing must count: see `defs::chunk_queue::fail_chunk`'s Re-derive
/// arm.
pub async fn qualifies(
    client: &impl GenericClient,
    definition: &Definition,
) -> Result<bool, catalog::CatalogError> {
    if buildable_shape(definition).is_none() && !one_to_one::buildable(definition) {
        return Ok(false);
    }
    Ok(!catalog::is_definition_target(client, &definition.source_table).await?)
}

/// Starts a Re-derive build for every definition in `ready` that
/// [`qualifies`] (#625 F2). `ready` is what the staging worker's capture pass
/// found dispatchable (`capture::reconcile`): the definitions it would
/// otherwise park a registration marker for. Returns the ones the old build
/// path must leave alone: those started, and those waiting on their source's
/// capture gate.
///
/// The gate is the discharge's (`intake::markers`, Q2(a)): while any change
/// to the source at or below the gate its capture install or widen recorded
/// is still pending, a definition that started applying would meet rows
/// imaged without a column it reads. Such a definition is left
/// `waiting_to_backfill` for a later pass; nothing waits here.
///
/// A start is one transaction ([`start`]).
pub async fn start_ready_builds(
    client: &mut tokio_postgres::Client,
    pool: &Pool,
    ready: &[i64],
) -> Result<Vec<i64>, ApplyError> {
    let mut taken = Vec::new();
    for &id in ready {
        let Some(definition) = catalog::definition_by_id(pool, id).await? else {
            continue;
        };
        if definition.status != TransformStatus::WaitingToBackfill
            || !qualifies(&*client, &definition).await?
        {
            continue;
        }
        if capture_gate_holds(&*client, &definition.source_table).await? {
            tracing::debug!(
                definition_id = id,
                table = %definition.source_table,
                "re-derive build held by its source's capture gate"
            );
            taken.push(id);
            continue;
        }
        if awaits_capture(&*client, &definition).await? {
            tracing::debug!(
                definition_id = id,
                table = %definition.source_table,
                "re-derive build held until its source's capture images an edited field's column"
            );
            taken.push(id);
            continue;
        }
        if start(client, &definition).await? {
            taken.push(id);
        }
    }
    Ok(taken)
}

/// Whether `definition` has a field `ALTER TRANSFORM` holds out of Apply
/// until its source's capture images the field's column
/// (`column_status.awaiting_capture`, see the module doc's "Field builds")
/// and the installed capture doesn't image every column the definition
/// reads yet. A resumed definition can: its resume discards the field
/// build that would have released the field, and its rebuild's start
/// releases it instead ([`start`]), once this is false and the capture gate
/// is clear.
async fn awaits_capture(
    client: &impl GenericClient,
    definition: &Definition,
) -> Result<bool, ApplyError> {
    let awaiting: bool = client
        .query_one(
            "select exists (select 1 from column_status \
             where transform_table = $1 and awaiting_capture)",
            &[&definition.def.target],
        )
        .await?
        .get(0);
    if !awaiting {
        return Ok(false);
    }
    let read: std::collections::BTreeSet<String> =
        crate::defs::oracle::referenced_source_columns(&definition.def)
            .into_iter()
            .collect();
    Ok(!capture_images(client, &definition.source_table, &read).await?)
}

/// Releases `target`'s `awaiting_capture` pauses in `txn` (see the module
/// doc's "Field builds"), as only a build's start may: those the edit that
/// made them still owns are deleted, and one an operator pause, a fuse trip
/// or an upstream pause's cascade took over (issue #309) only stops
/// waiting, and stays paused for its own resume. With `fields`, only those.
async fn release_awaiting_capture(
    txn: &impl GenericClient,
    target: &str,
    fields: Option<&[String]>,
) -> Result<(), tokio_postgres::Error> {
    let fields: Option<Vec<String>> = fields.map(<[String]>::to_vec);
    txn.execute(
        "delete from column_status s \
         where s.transform_table = $1 and s.awaiting_capture \
           and ($2::text[] is null or s.column_name = any($2)) \
           and not s.local_fuse \
           and not exists ( \
               select 1 from column_pause_cascades c \
               where c.downstream_transform = s.transform_table \
                 and c.downstream_column = s.column_name)",
        &[&target, &fields],
    )
    .await?;
    txn.execute(
        "update column_status set awaiting_capture = false \
         where transform_table = $1 and awaiting_capture \
           and ($2::text[] is null or column_name = any($2))",
        &[&target, &fields],
    )
    .await?;
    Ok(())
}

/// Whether `table` has a capture gate a pending change still holds
/// (`pending_backfill.capture_gate_lsn`, as the discharge reads it).
async fn capture_gate_holds(client: &impl GenericClient, table: &str) -> Result<bool, ApplyError> {
    let gate: Option<crate::PgLsn> = client
        .query_opt(
            "select capture_gate_lsn from pending_backfill where table_name = $1",
            &[&table],
        )
        .await?
        .and_then(|row| row.get(0));
    Ok(match gate {
        Some(gate) => super::converge::table_changes_pending_through(client, table, gate).await?,
        None => false,
    })
}

/// Starts `definition`'s Re-derive build in one transaction (#625 F2, B1):
/// under its row lock, moves it `waiting_to_backfill -> backfilling` with
/// `build = 'rederive'` and enqueues its plan job. From that commit it
/// applies (`defs::model::APPLYING_SQL`): every page whose definition list
/// is read after it folds the source's changes into the ledger, and a
/// change committed before it is visible to every chunk's snapshot.
///
/// A definition whose ledger isn't empty (a resumed one, F3) also gets a
/// sweep job ([`run_sweep`]). Both rows carry the start's transaction id,
/// which the sweep's filter reads: every chunk's snapshot is taken after
/// the start commits, so its `xmax` is past that id.
///
/// **The start's segment (#733).** Pages drain batches out of order, so a
/// page that ran before the start can drain a key's later change while an
/// older one waits in an earlier batch, which a page drains after the
/// start. The later change never reaches the definition, and the older
/// one's image is stale: applied, it would count a key that a chunk then
/// can't find (it was deleted) back in for good. So the start also stores
/// `build_seg`, the segment active at its commit, read under a share lock
/// on `segment_pointer` that keeps seal phase 1 (`super::seal`) from moving
/// it until the start commits. That segment's fence is taken after the
/// start commits, so every change committed before the start is in a batch
/// at or below `build_seg`, and a page that drains one re-derives its keys
/// for the definition rather than applying their changes
/// ([`ledger::page_may_predate_build`]), on a ledger target and a 1-1
/// target alike. Every batch above it holds only
/// changes committed after the start, drained by pages that read the
/// definition list after it, so none is dropped.
///
/// Returns `false`, writing nothing, when the definition left
/// `waiting_to_backfill` meanwhile.
async fn start(
    client: &mut tokio_postgres::Client,
    definition: &Definition,
) -> Result<bool, ApplyError> {
    let txn = client.transaction().await?;
    // The column-pause lock, exclusive (#922): this start releases the
    // definition's `awaiting_capture` pauses ([`release_awaiting_capture`]),
    // which a pause, a resume and a define read and write. Before the
    // definition row, where every taker of the lock orders it
    // (`crate::locks::lock_column_pauses`); this start has no fence. A wait
    // that times out fails the start, and the capture pass takes it again.
    crate::locks::lock_column_pauses(
        &txn,
        crate::locks::ColumnPauseLock::Exclusive,
        crate::locks::ColumnPauseOp::Capture,
    )
    .await?;
    let status: Option<String> = txn
        .query_opt(
            "select status from transform_definitions where id = $1 for update",
            &[&definition.id],
        )
        .await?
        .map(|row| row.get(0));
    if status.as_deref() != Some(TransformStatus::WaitingToBackfill.as_str()) {
        txn.rollback().await?;
        return Ok(false);
    }
    let ledger = ddl::qualified_target_table_ident(&crate::defs::ledger::ledger_table_name(
        &definition.target_table,
    ));
    let empty: bool = txn
        .query_one(&format!("select not exists (select 1 from {ledger})"), &[])
        .await?
        .get(0);
    txn.execute(
        "update transform_definitions set status = $2, build = $3 where id = $1",
        &[
            &definition.id,
            &TransformStatus::Backfilling.as_str(),
            &BUILD_REDERIVE,
        ],
    )
    .await?;
    // The caller found any field awaiting capture ready (#625 F8b), so it
    // applies from this commit, and the build's chunks write it.
    release_awaiting_capture(&txn, &definition.def.target, None).await?;
    txn.execute(
        "insert into backfill_chunks (definition_id, kind, fuse_rearmed_at, start_xid) \
         select id, $2, fuse_rearmed_at, pg_current_xact_id() \
         from transform_definitions where id = $1",
        &[&definition.id, &chunk_queue::KIND_PLAN],
    )
    .await?;
    if !empty {
        txn.execute(
            "insert into backfill_chunks (definition_id, kind, fuse_rearmed_at, start_xid) \
             select id, $2, fuse_rearmed_at, pg_current_xact_id() \
             from transform_definitions where id = $1",
            &[&definition.id, &chunk_queue::KIND_SWEEP],
        )
        .await?;
    }
    // The start's segment (#733), last, so the share lock that keeps the
    // active segment from sealing until this commits is held only for the
    // commit. See this function's doc.
    txn.execute(
        "update transform_definitions set build_seg = p.active_seq \
         from (select active_seq from segment_pointer for share) p where id = $1",
        &[&definition.id],
    )
    .await?;
    txn.commit().await?;
    tracing::info!(
        definition_id = definition.id,
        target = %definition.target_table,
        from = %TransformStatus::WaitingToBackfill.as_str(),
        to = %TransformStatus::Backfilling.as_str(),
        rebuild = !empty,
        "transform status transition: re-derive build started"
    );
    Ok(true)
}

/// Whether a definition read as `status` with `build` can take a field
/// build ([`start_field_build`], #625 F8b): one that applies. That is a
/// `live` one, a `catching_up` one (whose catch-up's discharge then hands it
/// to this build rather than flipping it `live` while field chunks remain,
/// `intake::markers`' `go_live_caught_up`), or a `backfilling` one under a
/// Re-derive build, which the field build joins. A definition behind an
/// old-path build, or frozen, doesn't apply.
pub(crate) fn takes_field_build(status: TransformStatus, build: Option<&str>) -> bool {
    matches!(status, TransformStatus::Live | TransformStatus::CatchingUp)
        || (status == TransformStatus::Backfilling && build == Some(BUILD_REDERIVE))
}

/// Starts a field build of `fields` on definition `id` in `txn`, the
/// caller's transaction (see the module doc's "Field builds"): moves a
/// `live` definition to `backfilling` with `build = 'rederive'` (one already
/// building, or `catching_up`, stays as it is) and enqueues a plan job
/// scoped to `fields`. The caller holds the definition's row `for update`
/// and has checked
/// [`takes_field_build`] against what it read under that lock (`status`),
/// and commits. From that commit the definition applies as before, the
/// fields included unless a `column_status` row holds one out, and it reads
/// `backfilling` until the build's last chunk is done ([`try_complete`]).
pub(crate) async fn start_field_build(
    txn: &impl GenericClient,
    id: i64,
    status: TransformStatus,
    fields: &[String],
) -> Result<(), tokio_postgres::Error> {
    if status == TransformStatus::Live {
        txn.execute(
            "update transform_definitions set status = $2, build = $3 where id = $1",
            &[&id, &TransformStatus::Backfilling.as_str(), &BUILD_REDERIVE],
        )
        .await?;
    }
    txn.execute(
        "insert into backfill_chunks (definition_id, kind, fuse_rearmed_at, start_xid, fields) \
         select id, $2, fuse_rearmed_at, pg_current_xact_id(), $3 \
         from transform_definitions where id = $1",
        &[&id, &chunk_queue::KIND_PLAN, &fields],
    )
    .await?;
    let to = if status == TransformStatus::Live {
        TransformStatus::Backfilling
    } else {
        status
    };
    tracing::info!(
        definition_id = id,
        from = %status.as_str(),
        to = %to.as_str(),
        ?fields,
        "transform status transition: field build registered"
    );
    Ok(())
}

/// What a field build's plan job found at its start ([`field_build_ready`]).
enum FieldStart {
    /// Every field applies: the walk may begin.
    Go,
    /// A field still waits for its source's capture to image a column it
    /// reads; the job gives its claim back for a later try.
    Wait,
    /// The job's claim no longer holds.
    Superseded,
}

/// How long a field build's plan job waits before it looks at its source's
/// capture again ([`field_build_ready`]).
const FIELD_CAPTURE_RETRY: Duration = Duration::from_millis(500);

/// A field build's start (see the module doc's "Field builds"): releases
/// the `awaiting_capture` pauses `ALTER TRANSFORM` put on `fields` once the
/// definition's source is ready for them, in one transaction fenced by the
/// plan job's claim. Ready means what the backfill discharge required of a
/// registration (`intake::markers`): the installed capture images every
/// source column the definition reads ([`capture_images`]),
/// and no change at or below the source's capture gate is still pending
/// ([`capture_gate_holds`]), so no row staged without a column is left for a
/// page to meet.
///
/// A pause the edit no longer owns (an operator pause or a fuse trip
/// upgraded it to `local_fuse`, or an upstream pause cascaded onto it,
/// issue #309) only stops waiting: the field stays out of Apply and out of
/// the build's chunks until its own resume.
///
/// The release bumps the version fence ([`bump_version_fence`]) as its
/// transaction's first lock, so its wait for the pages in flight holds
/// nothing a page could be waiting on (issue #744). A look under the claim
/// alone comes first, so a call that has nothing to release, or whose
/// capture isn't ready, doesn't bump the fence; the release then looks again
/// under its locks.
async fn field_build_ready(
    pool: &Pool,
    definition: &Definition,
    fields: &[String],
    fence: &ClaimFence<'_>,
) -> Result<FieldStart, ChunkQueueError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    if !fence.hold(&*txn).await? {
        txn.rollback().await?;
        return Ok(FieldStart::Superseded);
    }
    let release = capture_release(&txn, definition, fields, false).await?;
    txn.rollback().await?;
    match release {
        CaptureRelease::Nothing => return Ok(FieldStart::Go),
        CaptureRelease::NotReady => return Ok(FieldStart::Wait),
        CaptureRelease::Ready => {}
    }

    let txn = client.transaction().await?;
    // A page that read the paused fields before this commit must not apply
    // after it. The claim's idle timeout goes first: from the bump on, a
    // stalled worker holds up every page of the source, not just its chunk.
    fence.arm(&*txn).await?;
    bump_version_fence(&*txn, &definition.source_table).await?;
    // The column-pause lock, exclusive (#922), after the fence wait and
    // before the claim and the rows: the release below writes pause state
    // (`release_awaiting_capture`). See `crate::locks::lock_column_pauses`.
    // A wait that times out fails this job's attempt, which retries.
    crate::locks::lock_column_pauses(
        &*txn,
        crate::locks::ColumnPauseLock::Exclusive,
        crate::locks::ColumnPauseOp::Capture,
    )
    .await?;
    if !fence.hold(&*txn).await? {
        txn.rollback().await?;
        return Ok(FieldStart::Superseded);
    }
    match capture_release(&txn, definition, fields, true).await? {
        CaptureRelease::Nothing => {
            txn.rollback().await?;
            return Ok(FieldStart::Go);
        }
        CaptureRelease::NotReady => {
            txn.rollback().await?;
            return Ok(FieldStart::Wait);
        }
        CaptureRelease::Ready => {}
    }
    release_awaiting_capture(&*txn, &definition.def.target, Some(fields)).await?;
    txn.commit().await?;
    tracing::info!(
        definition_id = definition.id,
        ?fields,
        "field build's capture is ready; its fields apply from here"
    );
    Ok(FieldStart::Go)
}

/// What [`capture_release`] found for a field build's `fields`.
enum CaptureRelease {
    /// None of them awaits its capture.
    Nothing,
    /// One does, and the source's capture isn't ready for it yet.
    NotReady,
    /// One does, and the capture is ready: release them.
    Ready,
}

/// Whether [`field_build_ready`] has `fields`' `awaiting_capture` pauses to
/// release, reading them `for update` when `lock`.
async fn capture_release(
    txn: &Transaction<'_>,
    definition: &Definition,
    fields: &[String],
    lock: bool,
) -> Result<CaptureRelease, ChunkQueueError> {
    let lock = if lock { " for update" } else { "" };
    let awaiting = txn
        .query(
            &format!(
                "select column_name from column_status \
                 where transform_table = $1 and column_name = any($2) and awaiting_capture{lock}"
            ),
            &[&definition.def.target, &fields],
        )
        .await?;
    if awaiting.is_empty() {
        return Ok(CaptureRelease::Nothing);
    }
    let read: std::collections::BTreeSet<String> =
        crate::defs::oracle::referenced_source_columns(&definition.def)
            .into_iter()
            .collect();
    let images = capture_images(txn, &definition.source_table, &read)
        .await
        .map_err(build_error)?;
    if !images
        || capture_gate_holds(txn, &definition.source_table)
            .await
            .map_err(build_error)?
    {
        return Ok(CaptureRelease::NotReady);
    }
    Ok(CaptureRelease::Ready)
}

/// Bumps `source_table`'s version fence (`source_table_versions`) in `txn`,
/// as `ALTER TRANSFORM` does, for a commit that releases a 1-1 field's
/// `column_status` pause into a field build: a column resume, or the field
/// build's capture release (#625 F8b). A column pause bumps it too, for the
/// opposite direction (`quarantine::bump_pause_fence`, issue #903).
///
/// A page reads the paused columns in Phase 2 and leaves them out of its
/// writes. One that read them before the release must not apply after it:
/// it would write a key's other columns from its change and leave the
/// field as the page of an older change wrote it, computed after the
/// release and drained first, out of order. I2 passes both (the older
/// change's `lsn` is below the newer one's), the build's chunk for the key
/// may have run already, and no catch-up follows. The bump waits for every
/// page holding the fence `for share` (Phase 3 holds it to its commit), and
/// a page that reaches the fence after the commit misses it and computes
/// again, with the field.
///
/// Callers bump it as their transaction's first lock, as `ALTER TRANSFORM`
/// does (issue #744). A page takes the fence before any other lock, so a
/// bump that holds nothing while it waits can't close a cycle; one that
/// holds the definition row can, through a page queued on a third
/// transaction that wants that row.
pub(crate) async fn bump_version_fence(
    txn: &impl GenericClient,
    source_table: &str,
) -> Result<(), tokio_postgres::Error> {
    txn.execute(
        "insert into source_table_versions (source_table, version) values ($1, 1) \
         on conflict (source_table) \
         do update set version = source_table_versions.version + 1",
        &[&source_table],
    )
    .await?;
    Ok(())
}

/// Whether every row staged for `table` from now on carries `columns`
/// ([`field_build_ready`]): `table` is another definition's target, fed by
/// the target-mutation seam rather than captured; or nothing is installed on
/// it, so no row lacking a column can exist and the install, from a catalog
/// read after the edit, images them all; or its installed functions are
/// current and image every one of `columns`.
async fn capture_images(
    client: &impl GenericClient,
    table: &str,
    columns: &std::collections::BTreeSet<String>,
) -> Result<bool, ApplyError> {
    use crate::capture::{CaptureError, install::Installed};
    use crate::intake::IntakeError;

    if catalog::is_definition_target(client, table).await? {
        return Ok(true);
    }
    let schema: String = client
        .query_one("select pg_catalog.current_schema()::text", &[])
        .await?
        .get(0);
    let installed = crate::capture::install::installed(client, &schema, table)
        .await
        .map_err(|err| match err {
            CaptureError::Db(err) => ApplyError::from(err),
            CaptureError::Catalog(err) => ApplyError::from(err),
            CaptureError::Marker(err) => ApplyError::from(err),
            other => ApplyError::from(IntakeError::InvalidTableName(other.to_string())),
        })?;
    Ok(match installed {
        Installed::Absent => true,
        Installed::Partial { .. } => false,
        Installed::Complete { spec, current } => {
            current && columns.iter().all(|c| spec.columns().contains(c))
        }
    })
}

/// Gives `chunk`'s claim back without a charge, to be claimed again after
/// `delay` (a field build's plan job waiting on capture). A claim already
/// reclaimed from `claimed_by` is left to its new holder.
async fn defer_claim(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
    delay: Duration,
) -> Result<(), ChunkQueueError> {
    let client = pool.get().await?;
    client
        .execute(
            "update backfill_chunks \
             set claimed_by = null, claimed_at = null, \
                 next_attempt_at = now() + make_interval(secs => $3) \
             where id = $1 and claimed_by = $2",
            &[&chunk.id, &claimed_by, &delay.as_secs_f64()],
        )
        .await?;
    Ok(())
}

/// How a drain worker runs the Re-derive build's work ([`work_once`]).
#[derive(Debug, Clone, Copy)]
pub struct WorkerOptions {
    /// Source rows per chunk the plan job enqueues
    /// (`ClientOptions::build_chunk_rows`).
    pub chunk_rows: i64,
    /// `ClientOptions::drain_batch_cap`: a chunk is claimed only while the
    /// sealed, undrained rows are under twice this (#625 Q4).
    pub drain_batch_cap: usize,
    /// How often a claimed plan job or chunk refreshes its claim.
    pub heartbeat_interval: Duration,
    /// The fleet's reclaim TTL ([`ClaimFence`]'s idle bound).
    pub reclaim_ttl: Duration,
}

/// What one [`work_once`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// No Re-derive build is running, or none had work for this worker.
    Idle,
    /// The ring's backlog held the build's chunks back (#625 B6).
    Backpressure,
    /// Folded one batch of group deltas.
    Merged,
    /// Ran (some of) a plan job.
    Planned,
    /// Ran one chunk, or a sweep job's batches.
    Chunk,
    /// Moved a finished build's definition to `live`.
    Completed,
}

impl Step {
    /// Whether the step did work, so the caller loops straight back.
    pub fn progressed(self) -> bool {
        !matches!(self, Step::Idle | Step::Backpressure)
    }
}

/// One step of the Re-derive builds' work for a drain worker, which runs it
/// after its segments (#625 B6): a merge when a building target has group
/// deltas, else a plan job, else one chunk or a sweep batch run if the
/// ring's backlog allows ([`chunk_allowed`]). A building definition with
/// nothing left to do is moved to `live` ([`try_complete`]).
///
/// A definition that is frozen gets nothing: neither its deltas merged nor
/// its work claimed. Its deltas stay (#625 B4).
///
/// A merge that fails is given up through [`fail_merge`], and the step goes
/// on to the next target and the claims (#901). A target backing off after
/// a failed merge in `merges` is skipped until its backoff ends.
pub async fn work_once(
    pool: &Pool,
    claimed_by: &str,
    options: &WorkerOptions,
    merges: &mut MergeFailures,
) -> Result<Step, ChunkQueueError> {
    let building = building(pool).await?;
    merges.retain(|id| building.iter().any(|(building, ..)| *building == id));
    if building.is_empty() {
        return Ok(Step::Idle);
    }
    for (id, target, has_delta_table) in &building {
        // A target whose every partition with rows another worker is
        // merging is skipped (#625 F2b, #717): a pass that claims none falls
        // through to the next target, then to a chunk. A 1-1 target has no
        // delta table, so nothing merges for it (#625 F8a).
        if !*has_delta_table || merges.backing_off(*id) || !has_deltas(pool, target).await? {
            continue;
        }
        match merge_retrying(pool, *id).await {
            Ok(0) => {}
            Ok(_) => {
                merges.succeeded(*id);
                return Ok(Step::Merged);
            }
            Err(err) => fail_merge(pool, *id, target, &err, merges).await,
        }
    }

    let claimed = {
        let client = pool.get().await?;
        chunk_queue::claim_chunks_of(&**client, claimed_by, 1, &[chunk_queue::KIND_PLAN]).await?
    };
    if let Some(chunk) = claimed.into_iter().next() {
        run_claimed(pool, &chunk, claimed_by, options).await;
        return Ok(Step::Planned);
    }

    let allowed = {
        let client = pool.get().await?;
        chunk_allowed(&**client, options.drain_batch_cap).await?
    };
    if allowed {
        let claimed = {
            let client = pool.get().await?;
            chunk_queue::claim_chunks_of(
                &**client,
                claimed_by,
                1,
                &[chunk_queue::KIND_REDERIVE, chunk_queue::KIND_SWEEP],
            )
            .await?
        };
        if let Some(chunk) = claimed.into_iter().next() {
            run_claimed(pool, &chunk, claimed_by, options).await;
            return Ok(Step::Chunk);
        }
    }

    // Nothing to claim: a build whose last chunk and merge committed may be
    // done. Each of those checks after its own commit; this catches a worker
    // that died between the two.
    let mut completed = false;
    for (id, ..) in &building {
        completed |= try_complete(pool, *id).await?;
    }
    Ok(if completed {
        Step::Completed
    } else if allowed {
        Step::Idle
    } else {
        Step::Backpressure
    })
}

/// Every definition a Re-derive build is running for that isn't frozen,
/// with its target and whether the target has a group-delta table to merge
/// (a 1-1 target has none, #625 F8a), in id order: each `backfilling` one
/// under a Re-derive build, and each `catching_up` one with a field build's
/// chunk left (#625 F8b), which its catch-up hands to the Re-derive build
/// once it discharges.
async fn building(pool: &Pool) -> Result<Vec<(i64, String, bool)>, ChunkQueueError> {
    let client = pool.get().await?;
    Ok(client
        .query(
            &format!(
                "select d.id, d.target_table, {} from transform_definitions d \
                 where (d.build = $1 and d.status = $2) \
                    or (d.status = $3 and exists ( \
                        select 1 from backfill_chunks bc \
                        where bc.definition_id = d.id and not bc.done \
                          and bc.fields is not null and not ({}))) \
                 order by d.id",
                deltas_exist_sql("d.target_table"),
                chunk_queue::STALE,
            ),
            &[
                &BUILD_REDERIVE,
                &TransformStatus::Backfilling.as_str(),
                &TransformStatus::CatchingUp.as_str(),
            ],
        )
        .await?
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect())
}

/// SQL over a `transform_definitions` row: whether the target `column`
/// names has a group-delta table. Only a ledger aggregate's target has one;
/// a 1-1 target's build writes its rows directly (#625 F8a).
fn deltas_exist_sql(column: &str) -> String {
    format!(
        "pg_catalog.to_regclass(pg_catalog.format('%I.%I', \
             split_part({column}, '.', 1), split_part({column}, '.', 2) || '{}')) is not null",
        crate::defs::ledger::DELTAS_SUFFIX
    )
}

/// Whether `target`'s group-delta table has a row. A table dropped since the
/// caller listed it has none.
async fn has_deltas(pool: &Pool, target: &str) -> Result<bool, ChunkQueueError> {
    let deltas = ddl::qualified_target_table_ident(&crate::defs::ledger::deltas_table_name(target));
    let client = pool.get().await?;
    match client.query_one(&any_delta_sql(&deltas), &[]).await {
        Ok(row) => Ok(row.get(0)),
        Err(err) if super::quarantine::is_undefined_table(&err) => Ok(false),
        Err(err) => Err(err.into()),
    }
}

/// Whether a drain worker may claim a build chunk now (#625 B6, Q4): the
/// sealed, undrained rows are under twice `drain_batch_cap`, and the ring
/// has a free slot for the next seal (the seal's own refusal predicate,
/// `append::ring_slot_is_free`). A chunk claimed past either would compete
/// with the drains the backlog is waiting on, or hold off the seal.
pub async fn chunk_allowed(
    client: &impl GenericClient,
    drain_batch_cap: usize,
) -> Result<bool, ChunkQueueError> {
    let bound = i64::try_from(drain_batch_cap.max(1))
        .unwrap_or(i64::MAX / 2)
        .saturating_mul(2);
    Ok(client
        .query_one(
            "select coalesce((select sum(row_count)::bigint from segments \
                     where state in ('sealed', 'draining') \
                       and drained_mask <> ((1::bigint << bucket_count) - 1)), 0) < $1 \
                 and not exists (select 1 from segments \
                     where ring_slot = ((select ring_slot from segment_pointer) + 1) % $2)",
            &[&bound, &i32::from(super::append::RING_SIZE)],
        )
        .await?
        .get(0))
}

/// Runs a claimed plan job or chunk ([`chunk_queue::claim_chunks_of`]), and
/// gives it up through [`chunk_queue::fail_chunk`] if it fails, which
/// records, logs and backs it off (#616). [`work_once`] claims and runs one;
/// a test can claim one by hand and run it here.
pub async fn run_claimed(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
    options: &WorkerOptions,
) {
    let ran = match &chunk.work {
        ChunkWork::Plan { cursor } => {
            run_plan(pool, chunk, claimed_by, cursor.clone(), options).await
        }
        ChunkWork::Rederive { lo, hi } => {
            run_rederive(pool, chunk, claimed_by, lo.as_deref(), hi, options).await
        }
        ChunkWork::Sweep { cursor } => {
            run_sweep(pool, chunk, claimed_by, cursor.clone(), options).await
        }
        work => Err(ChunkQueueError::UnknownKind {
            kind: work.kind().to_string(),
        }),
    };
    let Err(err) = ran else {
        return;
    };
    if crate::locks::is_lock_not_available(&err) {
        metrics::increment_build_chunk_lock_timeouts();
    }
    if let Err(fail_err) = chunk_queue::fail_chunk(pool, chunk, claimed_by, &err).await {
        tracing::warn!(
            definition_id = chunk.definition_id,
            chunk_id = chunk.id,
            error = %err,
            record_error = %fail_err,
            "re-derive build step failed, and recording the failure failed too; the \
             stale-claim sweep frees it for a retry"
        );
    }
}

fn build_error(err: impl Into<ApplyError>) -> ChunkQueueError {
    ChunkQueueError::Build(Box::new(err.into()))
}

/// Runs one claimed Re-derive chunk (#625 F2): [`run_chunk`] and the chunk's
/// done mark in one `read committed` transaction (ADR-0002 I1: the chunk's
/// snapshot must be its read statement's own), fenced by its claim
/// ([`ClaimFence`]). The done mark commits with the entries and deltas it
/// stands for, so a chunk that dies before its commit runs again whole, and
/// one that committed is never run again. Then [`try_complete`].
async fn run_rederive(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
    lo: Option<&str>,
    hi: &str,
    options: &WorkerOptions,
) -> Result<(), ChunkQueueError> {
    let setup_started = Instant::now();
    // Before anything the plan is made from ([`plan_epoch`]).
    let epoch = plan_epoch(&**pool.get().await?, chunk.definition_id).await?;
    let definition = catalog::definition_by_id(pool, chunk.definition_id)
        .await?
        .ok_or(ChunkQueueError::DefinitionNotFound {
            definition_id: chunk.definition_id,
        })?;
    // The range is read under the key collations its plan job walked under
    // (issue #769), whatever the key's collation is now.
    let key_collations = chunk.key_collations.as_deref();
    let plan = match &chunk.fields {
        None => {
            let mut plan = AnyPlan::for_definition(pool, &definition).await?;
            plan.pin_key_collations(key_collations);
            ChunkPlan::Whole(plan)
        }
        Some(fields) => {
            FieldPlan::for_chunk(pool, &definition, fields, lo, hi, key_collations).await?
        }
    };
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        &**pool.get().await?,
        super::interleave::PausePoint::BeforeChunkTransaction,
        &definition.target_table,
    )
    .await?;
    let _heartbeat = chunk_queue::ChunkHeartbeat::spawn(
        pool.clone(),
        chunk.id,
        claimed_by.to_string(),
        options.heartbeat_interval,
    );
    let fence = ClaimFence::new(chunk.id, claimed_by, options.reclaim_ttl);
    let mut client = pool.get().await?;
    let started = Instant::now();
    let txn = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .start()
        .await?;
    txn.batch_execute(&format!(
        "set local application_name = '{CHUNK_APPLICATION_NAME}'"
    ))
    .await?;
    if plan.edits_change_it() && !hold_plan_epoch(&*txn, &fence, chunk.definition_id, epoch).await?
    {
        txn.rollback().await?;
        tracing::debug!(
            definition_id = chunk.definition_id,
            chunk_id = chunk.id,
            "build chunk planned before an edit to its definition; planning it again"
        );
        return defer_claim(pool, chunk, claimed_by, Duration::ZERO).await;
    }
    if !fence.hold(&*txn).await? {
        txn.rollback().await?;
        return chunk_queue::discard_if_superseded(pool, chunk, claimed_by).await;
    }
    metrics::record_build_statement(BuildStatement::ChunkSetup, setup_started.elapsed());
    let (keys, delta_rows) = match &plan {
        ChunkPlan::Whole(AnyPlan::Ledger(plan)) => {
            let outcome = run_chunk(&txn, plan, lo, hi).await.map_err(build_error)?;
            (outcome.keys, outcome.delta_rows)
        }
        ChunkPlan::Whole(AnyPlan::OneToOne(plan)) => {
            let outcome = one_to_one::run_chunk(&txn, plan, lo, hi)
                .await
                .map_err(build_error)?;
            (outcome.keys, 0)
        }
        ChunkPlan::Field(FieldPlan::OneToOne(plan, fields)) => {
            let outcome = one_to_one::run_field_chunk(&txn, plan, fields, lo, hi)
                .await
                .map_err(build_error)?;
            (outcome.keys, 0)
        }
        ChunkPlan::Field(FieldPlan::Direct(rederive, keys)) => {
            let mut mutations = TargetMutations::new();
            rederive
                .settle(&txn, &mut mutations)
                .await
                .map_err(build_error)?;
            mutations.flush(&txn).await.map_err(build_error)?;
            (*keys, 0)
        }
        ChunkPlan::Field(FieldPlan::Empty) => (0, 0),
    };
    let commit_started = Instant::now();
    txn.execute(
        "update backfill_chunks set done = true, claimed_by = null, claimed_at = null \
         where id = $1",
        &[&chunk.id],
    )
    .await?;
    txn.commit().await?;
    metrics::record_build_statement(BuildStatement::ChunkCommit, commit_started.elapsed());
    metrics::record_build_chunk(
        started.elapsed(),
        keys as u64,
        u64::try_from(delta_rows).unwrap_or(0),
    );
    try_complete(pool, chunk.definition_id).await?;
    Ok(())
}

/// What one claimed `rederive` row runs.
enum ChunkPlan {
    /// A whole build's chunk.
    Whole(AnyPlan),
    /// A field build's chunk (#625 F8b).
    Field(FieldPlan),
}

impl ChunkPlan {
    /// Whether an edit committed after the plan was made can make it wrong
    /// ([`plan_epoch`]): a 1-1 target's, whose fields `ALTER TRANSFORM`
    /// changes and whose paused columns a column pause or resume moves.
    fn edits_change_it(&self) -> bool {
        !matches!(self, ChunkPlan::Whole(AnyPlan::Ledger(_)))
    }
}

/// The version fence of definition `id`'s source (`source_table_versions`),
/// which a chunk or a sweep batch of a 1-1 target reads before it plans,
/// and takes `for share` as its transaction's first lock
/// ([`hold_plan_epoch`]) (#625 F8b). A chunk plans before its transaction
/// (its fields, and for a relationship-enriched target its paused columns),
/// so an edit can commit in between: `ALTER TRANSFORM`, a column pause or
/// resume, or a field build's capture release, each of which bumps the
/// fence. If the chunk then went on, the edit's own field build could
/// already have written keys the chunk had yet to lock, and the chunk would
/// put its older plan's values over them, or stamp an entry's `basis` past
/// a change whose page then can't write the new field; a chunk planned
/// before a pause would write the paused column after it (issue #903). One
/// that finds the fence moved rolls back and plans again. One that finds it
/// unmoved holds it to its commit, so a bump that comes later waits for the
/// chunk: the edit's build reaches its keys only after it commits, and a
/// pause returns only once its write has landed. A define on the same
/// source bumps the fence too, which costs such a chunk a retry.
async fn plan_epoch(
    client: &impl GenericClient,
    id: i64,
) -> Result<Option<i64>, tokio_postgres::Error> {
    Ok(client
        .query_opt(
            "select v.version from transform_definitions d \
             join source_table_versions v on v.source_table = d.source_table \
             where d.id = $1",
            &[&id],
        )
        .await?
        .map(|row| row.get(0)))
}

/// Takes the version fence of definition `id`'s source `for share` in a
/// chunk's or sweep batch's `txn`, and returns whether it still reads
/// `epoch`, the [`plan_epoch`] the plan was made under. It is the
/// transaction's first lock, before the claim's ([`ClaimFence::hold`]) and
/// the keys' entries, as a page takes the fence before any other lock: a
/// bump waits for the transactions holding the fence, and holds nothing
/// while it does (issue #744), so a writer that holds no other lock when it
/// queues on a bump can't close a cycle through it. A resume, say, bumps
/// the fence and then locks every chunk a worker holds.
///
/// The claim's idle timeout goes first: from here on, a stalled worker
/// holds up every bump of the source's fence, not just its chunk.
async fn hold_plan_epoch(
    txn: &impl GenericClient,
    claim: &ClaimFence<'_>,
    id: i64,
    epoch: Option<i64>,
) -> Result<bool, tokio_postgres::Error> {
    claim.arm(txn).await?;
    let held: Option<i64> = txn
        .query_opt(
            "select v.version from transform_definitions d \
             join source_table_versions v on v.source_table = d.source_table \
             where d.id = $1 for share of v",
            &[&id],
        )
        .await?
        .map(|row| row.get(0));
    Ok(held == epoch)
}

/// A field build's chunk (see the module doc's "Field builds").
enum FieldPlan {
    /// A plain 1-1 target's: [`one_to_one::run_field_chunk`] over the
    /// fields.
    OneToOne(one_to_one::OneToOnePlan, Vec<String>),
    /// A 1-1 target the Re-derive build's SQL can't render (a
    /// relationship-enriched one): a page's whole-row Re-derive of the
    /// range's keys, built before the chunk's transaction (it resolves its
    /// relationship reads from the catalog through the pool, and runs them
    /// in the transaction, after the entry lock), and how many keys it
    /// re-derives.
    Direct(Box<super::apply::DirectRederive>, usize),
    /// A range with no key to re-derive.
    Empty,
}

impl FieldPlan {
    /// The chunk `(lo, hi]` of `definition`'s field build over `fields`.
    /// Only a 1-1 target holds a field out of Apply, so only a 1-1 target
    /// has a field build; anything else is
    /// [`BackfillError::Unsupported`](crate::defs::backfill::BackfillError::Unsupported).
    async fn for_chunk(
        pool: &Pool,
        definition: &Definition,
        fields: &[String],
        lo: Option<&str>,
        hi: &str,
        key_collations: Option<&[Option<String>]>,
    ) -> Result<ChunkPlan, ChunkQueueError> {
        if let Some(mut plan) = one_to_one::OneToOnePlan::for_definition(pool, definition)
            .await
            .map_err(build_error)?
        {
            plan.pin_key_collations(key_collations);
            // With every field that reads one of `fields` by alias in the
            // definition as it is now (issue #748): a later edit can add a
            // reader of a field this build releases (one awaiting its
            // capture), which Apply holds out with that field and which no
            // other build writes once it is released. A chunk planned
            // before such an edit is planned again ([`plan_epoch`]).
            let mut fields: std::collections::HashSet<String> = fields.iter().cloned().collect();
            crate::defs::eval::AliasReaders::of(&definition.def).close(&mut fields);
            let fields: Vec<String> = definition
                .def
                .fields
                .iter()
                .filter(|f| fields.contains(&f.name))
                .map(|f| f.name.clone())
                .collect();
            return Ok(ChunkPlan::Field(FieldPlan::OneToOne(plan, fields)));
        }
        if !matches!(
            definition.def.key_space,
            crate::defs::ast::KeySpace::OneToOne
        ) {
            return Err(build_error(
                crate::defs::backfill::BackfillError::Unsupported(
                    "only a 1-1 target has a field build".to_string(),
                ),
            ));
        }
        let mut pk = ddl::source_primary_key(pool, &definition.source_table)
            .await
            .map_err(build_error)?;
        ddl::pin_key_collations(&mut pk, key_collations);
        let decode = |text: &str| -> Result<Vec<String>, ChunkQueueError> {
            Ok(ddl::split_pk_key(&pk, &definition.source_table, text)
                .map_err(build_error)?
                .into_iter()
                .map(|part| part.map(|c| c.into_owned()).unwrap_or_default())
                .collect())
        };
        let (lo, hi) = (lo.map(decode).transpose()?, decode(hi)?);
        let keys = {
            let client = pool.get().await?;
            crate::defs::backfill::range_keys(
                &**client,
                &definition.source_table,
                &definition.def.target,
                &pk,
                &lo,
                &hi,
            )
            .await?
        };
        if keys.is_empty() {
            return Ok(ChunkPlan::Field(FieldPlan::Empty));
        }
        let excluded = super::quarantine::paused_columns_for(pool, &definition.def)
            .await
            .map_err(build_error)?;
        let rederive = super::apply::DirectRederive::new(
            pool,
            &definition.def,
            &definition.source_table,
            &definition.target_table,
            &definition.source_columns,
            excluded,
            false,
            &keys,
        )
        .await
        .map_err(build_error)?;
        Ok(ChunkPlan::Field(FieldPlan::Direct(
            Box::new(rederive),
            keys.len(),
        )))
    }
}

/// The plan of either kind of target a Re-derive build serves.
enum AnyPlan {
    /// A ledger aggregate's: chunks append group deltas a merger folds.
    Ledger(Box<BuildPlan>),
    /// A plain 1-1 target's (#625 F8a): chunks write the target rows.
    OneToOne(one_to_one::OneToOnePlan),
}

impl AnyPlan {
    /// Pins the source key's collations to the ones the chunk's range was
    /// planned under (`ddl::pin_key_collations`, issue #769).
    fn pin_key_collations(&mut self, recorded: Option<&[Option<String>]>) {
        match self {
            AnyPlan::Ledger(plan) => plan.ledger.pin_source_key_collations(recorded),
            AnyPlan::OneToOne(plan) => plan.pin_key_collations(recorded),
        }
    }

    /// `definition`'s plan, or [`BackfillError::Unsupported`] when its shape
    /// no longer takes a Re-derive build.
    ///
    /// [`BackfillError::Unsupported`]: crate::defs::backfill::BackfillError::Unsupported
    async fn for_definition(pool: &Pool, definition: &Definition) -> Result<Self, ChunkQueueError> {
        if let Some(plan) = BuildPlan::for_definition(pool, definition)
            .await
            .map_err(build_error)?
        {
            return Ok(AnyPlan::Ledger(Box::new(plan)));
        }
        if let Some(plan) = one_to_one::OneToOnePlan::for_definition(pool, definition)
            .await
            .map_err(build_error)?
        {
            return Ok(AnyPlan::OneToOne(plan));
        }
        Err(build_error(
            crate::defs::backfill::BackfillError::Unsupported(
                "the definition's shape no longer takes a re-derive build".to_string(),
            ),
        ))
    }
}

/// Runs a claimed plan job (#625 F2, Q13): walks the source's primary key
/// from the job's cursor, [`PLAN_BATCH`] boundaries at a time, each batch
/// committed as `rederive` chunks together with the cursor's advance, so
/// chunks run while the walk goes on and a job that dies resumes where its
/// last batch committed. The batch that reaches the last row marks the job
/// done. Each commit is fenced by the claim ([`ClaimFence`]).
async fn run_plan(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
    mut cursor: Option<String>,
    options: &WorkerOptions,
) -> Result<(), ChunkQueueError> {
    let definition = catalog::definition_by_id(pool, chunk.definition_id)
        .await?
        .ok_or(ChunkQueueError::DefinitionNotFound {
            definition_id: chunk.definition_id,
        })?;
    // The walk compares under the collations the job's first batch walked
    // under, and every chunk records them (issue #769): an `alter column …
    // collate …` between two batches, or between a batch and a chunk, would
    // otherwise reorder the key under the boundaries already enqueued.
    let mut pk = ddl::source_primary_key(pool, &definition.source_table)
        .await
        .map_err(build_error)?;
    ddl::pin_key_collations(&mut pk, chunk.key_collations.as_deref());
    let key_collations = ddl::key_collations(&pk);
    let _heartbeat = chunk_queue::ChunkHeartbeat::spawn(
        pool.clone(),
        chunk.id,
        claimed_by.to_string(),
        options.heartbeat_interval,
    );
    let fence = ClaimFence::new(chunk.id, claimed_by, options.reclaim_ttl);
    // A field build starts once its fields apply (#625 F8b), and its walk,
    // so every chunk, comes after that commit.
    if let Some(fields) = &chunk.fields
        && cursor.is_none()
    {
        match field_build_ready(pool, &definition, fields, &fence).await? {
            FieldStart::Go => {}
            FieldStart::Wait => {
                return defer_claim(pool, chunk, claimed_by, FIELD_CAPTURE_RETRY).await;
            }
            FieldStart::Superseded => {
                return chunk_queue::discard_if_superseded(pool, chunk, claimed_by).await;
            }
        }
    }
    let mut client = pool.get().await?;
    loop {
        let started = Instant::now();
        let ranges = crate::defs::backfill::next_pk_ranges(
            &**client,
            &definition.source_table,
            &pk,
            cursor.as_deref(),
            options.chunk_rows,
            PLAN_BATCH,
        )
        .await
        .map_err(build_error)?;
        let finished = ranges.len() < PLAN_BATCH;
        let next = ranges.last().map(|(_, hi)| hi.clone()).or(cursor.clone());
        let txn = client.transaction().await?;
        if !fence.hold(&*txn).await? {
            txn.rollback().await?;
            return chunk_queue::discard_if_superseded(pool, chunk, claimed_by).await;
        }
        let (los, his): (Vec<Option<&str>>, Vec<&str>) = ranges
            .iter()
            .map(|(lo, hi)| (lo.as_deref(), hi.as_str()))
            .unzip();
        txn.execute(
            "insert into backfill_chunks \
                 (definition_id, kind, lo, hi, fuse_rearmed_at, fields, key_collations) \
             select bc.definition_id, $4, r.lo, r.hi, bc.fuse_rearmed_at, bc.fields, $5 \
             from unnest($2::text[], $3::text[]) with ordinality as r(lo, hi, n) \
             cross join backfill_chunks bc \
             where bc.id = $1 \
             order by r.n",
            &[
                &chunk.id,
                &los,
                &his,
                &chunk_queue::KIND_REDERIVE,
                &key_collations,
            ],
        )
        .await?;
        if finished {
            txn.execute(
                "update backfill_chunks set lo = $2, done = true, claimed_by = null, \
                     claimed_at = null, key_collations = $3 \
                 where id = $1",
                &[&chunk.id, &next, &key_collations],
            )
            .await?;
        } else {
            txn.execute(
                "update backfill_chunks set lo = $2, key_collations = $3 where id = $1",
                &[&chunk.id, &next, &key_collations],
            )
            .await?;
        }
        txn.commit().await?;
        metrics::record_build_statement(BuildStatement::Plan, started.elapsed());
        if finished {
            tracing::debug!(
                definition_id = chunk.definition_id,
                "re-derive build planned its last chunk"
            );
            break;
        }
        cursor = next;
    }
    try_complete(pool, chunk.definition_id).await?;
    Ok(())
}

/// Runs a claimed sweep job (#625 F3; see the module doc): [`sweep_batch`]es
/// of [`WorkerOptions::chunk_rows`] entries from the job's cursor, each in
/// its own `read committed` transaction with the cursor's advance, fenced by
/// the claim ([`ClaimFence`]), so a job that dies resumes after its last
/// committed batch. The batch that reads the ledger's end marks the job done.
/// Then [`try_complete`].
async fn run_sweep(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
    mut cursor: Option<String>,
    options: &WorkerOptions,
) -> Result<(), ChunkQueueError> {
    // Before anything the plan is made from ([`plan_epoch`]).
    let epoch = plan_epoch(&**pool.get().await?, chunk.definition_id).await?;
    let definition = catalog::definition_by_id(pool, chunk.definition_id)
        .await?
        .ok_or(ChunkQueueError::DefinitionNotFound {
            definition_id: chunk.definition_id,
        })?;
    let plan = AnyPlan::for_definition(pool, &definition).await?;
    let _heartbeat = chunk_queue::ChunkHeartbeat::spawn(
        pool.clone(),
        chunk.id,
        claimed_by.to_string(),
        options.heartbeat_interval,
    );
    let fence = ClaimFence::new(chunk.id, claimed_by, options.reclaim_ttl);
    let mut client = pool.get().await?;
    let start_xid: String = client
        .query_one(
            "select start_xid::text from backfill_chunks where id = $1",
            &[&chunk.id],
        )
        .await?
        .get(0);
    loop {
        let started = Instant::now();
        let txn = client
            .build_transaction()
            .isolation_level(IsolationLevel::ReadCommitted)
            .start()
            .await?;
        txn.batch_execute(&format!(
            "set local application_name = '{CHUNK_APPLICATION_NAME}'"
        ))
        .await?;
        // The batches done so far keep their cursor; the rest plan again.
        if matches!(plan, AnyPlan::OneToOne(_))
            && !hold_plan_epoch(&*txn, &fence, chunk.definition_id, epoch).await?
        {
            txn.rollback().await?;
            tracing::debug!(
                definition_id = chunk.definition_id,
                "re-derive sweep planned before an edit to its definition; planning it again"
            );
            return defer_claim(pool, chunk, claimed_by, Duration::ZERO).await;
        }
        if !fence.hold(&*txn).await? {
            txn.rollback().await?;
            return chunk_queue::discard_if_superseded(pool, chunk, claimed_by).await;
        }
        let scan = options.chunk_rows.max(1);
        let outcome = match &plan {
            AnyPlan::Ledger(plan) => {
                sweep_batch(&txn, plan, &start_xid, cursor.as_deref(), scan).await
            }
            AnyPlan::OneToOne(plan) => {
                one_to_one::sweep_batch(&txn, plan, &start_xid, cursor.as_deref(), scan).await
            }
        }
        .map_err(build_error)?;
        let next = outcome.next.clone().or(cursor.clone());
        txn.execute(
            "update backfill_chunks set lo = $2, done = $3, \
                 claimed_by = case when $3 then null else claimed_by end, \
                 claimed_at = case when $3 then null else claimed_at end \
             where id = $1",
            &[&chunk.id, &next, &outcome.finished],
        )
        .await?;
        txn.commit().await?;
        metrics::record_build_chunk(
            started.elapsed(),
            outcome.rederived as u64,
            u64::try_from(outcome.delta_rows).unwrap_or(0),
        );
        if outcome.finished {
            tracing::debug!(
                definition_id = chunk.definition_id,
                "re-derive build swept the last of its ledger"
            );
            break;
        }
        cursor = next;
    }
    try_complete(pool, chunk.definition_id).await?;
    Ok(())
}

/// Folds one [`MERGE_BATCH`] of definition `id`'s group deltas into its
/// groups (#625 B3), in a transaction holding the definition row
/// `for key share` so a pause waits for it rather than racing it: a frozen
/// definition's deltas aren't merged. A target dropped meanwhile ends the
/// pass quietly. Then [`try_complete`] when the pass drained the table.
/// Returns how many delta rows it folded.
async fn merge_once(pool: &Pool, id: i64) -> Result<i64, ChunkQueueError> {
    let setup_started = Instant::now();
    let Some(definition) = catalog::definition_by_id(pool, id).await? else {
        return Ok(0);
    };
    let Some(plan) = BuildPlan::for_definition(pool, &definition)
        .await
        .map_err(build_error)?
    else {
        return Ok(0);
    };
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    txn.batch_execute(&format!(
        "set local application_name = '{MERGE_APPLICATION_NAME}'"
    ))
    .await?;
    let building = txn
        .query_opt(
            "select 1 from transform_definitions where id = $1 and status = $2 and build = $3 \
             for key share",
            &[&id, &TransformStatus::Backfilling.as_str(), &BUILD_REDERIVE],
        )
        .await?
        .is_some();
    if !building {
        txn.rollback().await?;
        return Ok(0);
    }
    metrics::record_build_statement(BuildStatement::MergeSetup, setup_started.elapsed());
    let outcome = match merge_deltas(&txn, &plan, MERGE_BATCH).await {
        Ok(outcome) => outcome,
        Err(ApplyError::Db(err)) if super::quarantine::is_undefined_table(&err) => {
            return Ok(0);
        }
        Err(err) => return Err(build_error(err)),
    };
    if outcome.skipped {
        // Another worker is merging this target: leave it to that one.
        txn.rollback().await?;
        return Ok(0);
    }
    let started = Instant::now();
    txn.commit().await?;
    metrics::record_build_statement(BuildStatement::MergeCommit, started.elapsed());
    metrics::record_build_merge(u64::try_from(outcome.claimed).unwrap_or(0));
    if vacuum_due(id, outcome.claimed) {
        let started = Instant::now();
        let deltas = &plan.ledger.deltas_ident;
        if let Err(err) = vacuum_deltas(&**client, deltas).await {
            tracing::debug!(table = %deltas, error = %err, "vacuuming a group-delta table failed");
        }
        metrics::record_build_statement(BuildStatement::MergeVacuum, started.elapsed());
    }
    if outcome.claimed < MERGE_BATCH {
        try_complete(pool, id).await?;
    }
    Ok(outcome.claimed)
}

/// How many times [`merge_retrying`] retries a merge at once after a
/// transient failure before it gives the merge up to [`fail_merge`].
const MERGE_TRANSIENT_RETRIES: u32 = 3;

/// The wait before [`merge_retrying`]'s first retry, doubled for each one
/// after it.
const MERGE_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(50);

/// [`merge_once`], retried at once after a transient failure (a lost
/// connection, a lock or serialization conflict, at any statement or at
/// `COMMIT`) up to [`MERGE_TRANSIENT_RETRIES`] times, as a drain page is
/// (#901). A failed merge rolled back whole, so a retry claims the same
/// delta rows again.
async fn merge_retrying(pool: &Pool, id: i64) -> Result<i64, ChunkQueueError> {
    let mut delay = MERGE_RETRY_INITIAL_DELAY;
    let mut retries = 0;
    loop {
        match merge_once(pool, id).await {
            Err(err)
                if retries < MERGE_TRANSIENT_RETRIES
                    && super::quarantine::is_transient_error(&err) =>
            {
                tracing::debug!(
                    definition_id = id, error = %err, retry = retries + 1,
                    "group-delta merge failed transiently; retrying it"
                );
                tokio::time::sleep(delay).await;
                delay *= 2;
                retries += 1;
            }
            merged => return merged,
        }
    }
}

/// A drain worker's record of the group-delta merges that failed on it
/// (#901), by definition: how many times in a row each failed, how many of
/// those were charged, and when it may be merged again. [`work_once`] skips
/// a target until then, and forgets a definition once its merge succeeds or
/// it stops building. A drain worker keeps one for its life; a test makes
/// its own.
#[derive(Debug, Default)]
pub struct MergeFailures {
    by_definition: HashMap<i64, MergeFailure>,
}

/// One definition's entry in [`MergeFailures`].
#[derive(Debug, Clone, Copy)]
struct MergeFailure {
    attempts: i32,
    charged: i32,
    retry_at: Instant,
}

impl MergeFailures {
    /// Whether definition `id`'s merge is waiting out a backoff.
    fn backing_off(&self, id: i64) -> bool {
        self.by_definition
            .get(&id)
            .is_some_and(|failure| Instant::now() < failure.retry_at)
    }

    /// Records a failure of definition `id`'s merge, `charged` or not, and
    /// backs it off by the chunk queue's schedule for a Re-derive build's
    /// work. Returns the entry as it now stands, with how long it waits.
    fn failed(&mut self, id: i64, charged: bool) -> (MergeFailure, Duration) {
        let failure = self.by_definition.entry(id).or_insert(MergeFailure {
            attempts: 0,
            charged: 0,
            retry_at: Instant::now(),
        });
        failure.attempts += 1;
        failure.charged += i32::from(charged);
        let delay = chunk_queue::retry_delay(failure.attempts, chunk_queue::REDERIVE_RETRY_CAP);
        failure.retry_at = Instant::now() + delay;
        (*failure, delay)
    }

    /// Forgets definition `id`'s failures: its merge succeeded, or it was
    /// paused.
    fn succeeded(&mut self, id: i64) {
        self.by_definition.remove(&id);
    }

    /// Keeps only the definitions `keep` says are still building.
    fn retain(&mut self, keep: impl Fn(i64) -> bool) {
        self.by_definition.retain(|id, _| keep(*id));
    }

    /// Ends every backoff, so a test's next [`work_once`] merges at once.
    #[cfg(test)]
    fn expire(&mut self) {
        let now = Instant::now();
        for failure in self.by_definition.values_mut() {
            failure.retry_at = now;
        }
    }
}

/// Gives up a merge into definition `id`'s `target` that failed with `err`
/// after [`merge_retrying`]'s retries (#901), and logs at warn what it did.
/// A merge applies whole groups, so no key is charged:
///
/// - a **transient** failure backs the target off, uncharged;
/// - a **refused** read or write (`42501`) halts what the refusal reaches
///   (`super::halt::halt_failed_merge`), as it does a drain's. One the
///   catalog pins on no table is charged like any other failure;
/// - **anything else** (an application's check, foreign key or deferred
///   constraint a merged group breaks, a missing column) backs the target
///   off and is charged, and its [`chunk_queue::MAX_CHARGED_ATTEMPTS`]th
///   charge halts the definition, and everything downstream of its target,
///   with kind `halt`: `Trellis::status` reports the target and the error
///   as its `capture_failure`, and a resume rebuilds it.
///
/// It never fails itself: a failure to halt is logged, and the target backs
/// off as before.
async fn fail_merge(
    pool: &Pool,
    id: i64,
    target: &str,
    err: &ChunkQueueError,
    failures: &mut MergeFailures,
) {
    let transient = super::quarantine::is_transient_error(err);
    if !transient && super::quarantine::is_insufficient_privilege(err) {
        match super::halt::halt_failed_merge(pool, target, true, &err.to_string()).await {
            Ok(paused) if !paused.is_empty() => {
                failures.succeeded(id);
                tracing::warn!(
                    definition_id = id, target = %target, paused = ?paused, error = %err,
                    "group-delta merge was refused; paused the definitions the refusal reaches \
                     (resume them once the cause is fixed)"
                );
                return;
            }
            Ok(_) => {}
            Err(halt_err) => tracing::warn!(
                definition_id = id, target = %target, error = %err, halt_error = %halt_err,
                "group-delta merge was refused, and halting on it failed"
            ),
        }
    }
    let (failure, delay) = failures.failed(id, !transient);
    if !transient && failure.charged >= chunk_queue::MAX_CHARGED_ATTEMPTS {
        match super::halt::halt_failed_merge(pool, target, false, &err.to_string()).await {
            Ok(paused) => {
                failures.succeeded(id);
                tracing::warn!(
                    definition_id = id, target = %target, attempt = failure.attempts,
                    max_charged = chunk_queue::MAX_CHARGED_ATTEMPTS, paused = ?paused,
                    error = %err,
                    "group-delta merge kept failing; paused its definition and everything \
                     downstream of its target (resume it once the cause is fixed)"
                );
                return;
            }
            Err(halt_err) => tracing::warn!(
                definition_id = id, target = %target, error = %err, halt_error = %halt_err,
                "group-delta merge kept failing, and pausing its definition failed"
            ),
        }
    }
    tracing::warn!(
        definition_id = id, target = %target, transient, attempt = failure.attempts,
        charged = failure.charged, max_charged = chunk_queue::MAX_CHARGED_ATTEMPTS,
        next_attempt_in_secs = delay.as_secs_f64(), error = %err,
        "group-delta merge failed; retrying it after a backoff"
    );
}

/// The delta rows one process merges into a target between its vacuums of
/// the target's delta table (#625 F2b).
///
/// A merger's claim walks the claim key's index from its partition's oldest
/// entry, and every row merged since the table's last vacuum leaves a dead
/// entry there (killed on the first walk, but still on its leaf page until a
/// vacuum removes it). With the rows spread over the partitions (#717), a
/// claim walks only its own partition's share of them. Autovacuum comes at most once per `autovacuum_naptime`, a
/// minute by default, which at a merger's ~200,000 rows/s is ~12M dead
/// entries and ~60 ms of walking per claim. Vacuuming every this many rows
/// keeps the walk under ~300 leaf pages, at the cost of one vacuum (the
/// pages changed since the last one, and the index) per 20 merges.
pub const VACUUM_EVERY: i64 = 20 * MERGE_BATCH;

/// Counts `claimed` rows merged into definition `id`'s target by this
/// process, and says whether that brings it to [`VACUUM_EVERY`] since its
/// last vacuum (and restarts the count if so). A per-process count: each
/// process vacuums after its own merges, and a restart only delays the next
/// vacuum.
fn vacuum_due(id: i64, claimed: i64) -> bool {
    static MERGED: LazyLock<Mutex<HashMap<i64, i64>>> = LazyLock::new(Mutex::default);
    let mut merged = MERGED.lock().unwrap_or_else(PoisonError::into_inner);
    let since = merged.entry(id).or_insert(0);
    *since += claimed;
    if *since < VACUUM_EVERY {
        return false;
    }
    *since = 0;
    true
}

/// Vacuums the delta table `deltas` (quoted, qualified), outside any
/// transaction. The caller treats it as best-effort: a failure costs only
/// the next claims' walk (see [`VACUUM_EVERY`]). It skips the table rather
/// than wait if another vacuum holds it, always cleans the index (a vacuum
/// that bypassed index cleanup would leave the dead entries the claim
/// walks), and never truncates, so it never takes the lock that would make a
/// chunk's delta insert wait.
async fn vacuum_deltas(
    client: &impl GenericClient,
    deltas: &str,
) -> Result<(), tokio_postgres::Error> {
    client
        .batch_execute(&format!(
            "vacuum (skip_locked, index_cleanup on, truncate false) {deltas}"
        ))
        .await
}

/// Moves definition `id` `backfilling -> live` once its Re-derive build is
/// done (#625 B7, the strict flip): the plan job is done, no other chunk is
/// left undone (one held across a resume doesn't count, as for the old
/// build), and its group-delta table is empty. Returns whether it flipped.
///
/// Every transaction that can make that true (a chunk's, a merger's, the
/// plan job's last) calls this after it commits, so the last of them sees
/// all the others committed. It checks without a lock first, and only then
/// takes the definition row `for update` and checks again, so only a
/// possibly-last caller takes the lock. No catch-up is parked: the
/// definition applied from its start.
pub async fn try_complete(pool: &Pool, id: i64) -> Result<bool, ChunkQueueError> {
    let mut client = pool.get().await?;
    if !build_done(&**client, id).await? {
        return Ok(false);
    }
    let txn = client.transaction().await?;
    txn.execute(
        "select 1 from transform_definitions where id = $1 for update",
        &[&id],
    )
    .await?;
    if !build_done(&*txn, id).await? {
        txn.rollback().await?;
        return Ok(false);
    }
    txn.execute(
        "update transform_definitions set status = $2, build = null where id = $1",
        &[&id, &TransformStatus::Live.as_str()],
    )
    .await?;
    txn.commit().await?;
    tracing::info!(
        definition_id = id,
        from = %TransformStatus::Backfilling.as_str(),
        to = %TransformStatus::Live.as_str(),
        "transform status transition: re-derive build finished"
    );
    Ok(true)
}

/// [`try_complete`]'s test: `id` is under a Re-derive build, no current
/// chunk of it is undone, and its target's group-delta table is empty. A
/// 1-1 target has no delta table (#625 F8a), so the chunks are the whole
/// test. A dropped target reads as not done.
async fn build_done(client: &impl GenericClient, id: i64) -> Result<bool, ChunkQueueError> {
    let Some(row) = client
        .query_opt(
            &format!(
                "select d.target_table, not exists ( \
                     select 1 from backfill_chunks bc \
                     where bc.definition_id = d.id and not bc.done \
                       and not ({}) \
                 ), {} \
                 from transform_definitions d \
                 where d.id = $1 and d.status = $2 and d.build = $3",
                chunk_queue::STALE,
                deltas_exist_sql("d.target_table"),
            ),
            &[&id, &TransformStatus::Backfilling.as_str(), &BUILD_REDERIVE],
        )
        .await?
    else {
        return Ok(false);
    };
    let target: String = row.get(0);
    if !row.get::<_, bool>(1) {
        return Ok(false);
    }
    if !row.get::<_, bool>(2) {
        // A 1-1 target, whose definition row the same statement read, so
        // its target wasn't dropped.
        return Ok(true);
    }
    let deltas =
        ddl::qualified_target_table_ident(&crate::defs::ledger::deltas_table_name(&target));
    match client.query_one(&any_delta_sql(&deltas), &[]).await {
        Ok(row) => Ok(!row.get::<_, bool>(0)),
        Err(err) if super::quarantine::is_undefined_table(&err) => Ok(false),
        Err(err) => Err(err.into()),
    }
}

/// One statement: whether the delta table `deltas` (quoted, qualified) has
/// a row visible to it. It reads the first row through the claim key's
/// index (#625 F2b, #717): an `exists` over the table would scan the heap
/// from block 0, past every page the mergers emptied.
fn any_delta_sql(deltas: &str) -> String {
    let part = quote_ident(crate::defs::ledger::DELTA_PART_COLUMN);
    let seq = quote_ident(crate::defs::ledger::DELTA_SEQ_COLUMN);
    format!("select (select {seq} from {deltas} order by {part}, {seq} limit 1) is not null")
}

/// Runs every Re-derive build that is running to its end, in this task: a
/// synchronous stand-in for the drain workers' build work
/// ([`work_once`]) for a test with none running, which needs its builds
/// `live` before it goes on. It merges, plans and runs chunks and sweeps
/// with no backpressure (the test's ring may hold sealed segments it means
/// to drain later), until no definition is under a Re-derive build. A
/// frozen one is left as it is. Panics on any failure, or when the builds
/// make no progress for a while (a chunk that keeps failing backs off): it
/// is test harness.
///
/// A field build whose plan job waits on its source's capture (#625 F8b,
/// [`field_build_ready`]) can't go on until the staging worker's capture
/// pass, which this doesn't run: once that is all that is left, it returns
/// with the build still running.
#[cfg(any(test, feature = "internals"))]
pub async fn settle_builds(pool: &Pool) {
    const CLAIMED_BY: &str = "settle_rederive_builds";
    const STALL: Duration = Duration::from_secs(10);
    let options = WorkerOptions {
        chunk_rows: DEFAULT_CHUNK_ROWS,
        drain_batch_cap: usize::MAX,
        heartbeat_interval: Duration::from_secs(5),
        reclaim_ttl: Duration::from_secs(60),
    };
    let mut last_progress = Instant::now();
    // A field build's plan job that gave its claim back to wait on capture
    // is tried again now, once: the capture pass that may have readied it
    // ran before this call, not on the job's backoff.
    {
        let client = pool.get().await.expect("acquire a connection");
        client
            .execute(
                "update backfill_chunks set next_attempt_at = null \
                 where kind = $1 and fields is not null and lo is null and not done \
                   and claimed_by is null and last_error is null",
                &[&chunk_queue::KIND_PLAN],
            )
            .await
            .expect("retry the field builds waiting on capture");
    }
    loop {
        let building = building(pool).await.expect("list the running builds");
        if building.is_empty() {
            return;
        }
        let mut progressed = false;
        for (id, target, merges) in &building {
            while *merges
                && has_deltas(pool, target).await.expect("read a delta table")
                && merge_once(pool, *id).await.expect("merge group deltas") > 0
            {
                progressed = true;
            }
        }
        let claimed = {
            let client = pool.get().await.expect("acquire a connection");
            chunk_queue::claim_chunks_of(
                &**client,
                CLAIMED_BY,
                1,
                &[
                    chunk_queue::KIND_PLAN,
                    chunk_queue::KIND_REDERIVE,
                    chunk_queue::KIND_SWEEP,
                ],
            )
            .await
            .expect("claim build work")
        };
        for chunk in &claimed {
            run_claimed(pool, chunk, CLAIMED_BY, &options).await;
            progressed |= !deferred(pool, chunk).await;
        }
        for (id, ..) in &building {
            progressed |= try_complete(pool, *id).await.expect("complete a build");
        }
        if progressed {
            last_progress = Instant::now();
        } else if only_capture_waits(pool).await {
            return;
        } else {
            assert!(
                last_progress.elapsed() < STALL,
                "the running re-derive builds {building:?} made no progress for {STALL:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// Whether `chunk`, just run, gave its claim back to wait on capture
/// ([`defer_claim`]), which [`settle_builds`] doesn't count as progress.
#[cfg(any(test, feature = "internals"))]
async fn deferred(pool: &Pool, chunk: &ClaimedChunk) -> bool {
    let client = pool.get().await.expect("acquire a connection");
    client
        .query_one(
            "select exists (select 1 from backfill_chunks \
             where id = $1 and not done and claimed_by is null and lo is null \
               and fields is not null and next_attempt_at > now())",
            &[&chunk.id],
        )
        .await
        .expect("read a build row")
        .get(0)
}

/// Whether every unfinished row of a running build is a field build's plan
/// job, not yet started, whose fields still await their source's capture
/// ([`settle_builds`]'s stop).
#[cfg(any(test, feature = "internals"))]
async fn only_capture_waits(pool: &Pool) -> bool {
    let client = pool.get().await.expect("acquire a connection");
    client
        .query_one(
            &format!(
                "select not exists ( \
                     select 1 from backfill_chunks bc \
                     join transform_definitions d on d.id = bc.definition_id \
                     where not bc.done and not ({}) \
                       and not (bc.kind = $1 and bc.fields is not null and bc.lo is null \
                                and exists (select 1 from column_status cs \
                                            where cs.transform_table = \
                                                  split_part(d.target_table, '.', 2) \
                                              and cs.column_name = any(bc.fields) \
                                              and cs.awaiting_capture))) \
                 and exists (select 1 from backfill_chunks bc where not bc.done)",
                chunk_queue::STALE
            ),
            &[&chunk_queue::KIND_PLAN],
        )
        .await
        .expect("read the running builds' rows")
        .get(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Whether `text`'s ledger gets its `GROUP BY` index over a source of
    /// `(id, g, v numeric, x float8)`, `source_is_target` saying whether
    /// that source is another definition's target.
    fn indexes_groups(text: &str, source_is_target: bool) -> bool {
        let def = crate::defs::parse(text).expect("parse");
        let columns: HashMap<String, ValueType> = [
            ("id", ValueType::Numeric),
            ("g", ValueType::Numeric),
            ("v", ValueType::Numeric),
            ("x", ValueType::Float(crate::float::FloatWidth::Float8)),
        ]
        .into_iter()
        .map(|(c, t)| (c.to_string(), t))
        .collect();
        ledger_indexes_groups(&def, &columns, source_is_target)
    }

    /// #723: a target the Re-derive build takes, with only maintained
    /// fields, gets no `GROUP BY` index on its ledger. One with a recomputed
    /// field gets it, and so does one the Re-derive build doesn't take: a
    /// source that is another definition's target, or a relationship-fed
    /// target ([`buildable_shape`] has no relationships to route it with).
    #[test]
    fn only_an_invertible_re_derive_built_ledger_goes_without_its_group_index() {
        let invertible = "TRANSFORM t FROM src GROUP BY g \
                          SELECT SUM(v) AS total, AVG(v) AS mean, COUNT(v) AS nv, COUNT(*) AS n";
        assert!(
            !indexes_groups(invertible, false),
            "invertible, Re-derive built"
        );
        for recomputed in [
            "TRANSFORM t FROM src GROUP BY g SELECT MAX(v) AS hi, COUNT(*) AS n",
            "TRANSFORM t FROM src GROUP BY g SELECT SUM(x) AS total",
            "TRANSFORM t FROM src GROUP BY g SELECT SUM(v) + COUNT(v) AS both",
        ] {
            assert!(
                indexes_groups(recomputed, false),
                "recomputed: {recomputed}"
            );
        }
        assert!(
            indexes_groups(invertible, true),
            "a seam-fed source: !qualifies"
        );
        assert!(
            indexes_groups(
                "TRANSFORM t FROM src GROUP BY g SELECT SUM(parent.w) AS total",
                false
            ),
            "a relationship-fed target: !qualifies"
        );
    }

    /// #625 F-A5 for a Re-derive chunk (#616): a plain aggregate's build over
    /// a source of several chunks, whose `SUM(x + x)` overflows on one row.
    /// (The grammar has no `/`, so this is the overflow #616's own repro
    /// uses, not a division by zero: the same class 22 data failure.) The
    /// failing chunk is on the definition's status and in a warn line, it
    /// splits until the key fails alone, the key is quarantined, and the
    /// build goes `live` without it: the chunks and the merges leave it out.
    #[tokio::test]
    async fn a_chunk_that_fails_on_its_data_quarantines_the_key_and_the_build_goes_live() {
        use crate::client::log_capture::install_capture;
        use crate::defs::ast::ValueType;
        use crate::integer::IntWidth;

        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let config = crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
        let pool = Pool::new(&config).expect("build a same-crate pool");
        let trellis = crate::app::Trellis::connect(config, crate::app::TrellisOptions::default())
            .await
            .expect("connect");
        let (mut raw, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        raw.batch_execute(&format!(
            "set search_path to {}, public; \
             create table public.nums (id bigint primary key, g integer, x integer); \
             insert into public.nums select i, i % 3, i from generate_series(1, 300) i; \
             update public.nums set x = 2147483647 where id = 150",
            crate::config::DEFAULT_SCHEMA
        ))
        .await
        .expect("seed the source");
        let columns = HashMap::from([
            ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
            ("g".to_string(), ValueType::Integer(IntWidth::Int4)),
            ("x".to_string(), ValueType::Integer(IntWidth::Int4)),
        ]);
        let def = crate::defs::install_definition(
            &pool,
            "TRANSFORM sums FROM nums GROUP BY g SELECT SUM(x + x) AS doubled, COUNT(*) AS n",
            &columns,
            "public",
        )
        .await
        .expect("install_definition");
        crate::client::reconcile_pass(
            &mut raw,
            &pool,
            crate::config::DEFAULT_SCHEMA,
            "f5_wake",
            Duration::from_secs(5),
        )
        .await
        .expect("start the build");
        let status = || async {
            trellis
                .status("sums")
                .await
                .expect("status")
                .expect("the definition exists")
        };
        assert_eq!(status().await.status, TransformStatus::Backfilling);

        let (_guard, captured) = install_capture();
        let options = WorkerOptions {
            chunk_rows: 100,
            drain_batch_cap: 100_000,
            heartbeat_interval: Duration::from_secs(1),
            reclaim_ttl: Duration::from_secs(30),
        };
        let mut failure = None;
        for _ in 0..200 {
            let step = work_once(&pool, "worker", &options, &mut MergeFailures::default())
                .await
                .expect("a build step");
            if failure.is_none() {
                failure = status().await.backfill_failure;
            }
            if !step.progressed() {
                break;
            }
        }
        let failure = failure.expect("the failing chunk was on the status while it narrowed");
        assert!(
            failure.last_error.contains("out of range"),
            "{}",
            failure.last_error
        );
        assert_eq!(failure.source_table, "public.nums");
        let status = status().await;
        assert_eq!(
            status.status,
            TransformStatus::Live,
            "the build finished without the key"
        );
        assert_eq!(status.backfill_failure, None, "no chunk is left failing");

        let rows = |sql: &'static str| {
            let raw = &raw;
            async move {
                raw.query(sql, &[])
                    .await
                    .expect(sql)
                    .into_iter()
                    .map(|row| row.get::<_, String>(0))
                    .collect::<Vec<_>>()
            }
        };
        assert_eq!(
            rows("select (g, doubled, n)::text from public.sums order by g").await,
            rows(
                "select (g, sum(x + x), count(*))::text from public.nums \
                 where id <> 150 group by g order by g"
            )
            .await,
            "the target is the source without the quarantined key"
        );
        let quarantined = trellis
            .sample_quarantined("sums", None, 10)
            .await
            .expect("sample the quarantined keys");
        assert_eq!(quarantined.len(), 1);
        assert_eq!(quarantined[0].key, "150");
        assert!(quarantined[0].error_message.contains("out of range"));

        let events = captured.0.lock().unwrap().clone();
        let warned = |message: &str| {
            events.iter().any(|event| {
                event.level == tracing::Level::WARN
                    && event
                        .fields
                        .get("message")
                        .is_some_and(|m| m.contains(message))
                    && event.fields.get("definition_id") == Some(&def.id.to_string())
            })
        };
        assert!(warned("split it in two"), "{events:#?}");
        assert!(warned("quarantined the key"), "{events:#?}");
    }

    /// #625 F-A5 for a 1-1 target's Re-derive chunk (F8a): #616's own
    /// repro, a plain 1-1 `x + x` over a source of several chunks that
    /// overflows on one row. The failing chunk splits until the key fails
    /// alone, the key is quarantined, and the build goes `live` without it.
    #[tokio::test]
    async fn a_one_to_one_chunk_that_fails_on_its_data_quarantines_the_key() {
        use crate::defs::ast::ValueType;
        use crate::integer::IntWidth;

        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let config = crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
        let pool = Pool::new(&config).expect("build a same-crate pool");
        let trellis = crate::app::Trellis::connect(config, crate::app::TrellisOptions::default())
            .await
            .expect("connect");
        let (mut raw, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        raw.batch_execute(&format!(
            "set search_path to {}, public; \
             create table public.nums (id bigint primary key, x integer); \
             insert into public.nums select i, i from generate_series(1, 300) i; \
             update public.nums set x = 2147483647 where id = 150",
            crate::config::DEFAULT_SCHEMA
        ))
        .await
        .expect("seed the source");
        let columns = HashMap::from([
            ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
            ("x".to_string(), ValueType::Integer(IntWidth::Int4)),
        ]);
        crate::defs::install_definition(
            &pool,
            "TRANSFORM doubles FROM nums SELECT x + x AS doubled",
            &columns,
            "public",
        )
        .await
        .expect("install_definition");
        crate::client::reconcile_pass(
            &mut raw,
            &pool,
            crate::config::DEFAULT_SCHEMA,
            "f8a_wake",
            Duration::from_secs(5),
        )
        .await
        .expect("start the build");
        let status = || async {
            trellis
                .status("doubles")
                .await
                .expect("status")
                .expect("the definition exists")
        };
        assert_eq!(status().await.status, TransformStatus::Backfilling);

        let options = WorkerOptions {
            chunk_rows: 100,
            drain_batch_cap: 100_000,
            heartbeat_interval: Duration::from_secs(1),
            reclaim_ttl: Duration::from_secs(30),
        };
        let mut failure = None;
        for _ in 0..200 {
            let step = work_once(&pool, "worker", &options, &mut MergeFailures::default())
                .await
                .expect("a build step");
            if failure.is_none() {
                failure = status().await.backfill_failure;
            }
            if !step.progressed() {
                break;
            }
        }
        let failure = failure.expect("the failing chunk was on the status while it narrowed");
        assert!(
            failure.last_error.contains("out of range"),
            "{}",
            failure.last_error
        );
        let status = status().await;
        assert_eq!(status.status, TransformStatus::Live);
        assert_eq!(status.backfill_failure, None);
        let target = raw
            .query_one(
                "select count(*), count(*) filter (where id = 150), \
                        count(*) filter (where doubled = 2 * id) \
                 from public.doubles",
                &[],
            )
            .await
            .expect("read the target");
        assert_eq!(target.get::<_, i64>(0), 299);
        assert_eq!(target.get::<_, i64>(1), 0, "the failing key is left out");
        assert_eq!(target.get::<_, i64>(2), 299);
        let quarantined = trellis
            .sample_quarantined("doubles", None, 10)
            .await
            .expect("sample the quarantined keys");
        assert_eq!(quarantined.len(), 1);
        assert_eq!(quarantined[0].key, "150");
    }

    #[test]
    fn a_process_vacuums_a_target_every_vacuum_every_merged_rows() {
        // Ids no other test uses: the count is process-wide.
        let (a, b) = (-6_251, -6_252);
        assert!(!vacuum_due(a, VACUUM_EVERY - 1));
        assert!(!vacuum_due(b, VACUUM_EVERY - 1));
        assert!(vacuum_due(a, 1), "a reaches the threshold");
        assert!(!vacuum_due(a, VACUUM_EVERY - 1), "and counts again from 0");
        assert!(vacuum_due(b, MERGE_BATCH), "b counts on its own");
    }

    #[tokio::test]
    async fn the_delta_vacuum_runs_on_an_indexed_queue() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = db.pool.get().await.expect("pool");
        client
            .batch_execute(
                "create table public.q (__seq bigint generated always as identity, g int); \
                 create index on public.q (__seq); \
                 insert into public.q (g) select i from generate_series(1, 1000) i; \
                 delete from public.q where __seq <= 900",
            )
            .await
            .expect("a queue with dead rows");
        vacuum_deltas(&**client, "public.q")
            .await
            .expect("vacuum the queue");
        // A vacuum records the live rows it counted in `pg_class` as it
        // finishes (an analyze would too, and nothing ran one).
        let reltuples: f32 = client
            .query_one(
                "select reltuples from pg_class where oid = 'public.q'::regclass",
                &[],
            )
            .await
            .expect("read the table's row count")
            .get(0);
        assert_eq!(reltuples, 100.0, "the vacuum counted the live rows");
    }

    /// A database with Re-derive builds to merge (#901): `define` defines a
    /// `GROUP BY` transform over a fresh source of 30 rows in 3 groups and
    /// starts its build, and `stage` runs a definition's plan job and chunks
    /// by hand, so its group deltas wait for a merge.
    struct MergeFixture {
        _db: testkit::TestDatabase,
        pool: Pool,
        trellis: crate::app::Trellis,
        raw: tokio_postgres::Client,
    }

    const MERGE_OPTIONS: WorkerOptions = WorkerOptions {
        chunk_rows: 100,
        drain_batch_cap: 100_000,
        heartbeat_interval: Duration::from_secs(1),
        reclaim_ttl: Duration::from_secs(30),
    };

    impl MergeFixture {
        async fn new(cluster: &testkit::TestCluster) -> Self {
            let db = cluster.create_isolated_database().await;
            let config = crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
            let pool = Pool::new(&config).expect("build a same-crate pool");
            let trellis =
                crate::app::Trellis::connect(config, crate::app::TrellisOptions::default())
                    .await
                    .expect("connect");
            let (raw, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
                .await
                .expect("connect");
            tokio::spawn(async move {
                let _ = connection.await;
            });
            raw.batch_execute(&format!(
                "set search_path to {}, public",
                crate::config::DEFAULT_SCHEMA
            ))
            .await
            .expect("set the search path");
            Self {
                _db: db,
                pool,
                trellis,
                raw,
            }
        }

        async fn define(&mut self, target: &str) -> i64 {
            use crate::defs::ast::ValueType;
            use crate::integer::IntWidth;
            self.raw
                .batch_execute(&format!(
                    "create table public.{target}_src (id bigint primary key, g integer, x integer); \
                     insert into public.{target}_src select i, i % 3, i from generate_series(1, 30) i"
                ))
                .await
                .expect("seed the source");
            let columns = HashMap::from([
                ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
                ("g".to_string(), ValueType::Integer(IntWidth::Int4)),
                ("x".to_string(), ValueType::Integer(IntWidth::Int4)),
            ]);
            let def = crate::defs::install_definition(
                &self.pool,
                &format!(
                    "TRANSFORM {target} FROM {target}_src GROUP BY g \
                     SELECT SUM(x) AS total, COUNT(*) AS n"
                ),
                &columns,
                "public",
            )
            .await
            .expect("install_definition");
            crate::client::reconcile_pass(
                &mut self.raw,
                &self.pool,
                crate::config::DEFAULT_SCHEMA,
                "merge_wake",
                Duration::from_secs(5),
            )
            .await
            .expect("start the build");
            assert_eq!(
                self.status(target).await.status,
                TransformStatus::Backfilling
            );
            def.id
        }

        /// Runs every claimable plan job and chunk by hand, so every
        /// building target's deltas wait for a merge.
        async fn stage(&self) {
            for kinds in [
                &[chunk_queue::KIND_PLAN][..],
                &[chunk_queue::KIND_REDERIVE, chunk_queue::KIND_SWEEP][..],
            ] {
                loop {
                    let claimed = {
                        let client = self.pool.get().await.expect("pool");
                        chunk_queue::claim_chunks_of(&**client, "stager", 1, kinds)
                            .await
                            .expect("claim")
                    };
                    let Some(chunk) = claimed.into_iter().next() else {
                        break;
                    };
                    run_claimed(&self.pool, &chunk, "stager", &MERGE_OPTIONS).await;
                }
            }
        }

        /// The fixture database's connection string.
        fn pool_dsn(&self) -> String {
            self._db.dsn().to_string()
        }

        async fn status(&self, target: &str) -> crate::app::DefinitionStatus {
            self.trellis
                .status(target)
                .await
                .expect("status")
                .expect("the definition exists")
        }

        async fn deltas(&self, target: &str) -> i64 {
            self.raw
                .query_one(
                    &format!("select count(*) from public.{target}__deltas"),
                    &[],
                )
                .await
                .expect("count the deltas")
                .get(0)
        }

        async fn rows(&self, target: &str) -> Vec<String> {
            self.raw
                .query(
                    &format!("select (g, total, n)::text from public.{target} order by g"),
                    &[],
                )
                .await
                .expect("read the target")
                .into_iter()
                .map(|row| row.get(0))
                .collect()
        }
    }

    /// The source's groups, as the target should hold them.
    const EXPECTED_GROUPS: [&str; 3] = ["(0,165,10)", "(1,145,10)", "(2,155,10)"];

    /// #901: a target whose merge fails deterministically (a check
    /// constraint every merged group breaks) is set aside, and the same
    /// `work_once` goes on to the next building definition's work: its plan
    /// job, its chunk and its merge, until it goes `live`, while the failing
    /// target keeps its deltas and stays `backfilling`.
    #[tokio::test]
    async fn a_failing_merge_does_not_stop_another_targets_claims_or_merge() {
        let cluster = testkit::TestCluster::start();
        let mut it = MergeFixture::new(&cluster).await;
        it.define("broken").await;
        it.stage().await;
        assert!(it.deltas("broken").await > 0, "broken's deltas wait");
        it.raw
            .batch_execute(
                "alter table public.broken add constraint broken_never check (total < 0)",
            )
            .await
            .expect("a check constraint every merged group breaks");
        it.define("healthy").await;

        let mut merges = MergeFailures::default();
        let first = work_once(&it.pool, "worker", &MERGE_OPTIONS, &mut merges)
            .await
            .expect("a failing merge is no failure of the step");
        assert_eq!(
            first,
            Step::Planned,
            "the step went on past the failing merge to the next build's plan job"
        );
        let mut steps = vec![first];
        for _ in 0..20 {
            if it.status("healthy").await.status == TransformStatus::Live {
                break;
            }
            steps.push(
                work_once(&it.pool, "worker", &MERGE_OPTIONS, &mut merges)
                    .await
                    .expect("a failing merge is no failure of the step"),
            );
        }
        assert_eq!(
            it.status("healthy").await.status,
            TransformStatus::Live,
            "{steps:?}"
        );
        assert!(steps.contains(&Step::Chunk), "{steps:?}");
        assert!(steps.contains(&Step::Merged), "{steps:?}");
        assert_eq!(it.rows("healthy").await, EXPECTED_GROUPS);
        assert_eq!(
            it.status("broken").await.status,
            TransformStatus::Backfilling
        );
        assert!(it.deltas("broken").await > 0, "broken's deltas still wait");
        assert!(it.rows("broken").await.is_empty());
    }

    /// #901: a transient failure at the merge's `COMMIT` is retried. A
    /// deferred constraint trigger on the target raises a serialization
    /// failure the first time it fires, counted by a sequence, which a
    /// rollback doesn't take back. One `work_once` retries the merge, and it
    /// commits.
    #[tokio::test]
    async fn a_transient_failure_at_the_merges_commit_is_retried() {
        let cluster = testkit::TestCluster::start();
        let mut it = MergeFixture::new(&cluster).await;
        it.define("flaky").await;
        it.stage().await;
        it.raw
            .batch_execute(
                "create sequence public.commit_attempts; \
                 create function public.fail_first_commit() returns trigger \
                   language plpgsql as $$ \
                   begin \
                     if nextval('public.commit_attempts') = 1 then \
                       raise exception 'the first commit fails' using errcode = '40001'; \
                     end if; \
                     return null; \
                   end $$; \
                 create constraint trigger flaky_fail_first_commit \
                   after insert or update or delete on public.flaky \
                   deferrable initially deferred \
                   for each row execute function public.fail_first_commit()",
            )
            .await
            .expect("a deferred trigger failing the first commit");

        let mut merges = MergeFailures::default();
        let step = work_once(&it.pool, "worker", &MERGE_OPTIONS, &mut merges)
            .await
            .expect("the merge is retried past the failed commit");
        assert_eq!(step, Step::Merged);
        let fired: i64 = it
            .raw
            .query_one("select last_value from public.commit_attempts", &[])
            .await
            .expect("read the sequence")
            .get(0);
        assert!(
            fired >= 2,
            "the first commit failed and a retry fired it again"
        );
        assert!(merges.by_definition.is_empty(), "nothing was charged");
        // A merge pass takes one merge partition; the rest follow.
        for _ in 0..10 {
            let step = work_once(&it.pool, "worker", &MERGE_OPTIONS, &mut merges)
                .await
                .expect("a build step");
            if !step.progressed() {
                break;
            }
        }
        assert_eq!(it.rows("flaky").await, EXPECTED_GROUPS);
        assert_eq!(it.deltas("flaky").await, 0);
        let status = it.status("flaky").await;
        assert_eq!(status.status, TransformStatus::Live);
        assert_eq!(status.capture_failure, None);
    }

    /// #901: a merge that keeps failing deterministically is charged once
    /// per failure, and its `MAX_CHARGED_ATTEMPTS`th charge halts the
    /// definition with kind `halt`, naming the target and the error on its
    /// `capture_failure`, where it waits for a resume.
    #[tokio::test]
    async fn a_merge_that_keeps_failing_pauses_its_definition_with_the_error() {
        let cluster = testkit::TestCluster::start();
        let mut it = MergeFixture::new(&cluster).await;
        let id = it.define("broken").await;
        it.stage().await;
        it.raw
            .batch_execute(
                "alter table public.broken add constraint broken_never check (total < 0)",
            )
            .await
            .expect("a check constraint every merged group breaks");

        let mut merges = MergeFailures::default();
        for attempt in 1..=chunk_queue::MAX_CHARGED_ATTEMPTS {
            assert_eq!(
                it.status("broken").await.status,
                TransformStatus::Backfilling,
                "still building before attempt {attempt}"
            );
            work_once(&it.pool, "worker", &MERGE_OPTIONS, &mut merges)
                .await
                .expect("a failing merge is no failure of the step");
            if attempt < chunk_queue::MAX_CHARGED_ATTEMPTS {
                assert!(
                    merges.backing_off(id),
                    "backing off after attempt {attempt}"
                );
                assert_eq!(merges.by_definition[&id].charged, attempt);
            }
            merges.expire();
        }

        let status = it.status("broken").await;
        assert_eq!(status.status, TransformStatus::Paused);
        let failure = status.capture_failure.expect("the halt's record");
        assert_eq!(failure.kind, crate::app::CaptureFailureKind::Halt);
        assert!(failure.source_table.ends_with("broken"), "{failure:?}");
        assert!(failure.error.contains("merge into"), "{}", failure.error);
        assert!(failure.error.contains("broken_never"), "{}", failure.error);
        assert!(
            merges.by_definition.is_empty(),
            "the pause forgets the failures"
        );
        assert!(it.deltas("broken").await > 0, "a pause keeps the deltas");
        assert_eq!(
            work_once(&it.pool, "worker", &MERGE_OPTIONS, &mut merges)
                .await
                .expect("a step"),
            Step::Idle,
            "nothing merges for a paused definition"
        );
    }

    /// #901: a transient failure that outlasts [`merge_retrying`]'s retries
    /// backs the target off uncharged, and the next step leaves the target
    /// alone until the backoff ends. A deferred constraint trigger raises a
    /// serialization failure at the merge's `COMMIT` for its first
    /// `MERGE_TRANSIENT_RETRIES + 1` firings, counted by a sequence.
    #[tokio::test]
    async fn a_transient_failure_that_outlasts_the_retries_backs_off_uncharged() {
        let cluster = testkit::TestCluster::start();
        let mut it = MergeFixture::new(&cluster).await;
        let id = it.define("flaky").await;
        it.stage().await;
        let failing = MERGE_TRANSIENT_RETRIES + 1;
        it.raw
            .batch_execute(&format!(
                "create sequence public.commit_attempts; \
                 create function public.fail_early_commits() returns trigger \
                   language plpgsql as $$ \
                   begin \
                     if nextval('public.commit_attempts') <= {failing} then \
                       raise exception 'an early commit fails' using errcode = '40001'; \
                     end if; \
                     return null; \
                   end $$; \
                 create constraint trigger flaky_fail_early_commits \
                   after insert or update or delete on public.flaky \
                   deferrable initially deferred \
                   for each row execute function public.fail_early_commits()"
            ))
            .await
            .expect("a deferred trigger failing the early commits");
        let fired = async |it: &MergeFixture| -> i64 {
            it.raw
                .query_one("select last_value from public.commit_attempts", &[])
                .await
                .expect("read the sequence")
                .get(0)
        };

        let mut merges = MergeFailures::default();
        let step = work_once(&it.pool, "worker", &MERGE_OPTIONS, &mut merges)
            .await
            .expect("a failing merge is no failure of the step");
        assert_ne!(step, Step::Merged);
        assert_eq!(
            fired(&it).await,
            i64::from(failing),
            "the merge ran once and was retried {MERGE_TRANSIENT_RETRIES} times"
        );
        let failure = merges.by_definition[&id];
        assert_eq!((failure.attempts, failure.charged), (1, 0), "uncharged");
        assert!(merges.backing_off(id));

        let step = work_once(&it.pool, "worker", &MERGE_OPTIONS, &mut merges)
            .await
            .expect("a build step");
        assert_ne!(step, Step::Merged);
        assert_eq!(
            fired(&it).await,
            i64::from(failing),
            "a target backing off isn't merged"
        );

        merges.expire();
        for _ in 0..10 {
            let step = work_once(&it.pool, "worker", &MERGE_OPTIONS, &mut merges)
                .await
                .expect("a build step");
            if !step.progressed() {
                break;
            }
        }
        assert!(
            merges.by_definition.is_empty(),
            "a merge forgets the failures"
        );
        assert_eq!(it.rows("flaky").await, EXPECTED_GROUPS);
        assert_eq!(it.status("flaky").await.status, TransformStatus::Live);
    }

    /// #901: a merge Postgres refuses (`42501`, the drain worker's role lacks
    /// the writes on the target) halts its definition at once, uncharged,
    /// with the catalog's reason on a kind `halt` `capture_failure`, and
    /// keeps the deltas. Once the grant is back, a resume rebuilds the
    /// definition to the source's groups, applying each change once.
    #[tokio::test]
    async fn a_refused_merge_halts_at_once_and_a_resume_rebuilds_it() {
        let cluster = testkit::TestCluster::start();
        let mut it = MergeFixture::new(&cluster).await;
        let id = it.define("broken").await;
        it.stage().await;
        let schema = crate::config::DEFAULT_SCHEMA;
        it.raw
            .batch_execute(&format!(
                "do $$ begin \
                   if not exists (select from pg_roles where rolname = 'merge_worker') then \
                     create role merge_worker login; \
                   end if; \
                 end $$; \
                 grant usage on schema {schema}, public to merge_worker; \
                 grant all on all tables in schema {schema} to merge_worker; \
                 grant all on all sequences in schema {schema} to merge_worker; \
                 grant all on all tables in schema public to merge_worker; \
                 revoke all on public.broken from merge_worker; \
                 grant select on public.broken to merge_worker;"
            ))
            .await
            .expect("a drain role that can't write the target");
        let dsn = it.pool_dsn().replace("user=postgres", "user=merge_worker");
        assert!(dsn.contains("user=merge_worker"), "{dsn}");
        let worker = Pool::new(&crate::config::Config::from_dsn(dsn).expect("valid dsn"))
            .expect("a pool logged in as the drain role");

        let mut merges = MergeFailures::default();
        work_once(&worker, "worker", &MERGE_OPTIONS, &mut merges)
            .await
            .expect("a refused merge is no failure of the step");
        let status = it.status("broken").await;
        assert_eq!(status.status, TransformStatus::Paused, "halted at once");
        let failure = status.capture_failure.expect("the halt's record");
        assert_eq!(failure.kind, crate::app::CaptureFailureKind::Halt);
        assert!(failure.source_table.ends_with("broken"), "{failure:?}");
        assert!(failure.error.contains("was refused"), "{}", failure.error);
        assert!(
            failure.error.contains("role merge_worker lacks INSERT"),
            "{}",
            failure.error
        );
        assert!(!merges.by_definition.contains_key(&id), "nothing charged");
        assert!(it.deltas("broken").await > 0, "a halt keeps the deltas");

        it.raw
            .batch_execute("grant all on public.broken to merge_worker")
            .await
            .expect("give the grant back");
        it.trellis
            .apply("RESUME TRANSFORM broken")
            .await
            .expect("resume");
        crate::client::reconcile_pass(
            &mut it.raw,
            &it.pool,
            schema,
            "merge_wake",
            Duration::from_secs(5),
        )
        .await
        .expect("start the rebuild");
        assert_eq!(
            it.status("broken").await.status,
            TransformStatus::Backfilling
        );
        it.stage().await;
        for _ in 0..20 {
            let step = work_once(&worker, "worker", &MERGE_OPTIONS, &mut merges)
                .await
                .expect("a build step");
            if !step.progressed() {
                break;
            }
        }
        let status = it.status("broken").await;
        assert_eq!(status.status, TransformStatus::Live);
        assert_eq!(status.capture_failure, None);
        assert_eq!(it.rows("broken").await, EXPECTED_GROUPS);
        assert_eq!(it.deltas("broken").await, 0);
    }
}
