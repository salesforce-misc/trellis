//! A 1-1 `JOIN`'s fan-out in chunks (ADR-0021): the target rows of `a` a
//! `b` change recomputes, past what one transaction should hold.
//!
//! A `b` change recomputes every `a` row on its join key while it holds the
//! key's `J` row, and an `a` change on the key waits for that lock. So no
//! transaction recomputes more than one chunk of rows ([`chunk_rows`]):
//!
//! - **The page** ([`split`]) recomputes the keys whose rows fit one chunk
//!   between them, and writes a pending marker in `join1_fan_out` for each
//!   other key: `(key, gen, cursor)`, with the cursor at the key's start and
//!   the origin of the page's `b` changes. A key it recomputes whole has its
//!   marker deleted. A later page on a pending key sets its cursor back to
//!   the start, since every row of the key now joins the newer `b` row, and
//!   merges the origins, earliest first.
//! - **A chunk** ([`step`]), one transaction run by a drain worker as build
//!   work, takes the key's `J` row and its marker, recomputes the next
//!   chunk of live `R` entries after the cursor in key order, and advances
//!   the cursor, or deletes the marker after the key's last entry. A chunk
//!   that dies changes nothing, and the next one starts where the last
//!   commit left the cursor.
//!
//! Each recompute reads `R` and the newest live `M` entry under the key's
//! `J` lock, so it is exact whatever ran on the key between two chunks: an
//! `a` change writes its own row from the same state, and a row that leaves
//! the key, or joins it behind the cursor, has been written by its own page.
//! A row behind the cursor was recomputed with `M` as it is, since a newer
//! `b` change would have reset the cursor.
//!
//! The lock order is the page's: the version fences, the definition's row
//! `FOR KEY SHARE` (a chunk may hold a row in quarantine, as a page's fan-out
//! may), the pair's lock row `FOR SHARE`, the key's `J` row, its marker, then
//! the target rows. A page writes its markers after its `J` lock, in key
//! order. So a `TRUNCATE`'s page, which holds the lock row `FOR UPDATE`,
//! orders against a chunk as against a page, and deletes the definition's
//! markers ([`clear_all`]): a `TRUNCATE` of `b` starts a rebuild, which
//! recomputes every row, and one of `a` leaves no row to recompute.
//!
//! A marker holds back every watermark token at or past its origin, as a
//! parked row does (`super::super::converge`'s fifth condition), so a read
//! after `await_converged` sees the fan-out done. A row a chunk holds in
//! quarantine parks with the marker's origin and segment, and the rows it
//! writes reach the target's readers with that origin, staged in the chunk's
//! transaction (`super::super::target_mutations`): a chunk that dies stages
//! nothing.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::SystemTime;

use tokio_postgres::IsolationLevel;
use tokio_postgres::Transaction;
use tokio_postgres::types::{PgLsn, ToSql};

use crate::defs::join::Side;
use crate::defs::ledger as schema;
use crate::defs::model::StoredStatus;
use crate::pool::{Pool, quote_ident};

use super::super::apply::ApplyError;
use super::super::join::{LockRowMode, lock_row_sql};
use super::super::target_mutations::TargetMutations;
use super::{JoinOneToOnePlan, JoinShape, KeyText, cast_to, held};

/// The most `R` entries one transaction of a fan-out recomputes when
/// nothing has set [`set_chunk_rows`]: the model's chunk (#1375 Y5), and one
/// batch of the target write.
pub const DEFAULT_CHUNK_ROWS: usize = super::WRITE_BATCH;

/// 0: the default; otherwise the chunk.
static CHUNK_ROWS: AtomicUsize = AtomicUsize::new(0);

/// The most `R` entries one transaction of a fan-out recomputes: a page's
/// fan-out in all, or one chunk of a pending key.
pub fn chunk_rows() -> usize {
    match CHUNK_ROWS.load(Ordering::Relaxed) {
        0 => DEFAULT_CHUNK_ROWS,
        rows => rows,
    }
}

/// Sets [`chunk_rows`] for this process, for tests; `0` restores the
/// default.
#[cfg_attr(not(any(test, feature = "test-util")), allow(dead_code))]
pub fn set_chunk_rows(rows: usize) {
    CHUNK_ROWS.store(rows, Ordering::Relaxed);
}

/// How many keys one statement of [`split`]'s count reads. Each key's count
/// stops one past the budget left, so a statement reads at most this many
/// times a chunk of index entries however large the keys are.
const COUNT_GROUP: usize = 256;

/// The keys [`step`] looks at in one pass, in key order: the first whose
/// `J` row is free runs.
const CANDIDATES: i64 = 16;

/// The one statement shape every marker key is matched by: the key's columns
/// as `text[]`, in `ON` order.
fn key_array_sql(n: usize, first_param: usize) -> String {
    let parts: Vec<String> = (0..n)
        .map(|i| format!("${}::text", first_param + i))
        .collect();
    format!("array[{}]::text[]", parts.join(", "))
}

/// `(alias.k1, ..) = (cast($first), ..)`: the key columns of `alias` (`R`
/// or `J`) against one key's texts, bound from parameter `$first_param`.
fn key_match_sql(shape: &JoinShape, alias: &str, first_param: usize) -> String {
    let columns: Vec<String> = shape
        .key
        .iter()
        .map(|c| format!("{alias}.{}", quote_ident(&c.name)))
        .collect();
    let values: Vec<String> = shape
        .key
        .iter()
        .enumerate()
        .map(|(i, c)| cast_to(c, &format!("${}::text", first_param + i)))
        .collect();
    format!("({}) = ({})", columns.join(", "), values.join(", "))
}

/// A page's split of its fan-out `keys` (the module doc): returns the keys it
/// recomputes itself, whose `R` entries fit [`chunk_rows`] between them, and
/// deletes their markers; writes a marker for every other key, or resets the
/// one it has. The count is of every entry on the key, tombstones and held
/// rows included, so it never undercounts what the page writes.
pub(super) async fn split(
    txn: &Transaction<'_>,
    plan: &JoinOneToOnePlan,
    keys: &BTreeSet<KeyText>,
    seg_seq: i64,
) -> Result<BTreeSet<KeyText>, ApplyError> {
    let shape = &*plan.shape;
    let keys: Vec<&KeyText> = keys.iter().collect();
    let mut remaining = chunk_rows();
    let mut inline: BTreeSet<KeyText> = BTreeSet::new();
    let mut pending: BTreeSet<KeyText> = BTreeSet::new();
    let mut at = 0;
    while at < keys.len() {
        let group = &keys[at..(at + COUNT_GROUP).min(keys.len())];
        at += group.len();
        if remaining == 0 {
            pending.extend(group.iter().map(|k| (*k).clone()));
            continue;
        }
        let counts = capped_counts(txn, shape, group, remaining + 1).await?;
        for (key, count) in group.iter().zip(counts) {
            if count <= remaining {
                remaining -= count;
                inline.insert((*key).clone());
            } else {
                pending.insert((*key).clone());
            }
        }
    }
    clear(txn, shape, &inline).await?;
    mark(txn, shape, &pending, seg_seq, &plan.held_origin).await?;
    Ok(inline)
}

/// The number of `R` entries on each of `keys`, each stopping at `cap`.
async fn capped_counts(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    keys: &[&KeyText],
    cap: usize,
) -> Result<Vec<usize>, ApplyError> {
    let n = shape.key.len();
    let columns: Vec<Vec<&str>> = (0..n)
        .map(|i| keys.iter().map(|key| key[i].as_str()).collect())
        .collect();
    let params: Vec<&(dyn ToSql + Sync)> = columns.iter().map(|c| c as _).collect();
    let arrays: Vec<String> = (1..=n).map(|i| format!("${i}::text[]")).collect();
    let names: Vec<String> = (0..n).map(|i| format!("c{i}")).collect();
    let qualified: Vec<String> = shape
        .key
        .iter()
        .map(|c| format!("r.{}", quote_ident(&c.name)))
        .collect();
    let casts: Vec<String> = shape
        .key
        .iter()
        .enumerate()
        .map(|(i, c)| cast_to(c, &format!("u.c{i}")))
        .collect();
    let rows = txn
        .query(
            &format!(
                "select (select count(*) from \
                         (select 1 from {r} r where ({qualified}) = ({casts}) limit {cap}) s) \
                 from unnest({arrays}) with ordinality as u({names}, ord) \
                 order by u.ord",
                r = shape.r.ident,
                qualified = qualified.join(", "),
                casts = casts.join(", "),
                arrays = arrays.join(", "),
                names = names.join(", "),
            ),
            &params,
        )
        .await?;
    Ok(rows
        .iter()
        .map(|row| usize::try_from(row.get::<_, i64>(0)).unwrap_or(usize::MAX))
        .collect())
}

/// `keys` as one text array per key column, then the definition id: the
/// parameters of [`clear`] and [`mark`].
fn marker_keys(shape: &JoinShape, keys: &BTreeSet<KeyText>) -> Vec<Vec<String>> {
    (0..shape.key.len())
        .map(|i| keys.iter().map(|key| key[i].clone()).collect())
        .collect()
}

/// The `unnest` of [`marker_keys`] from `$2` as `u(c0, ..)`, and the
/// `text[]` key it builds.
fn marker_unnest(n: usize) -> (String, String) {
    let arrays: Vec<String> = (0..n).map(|i| format!("${}::text[]", i + 2)).collect();
    let names: Vec<String> = (0..n).map(|i| format!("c{i}")).collect();
    let key: Vec<String> = names.iter().map(|c| format!("u.{c}")).collect();
    (
        format!("unnest({}) as u({})", arrays.join(", "), names.join(", ")),
        format!("array[{}]::text[]", key.join(", ")),
    )
}

/// Deletes the markers of `keys`: a page recomputed each of them whole.
async fn clear(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    keys: &BTreeSet<KeyText>,
) -> Result<(), ApplyError> {
    if keys.is_empty() {
        return Ok(());
    }
    let columns = marker_keys(shape, keys);
    let mut params: Vec<&(dyn ToSql + Sync)> = vec![&shape.id];
    params.extend(columns.iter().map(|c| c as &(dyn ToSql + Sync)));
    let (unnest, key) = marker_unnest(shape.key.len());
    txn.execute(
        &format!(
            "delete from join1_fan_out p using {unnest} \
             where p.transform_id = $1 and p.join_key = {key}"
        ),
        &params,
    )
    .await?;
    Ok(())
}

/// Writes a marker at its key's start for each of `keys`, in key order, or
/// sets the one a key has back to its start, merging `origin` into it.
async fn mark(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    keys: &BTreeSet<KeyText>,
    seg_seq: i64,
    origin: &held::HeldOrigin,
) -> Result<(), ApplyError> {
    if keys.is_empty() {
        return Ok(());
    }
    let n = shape.key.len();
    let columns = marker_keys(shape, keys);
    let (origin_lsn, src_changed, hop_gen) = origin.parts();
    let mut params: Vec<&(dyn ToSql + Sync)> = vec![&shape.id];
    params.extend(columns.iter().map(|c| c as &(dyn ToSql + Sync)));
    params.extend([
        &seg_seq as &(dyn ToSql + Sync),
        &origin_lsn,
        &src_changed,
        &hop_gen,
    ]);
    let (unnest, key) = marker_unnest(n);
    let p = n + 2;
    txn.execute(
        &format!(
            "insert into join1_fan_out as f \
                 (transform_id, join_key, seg_seq, origin_lsn, src_changed, hop_gen) \
             select $1::bigint, {key}, ${p}::bigint, ${}::pg_lsn, ${}::timestamptz, \
                    ${}::integer \
             from {unnest} order by 2 \
             on conflict (transform_id, join_key) do update set \
                 gen = f.gen + 1, \
                 cursor = null, \
                 seg_seq = excluded.seg_seq, \
                 origin_lsn = case when f.origin_lsn is null or excluded.origin_lsn is null \
                                   then null else least(f.origin_lsn, excluded.origin_lsn) end, \
                 src_changed = least(f.src_changed, excluded.src_changed), \
                 hop_gen = greatest(f.hop_gen, excluded.hop_gen)",
            p + 1,
            p + 2,
            p + 3,
        ),
        &params,
    )
    .await?;
    Ok(())
}

/// Deletes every marker of `shape`'s definition: a `TRUNCATE` of either side
/// (the module doc), in its page, under the lock row held `FOR UPDATE`.
pub(super) async fn clear_all(txn: &Transaction<'_>, shape: &JoinShape) -> Result<(), ApplyError> {
    txn.execute(
        "delete from join1_fan_out where transform_id = $1",
        &[&shape.id],
    )
    .await?;
    Ok(())
}

/// What one [`step`] came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanOutStep {
    /// No marker of a definition that applies.
    Idle,
    /// A chunk committed (or found its marker gone).
    Chunk,
    /// Markers wait, but this pass ran none: each key's `J` row was held,
    /// or a version fence moved.
    Waiting,
}

impl FanOutStep {
    /// Whether the step did work.
    pub fn progressed(self) -> bool {
        self == FanOutStep::Chunk
    }
}

/// A definition's shape for its chunks, and the version of each of its two
/// tables it was read at.
struct Loaded {
    shape: Arc<JoinShape>,
    versions: Vec<(String, Option<i64>)>,
}

/// Loads `id`'s shape, or `None` when it is gone or is no 1-1 `JOIN`.
async fn load(pool: &Pool, id: i64) -> Result<Option<Loaded>, ApplyError> {
    let Some(definition) = crate::defs::catalog::definition_by_id(pool, id).await? else {
        return Ok(None);
    };
    if !crate::defs::join::is_one_to_one(&definition.def) {
        return Ok(None);
    }
    let Some(join) = crate::defs::join::spec(&definition.def) else {
        return Ok(None);
    };
    // The versions first: a definition change after them misses the fence
    // the chunk then holds, rather than run on a shape it doesn't hold.
    let mut tables = vec![definition.source_table.clone(), join.right.clone()];
    tables.sort();
    tables.dedup();
    let mut versions = Vec::with_capacity(tables.len());
    {
        let client = pool.get().await?;
        for table in tables {
            let version: Option<i64> = client
                .query_opt(
                    "select version from source_table_versions where source_table = $1",
                    &[&table],
                )
                .await?
                .map(|row| row.get(0));
            versions.push((table, version));
        }
    }
    let pk = crate::defs::ddl::source_primary_key(pool, &definition.source_table).await?;
    let plan = super::plan_for(pool, &definition, Side::From, pk).await?;
    Ok(Some(Loaded {
        shape: plan.shape,
        versions,
    }))
}

/// One step of the pending fan-outs (the module doc): a chunk of the first
/// marker, in key order among the first few, whose `J` row is free. It never
/// queues for a `J` row: a key another worker's chunk holds is that worker's
/// next chunk too, and a worker queued behind it would put every `a` writer
/// on the key behind one more chunk, and drain no segment while it waited.
/// So when every candidate's row is held it does nothing, and the worker
/// looks again at its poll floor. Run by a drain worker as build work, after
/// its segments.
pub async fn step(pool: &Pool) -> Result<FanOutStep, ApplyError> {
    let candidates: Vec<(i64, Vec<String>)> = {
        let client = pool.get().await?;
        let dispatchable = StoredStatus::dispatchable();
        client
            .query(
                "select p.transform_id, p.join_key from join1_fan_out p \
                 join transform_definitions d on d.id = p.transform_id \
                 where d.status = any($1) \
                 order by p.transform_id, p.join_key limit $2",
                &[&dispatchable, &CANDIDATES],
            )
            .await?
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect()
    };
    if candidates.is_empty() {
        return Ok(FanOutStep::Idle);
    }
    let mut shapes: HashMap<i64, Option<Loaded>> = HashMap::new();
    for (id, key) in &candidates {
        if !shapes.contains_key(id) {
            shapes.insert(*id, load(pool, *id).await?);
        }
        let Some(loaded) = shapes.get(id).and_then(Option::as_ref) else {
            continue;
        };
        if key.len() != loaded.shape.key.len() {
            continue;
        }
        match run_chunk(pool, loaded, key).await? {
            Chunk::Locked => continue,
            Chunk::Ran => return Ok(FanOutStep::Chunk),
            Chunk::Stale => return Ok(FanOutStep::Waiting),
        }
    }
    Ok(FanOutStep::Waiting)
}

/// What one [`run_chunk`] came to.
enum Chunk {
    /// It committed, or found the marker gone.
    Ran,
    /// The key's `J` row was held: by another chunk, a page, or past the
    /// lock timeout.
    Locked,
    /// A version fence moved since the shape was read, or the definition no
    /// longer applies: nothing ran.
    Stale,
}

/// One chunk of the marker of `key` (its columns' texts) for `loaded`'s
/// definition, in its own transaction (the module doc). A held `J` row is
/// skipped, not waited for.
async fn run_chunk(pool: &Pool, loaded: &Loaded, key: &[String]) -> Result<Chunk, ApplyError> {
    let shape = &*loaded.shape;
    let held = held::Held::default();
    {
        let mut client = pool.get().await?;
        let raw: &mut tokio_postgres::Client = &mut client;
        let txn = raw
            .build_transaction()
            .isolation_level(IsolationLevel::ReadCommitted)
            .start()
            .await?;
        crate::locks::set_local_lock_timeout(&txn, super::super::build::CHUNK_LOCK_TIMEOUT).await?;
        match chunk_in(&txn, loaded, key, &held).await {
            Ok(Chunk::Ran) => txn.commit().await?,
            Ok(other) => {
                txn.rollback().await?;
                return Ok(other);
            }
            Err(err) if crate::locks::is_lock_not_available(&err) => {
                txn.rollback().await?;
                return Ok(Chunk::Locked);
            }
            Err(err) => {
                let _ = txn.rollback().await;
                drop(client);
                // A failure no retry gets past (a bisection out of attempts)
                // pauses the transform, as a page's does, rather than fail
                // every pass.
                if super::super::quarantine::classify(&err)
                    == super::super::quarantine::FailureClass::Halting
                {
                    let paused = super::super::halt::halt_closure(pool, &err).await?;
                    crate::instance_log::warn!(
                        transform = %shape.target,
                        error = %err,
                        paused = ?paused,
                        "a 1-1 JOIN fan-out chunk halted its transform"
                    );
                    return Ok(Chunk::Stale);
                }
                return Err(err);
            }
        }
    }
    let rows = held.take();
    if !rows.is_empty()
        && let Err(err) = held::charge(pool, shape, &rows).await
    {
        crate::instance_log::warn!(
            transform = %shape.target,
            error = %err,
            "could not charge the fuses for the rows a 1-1 JOIN's fan-out chunk held; \
             they stay held"
        );
    }
    Ok(Chunk::Ran)
}

/// [`run_chunk`]'s transaction, which the caller commits on [`Chunk::Ran`].
async fn chunk_in(
    txn: &Transaction<'_>,
    loaded: &Loaded,
    key: &[String],
    held: &held::Held,
) -> Result<Chunk, ApplyError> {
    let shape = &*loaded.shape;
    for (table, version) in &loaded.versions {
        let current: Option<i64> = txn
            .query_opt(
                "select version from source_table_versions where source_table = $1 for share",
                &[table],
            )
            .await?
            .map(|row| row.get(0));
        if current != *version {
            return Ok(Chunk::Stale);
        }
    }
    let dispatchable = StoredStatus::dispatchable();
    let applies = txn
        .query_opt(
            "select 1 from transform_definitions where id = $1 and status = any($2) \
             for key share",
            &[&shape.id, &dispatchable],
        )
        .await?;
    if applies.is_none() {
        return Ok(Chunk::Stale);
    }
    txn.execute(&lock_row_sql(&shape.lock, LockRowMode::Share), &[])
        .await?;
    let n = shape.key.len();
    let key_params: Vec<&(dyn ToSql + Sync)> =
        key.iter().map(|k| k as &(dyn ToSql + Sync)).collect();
    let locked = txn
        .query_opt(
            &format!(
                "select 1 from {j} j where {matches} for update skip locked",
                j = shape.j,
                matches = key_match_sql(shape, "j", 1),
            ),
            &key_params,
        )
        .await?;
    if locked.is_none() {
        // A `J` row is never deleted, so a key with a marker has one: it is
        // held.
        return Ok(Chunk::Locked);
    }
    let mut params: Vec<&(dyn ToSql + Sync)> = vec![&shape.id];
    params.extend(key_params.iter().copied());
    let marker = txn
        .query_opt(
            &format!(
                "select cursor, seg_seq, origin_lsn, src_changed, hop_gen from join1_fan_out \
                 where transform_id = $1 and join_key = {} for update",
                key_array_sql(n, 2),
            ),
            &params,
        )
        .await?;
    let Some(marker) = marker else {
        return Ok(Chunk::Ran);
    };
    let cursor: Option<String> = marker.get(0);
    let seg_seq: i64 = marker.get(1);
    let origin = held::HeldOrigin::of_parts(
        marker.get::<_, Option<PgLsn>>(2),
        marker.get::<_, Option<SystemTime>>(3),
        marker.get::<_, i32>(4),
    );
    // Test-only pause point. See `super::super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::super::interleave::pause_at(
        txn,
        super::super::interleave::PausePoint::AfterJoin1FanOutLock,
        &shape.target,
    )
    .await?;

    let rows = chunk_rows();
    let entries = walk(txn, shape, key, cursor.as_deref(), rows + 1).await?;
    let next = (entries.len() > rows).then(|| entries[rows - 1].clone());
    let batch = &entries[..entries.len().min(rows)];
    let mut mutations = TargetMutations::new();
    super::write_fan_out(txn, shape, batch, &origin, seg_seq, &mut mutations, held).await?;
    let n_params = n + 1;
    match &next {
        Some(next) => {
            let mut update = params.clone();
            update.push(next);
            txn.execute(
                &format!(
                    "update join1_fan_out set cursor = ${} \
                     where transform_id = $1 and join_key = {}",
                    n_params + 1,
                    key_array_sql(n, 2),
                ),
                &update,
            )
            .await?;
        }
        None => {
            txn.execute(
                &format!(
                    "delete from join1_fan_out where transform_id = $1 and join_key = {}",
                    key_array_sql(n, 2),
                ),
                &params,
            )
            .await?;
        }
    }
    // The rows the chunk changed reach the target's readers in its own
    // transaction, with the marker's origin (the module doc).
    mutations.flush(txn).await?;
    // Test-only pause point. See `super::super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::super::interleave::pause_at(
        txn,
        super::super::interleave::PausePoint::BeforeJoin1FanOutCommit,
        &shape.target,
    )
    .await?;
    crate::instance_log::debug!(
        target_table = %shape.target,
        rows = batch.len(),
        done = next.is_none(),
        "1-1 JOIN fan-out chunk recomputed its rows"
    );
    Ok(Chunk::Ran)
}

/// Up to `limit` keys of the live `R` entries on `key` after `cursor`, in key
/// order, but those of the rows held in quarantine: an index range of `R`'s
/// `(join key, entry key)` index.
async fn walk(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    key: &[String],
    cursor: Option<&str>,
    limit: usize,
) -> Result<Vec<String>, ApplyError> {
    let n = shape.key.len();
    let mut params: Vec<&(dyn ToSql + Sync)> =
        key.iter().map(|k| k as &(dyn ToSql + Sync)).collect();
    params.push(&shape.id);
    params.push(&shape.source_table);
    let not_held = super::not_held_filter(txn, shape, n + 1, n + 2).await?;
    let after = match &cursor {
        Some(_) => format!(
            " and r.{} > ${}::text",
            quote_ident(schema::KEY_COLUMN),
            n + 3
        ),
        None => String::new(),
    };
    if let Some(cursor) = &cursor {
        params.push(cursor);
    }
    let rows = super::super::ledger::query_by_entry_key(
        txn,
        &format!(
            "select r.{entry} from {r} r \
             where {matches} and not r.{tomb}{after}{not_held} \
             order by r.{entry} limit {limit}",
            entry = quote_ident(schema::KEY_COLUMN),
            r = shape.r.ident,
            matches = key_match_sql(shape, "r", 1),
            tomb = quote_ident(schema::TOMBSTONE_COLUMN),
        ),
        &params,
    )
    .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

/// Runs [`step`] until no chunk is left to run, for tests: a bounded loop,
/// not a wait. Returns the chunks that ran.
#[cfg(all(any(test, feature = "test-util"), any(test, feature = "internals")))]
pub async fn run_pending(pool: &Pool) -> Result<usize, ApplyError> {
    let mut chunks = 0;
    for _ in 0..100_000 {
        match step(pool).await? {
            FanOutStep::Chunk => chunks += 1,
            FanOutStep::Idle => return Ok(chunks),
            FanOutStep::Waiting => {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
    }
    Ok(chunks)
}
