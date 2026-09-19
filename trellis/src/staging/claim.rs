//! Bucket partitioning and multi-worker claims (issue #14, stage 04's
//! second half). See docs/staging-and-claiming/04-claiming-and-the-fold.md,
//! "Partitioning a batch across workers" and "The claim is a cursor".
//!
//! Scope: this module decides how many buckets a batch gets
//! ([`SEG_BUCKETS`]/[`MIN_ROWS_TO_SPLIT`], applied by
//! `super::seal::seal_phase1`), tracks live workers as the share
//! denominator ([`register_drainer`]/[`count_live_drainers`]), and lets a
//! worker claim a share of a batch's free buckets ([`claim`]) with the
//! `sealed -> draining` flip committing in the same statement. It does
//! *not* compute deltas, apply them, or complete a batch (`draining ->
//! drained`, issue #11, blocked on aggregate transform-defs) — a claimed
//! batch sits in `draining` with rows in `seg_claims` until #11 closes the
//! loop. Keeping a claim alive once it's made — the heartbeat cadence and
//! reclaim TTL — is [`super::liveness`], issue #15.

use std::time::Duration;

use tokio_postgres::GenericClient;

use super::error::StagingError;
use super::fold::BucketFilter;

/// How many buckets a batch that clears [`MIN_ROWS_TO_SPLIT`] is split
/// into. Chosen once, at seal, from configuration and batch size alone —
/// never from the live-worker registry (doc 04, "Partitioning a batch
/// across workers": workers register lazily, so a bulk batch can seal
/// before the pool's threads arrive, and a live count sampled at seal time
/// would make the partition itself swing run to run; the partition is
/// immutable and safety-critical, so it must not be sampled from a registry
/// that moves under it).
///
/// Bounded to fit an `int8` bit-mask: issue #11's completion tracking is
/// expected to fold a batch's drained buckets into a single signed
/// `bigint` mask, so every bucket index (`0..SEG_BUCKETS`) must fit one of
/// its 63 usable bits (bit 63 is the sign bit).
pub const SEG_BUCKETS: i64 = 8;

const _: () = assert!(
    SEG_BUCKETS <= 63,
    "SEG_BUCKETS must fit issue #11's planned int8 drained-bucket bit-mask"
);

/// The minimum row count (measured over the active slot, at seal time) a
/// batch needs to be split into [`SEG_BUCKETS`] buckets; anything smaller
/// seals to a single bucket — today's pre-#14 unsplit behaviour, exactly.
pub const MIN_ROWS_TO_SPLIT: i64 = 256;

/// The share denominator's default staleness window (see
/// [`count_live_drainers`]): a drainer that hasn't registered within this
/// long doesn't count as live. Distinct from
/// [`super::liveness::reclaim_stale`]'s TTL — that one decides when a
/// *claim* is dead; this one decides when a *drainer* stops counting
/// toward the share denominator claim-time sizing divides by.
pub const DEFAULT_DRAINER_WINDOW: Duration = Duration::from_secs(30);

/// Upserts `id`'s row in the drainer registry, bumping `last_seen` to now —
/// the registration half of the share denominator [`count_live_drainers`]
/// reads. This is just "a worker exists," called once per claim; it does
/// not ride on [`super::liveness::HeartbeatDaemon`], which refreshes
/// `seg_claims`, not `drainers`.
pub async fn register_drainer(client: &impl GenericClient, id: &str) -> Result<(), StagingError> {
    client
        .execute(
            "insert into drainers (drainer_id, last_seen) values ($1, now()) \
             on conflict (drainer_id) do update set last_seen = excluded.last_seen",
            &[&id],
        )
        .await?;
    Ok(())
}

/// Counts drainers seen within `window` of now — the share denominator
/// [`claim`] divides a batch's free buckets by. Floored at 1 so a claim
/// never divides by zero, e.g. the very first worker, claiming before any
/// other has registered.
pub async fn count_live_drainers(
    client: &impl GenericClient,
    window: Duration,
) -> Result<i64, StagingError> {
    let window_secs = window.as_secs_f64();
    let count: i64 = client
        .query_one(
            "select count(*) from drainers where last_seen > now() - (interval '1 second' * $1)",
            &[&window_secs],
        )
        .await?
        .get(0);
    Ok(count.max(1))
}

/// One statement: compute `seg_seq`'s free buckets, take this call's share
/// of them, insert them (exclusivity via `ON CONFLICT (seg_seq, bucket) DO
/// NOTHING`), and flip the batch `sealed -> draining` — all in one `WITH`,
/// so the claim rows and the flip commit together (doc 04, "The claim is
/// one statement": splitting them would open a crash window where claim
/// rows sit on a still-`sealed` batch and #11's `state = 'draining'`
/// completion guard never matches).
///
/// **Drained buckets stay claimed-out.** #11's Phase-3 completion deletes a
/// worker's `seg_claims` rows *and* ORs its buckets into `segments.drained_mask`
/// in one commit, but leaves the batch `draining` while peers' buckets are still
/// outstanding. Without the `drained_mask` term in `free`, those just-drained
/// buckets — no longer in `seg_claims` — would look free again and be re-claimed,
/// re-folded and re-applied every loop until the last peer flips the batch to
/// `drained`. Idempotent for the 1-1 scalar path (no-op-suppressed upsert), but a
/// double-count the day invertible aggregate deltas land, so the free set is
/// `all buckets − taken − drained`.
///
/// **The `count(*)` guard.** `flip_guard` is a `FROM`-item: a scalar
/// subquery counting `flipped`'s own output, so it is always evaluated to
/// produce its one row, independent of how many rows `mine` contributes.
/// That is what forces `flipped` — a data-modifying CTE — to actually run
/// even when this call's `mine` claims nothing (every free bucket already
/// taken by a concurrent caller): an *unreferenced* data-modifying CTE is
/// the documented Postgres gotcha (silently planned away, doc 04's
/// "Postgres gotcha" callout). Putting the guard in a lazily-evaluated
/// `SELECT`-list expression instead would reintroduce exactly that bug
/// whenever `mine` is empty, since a zero-row `FROM mine` never reaches the
/// projection; a `FROM`-item's subquery has no such out — it must produce
/// its row (or none, but `count(*)` always returns exactly one) before the
/// join above it can run at all.
const CLAIM_SQL: &str = "\
    with taken as ( \
        select bucket from seg_claims where seg_seq = $1 \
    ), \
    free as ( \
        select gs as bucket, row_number() over (order by gs) as rn \
        from generate_series(0, (select bucket_count - 1 from segments where seg_seq = $1)) as gs \
        where gs not in (select bucket from taken) \
          and ((select drained_mask from segments where seg_seq = $1) & (1::bigint << gs)) = 0 \
    ), \
    share as ( \
        select ceil(count(*)::float8 / $3::float8)::bigint as n from free \
    ), \
    mine as ( \
        insert into seg_claims (seg_seq, bucket, claimed_by) \
        select $1, free.bucket, $2 \
        from free, share \
        where free.rn <= share.n \
        on conflict (seg_seq, bucket) do nothing \
        returning bucket \
    ), \
    flipped as ( \
        update segments set state = 'draining' \
        where seg_seq = $1 and state = 'sealed' \
        returning seg_seq \
    ) \
    select mine.bucket \
    from (select count(*) as n from flipped) as flip_guard \
    left join mine on true \
    where mine.bucket is not null";

/// Claims this worker's share of `seg_seq`'s free buckets and returns the
/// buckets *this* call won. See [`CLAIM_SQL`]'s doc comment for the
/// statement itself and why the flip can't be split out of it.
///
/// The flip is idempotent from the caller's point of view: a claim against
/// an already-`draining` batch (a second or later claim of the same batch)
/// still returns its share normally — the flip's `WHERE state = 'sealed'`
/// just matches zero rows that time, which is not an error.
pub async fn claim(
    client: &impl GenericClient,
    seg_seq: i64,
    claimed_by: &str,
    live_workers: i64,
) -> Result<Vec<i16>, StagingError> {
    // Bound as `f64`, not `i64`: `$3` appears in the query only inside a
    // `::float8` cast, so Postgres's parse analyzer infers its parameter
    // type as `float8` — an `i64` binding would then fail server-side type
    // checking (`WrongType`) rather than silently widening.
    let live_workers = (live_workers.max(1)) as f64;
    let rows = client
        .query(CLAIM_SQL, &[&seg_seq, &claimed_by, &live_workers])
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

/// Builds the [`BucketFilter`] for the buckets `claimed_by` actually holds
/// on `seg_seq`, read from `seg_claims` — never recomputed (doc 04: "Which
/// buckets a worker holds is read from the claims table, never
/// recomputed") — paired with the batch's own fixed `bucket_count`.
pub async fn owned_bucket_filter(
    client: &impl GenericClient,
    seg_seq: i64,
    claimed_by: &str,
) -> Result<BucketFilter, StagingError> {
    let bucket_count: i16 = client
        .query_one(
            "select bucket_count from segments where seg_seq = $1",
            &[&seg_seq],
        )
        .await?
        .get(0);
    let buckets: Vec<i64> = client
        .query(
            "select bucket from seg_claims where seg_seq = $1 and claimed_by = $2",
            &[&seg_seq, &claimed_by],
        )
        .await?
        .into_iter()
        .map(|row| {
            let bucket: i16 = row.get(0);
            bucket as i64
        })
        .collect();
    Ok(BucketFilter::buckets(bucket_count as i64, buckets))
}
