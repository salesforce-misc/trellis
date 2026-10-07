//! Drain holdups (#817): a page the drain keeps failing on while charging no
//! key and pausing no definition.
//!
//! Most of a page's failures end somewhere an operator can see: a key
//! charged and eventually held for the definition it fails in
//! ([`super::quarantine`]), or a definition paused by a halt
//! ([`super::halt`]). Some are charged to no one, and the drain surfaces the
//! error on every pass while the watermark stalls:
//!
//! - a refused read or write (`42501`) the catalog can't pin on a table, so
//!   the halt pauses nothing (a column grant, a function's `EXECUTE`, one of
//!   Trellis's own tables);
//! - isolation that reproduced nothing (records that fail only together) or
//!   stopped at its probe limit without pinning a key;
//! - an isolate-eligible failure whose retries ran out.
//!
//! Each records a *holdup* for the page's segments ([`record`]), in a short
//! transaction of its own after the page's transaction rolled back.
//! `Trellis::status` reports it as `drain_failure` on every unfrozen
//! definition that reads a table on the page, directly or through a
//! relationship ([`for_definition`]), and `self_check` reports every open
//! one ([`open`]). Recording charges and pauses nothing. A transient failure
//! (a lock timeout, a serialization failure, a storm of them during
//! isolation) isn't recorded: retrying it is the expected behaviour.
//!
//! The row is per segment (`drain_holdups.seg_seq`), with the buckets the
//! failing pages covered: several drain workers can hold different buckets
//! of one segment. The transaction that commits a page clears the page's
//! buckets ([`clear`]) and deletes the row once none is left, and the
//! segment's deletion cascades to it, so a holdup never outlives its page.
//! [`record`] writes only the buckets the failing worker still claims, and
//! locks those claims while it does, so a peer that reclaimed them (and may
//! already have committed the page) can't be left a stale row.

use std::collections::BTreeSet;
use std::time::SystemTime;

use tokio_postgres::{GenericClient, Transaction};

use crate::pool::Pool;

use super::apply::{ApplyError, SegmentStep};
use super::fold::FoldedChange;
use super::quarantine::CanonicalSrcTables;

/// A drain page that keeps failing with nothing charged or paused (#817):
/// one `drain_holdups` row, as `Trellis::status` reports it on each reader
/// (`DefinitionStatus::drain_failure`) and `self_check` reports every one
/// (`SelfCheckReport::drain_failures`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainFailure {
    /// The segment whose page fails.
    pub seg_seq: i64,
    /// The qualified source tables the failing page holds changes to.
    pub tables: Vec<String>,
    /// The latest failure's error, as the drain surfaced it.
    pub error: String,
    /// The latest failure's SQLSTATE, `None` when it didn't come from
    /// Postgres (an evaluator error, say).
    pub sqlstate: Option<String>,
    /// When a drain first failed on the page.
    pub since: SystemTime,
    /// When a drain last failed on it.
    pub last_seen: SystemTime,
    /// How many drain passes have failed on it.
    pub attempts: u32,
}

/// Records that the drain surfaced `err` for the page made of `steps` and
/// `folded`, charging and pausing nothing: upserts one `drain_holdups` row
/// per segment in one short transaction of its own. Call it once the page's
/// transaction has rolled back, and only for a failure charged to no one
/// (see the module doc).
///
/// Only the page's buckets `claimed_by` still claims are recorded, with
/// those claims locked (`for key share`) until the row commits: a reclaim
/// deletes the claim, so it waits for this transaction, and the new
/// claimant's commit then clears the row. A segment none of whose page
/// buckets is still claimed records nothing.
///
/// Never fails: if the write itself fails (Postgres refusing the drain's role
/// Trellis's own table is one of the failures it records), it logs a warning
/// and the drain surfaces `err` as it would have anyway.
pub(crate) async fn record(
    pool: &Pool,
    steps: &[SegmentStep],
    claimed_by: &str,
    folded: &[FoldedChange],
    err: &ApplyError,
) {
    if let Err(write_err) = try_record(pool, steps, claimed_by, folded, err).await {
        tracing::warn!(
            seg_seq = steps.first().map(|step| step.seg_seq),
            error = %err,
            write_error = %write_err,
            "couldn't record the drain holdup; the failure is surfaced unrecorded"
        );
    }
}

async fn try_record(
    pool: &Pool,
    steps: &[SegmentStep],
    claimed_by: &str,
    folded: &[FoldedChange],
    err: &ApplyError,
) -> Result<(), ApplyError> {
    let tables = page_tables(pool, folded).await?;
    let error = err.to_string();
    let sqlstate = sqlstate(err);
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    for step in steps {
        // The drain always pages with explicit buckets; only a direct caller
        // of `apply_and_mark_drained_many` (a probe, a test) passes none.
        let Some(page) = &step.page else { continue };
        txn.execute(
            "with held as ( \
               select bucket from seg_claims \
               where seg_seq = $1 and claimed_by = $6 and bucket = any($2::smallint[]) \
               for key share) \
             insert into drain_holdups as h \
               (seg_seq, buckets, tables, error, sqlstate, since, last_seen, attempts) \
             select $1, array(select bucket from held order by bucket), $3, $4, $5, \
                    clock_timestamp(), clock_timestamp(), 1 \
             where exists (select 1 from held) \
             on conflict (seg_seq) do update set \
               buckets = array(select distinct b from unnest(h.buckets || excluded.buckets) b \
                               order by b), \
               tables = array(select distinct t from unnest(h.tables || excluded.tables) t \
                              order by t), \
               error = excluded.error, \
               sqlstate = excluded.sqlstate, \
               last_seen = excluded.last_seen, \
               attempts = h.attempts + 1",
            &[
                &step.seg_seq,
                &page.buckets,
                &tables,
                &error,
                &sqlstate,
                &claimed_by,
            ],
        )
        .await?;
    }
    txn.commit().await?;
    Ok(())
}

/// The canonical source tables `folded` holds changes to, sorted. A deferred
/// relationship reverse's synthetic table names no table, so it is left out.
async fn page_tables(pool: &Pool, folded: &[FoldedChange]) -> Result<Vec<String>, ApplyError> {
    let mut canonical = CanonicalSrcTables::default();
    let mut tables = BTreeSet::new();
    for change in folded {
        if change.relationship_reverse_deferred.is_some() {
            continue;
        }
        tables.insert(canonical.get(pool, &change.src_table).await?);
    }
    Ok(tables.into_iter().collect())
}

/// The SQLSTATE of the first Postgres error on `err`'s source chain.
fn sqlstate(err: &(dyn std::error::Error + 'static)) -> Option<String> {
    let mut link = Some(err);
    while let Some(err) = link {
        if let Some(pg) = err.downcast_ref::<tokio_postgres::Error>() {
            return pg.code().map(|code| code.code().to_string());
        }
        link = err.source();
    }
    None
}

/// Clears, in the transaction `txn` that commits `step`'s page, the holdup
/// on the page's buckets of its segment: the row goes once no failing
/// bucket is left, and keeps the others' (a peer's page of the same segment
/// may still be failing). A step with no explicit page completes the whole
/// claim, so it deletes the row.
pub(super) async fn clear(txn: &Transaction<'_>, step: &SegmentStep) -> Result<(), ApplyError> {
    match &step.page {
        Some(page) => {
            txn.execute(
                "with cleared as ( \
                   delete from drain_holdups \
                   where seg_seq = $1 and buckets <@ $2::smallint[]) \
                 update drain_holdups h \
                 set buckets = array(select b from unnest(h.buckets) b \
                                     where b <> all($2::smallint[]) order by b) \
                 where h.seg_seq = $1 and not h.buckets <@ $2::smallint[]",
                &[&step.seg_seq, &page.buckets],
            )
            .await?;
        }
        None => {
            txn.execute(
                "delete from drain_holdups where seg_seq = $1",
                &[&step.seg_seq],
            )
            .await?;
        }
    }
    Ok(())
}

const COLUMNS: &str = "h.seg_seq, h.tables, h.error, h.sqlstate, h.since, h.last_seen, h.attempts";

fn from_row(row: &tokio_postgres::Row) -> DrainFailure {
    let attempts: i32 = row.get("attempts");
    DrainFailure {
        seg_seq: row.get("seg_seq"),
        tables: row.get("tables"),
        error: row.get("error"),
        sqlstate: row.get("sqlstate"),
        since: row.get("since"),
        last_seen: row.get("last_seen"),
        attempts: u32::try_from(attempts).unwrap_or(0),
    }
}

/// Every open holdup, oldest first: what `self_check` reports for the
/// instance.
pub(crate) async fn open(
    client: &impl GenericClient,
) -> Result<Vec<DrainFailure>, tokio_postgres::Error> {
    Ok(client
        .query(
            &format!("select {COLUMNS} from drain_holdups h order by h.since, h.seg_seq"),
            &[],
        )
        .await?
        .iter()
        .map(from_row)
        .collect())
}

/// The oldest open holdup on a page holding a table the definition sourced
/// from `source_table` reads: the source itself, or the to-side of a
/// relationship its fields read through. `definition_text` is the
/// definition's, to tell which of its source's relationships it reads
/// through. The caller decides whether the definition is unfrozen.
pub(crate) async fn for_definition(
    client: &impl GenericClient,
    source_table: &str,
    definition_text: &str,
) -> Result<Option<DrainFailure>, tokio_postgres::Error> {
    // `drain_holdups` holds a row per failing segment, so it's empty, or
    // nearly so, and read whole. `through` names the source's relationships
    // whose to-side is on the page.
    let rows = client
        .query(
            &format!(
                "select {COLUMNS}, $1 = any(h.tables) as direct, \
                        array(select r.name from relationship_definitions r \
                              where r.from_schema || '.' || r.from_table = $1 \
                                and r.to_schema || '.' || r.to_table = any(h.tables)) \
                          as through \
                 from drain_holdups h \
                 order by h.since, h.seg_seq"
            ),
            &[&source_table],
        )
        .await?;
    // The relationships the definition reads through, parsed only once a
    // held page holds a relationship's to-side. A stored definition always
    // parses; if it somehow didn't (`None`), every relationship of its
    // source counts as read.
    let mut read_through: Option<Option<BTreeSet<String>>> = None;
    for row in &rows {
        if row.get::<_, bool>("direct") {
            return Ok(Some(from_row(row)));
        }
        let through: Vec<String> = row.get("through");
        if through.is_empty() {
            continue;
        }
        let names = read_through.get_or_insert_with(|| {
            crate::defs::parse(definition_text).ok().map(|def| {
                crate::defs::eval::relationship_references(&def)
                    .into_iter()
                    .map(|(name, _)| name)
                    .collect()
            })
        });
        let reads = match names {
            Some(names) => through.iter().any(|name| names.contains(name)),
            None => true,
        };
        if reads {
            return Ok(Some(from_row(row)));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::staging::apply::PageClaim;

    /// A worker whose claim was reclaimed (a peer may already have committed
    /// the page, clearing any holdup) records nothing on the buckets it lost,
    /// and only the buckets it still claims on the ones it kept.
    #[tokio::test]
    async fn a_holdup_is_recorded_only_on_the_buckets_the_worker_still_claims() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let config = crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
        let trellis = crate::app::Trellis::connect(config.clone(), Default::default())
            .await
            .expect("connect, which migrates");
        let pool = Pool::new(&config).expect("a same-crate pool");
        let client = pool.get().await.expect("a connection");
        client
            .batch_execute(
                "insert into segments (seg_seq, ring_slot, state) \
                   values (9001, 0, 'sealed'), (9002, 0, 'sealed'); \
                 insert into seg_claims (seg_seq, bucket, claimed_by) values \
                   (9001, 3, 'peer'), (9002, 4, 'worker-a'), (9002, 5, 'peer')",
            )
            .await
            .expect("seed segments and claims");
        let step = |seg_seq, buckets: Vec<i16>| SegmentStep {
            seg_seq,
            page: Some(PageClaim {
                held: buckets.clone(),
                buckets,
                next: None,
            }),
        };

        record(
            &pool,
            &[step(9001, vec![3]), step(9002, vec![4, 5])],
            "worker-a",
            &[],
            &ApplyError::ClaimLost,
        )
        .await;

        let rows = client
            .query(
                "select seg_seq, buckets from drain_holdups order by seg_seq",
                &[],
            )
            .await
            .expect("read holdups");
        let rows: Vec<(i64, Vec<i16>)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
        assert_eq!(rows, vec![(9002, vec![4])]);
        trellis.shutdown().await.expect("shutdown");
    }
}
