//! Bounded drain paging (issue #620, epic #556 milestone A2a).
//!
//! A drain holds at most `drain_batch_cap` folded records at once: a share
//! larger than the cap drains in keyset pages on `(route, src_table, key)`,
//! each its own compute-and-apply transaction that advances the bucket's
//! `drain_cursor` behind a claim check. These tests drive the pages directly
//! (`drain_many_with_hooks`) with small caps, never by waiting for a running
//! client to converge (#297), and check the target against the oracle: SUM and
//! COUNT over the live source for the aggregate, the source's own rows for the
//! 1-1 copy.

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::{
    create_aggregate_target_table, create_definition, create_target_table, parse,
    source_primary_key,
};
use trellis::staging::apply::{self, ApplyError, DrainHooks, ManyApplyOutcome};
use trellis::staging::{StagedWatermark, TRUNCATE_SENTINEL_KEY, claim, liveness};

const WAKE: &str = "trellis_drain_paging_test";

const GROUP_TOTALS: &str = "TRANSFORM grp_totals FROM items GROUP BY grp \
     SELECT grp AS grp, SUM(amount) AS total, COUNT(*) AS n";

const ITEM_COPY: &str = "TRANSFORM item_copy FROM items SELECT amount AS amount";

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

fn source_columns() -> HashMap<String, ValueType> {
    ["id", "grp", "amount"]
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

/// `items` plus the SUM/COUNT aggregate over it.
async fn setup_aggregate(db: &testkit::TestDatabase, client: &Client) {
    client
        .batch_execute(
            "create table items (id integer primary key, grp integer, amount numeric); \
             alter table items replica identity full",
        )
        .await
        .expect("create items");
    let def = parse(GROUP_TOTALS).expect("parse");
    create_definition(&db.pool, GROUP_TOTALS, &source_columns())
        .await
        .expect("create aggregate definition");
    create_aggregate_target_table(&db.pool, &def, "public", &source_columns())
        .await
        .expect("create aggregate target");
}

/// `items` plus a 1-1 copy of it.
async fn setup_copy(db: &testkit::TestDatabase, client: &Client) {
    client
        .batch_execute(
            "create table items (id integer primary key, grp integer, amount numeric); \
             alter table items replica identity full",
        )
        .await
        .expect("create items");
    let def = parse(ITEM_COPY).expect("parse");
    create_definition(&db.pool, ITEM_COPY, &source_columns())
        .await
        .expect("create 1-1 definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("source pk");
    create_target_table(
        &db.pool,
        &def,
        "public",
        &pk,
        &source_columns(),
        &def.source,
    )
    .await
    .expect("create 1-1 target");
}

async fn active_slot_table(client: &Client) -> String {
    let slot = trellis::staging::active_ring_slot(client)
        .await
        .expect("active ring slot");
    format!("seg_{slot}")
}

/// Stages one change per id in `ids` into the active ring slot, in id order:
/// `op` with images built from `old`/`new` (`(grp, amount)` per id, or `None`).
async fn stage(
    client: &Client,
    op: &str,
    ids: &[i32],
    old: impl Fn(i32) -> Option<(i32, i32)>,
    new: impl Fn(i32) -> Option<(i32, i32)>,
) {
    let table = active_slot_table(client).await;
    let src_table = format!("{DEFAULT_SCHEMA}.items");
    let image = |v: Option<(i32, i32)>| {
        v.map(|(grp, amount)| format!(r#"{{"grp":"{grp}","amount":"{amount}"}}"#))
    };
    for &id in ids {
        let key = id.to_string();
        let old_image = image(old(id));
        let new_image = image(new(id));
        client
            .execute(
                &format!(
                    "insert into {table} (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                     values ($1, $2, $3, pg_current_wal_insert_lsn(), \
                             $4::text::jsonb, $5::text::jsonb, 0)"
                ),
                &[&src_table, &key, &op, &old_image, &new_image],
            )
            .await
            .expect("stage change");
    }
}

async fn stage_truncate(client: &Client) {
    let table = active_slot_table(client).await;
    let src_table = format!("{DEFAULT_SCHEMA}.items");
    client
        .execute(
            &format!(
                "insert into {table} (src_table, key, op, lsn, hop_gen) \
                 values ($1, $2, 'truncate', pg_current_wal_insert_lsn(), 0)"
            ),
            &[&src_table, &TRUNCATE_SENTINEL_KEY],
        )
        .await
        .expect("stage truncate");
}

async fn seal(client: &mut Client) -> i64 {
    use trellis::staging::seal;
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

async fn drain(
    pool: &trellis::Pool,
    seg_seq: i64,
    claimed_by: &str,
    cap: usize,
    hooks: &mut DrainHooks,
) -> Result<Option<ManyApplyOutcome>, ApplyError> {
    apply::drain_many_with_hooks(
        pool,
        &[seg_seq],
        claimed_by,
        1,
        WAKE,
        &StagedWatermark::saturated(),
        cap,
        hooks,
    )
    .await
}

async fn drain_all(pool: &trellis::Pool, seg_seq: i64, cap: usize) -> ManyApplyOutcome {
    drain(pool, seg_seq, "worker", cap, &mut DrainHooks::default())
        .await
        .expect("drain")
        .expect("drain claims something")
}

/// `grp -> (total, n)` from the target, as text.
async fn read_totals(client: &Client) -> HashMap<String, (String, String)> {
    client
        .query(
            "select grp::text, total::text, n::text from grp_totals",
            &[],
        )
        .await
        .expect("read grp_totals")
        .into_iter()
        .map(|r| (r.get(0), (r.get(1), r.get(2))))
        .collect()
}

/// The oracle: SUM/COUNT over the live `items`, in [`read_totals`]' shape.
async fn oracle_totals(client: &Client) -> HashMap<String, (String, String)> {
    client
        .query(
            "select grp::text, sum(amount)::text, count(*)::text from items group by grp",
            &[],
        )
        .await
        .expect("oracle")
        .into_iter()
        .map(|r| (r.get(0), (r.get(1), r.get(2))))
        .collect()
}

async fn read_copy(client: &Client) -> HashMap<String, String> {
    client
        .query("select id::text, amount::text from item_copy", &[])
        .await
        .expect("read item_copy")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

async fn oracle_copy(client: &Client) -> HashMap<String, String> {
    client
        .query("select id::text, amount::text from items", &[])
        .await
        .expect("read items")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

async fn segment_state(client: &Client, seg_seq: i64) -> (String, i64) {
    let row = client
        .query_one(
            "select state, drained_mask from segments where seg_seq = $1",
            &[&seg_seq],
        )
        .await
        .expect("segment row");
    (row.get(0), row.get(1))
}

async fn cursor_count(client: &Client, seg_seq: i64) -> i64 {
    client
        .query_one(
            "select count(*) from drain_cursor where seg_seq = $1",
            &[&seg_seq],
        )
        .await
        .expect("count cursors")
        .get(0)
}

/// Ages every claim on `seg_seq` past the default TTL and reclaims it, as the
/// maintenance loop does for a worker that died.
async fn reclaim(client: &Client, seg_seq: i64) -> u64 {
    client
        .execute(
            "update seg_claims set claimed_at = now() - interval '1 hour' where seg_seq = $1",
            &[&seg_seq],
        )
        .await
        .expect("age claims");
    liveness::reclaim_stale(client, Duration::from_secs(30))
        .await
        .expect("reclaim stale claims")
}

/// Stages `ids` as inserts into the live table and the ring alike, with
/// `grp = id % 7` and `amount = id`.
async fn insert_items(client: &Client, ids: &[i32]) {
    for &id in ids {
        client
            .execute(
                "insert into items (id, grp, amount) values ($1, $1 % 7, $1)",
                &[&id],
            )
            .await
            .expect("insert live item");
    }
    stage(client, "insert", ids, |_| None, |id| Some((id % 7, id))).await;
}

/// A share of 300 distinct keys at cap 40 drains in ⌈300 / 40⌉ = 8 pages, and
/// lands exactly the oracle. The cap counts folded records, not ring rows:
/// 20 keys staged 30 times each at cap 5 is ⌈20 / 5⌉ = 4 pages, not 120.
#[tokio::test]
async fn a_share_over_the_cap_drains_in_at_most_ceil_share_over_cap_pages() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    setup_aggregate(&db, &client).await;

    let ids: Vec<i32> = (1..=300).collect();
    insert_items(&client, &ids).await;
    let seg = seal(&mut client).await;

    let outcome = drain_all(&db.pool, seg, 40).await;
    assert!(
        outcome.pages > 1 && outcome.pages <= 300usize.div_ceil(40),
        "300 records at cap 40 must page, in at most 8 pages: {outcome:?}"
    );
    assert_eq!(outcome.segments_drained, vec![(seg, true)]);
    assert_eq!(read_totals(&client).await, oracle_totals(&client).await);
    let (state, mask) = segment_state(&client, seg).await;
    assert_eq!(state, "drained");
    assert_eq!(mask, (1 << trellis::staging::claim::SEG_BUCKETS) - 1);

    // 20 keys, each staged 30 times (an insert, then 29 amount updates).
    let hot: Vec<i32> = (1001..=1020).collect();
    for &id in &hot {
        client
            .execute(
                "insert into items (id, grp, amount) values ($1, $1 % 7, $1 + 29)",
                &[&id],
            )
            .await
            .expect("insert hot item");
    }
    stage(&client, "insert", &hot, |_| None, |id| Some((id % 7, id))).await;
    for step in 1..30 {
        stage(
            &client,
            "update",
            &hot,
            |id| Some((id % 7, id + step - 1)),
            |id| Some((id % 7, id + step)),
        )
        .await;
    }
    let seg = seal(&mut client).await;
    let outcome = drain_all(&db.pool, seg, 5).await;
    assert!(
        outcome.pages > 1 && outcome.pages <= 20usize.div_ceil(5),
        "600 ring rows folding to 20 records at cap 5 is at most 4 pages: {outcome:?}"
    );
    assert_eq!(read_totals(&client).await, oracle_totals(&client).await);
}

/// A share that fits the cap takes the direct path: one page, no cursor.
#[tokio::test]
async fn a_share_under_the_cap_drains_in_one_page_without_a_cursor() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    setup_aggregate(&db, &client).await;

    let ids: Vec<i32> = (1..=300).collect();
    insert_items(&client, &ids).await;
    let seg = seal(&mut client).await;

    let outcome = drain_all(&db.pool, seg, 1000).await;
    assert_eq!(outcome.pages, 1);
    assert_eq!(outcome.segments_drained, vec![(seg, true)]);
    assert_eq!(cursor_count(&client, seg).await, 0);
    assert_eq!(read_totals(&client).await, oracle_totals(&client).await);
}

/// The exactly-once property: a drainer that stops after page k (its claim
/// held, its cursor committed) is reclaimed, and the next claimant resumes at
/// page k + 1. Re-applying pages 1..k would double their SUM/COUNT deltas;
/// skipping page k + 1 would lose them. Either shows up against the oracle.
#[tokio::test]
async fn a_reclaim_after_page_k_resumes_at_page_k_plus_one_and_matches_the_oracle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    setup_aggregate(&db, &client).await;

    // Seed groups through an ordinary drain, then a mixed batch: inserts,
    // amount updates, group moves and deletes, so a double or missed page
    // moves SUM and COUNT both.
    let seed: Vec<i32> = (1..=200).collect();
    insert_items(&client, &seed).await;
    let seg = seal(&mut client).await;
    drain_all(&db.pool, seg, 1000).await;
    assert_eq!(read_totals(&client).await, oracle_totals(&client).await);

    let fresh: Vec<i32> = (201..=320).collect();
    insert_items(&client, &fresh).await;
    let bumped: Vec<i32> = (1..=200).filter(|id| id % 3 == 0).collect();
    for &id in &bumped {
        client
            .execute(
                "update items set amount = amount + 1000 where id = $1",
                &[&id],
            )
            .await
            .expect("bump");
    }
    stage(
        &client,
        "update",
        &bumped,
        |id| Some((id % 7, id)),
        |id| Some((id % 7, id + 1000)),
    )
    .await;
    let moved: Vec<i32> = (1..=200).filter(|id| id % 5 == 1).collect();
    for &id in &moved {
        client
            .execute("update items set grp = 100 where id = $1", &[&id])
            .await
            .expect("move");
    }
    stage(
        &client,
        "update",
        &moved,
        |id| {
            let amount = if id % 3 == 0 { id + 1000 } else { id };
            Some((id % 7, amount))
        },
        |id| {
            let amount = if id % 3 == 0 { id + 1000 } else { id };
            Some((100, amount))
        },
    )
    .await;
    let deleted: Vec<i32> = (1..=200).filter(|id| id % 5 == 2).collect();
    for &id in &deleted {
        client
            .execute("delete from items where id = $1", &[&id])
            .await
            .expect("delete");
    }
    stage(
        &client,
        "delete",
        &deleted,
        |id| {
            let amount = if id % 3 == 0 { id + 1000 } else { id };
            Some((id % 7, amount))
        },
        |_| None,
    )
    .await;
    let seg = seal(&mut client).await;

    // Worker A commits three pages and stops, claim held.
    let mut stop = DrainHooks {
        stop_after_pages: Some(3),
        ..DrainHooks::default()
    };
    let partial = drain(&db.pool, seg, "worker-a", 25, &mut stop)
        .await
        .expect("worker A's pages")
        .expect("worker A claims");
    assert_eq!(partial.pages, 3);
    assert_eq!(partial.segments_drained, vec![(seg, false)]);
    assert_ne!(
        read_totals(&client).await,
        oracle_totals(&client).await,
        "three pages of a larger share can't match the oracle yet"
    );
    assert!(cursor_count(&client, seg).await > 0, "page 3 left a cursor");
    let (state, mask) = segment_state(&client, seg).await;
    assert_eq!((state.as_str(), mask), ("draining", 0));

    // A peer can't take the buckets while A's claim stands.
    assert!(
        drain(&db.pool, seg, "worker-b", 25, &mut DrainHooks::default())
            .await
            .expect("worker B's attempt")
            .is_none(),
        "A still holds every bucket"
    );

    assert!(reclaim(&client, seg).await > 0, "A's claim is reclaimed");

    // Worker B resumes where A's cursor says, and finishes.
    let rest = drain(&db.pool, seg, "worker-b", 25, &mut DrainHooks::default())
        .await
        .expect("worker B's pages")
        .expect("worker B claims");
    assert_eq!(rest.segments_drained, vec![(seg, true)]);
    let records = 120 + bumped.len() + moved.len() + deleted.len();
    assert!(
        partial.pages + rest.pages <= records.div_ceil(25),
        "B resumed after page 3 rather than starting over: {} + {} pages",
        partial.pages,
        rest.pages
    );
    assert_eq!(read_totals(&client).await, oracle_totals(&client).await);
    let (state, _) = segment_state(&client, seg).await;
    assert_eq!(state, "drained");
}

/// A stale claimant's page rolls back. Worker A pauses before page 3; its
/// claim is aged, reclaimed and taken by worker B; A's page 3 then fails its
/// claim check (`ClaimLost`) and applies nothing. B resumes after A's page 2,
/// and the target matches the oracle.
#[tokio::test]
async fn a_stale_claimants_page_rolls_back_as_claim_lost() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    setup_aggregate(&db, &client).await;

    let ids: Vec<i32> = (1..=300).collect();
    insert_items(&client, &ids).await;
    let seg = seal(&mut client).await;

    let (paused_tx, paused_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let pool = db.pool.clone();
    let worker_a = tokio::spawn(async move {
        let mut hooks = DrainHooks {
            pause_before_page: Some((3, paused_tx, resume_rx)),
            ..DrainHooks::default()
        };
        drain(&pool, seg, "worker-a", 40, &mut hooks).await
    });
    paused_rx.await.expect("worker A reaches page 3");
    let after_two_pages = read_totals(&client).await;

    assert!(reclaim(&client, seg).await > 0);
    let won = claim::claim(&client, seg, "worker-b", 1)
        .await
        .expect("worker B claims");
    assert!(!won.is_empty(), "B takes the reclaimed buckets");

    resume_tx.send(()).expect("resume worker A");
    let result = worker_a.await.expect("worker A's task");
    assert!(
        matches!(result, Err(ApplyError::ClaimLost)),
        "A's page 3 must fail its claim check: {result:?}"
    );
    assert_eq!(
        read_totals(&client).await,
        after_two_pages,
        "A's rolled-back page applied nothing"
    );
    let deaths: i64 = client
        .query_one("select count(*) from key_deaths", &[])
        .await
        .expect("count key deaths")
        .get(0);
    assert_eq!(
        deaths, 0,
        "a lost claim is no key's fault: isolation must not charge the page's keys"
    );

    let rest = drain(&db.pool, seg, "worker-b", 40, &mut DrainHooks::default())
        .await
        .expect("worker B's pages")
        .expect("worker B holds the buckets");
    assert_eq!(rest.segments_drained, vec![(seg, true)]);
    assert!(rest.pages <= 300usize.div_ceil(40) - 2);
    assert_eq!(read_totals(&client).await, oracle_totals(&client).await);
}

/// A key never splits across pages: every key here is staged five times
/// (insert, then four updates), so page boundaries fall between rows of one
/// key wherever the keyset lands. A split key would be written by two pages,
/// so the pages' write counts would sum past the key count.
#[tokio::test]
async fn a_boundary_key_lands_whole_in_one_page() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    setup_copy(&db, &client).await;

    let ids: Vec<i32> = (1..=60).collect();
    for &id in &ids {
        client
            .execute(
                "insert into items (id, grp, amount) values ($1, 0, $1 * 10 + 4)",
                &[&id],
            )
            .await
            .expect("insert live item");
    }
    stage(&client, "insert", &ids, |_| None, |id| Some((0, id * 10))).await;
    for step in 1..=4 {
        stage(
            &client,
            "update",
            &ids,
            |id| Some((0, id * 10 + step - 1)),
            |id| Some((0, id * 10 + step)),
        )
        .await;
    }
    let seg = seal(&mut client).await;

    let outcome = drain_all(&db.pool, seg, 7).await;
    assert!(outcome.pages > 1 && outcome.pages <= 60usize.div_ceil(7));
    assert_eq!(
        outcome.keys_written, 60,
        "each key written by exactly one page: {outcome:?}"
    );
    assert_eq!(read_copy(&client).await, oracle_copy(&client).await);
}

/// A key staged in two segments, each drained in pages: the second segment's
/// pages move and delete keys the first segment's pages inserted.
#[tokio::test]
async fn a_key_straddling_two_paged_segments_matches_the_oracle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    setup_aggregate(&db, &client).await;

    let ids: Vec<i32> = (1..=280).collect();
    insert_items(&client, &ids).await;
    let first = seal(&mut client).await;

    let moved: Vec<i32> = ids.iter().copied().filter(|id| id % 2 == 0).collect();
    for &id in &moved {
        client
            .execute(
                "update items set grp = 50, amount = amount * 2 where id = $1",
                &[&id],
            )
            .await
            .expect("move");
    }
    stage(
        &client,
        "update",
        &moved,
        |id| Some((id % 7, id)),
        |id| Some((50, id * 2)),
    )
    .await;
    let deleted: Vec<i32> = ids.iter().copied().filter(|id| id % 6 == 3).collect();
    for &id in &deleted {
        client
            .execute("delete from items where id = $1", &[&id])
            .await
            .expect("delete");
    }
    stage(
        &client,
        "delete",
        &deleted,
        |id| Some((id % 7, id)),
        |_| None,
    )
    .await;
    // Pad the second segment past the split threshold so it has buckets too.
    let more: Vec<i32> = (281..=400).collect();
    insert_items(&client, &more).await;
    let second = seal(&mut client).await;

    let one = drain_all(&db.pool, first, 30).await;
    assert!(one.pages > 1);
    let two = drain_all(&db.pool, second, 30).await;
    assert!(two.pages > 1);
    assert_eq!(read_totals(&client).await, oracle_totals(&client).await);
}

/// Stages a truncate segment: 60 pre-truncate inserts (keys 1..=60, voided by
/// the truncate), the sentinel, then 40 post-truncate inserts (keys 41..=80:
/// 41..=60 re-inserted, 61..=80 new). The live table ends holding 41..=80.
/// Keys 1..=40 were seeded by an earlier drain, so the clear has rows to erase.
async fn stage_truncate_segment(client: &mut Client, pool: &trellis::Pool) -> i64 {
    let seed: Vec<i32> = (1..=40).collect();
    insert_items(client, &seed).await;
    let seg = seal(client).await;
    drain_all(pool, seg, 1000).await;

    let pre: Vec<i32> = (1..=60).collect();
    stage(
        client,
        "insert",
        &pre[40..],
        |_| None,
        |id| Some((id % 7, id)),
    )
    .await;
    stage(
        client,
        "update",
        &pre,
        |id| Some((id % 7, id)),
        |id| Some((id % 7, id + 1)),
    )
    .await;
    stage_truncate(client).await;
    client
        .batch_execute("truncate items")
        .await
        .expect("truncate live items");
    let post: Vec<i32> = (41..=80).collect();
    insert_items(client, &post).await;
    seal(client).await
}

/// A truncate segment larger than the cap pages like any other, and the
/// sentinel sorts first, so the clear runs on page 1 before any key applies.
#[tokio::test]
async fn a_paged_truncate_segment_clears_once_and_matches_the_oracle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    setup_aggregate(&db, &client).await;

    let seg = stage_truncate_segment(&mut client, &db.pool).await;
    let outcome = drain_all(&db.pool, seg, 10).await;
    assert!(
        outcome.pages > 1,
        "81 keys at cap 10 must page: {outcome:?}"
    );
    assert_eq!(outcome.segments_drained, vec![(seg, true)]);
    assert_eq!(read_totals(&client).await, oracle_totals(&client).await);
}

/// Each rescan page re-folds its key range against the whole window, truncate
/// sentinel included: a page after the one holding the sentinel must still
/// void its keys' pre-truncate rows, or a key the truncate erased (1..=40, only
/// ever updated before it) would come back. Driven across a reclaim, so pages
/// 2.. are folded by a fresh claimant that never saw page 1 and holds nothing
/// of the truncate but what its own page's fold finds in the window.
#[tokio::test]
async fn later_pages_of_a_truncate_segment_still_void_pre_truncate_rows_after_a_reclaim() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    setup_aggregate(&db, &client).await;

    let seg = stage_truncate_segment(&mut client, &db.pool).await;
    let mut stop = DrainHooks {
        stop_after_pages: Some(1),
        ..DrainHooks::default()
    };
    let first = drain(&db.pool, seg, "worker-a", 10, &mut stop)
        .await
        .expect("page 1")
        .expect("claims");
    assert_eq!(first.pages, 1);
    let after_page_one = read_totals(&client).await;
    let seeded_rows: i64 = after_page_one
        .values()
        .map(|(_, n)| n.parse::<i64>().expect("count"))
        .sum();
    assert!(
        seeded_rows < 40,
        "page 1 cleared the 40 seeded rows before applying its own keys: {after_page_one:?}"
    );

    assert!(reclaim(&client, seg).await > 0);
    let rest = drain(&db.pool, seg, "worker-b", 10, &mut DrainHooks::default())
        .await
        .expect("remaining pages")
        .expect("claims");
    assert!(rest.pages > 1);
    assert_eq!(rest.segments_drained, vec![(seg, true)]);
    assert_eq!(read_totals(&client).await, oracle_totals(&client).await);
}

/// Small segments coalesce into one direct drain while their row counts fit
/// the cap; a segment over the cap drains alone, in pages, and the small ones
/// behind it wait for the next call rather than riding along.
#[tokio::test]
async fn an_oversized_segment_drains_alone_and_small_ones_coalesce_after_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    setup_aggregate(&db, &client).await;

    insert_items(&client, &(1..=300).collect::<Vec<_>>()).await;
    let big = seal(&mut client).await;
    insert_items(&client, &(301..=320).collect::<Vec<_>>()).await;
    let small_a = seal(&mut client).await;
    insert_items(&client, &(321..=340).collect::<Vec<_>>()).await;
    let small_b = seal(&mut client).await;

    let batch = apply::next_claimable_segments(&client, 100)
        .await
        .expect("claimable");
    assert_eq!(
        batch,
        vec![big, small_a, small_b],
        "the oversized first segment doesn't count against the ones after it"
    );

    let first = apply::drain_many_with_hooks(
        &db.pool,
        &batch,
        "worker",
        1,
        WAKE,
        &StagedWatermark::saturated(),
        100,
        &mut DrainHooks::default(),
    )
    .await
    .expect("drain")
    .expect("claims");
    assert_eq!(first.segments_drained, vec![(big, true)], "{first:?}");
    assert!(first.pages > 1);
    let unclaimed: i64 = client
        .query_one(
            "select count(*) from seg_claims where seg_seq = any($1)",
            &[&vec![small_a, small_b]],
        )
        .await
        .expect("count claims")
        .get(0);
    assert_eq!(
        unclaimed, 0,
        "the small segments weren't held behind the big one"
    );

    let batch = apply::next_claimable_segments(&client, 100)
        .await
        .expect("claimable");
    assert_eq!(batch, vec![small_a, small_b]);
    let second = apply::drain_many_with_hooks(
        &db.pool,
        &batch,
        "worker",
        1,
        WAKE,
        &StagedWatermark::saturated(),
        100,
        &mut DrainHooks::default(),
    )
    .await
    .expect("drain")
    .expect("claims");
    assert_eq!(second.pages, 1);
    assert_eq!(
        second.segments_drained,
        vec![(small_a, true), (small_b, true)]
    );
    assert_eq!(read_totals(&client).await, oracle_totals(&client).await);
}

/// The claim commits before the fold now, so it must not commit on a segment
/// whose fence isn't published yet: `CLAIM_SQL`'s `sealed -> draining` flip
/// would commit, and seal phase 2 only fences a segment still `sealed`, so the
/// segment would never be fenced, and never drain. The drain refuses the
/// unfenced segment without claiming it, and drains it once phase 2 runs.
#[tokio::test]
async fn a_drain_never_claims_a_segment_before_its_fence_is_published() {
    use trellis::staging::seal;

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    setup_aggregate(&db, &client).await;

    insert_items(&client, &(1..=20).collect::<Vec<_>>()).await;
    let seg = seal::seal_phase1(&mut client)
        .await
        .expect("seal phase 1")
        .sealed_seg_seq;

    let result = drain(&db.pool, seg, "worker", 1000, &mut DrainHooks::default()).await;
    assert!(
        matches!(
            result,
            Err(ApplyError::Staging(
                trellis::staging::StagingError::UnfencedSealedSegment { .. }
            ))
        ),
        "an unfenced segment can't drain yet: {result:?}"
    );
    let (state, _) = segment_state(&client, seg).await;
    assert_eq!(state, "sealed", "the refused drain left no committed flip");
    let claims: i64 = client
        .query_one(
            "select count(*) from seg_claims where seg_seq = $1",
            &[&seg],
        )
        .await
        .expect("count claims")
        .get(0);
    assert_eq!(claims, 0);

    seal::seal_phase2(&client, seg, "wake")
        .await
        .expect("seal phase 2");
    let outcome = drain_all(&db.pool, seg, 1000).await;
    assert_eq!(outcome.segments_drained, vec![(seg, true)]);
    assert_eq!(read_totals(&client).await, oracle_totals(&client).await);
}

/// Ages and reclaims only `bucket`'s claim on `seg_seq`: the partial reclaim a
/// sweep makes when it wins some of a worker's rows and skips others.
async fn reclaim_bucket(client: &Client, seg_seq: i64, bucket: i16) -> u64 {
    client
        .execute(
            "update seg_claims set claimed_at = now() - interval '1 hour' \
             where seg_seq = $1 and bucket = $2",
            &[&seg_seq, &bucket],
        )
        .await
        .expect("age one claim");
    liveness::reclaim_stale(client, Duration::from_secs(30))
        .await
        .expect("reclaim stale claims")
}

/// Pauses worker A before page `page` of `seg_seq` at `cap`, takes bucket 0
/// away from it (reclaimed, claimed and fully drained by worker B), then lets A
/// resume. Returns A's result.
async fn lose_bucket_zero_before_page(
    db: &testkit::TestDatabase,
    client: &Client,
    seg_seq: i64,
    cap: usize,
    page: usize,
) -> Result<Option<ManyApplyOutcome>, ApplyError> {
    let (paused_tx, paused_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let pool = db.pool.clone();
    let worker_a = tokio::spawn(async move {
        let mut hooks = DrainHooks {
            pause_before_page: Some((page, paused_tx, resume_rx)),
            ..DrainHooks::default()
        };
        drain(&pool, seg_seq, "worker-a", cap, &mut hooks).await
    });
    paused_rx.await.expect("worker A reaches the page");

    assert_eq!(reclaim_bucket(client, seg_seq, 0).await, 1);
    assert_eq!(
        claim::claim(client, seg_seq, "worker-b", 1)
            .await
            .expect("worker B claims"),
        vec![0],
        "B takes exactly the reclaimed bucket"
    );
    let b = drain(
        &db.pool,
        seg_seq,
        "worker-b",
        cap,
        &mut DrainHooks::default(),
    )
    .await
    .expect("worker B drains bucket 0")
    .expect("worker B holds bucket 0");
    assert_eq!(b.segments_drained, vec![(seg_seq, false)]);

    resume_tx.send(()).expect("resume worker A");
    worker_a.await.expect("worker A's task")
}

/// Releases worker A's leftover claims (as the client loop does on error) and
/// lets worker C drain what remains.
async fn finish_with_worker_c(
    db: &testkit::TestDatabase,
    client: &Client,
    seg_seq: i64,
    cap: usize,
) {
    liveness::release(client, seg_seq, "worker-a")
        .await
        .expect("release worker A");
    let c = drain(
        &db.pool,
        seg_seq,
        "worker-c",
        cap,
        &mut DrainHooks::default(),
    )
    .await
    .expect("worker C drains the rest")
    .expect("worker C claims the rest");
    assert_eq!(c.segments_drained, vec![(seg_seq, true)]);
}

/// The completion rule: a page completes only if *every* one of its buckets is
/// still claimed. Worker A folds its whole direct share (8 buckets), then loses
/// bucket 0 to worker B, who drains it. Under the old "at least one bucket"
/// rule A's completion would still have committed, applying bucket 0's deltas
/// a second time.
#[tokio::test]
async fn a_direct_drain_that_lost_one_of_its_buckets_commits_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    setup_aggregate(&db, &client).await;

    let ids: Vec<i32> = (1..=300).collect();
    insert_items(&client, &ids).await;
    let seg = seal(&mut client).await;

    let result = lose_bucket_zero_before_page(&db, &client, seg, 1000, 1).await;
    assert!(
        matches!(result, Err(ApplyError::ClaimLost)),
        "A's completion must find every bucket it folded: {result:?}"
    );
    finish_with_worker_c(&db, &client, seg, 1000).await;
    assert_eq!(read_totals(&client).await, oracle_totals(&client).await);
}

/// The page claim check: a non-final page commits only if the claim still
/// holds *every* bucket the worker paged. Worker A pages its 8 buckets as one
/// union; before page 3 it loses bucket 0 to worker B, who resumes bucket 0
/// from A's cursor and finishes it. A's page 3 must then roll back, or it
/// would apply bucket 0's keys in that page a second time.
#[tokio::test]
async fn a_page_whose_worker_lost_one_held_bucket_rolls_back() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    setup_aggregate(&db, &client).await;

    let ids: Vec<i32> = (1..=300).collect();
    insert_items(&client, &ids).await;
    let seg = seal(&mut client).await;

    let result = lose_bucket_zero_before_page(&db, &client, seg, 40, 3).await;
    assert!(
        matches!(result, Err(ApplyError::ClaimLost)),
        "A's page 3 must fail its claim check: {result:?}"
    );
    finish_with_worker_c(&db, &client, seg, 40).await;
    assert_eq!(read_totals(&client).await, oracle_totals(&client).await);
}

/// A claimant whose buckets sit at different cursors pages each cursor group
/// separately. Worker A pages all 8 buckets for two pages; after a reclaim,
/// worker B takes 4 of them and pages one more; after another reclaim, worker
/// C holds all 8 across two cursors, and finishes both groups to the oracle.
#[tokio::test]
async fn a_claimant_resuming_buckets_at_two_cursors_finishes_each_to_the_oracle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    setup_aggregate(&db, &client).await;

    let ids: Vec<i32> = (1..=300).collect();
    insert_items(&client, &ids).await;
    let seg = seal(&mut client).await;

    let mut stop = DrainHooks {
        stop_after_pages: Some(2),
        ..DrainHooks::default()
    };
    drain(&db.pool, seg, "worker-a", 25, &mut stop)
        .await
        .expect("worker A's pages")
        .expect("worker A claims");
    assert!(reclaim(&client, seg).await > 0);

    let mut stop = DrainHooks {
        stop_after_pages: Some(1),
        ..DrainHooks::default()
    };
    let b = apply::drain_many_with_hooks(
        &db.pool,
        &[seg],
        "worker-b",
        2,
        WAKE,
        &StagedWatermark::saturated(),
        25,
        &mut stop,
    )
    .await
    .expect("worker B's page")
    .expect("worker B claims half");
    assert_eq!(b.pages, 1);
    assert!(reclaim(&client, seg).await > 0);

    let cursors: i64 = client
        .query_one(
            "select count(distinct (after_route, after_src_table, after_key)) \
             from drain_cursor where seg_seq = $1",
            &[&seg],
        )
        .await
        .expect("distinct cursors")
        .get(0);
    assert_eq!(cursors, 2, "B's half moved past A's cursor");

    let c = drain(&db.pool, seg, "worker-c", 25, &mut DrainHooks::default())
        .await
        .expect("worker C's pages")
        .expect("worker C claims everything");
    assert_eq!(c.segments_drained, vec![(seg, true)]);
    assert_eq!(read_totals(&client).await, oracle_totals(&client).await);
}

/// Isolation never blames keys for a lost claim. A probe whose rollback-only
/// apply fails with `ClaimLost` reproduced the lost claim, not the key's
/// failure; charging it would give every key in the page a death whenever a
/// genuinely failing page also loses its claim.
#[tokio::test]
async fn isolation_surfaces_a_lost_claim_without_charging_any_key() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    setup_aggregate(&db, &client).await;

    insert_items(&client, &(1..=20).collect::<Vec<_>>()).await;
    let seg = seal(&mut client).await;
    let folded = {
        let mut conn = db.pool.get().await.expect("pool");
        let txn = conn.transaction().await.expect("txn");
        let folded = trellis::staging::fold(&txn, seg, trellis::staging::BucketFilter::all())
            .await
            .expect("fold");
        txn.commit().await.expect("commit");
        folded
    };
    assert_eq!(folded.len(), 20);

    // "worker-a" holds no claim on `seg`: every probe's completion step finds
    // none, exactly as it would after a reclaim.
    let outcome = trellis::staging::quarantine::isolate_and_evict(
        &db.pool,
        seg,
        "worker-a",
        WAKE,
        &folded,
        trellis::staging::quarantine::DEFAULT_DEATH_THRESHOLD,
    )
    .await;
    assert!(
        matches!(outcome, Err(ApplyError::ClaimLost)),
        "a probe that loses the claim surfaces it: {outcome:?}"
    );
    let deaths: i64 = client
        .query_one("select count(*) from key_deaths", &[])
        .await
        .expect("count key deaths")
        .get(0);
    assert_eq!(deaths, 0);
}

/// The first `n` ids in `1..=20000` whose ring route falls in `bucket` (of
/// `SEG_BUCKETS`), in route order: what a page walks first.
async fn ids_in_bucket(client: &Client, bucket: i64, n: i64) -> Vec<i32> {
    client
        .query(
            "select id from generate_series(1, 20000) as id \
             where (hashtextextended($1 || E'\\x1f' || id::text, 0) & 2147483647) % $2 = $3 \
             order by hashtextextended($1 || E'\\x1f' || id::text, 0) & 2147483647 \
             limit $4",
            &[
                &format!("{DEFAULT_SCHEMA}.items"),
                &trellis::staging::claim::SEG_BUCKETS,
                &bucket,
                &n,
            ],
        )
        .await
        .expect("ids in bucket")
        .into_iter()
        .map(|r| r.get(0))
        .collect()
}

/// The ids of a skewed segment: bucket 0 holds `heavy` keys spread over the
/// whole route space, bucket 1 holds the 3 lowest-route keys of its own (so a
/// page walking buckets 0 and 1 as one union runs out of bucket 1 almost at
/// once), buckets 2 to 6 hold 2 keys each, and bucket 7 holds none.
async fn skewed_ids(client: &Client, heavy: i64) -> (Vec<i32>, Vec<i32>, Vec<i32>) {
    // Every 40th of the first 40 × `heavy` bucket-0 ids: spread, not clustered
    // at the low end of the route space.
    let heavy_ids: Vec<i32> = ids_in_bucket(client, 0, heavy * 40)
        .await
        .into_iter()
        .step_by(40)
        .collect();
    assert_eq!(heavy_ids.len() as i64, heavy);
    let light = ids_in_bucket(client, 1, 3).await;
    let mut rest = Vec::new();
    for bucket in 2..7 {
        rest.extend(ids_in_bucket(client, bucket, 2).await);
    }
    (heavy_ids, light, rest)
}

/// Inserts `ids` as [`insert_items`] does, then stages 3 more rounds of
/// same-value updates, so the segment's ring rows clear `MIN_ROWS_TO_SPLIT`
/// and it seals into `SEG_BUCKETS` buckets.
async fn stage_skewed(client: &Client, ids: &[i32]) {
    insert_items(client, ids).await;
    for _ in 0..3 {
        stage(
            client,
            "update",
            ids,
            |id| Some((id % 7, id)),
            |id| Some((id % 7, id)),
        )
        .await;
    }
    assert!(
        ids.len() as i64 * 4 >= trellis::staging::claim::MIN_ROWS_TO_SPLIT,
        "enough ring rows to split into buckets"
    );
}

/// Hands `seg_seq`'s undrained, unclaimed buckets out by hand: `who` gets
/// `buckets`, and every other free bucket goes to `"parked"`, so a drain call
/// by `who` (which claims its share of whatever is free) holds exactly
/// `buckets`. Claiming first flips the segment `sealed -> draining`, as a real
/// claim does.
async fn hold(client: &Client, seg_seq: i64, who: &str, buckets: &[i16]) {
    claim::claim(client, seg_seq, "setup", 1)
        .await
        .expect("claim every free bucket");
    client
        .execute(
            "update seg_claims set claimed_by = case when bucket = any($3::smallint[]) \
                 then $2 else 'parked' end \
             where seg_seq = $1 and claimed_by = 'setup'",
            &[&seg_seq, &who, &buckets],
        )
        .await
        .expect("hand out claims");
    let held: Vec<i16> = client
        .query(
            "select bucket from seg_claims where seg_seq = $1 and claimed_by = $2 order by bucket",
            &[&seg_seq, &who],
        )
        .await
        .expect("read claims")
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(held, buckets, "{who} holds exactly its buckets");
}

/// Releases `who`'s and the parked claims, as the client loop does on error.
async fn release_all(client: &Client, seg_seq: i64, who: &[&str]) {
    for who in who.iter().chain(&["parked"]) {
        liveness::release(client, seg_seq, who)
            .await
            .expect("release");
    }
}

async fn cursor_route(client: &Client, seg_seq: i64, bucket: i16) -> Option<i64> {
    client
        .query_opt(
            "select after_route from drain_cursor where seg_seq = $1 and bucket = $2",
            &[&seg_seq, &bucket],
        )
        .await
        .expect("read cursor")
        .map(|r| r.get(0))
}

/// Runs one drain call with a deadline: a page loop that stopped advancing
/// would hang here rather than finish.
async fn drain_within(
    pool: &trellis::Pool,
    seg_seq: i64,
    who: &str,
    cap: usize,
    hooks: &mut DrainHooks,
) -> ManyApplyOutcome {
    tokio::time::timeout(
        Duration::from_secs(60),
        drain(pool, seg_seq, who, cap, hooks),
    )
    .await
    .expect("the drain finishes: no page loop spins in place")
    .expect("drain")
    .expect("drain holds something")
}

/// One worker holding two buckets of a skewed share, paged as one union: bucket
/// 0 holds 60 keys and bucket 1 only 3, all at the low end of the route space,
/// so bucket 1 is exhausted after the first page while bucket 0 runs on. A
/// first claimant commits two pages and dies; the next resumes from the shared
/// cursor. Every page advances: the whole union takes at most ⌈63 / 5⌉ pages
/// across both claimants, and the target matches the oracle.
#[tokio::test]
async fn a_worker_paging_two_skewed_buckets_as_one_union_finishes_in_bounded_pages() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    setup_aggregate(&db, &client).await;

    let (heavy, light, rest) = skewed_ids(&client, 60).await;
    let all: Vec<i32> = heavy.iter().chain(&light).chain(&rest).copied().collect();
    stage_skewed(&client, &all).await;
    let seg = seal(&mut client).await;
    let cap = 5;

    hold(&client, seg, "worker-y", &[0, 1]).await;
    let y = drain_within(
        &db.pool,
        seg,
        "worker-y",
        cap,
        &mut DrainHooks {
            stop_after_pages: Some(2),
            ..DrainHooks::default()
        },
    )
    .await;
    assert_eq!(y.pages, 2);
    let (k0, k1) = (
        cursor_route(&client, seg, 0).await,
        cursor_route(&client, seg, 1).await,
    );
    assert!(k0.is_some() && k0 == k1, "one union, one shared cursor");
    liveness::release(&client, seg, "worker-y")
        .await
        .expect("worker Y dies and is released");

    // Worker A takes both back (the parked buckets are still parked).
    claim::claim(&client, seg, "worker-a", 1)
        .await
        .expect("worker A claims");
    let a = drain_within(&db.pool, seg, "worker-a", cap, &mut DrainHooks::default()).await;
    assert!(
        y.pages + a.pages <= 63usize.div_ceil(cap),
        "63 keys at cap {cap} is at most 13 pages across both claimants: Y {y:?}, A {a:?}"
    );
    let (_, mask) = segment_state(&client, seg).await;
    assert_eq!(mask, 0b11, "A's last page completed both buckets");

    release_all(&client, seg, &[]).await;
    let b = drain_within(&db.pool, seg, "worker-b", cap, &mut DrainHooks::default()).await;
    assert_eq!(b.segments_drained, vec![(seg, true)]);
    assert_eq!(read_totals(&client).await, oracle_totals(&client).await);
}

/// One worker holding three buckets at three different cursors: bucket 7 has
/// no keys at all, bucket 1's shared cursor is already past its last key, and
/// bucket 0's cursor is a page further on. The claimant walks each cursor group
/// in turn: an empty final page for bucket 7, an empty final page for bucket
/// 1, then bucket 0's remaining pages. Every page advances, the total is at
/// most ⌈remaining / cap⌉ plus one final page per group, and the target
/// matches the oracle.
#[tokio::test]
async fn a_worker_resuming_skewed_buckets_at_different_cursors_finishes_in_bounded_pages() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    setup_aggregate(&db, &client).await;

    let (heavy, light, rest) = skewed_ids(&client, 60).await;
    let all: Vec<i32> = heavy.iter().chain(&light).chain(&rest).copied().collect();
    stage_skewed(&client, &all).await;
    let seg = seal(&mut client).await;
    let cap = 5;

    // Worker Y pages buckets 0 and 1 as one union for two pages (10 keys),
    // which runs past all 3 of bucket 1's keys, then dies.
    hold(&client, seg, "worker-y", &[0, 1]).await;
    let y = drain_within(
        &db.pool,
        seg,
        "worker-y",
        cap,
        &mut DrainHooks {
            stop_after_pages: Some(2),
            ..DrainHooks::default()
        },
    )
    .await;
    assert_eq!(y.pages, 2);
    let k1 = cursor_route(&client, seg, 1)
        .await
        .expect("bucket 1's cursor");
    let light_max: i64 = client
        .query_one(
            "select max(hashtextextended($1 || E'\\x1f' || id::text, 0) & 2147483647) \
             from unnest($2::int4[]) as id",
            &[&format!("{DEFAULT_SCHEMA}.items"), &light],
        )
        .await
        .expect("bucket 1's last route")
        .get(0);
    assert!(
        light_max <= k1,
        "bucket 1 is exhausted at its cursor ({light_max} <= {k1})"
    );
    release_all(&client, seg, &["worker-y"]).await;

    // Worker Z pages bucket 0 alone for one more page, then dies.
    hold(&client, seg, "worker-z", &[0]).await;
    let z = drain_within(
        &db.pool,
        seg,
        "worker-z",
        cap,
        &mut DrainHooks {
            stop_after_pages: Some(1),
            ..DrainHooks::default()
        },
    )
    .await;
    assert_eq!(z.pages, 1);
    let k0 = cursor_route(&client, seg, 0)
        .await
        .expect("bucket 0's cursor");
    assert!(k0 > k1, "bucket 0 moved past the shared cursor");
    release_all(&client, seg, &["worker-z"]).await;

    // Worker A holds buckets 0, 1 and 7: three cursor groups.
    hold(&client, seg, "worker-a", &[0, 1, 7]).await;
    let a = drain_within(&db.pool, seg, "worker-a", cap, &mut DrainHooks::default()).await;
    let heavy_left: usize = client
        .query_one(
            "select count(*) from unnest($1::int4[]) as id \
             where (hashtextextended($2 || E'\\x1f' || id::text, 0) & 2147483647) > $3",
            &[&heavy, &format!("{DEFAULT_SCHEMA}.items"), &k0],
        )
        .await
        .expect("bucket 0's keys after its cursor")
        .get::<_, i64>(0) as usize;
    assert!(heavy_left > cap, "bucket 0 still has to page");
    assert!(
        a.pages <= heavy_left.div_ceil(cap) + 2,
        "{heavy_left} keys at cap {cap}, plus one empty final page each for buckets 7 and 1: \
         {a:?}"
    );
    let (_, mask) = segment_state(&client, seg).await;
    assert_eq!(mask, 0b1000_0011, "A completed buckets 0, 1 and 7");

    release_all(&client, seg, &["worker-a"]).await;
    let b = drain_within(&db.pool, seg, "worker-b", cap, &mut DrainHooks::default()).await;
    assert_eq!(b.segments_drained, vec![(seg, true)]);
    assert_eq!(read_totals(&client).await, oracle_totals(&client).await);
}

/// How many paged-drain `TEMP` tables exist in this database, in any session.
async fn page_tables(client: &Client) -> i64 {
    client
        .query_one(
            "select count(*) from pg_class \
             where relname = 'trellis_drain_page' and relpersistence = 't'",
            &[],
        )
        .await
        .expect("count page tables")
        .get(0)
}

/// Waits (bounded) for every paged-drain `TEMP` table to be gone. A session
/// drops its temp tables as its backend exits, which trails the client
/// closing the socket by a moment; this waits on that teardown only.
async fn no_page_tables_within(client: &Client, deadline: Duration) {
    let start = std::time::Instant::now();
    while page_tables(client).await > 0 {
        assert!(
            start.elapsed() < deadline,
            "a paged drain's TEMP table outlived its drain call"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The backend of a pooled connection, and whether that session has a page
/// table of its own.
async fn pooled_session(pool: &trellis::Pool) -> (i32, bool) {
    let client = pool.get().await.expect("pooled connection");
    let row = client
        .query_one(
            "select pg_backend_pid(), to_regclass('pg_temp.trellis_drain_page') is not null",
            &[],
        )
        .await
        .expect("pooled session");
    (row.get(0), row.get(1))
}

/// Stages `ids` as inserts into the live table and the ring, each then
/// updated twice, for the 1-1 copy: grp 0, final amount `id * 10 + 2`.
async fn stage_copy_keys(client: &Client, ids: &[i32]) {
    for &id in ids {
        client
            .execute(
                "insert into items (id, grp, amount) values ($1, 0, $1 * 10 + 2)",
                &[&id],
            )
            .await
            .expect("insert live item");
    }
    stage(client, "insert", ids, |_| None, |id| Some((0, id * 10))).await;
    for step in 1..=2 {
        stage(
            client,
            "update",
            ids,
            |id| Some((0, id * 10 + step - 1)),
            |id| Some((0, id * 10 + step)),
        )
        .await;
    }
}

/// Issue #620 A2b: a reclaim while the first claimant still holds its
/// materialized share. Worker A materializes 60 keys at cap 10, commits page
/// 1 and pauses before page 2 with its `TEMP` table live. Its claim is
/// reclaimed, and worker B drains the rest start to finish in its own session,
/// materialized from A's cursor: B writes exactly the 50 keys A didn't, in at
/// most 5 pages. A then resumes on its stale table: page 2 fails its claim
/// check and applies nothing. Neither session's table outlives it.
#[tokio::test]
async fn a_reclaim_mid_materialized_drain_resumes_from_the_cursor_in_a_new_session() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    setup_copy(&db, &client).await;

    let ids: Vec<i32> = (1..=60).collect();
    stage_copy_keys(&client, &ids).await;
    let seg = seal(&mut client).await;

    let (paused_tx, paused_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let pool = db.pool.clone();
    let worker_a = tokio::spawn(async move {
        let mut hooks = DrainHooks {
            pause_before_page: Some((2, paused_tx, resume_rx)),
            ..DrainHooks::default()
        };
        drain(&pool, seg, "worker-a", 10, &mut hooks).await
    });
    paused_rx.await.expect("worker A reaches page 2");
    assert_eq!(
        page_tables(&client).await,
        1,
        "A's materialized share is live while it pauses"
    );
    let after_one_page = read_copy(&client).await;
    assert_eq!(after_one_page.len(), 10, "A committed page 1 only");

    assert!(reclaim(&client, seg).await > 0, "A's claim is reclaimed");
    let b = drain_within(&db.pool, seg, "worker-b", 10, &mut DrainHooks::default()).await;
    assert_eq!(b.segments_drained, vec![(seg, true)]);
    assert_eq!(
        b.keys_written, 50,
        "B resumed after A's cursor, not from the start: {b:?}"
    );
    assert!(b.pages <= 5, "50 records at cap 10: {b:?}");
    assert_eq!(read_copy(&client).await, oracle_copy(&client).await);

    resume_tx.send(()).expect("resume worker A");
    let result = worker_a.await.expect("worker A's task");
    assert!(
        matches!(result, Err(ApplyError::ClaimLost)),
        "A's page 2 must fail its claim check: {result:?}"
    );
    assert_eq!(read_copy(&client).await, oracle_copy(&client).await);
    no_page_tables_within(&client, Duration::from_secs(10)).await;
}

/// Issue #620 A2b: a paged drain's `TEMP` table never reaches a pooled
/// connection and never outlives its drain call, whether the call finishes,
/// fails its claim check, or is dropped mid-page. The pool here has exactly
/// one connection, reused by every call: its backend never changes, and it
/// never holds a page table. One connection also shows a paged drain never
/// waits on the pool for its session.
#[tokio::test]
async fn no_page_table_outlives_a_drain_call_or_reaches_a_pooled_connection() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    setup_copy(&db, &client).await;
    let config = trellis::Config::from_dsn(db.dsn().to_string())
        .expect("valid dsn")
        .with_pool_max_size(1)
        .expect("pool size");
    let pool = trellis::Pool::new(&config).expect("one-connection pool");
    let (backend, has_table) = pooled_session(&pool).await;
    assert!(!has_table);

    // Finished: a paged drain to the end.
    stage_copy_keys(&client, &(1..=40).collect::<Vec<_>>()).await;
    let seg = seal(&mut client).await;
    let outcome = drain_within(&pool, seg, "worker", 10, &mut DrainHooks::default()).await;
    assert!(outcome.pages > 1, "the share pages: {outcome:?}");
    no_page_tables_within(&client, Duration::from_secs(10)).await;
    assert_eq!(pooled_session(&pool).await, (backend, false));

    // Dropped mid-page: the drain's future is aborted while it pauses before
    // page 2 with its table live.
    stage_copy_keys(&client, &(101..=140).collect::<Vec<_>>()).await;
    let seg = seal(&mut client).await;
    let (paused_tx, paused_rx) = tokio::sync::oneshot::channel();
    let (_resume_tx, resume_rx) = tokio::sync::oneshot::channel::<()>();
    let task_pool = pool.clone();
    let worker = tokio::spawn(async move {
        let mut hooks = DrainHooks {
            pause_before_page: Some((2, paused_tx, resume_rx)),
            ..DrainHooks::default()
        };
        drain(&task_pool, seg, "worker-a", 10, &mut hooks).await
    });
    paused_rx.await.expect("the drain reaches page 2");
    assert_eq!(page_tables(&client).await, 1, "the table is live mid-drain");
    assert_eq!(
        pooled_session(&pool).await,
        (backend, false),
        "the pooled connection never holds the table"
    );
    worker.abort();
    let _ = worker.await;
    no_page_tables_within(&client, Duration::from_secs(10)).await;
    assert_eq!(pooled_session(&pool).await, (backend, false));

    // Failed: the next claimant resumes from the cursor, pauses before its
    // page 2, loses its claim, and that page fails with `ClaimLost`.
    assert!(reclaim(&client, seg).await > 0);
    let (paused_tx, paused_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let task_pool = pool.clone();
    let worker = tokio::spawn(async move {
        let mut hooks = DrainHooks {
            pause_before_page: Some((2, paused_tx, resume_rx)),
            ..DrainHooks::default()
        };
        drain(&task_pool, seg, "worker-b", 10, &mut hooks).await
    });
    paused_rx.await.expect("worker B reaches its page 2");
    assert!(reclaim(&client, seg).await > 0);
    let won = claim::claim(&client, seg, "worker-c", 1)
        .await
        .expect("worker C claims");
    assert!(!won.is_empty());
    resume_tx.send(()).expect("resume worker B");
    let result = worker.await.expect("worker B's task");
    assert!(
        matches!(result, Err(ApplyError::ClaimLost)),
        "B's page must fail its claim check: {result:?}"
    );
    no_page_tables_within(&client, Duration::from_secs(10)).await;
    assert_eq!(pooled_session(&pool).await, (backend, false));

    let rest = drain_within(&pool, seg, "worker-c", 10, &mut DrainHooks::default()).await;
    assert_eq!(rest.segments_drained, vec![(seg, true)]);
    no_page_tables_within(&client, Duration::from_secs(10)).await;
    assert_eq!(pooled_session(&pool).await, (backend, false));
    assert_eq!(read_copy(&client).await, oracle_copy(&client).await);
}

// ---------------------------------------------------------------------
// Lock timeouts (issue #621, ADR-0002 I7)
// ---------------------------------------------------------------------

/// The `lock_timeout` the impatient pool below gives each of its sessions.
const SHORT_LOCK_TIMEOUT_MS: u64 = 200;

/// Every backend other than `except` that is waiting for a heavyweight lock,
/// as `(pid, xact_start as text, seconds its transaction has been open)`.
async fn lock_waiters(client: &Client, except: i32) -> Vec<(i32, String, f64)> {
    client
        .query(
            "select pid, xact_start::text, \
                    extract(epoch from clock_timestamp() - xact_start)::float8 \
             from pg_stat_activity \
             where wait_event_type = 'Lock' and pid <> $1 and datname = current_database()",
            &[&except],
        )
        .await
        .expect("read pg_stat_activity")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect()
}

/// I7: a drain page that needs a lock someone else holds waits for it only
/// `lock_timeout` long inside its transaction. The transaction rolls back,
/// the drain backs off outside any transaction with its claim kept, and
/// retries. So while the lock is held no drain transaction stays open much
/// past the timeout, and the sealer, whose gate waits out every transaction
/// running when the last fence was taken, still seals. Once the lock is
/// released the same drain call applies the batch.
///
/// The holder takes a table lock, which assigns no transaction id, so it
/// doesn't hold the seal gate itself: only a drain transaction still open
/// from before the last seal could.
#[tokio::test]
async fn a_drain_waits_out_a_held_lock_outside_its_transaction() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    setup_aggregate(&db, &client).await;
    let impatient = trellis::Pool::new(
        &trellis::config::Config::from_dsn(format!(
            "{} options='-c lock_timeout={SHORT_LOCK_TIMEOUT_MS}'",
            db.dsn()
        ))
        .expect("valid dsn"),
    )
    .expect("pool");

    let ids: Vec<i32> = (1..=50).collect();
    insert_items(&client, &ids).await;
    let seg = seal(&mut client).await;

    let holder = connect_raw(db.dsn()).await;
    let holder_pid: i32 = holder
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("holder pid")
        .get(0);
    holder
        .batch_execute("begin; lock table grp_totals in exclusive mode")
        .await
        .expect("hold the target's lock");

    let task_pool = impatient.clone();
    let drainer = tokio::spawn(async move {
        drain(&task_pool, seg, "worker", 1000, &mut DrainHooks::default()).await
    });

    // Watch the drain's transactions for a while, sealing meanwhile. The
    // first seal's fence is taken while a drain transaction may be open;
    // the second seal's gate waits on that transaction, so it only passes
    // because the transaction ended.
    let hold = Duration::from_secs(3);
    let started = std::time::Instant::now();
    let mut transactions: std::collections::HashSet<(i32, String)> = Default::default();
    let mut longest = 0f64;
    let mut sealed = Vec::new();
    let mut next = 10_000;
    while started.elapsed() < hold {
        for (pid, xact_start, open_secs) in lock_waiters(&client, holder_pid).await {
            transactions.insert((pid, xact_start));
            longest = longest.max(open_secs);
        }
        if sealed.len() < 2 && !transactions.is_empty() {
            next += 1;
            insert_items(&client, &[next]).await;
            if let Some(outcome) = trellis::staging::seal_if_active_nonempty(&mut client, WAKE)
                .await
                .expect("seal")
            {
                sealed.push(outcome.sealed_seg_seq);
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        sealed.len(),
        2,
        "the sealer must keep sealing while the drain waits: {sealed:?}"
    );
    assert!(
        !drainer.is_finished(),
        "the drain keeps retrying while the lock is held"
    );
    assert!(
        transactions.len() >= 2,
        "the drain must roll back and retry in a fresh transaction, saw {transactions:?}"
    );
    // The timeout plus generous slack for a loaded box, and far below the
    // 3 s the lock was held.
    assert!(
        longest < 1.5,
        "a drain transaction stayed open waiting {longest:.3}s, lock_timeout is \
         {SHORT_LOCK_TIMEOUT_MS}ms"
    );

    holder
        .batch_execute("commit")
        .await
        .expect("release the lock");
    let outcome = tokio::time::timeout(Duration::from_secs(30), drainer)
        .await
        .expect("the drain finishes once the lock is released")
        .expect("drain task")
        .expect("drain")
        .expect("drain claims something");
    assert_eq!(outcome.segments_drained, vec![(seg, true)]);
    for &later in &sealed {
        drain_all(&db.pool, later, 1000).await;
    }
    assert_eq!(read_totals(&client).await, oracle_totals(&client).await);
}
