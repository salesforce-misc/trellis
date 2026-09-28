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
