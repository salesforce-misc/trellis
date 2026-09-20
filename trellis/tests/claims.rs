//! Integration tests for bucket partitioning and multi-worker claims (issue
//! #14, stage 04's second half), run against a real, ephemeral Postgres
//! instance via the shared harness (`testkit::TestCluster`).
//!
//! See docs/staging-and-claiming/04-claiming-and-the-fold.md,
//! "Partitioning a batch across workers" and "The claim is a cursor", for
//! the design these tests hold the implementation to. Completion
//! (`draining -> drained`, issue #11) and the real heartbeat/reclaim/pause
//! lease (issue #15) are out of scope — a claimed batch is expected to sit
//! in `draining` with rows in `seg_claims` by the end of every test here.

use std::collections::BTreeMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::staging::{BucketFilter, SegmentState, claim, fold, seal};

/// Connects directly to `dsn` (bypassing `trellis::Pool`), matching
/// `sealing.rs`/`fold.rs`'s convention.
async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'"))
        .await
        .expect("set search_path");
    client
}

async fn insert_recompute(client: &Client, table: &str, key: &str) {
    client
        .execute(
            &format!(
                "insert into {table} (src_table, key, op, hop_gen) \
                 values ('orders', $1, 'recompute', 0)"
            ),
            &[&key],
        )
        .await
        .expect("insert recompute row");
}

/// Inserts `n` distinct-keyed recompute rows into `table` as one multi-row
/// statement, fast enough for the `MIN_ROWS_TO_SPLIT`-crossing tests below.
async fn insert_many_recompute(client: &Client, table: &str, prefix: &str, n: usize) {
    let mut sql = format!("insert into {table} (src_table, key, op, hop_gen) values ");
    for i in 0..n {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("('orders', '{prefix}-{i}', 'recompute', 0)"));
    }
    client
        .batch_execute(&sql)
        .await
        .unwrap_or_else(|e| panic!("insert {n} recompute rows into {table} failed: {e}"));
}

async fn seal_active_segment(client: &mut Client) -> i64 {
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq)
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

async fn segment_state(client: &Client, seg_seq: i64) -> SegmentState {
    let state: String = client
        .query_one("select state from segments where seg_seq = $1", &[&seg_seq])
        .await
        .expect("read segment state")
        .get(0);
    SegmentState::from_sql(&state).unwrap_or_else(|| panic!("unrecognized state {state:?}"))
}

async fn bucket_count_of(client: &Client, seg_seq: i64) -> i16 {
    client
        .query_one(
            "select bucket_count from segments where seg_seq = $1",
            &[&seg_seq],
        )
        .await
        .expect("read bucket_count")
        .get(0)
}

/// The route Postgres assigned a key — deterministic (a stored generated
/// column, hashing `src_table || key`), so any one row for the key carries
/// the batch-wide answer.
async fn route_of(client: &Client, key: &str) -> i64 {
    let row = client
        .query_one(
            "select route from (
                 select route, key from seg_0
                 union all select route, key from seg_1
                 union all select route, key from seg_2
                 union all select route, key from seg_3
             ) rows where key = $1 limit 1",
            &[&key],
        )
        .await
        .unwrap_or_else(|e| panic!("route_of({key:?}) failed: {e}"));
    row.get(0)
}

/// The test oracle (doc 04: "Any host-language `bucket_of_route` should be
/// a test oracle written against the SQL"). Not a second production
/// definition — the engine's one bucket definition lives in
/// `fold.rs`'s SQL (`route % bucket_count = ANY(buckets)`); this exists
/// only so "the two agree" is a property these tests check.
fn bucket_of_route(route: i64, bucket_count: i64) -> i64 {
    route % bucket_count
}

#[tokio::test]
async fn partition_is_exact_the_union_of_per_bucket_folds_equals_the_whole_batch() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let keys: Vec<String> = (0..50).map(|i| format!("k{i}")).collect();
    for key in &keys {
        insert_recompute(&client, "seg_0", key).await;
    }

    let seg_seq = seal_active_segment(&mut client).await;
    let txn = client.transaction().await.expect("begin fold txn");

    for &bucket_count in &[1i64, 3, 8] {
        let mut union_keys: Vec<String> = Vec::new();
        for b in 0..bucket_count {
            let folded = fold::fold(&txn, seg_seq, BucketFilter::buckets(bucket_count, vec![b]))
                .await
                .expect("fold one bucket");
            union_keys.extend(folded.into_iter().map(|f| f.key));
        }
        // Deliberately NOT deduped: a key that folded into two buckets would
        // appear twice here, inflating the length past `keys.len()`; a key in
        // zero buckets would deflate it. Either way this catches the failure —
        // deduping first would silently mask "none in two".
        union_keys.sort();
        assert_eq!(
            union_keys.len(),
            keys.len(),
            "bucket_count={bucket_count}: every key must land in exactly one bucket \
             (none in two, none in zero)"
        );

        let whole = fold::fold(&txn, seg_seq, BucketFilter::all())
            .await
            .expect("fold whole batch");
        let mut whole_keys: Vec<String> = whole.into_iter().map(|f| f.key).collect();
        whole_keys.sort();
        assert_eq!(
            union_keys, whole_keys,
            "bucket_count={bucket_count}: the per-bucket union must equal the whole-batch fold"
        );
    }
}

#[tokio::test]
async fn the_sql_bucket_definition_agrees_with_the_host_oracle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let keys: Vec<String> = (0..30).map(|i| format!("route-{i}")).collect();
    for key in &keys {
        insert_recompute(&client, "seg_0", key).await;
    }

    // Read each key's route before sealing changes nothing about it (route
    // is a stored generated column, fixed at insert).
    let mut routes: BTreeMap<String, i64> = BTreeMap::new();
    for key in &keys {
        routes.insert(key.clone(), route_of(&client, key).await);
    }

    let seg_seq = seal_active_segment(&mut client).await;
    let txn = client.transaction().await.expect("begin fold txn");

    for &bucket_count in &[1i64, 4, 8] {
        for b in 0..bucket_count {
            let folded = fold::fold(&txn, seg_seq, BucketFilter::buckets(bucket_count, vec![b]))
                .await
                .expect("fold one bucket");
            let sql_keys: std::collections::BTreeSet<String> =
                folded.into_iter().map(|f| f.key).collect();
            let oracle_keys: std::collections::BTreeSet<String> = keys
                .iter()
                .filter(|k| bucket_of_route(routes[*k], bucket_count) == b)
                .cloned()
                .collect();
            assert_eq!(
                sql_keys, oracle_keys,
                "bucket_count={bucket_count}, bucket={b}: SQL routing and the host oracle disagree"
            );
        }
    }
}

#[tokio::test]
async fn the_flip_cte_actually_runs_even_when_this_calls_share_is_empty() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    insert_recompute(&client, "seg_0", "solo-key").await;
    let seg_seq = seal_active_segment(&mut client).await;
    assert_eq!(bucket_count_of(&client, seg_seq).await, 1);

    // Pre-claim the batch's one bucket directly (bypassing `claim`), so a
    // subsequent `claim()` call's own `mine` CTE returns zero rows — this
    // is exactly the shape that would silently no-op the flip if
    // `flip_guard` were referenced from a lazily-evaluated `SELECT`-list
    // expression instead of a `FROM`-item: a naive `SELECT bucket, (SELECT
    // count(*) FROM flipped) FROM mine` never evaluates that subquery when
    // `mine` is empty, so `flipped` would never run and the state would
    // stay `sealed`.
    client
        .execute(
            "insert into seg_claims (seg_seq, bucket, claimed_by) values ($1, 0, 'other')",
            &[&seg_seq],
        )
        .await
        .expect("pre-claim bucket 0");
    assert_eq!(segment_state(&client, seg_seq).await, SegmentState::Sealed);

    let won = claim::claim(&client, seg_seq, "me", 1)
        .await
        .expect("claim");
    assert!(
        won.is_empty(),
        "every bucket was already claimed, so this call must win nothing: {won:?}"
    );
    assert_eq!(
        segment_state(&client, seg_seq).await,
        SegmentState::Draining,
        "the flip must commit even when this call's own claim share is empty"
    );
}

#[tokio::test]
async fn a_second_claim_of_an_already_draining_batch_still_succeeds() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    insert_recompute(&client, "seg_0", "k").await;
    let seg_seq = seal_active_segment(&mut client).await;
    client
        .execute(
            "update segments set bucket_count = 4 where seg_seq = $1",
            &[&seg_seq],
        )
        .await
        .expect("widen bucket_count for this test");

    let first = claim::claim(&client, seg_seq, "w1", 2)
        .await
        .expect("first claim");
    assert!(!first.is_empty(), "the first claim must win something");
    assert_eq!(
        segment_state(&client, seg_seq).await,
        SegmentState::Draining
    );

    // A second claim against the now-draining batch must not fail just
    // because the flip's own `WHERE state = 'sealed'` no longer matches —
    // the flip is idempotent, scoped to "on the first claim only."
    let second = claim::claim(&client, seg_seq, "w2", 2)
        .await
        .expect("second claim against an already-draining batch must succeed");
    assert!(
        first.iter().all(|b| !second.contains(b)),
        "the two workers must not share a bucket: {first:?} vs {second:?}"
    );
}

#[tokio::test]
async fn two_overlapping_workers_get_disjoint_buckets_no_bucket_claimed_twice() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client_a = connect_raw(db.dsn()).await;
    let client_b = connect_raw(db.dsn()).await;

    insert_recompute(&client_a, "seg_0", "k").await;
    let seg_seq = seal_active_segment(&mut client_a).await;
    client_a
        .execute(
            "update segments set bucket_count = 8 where seg_seq = $1",
            &[&seg_seq],
        )
        .await
        .expect("widen bucket_count for this test");

    // Both workers claim concurrently, each believing it can take half of
    // the (same) free set — the overlapping-shares case doc 04 calls out.
    let (won_a, won_b) = tokio::join!(
        claim::claim(&client_a, seg_seq, "worker-a", 2),
        claim::claim(&client_b, seg_seq, "worker-b", 2),
    );
    let won_a = won_a.expect("worker a claim");
    let won_b = won_b.expect("worker b claim");

    for b in &won_a {
        assert!(
            !won_b.contains(b),
            "bucket {b} was returned to both workers: {won_a:?} / {won_b:?}"
        );
    }

    let all_buckets: Vec<i16> = client_a
        .query(
            "select bucket from seg_claims where seg_seq = $1 order by bucket",
            &[&seg_seq],
        )
        .await
        .expect("read seg_claims")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    let mut deduped = all_buckets.clone();
    deduped.dedup();
    assert_eq!(
        all_buckets.len(),
        deduped.len(),
        "no bucket may appear twice in seg_claims: {all_buckets:?}"
    );
    assert!(
        all_buckets.iter().all(|b| (0..8).contains(b)),
        "every claimed bucket must be within 0..bucket_count: {all_buckets:?}"
    );
}

#[tokio::test]
async fn share_sizing_a_lone_worker_takes_everything_n_workers_take_about_a_nth() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    insert_recompute(&client, "seg_0", "solo").await;
    let solo_seg = seal_active_segment(&mut client).await;
    client
        .execute(
            "update segments set bucket_count = 8 where seg_seq = $1",
            &[&solo_seg],
        )
        .await
        .expect("widen bucket_count");

    let solo_share = claim::claim(&client, solo_seg, "solo-worker", 1)
        .await
        .expect("solo claim");
    assert_eq!(
        solo_share.len(),
        8,
        "a lone worker (live=1) must claim every free bucket in one call"
    );

    insert_recompute(&client, "seg_1", "quad").await;
    let quad_seg = seal_active_segment(&mut client).await;
    client
        .execute(
            "update segments set bucket_count = 8 where seg_seq = $1",
            &[&quad_seg],
        )
        .await
        .expect("widen bucket_count");

    let quad_share = claim::claim(&client, quad_seg, "one-of-four", 4)
        .await
        .expect("one-of-four claim");
    assert_eq!(
        quad_share.len(),
        2,
        "ceil(8 free / 4 live) = 2 buckets for the first of four workers"
    );
}

#[tokio::test]
async fn bucket_count_is_fixed_at_seal_from_row_count_alone_and_never_moves() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // Below MIN_ROWS_TO_SPLIT (256): seals to a single bucket.
    insert_recompute(&client, "seg_0", "small-batch").await;
    let small_seg = seal_active_segment(&mut client).await;
    assert_eq!(
        bucket_count_of(&client, small_seg).await,
        1,
        "a batch under MIN_ROWS_TO_SPLIT must seal to a single bucket"
    );

    // At/above MIN_ROWS_TO_SPLIT: seals to SEG_BUCKETS.
    insert_many_recompute(
        &client,
        "seg_1",
        "big-batch",
        claim::MIN_ROWS_TO_SPLIT as usize,
    )
    .await;
    let big_seg = seal_active_segment(&mut client).await;
    assert_eq!(
        bucket_count_of(&client, big_seg).await,
        claim::SEG_BUCKETS as i16,
        "a batch at MIN_ROWS_TO_SPLIT must seal to SEG_BUCKETS buckets"
    );

    // Claiming the big batch (repeatedly) must never move its bucket_count.
    let _ = claim::claim(&client, big_seg, "w1", 1)
        .await
        .expect("claim big batch");
    assert_eq!(
        bucket_count_of(&client, big_seg).await,
        claim::SEG_BUCKETS as i16,
        "bucket_count must not change across claims"
    );
    let _ = claim::claim(&client, big_seg, "w2", 1)
        .await
        .expect("second claim against the same big batch");
    assert_eq!(
        bucket_count_of(&client, big_seg).await,
        claim::SEG_BUCKETS as i16,
        "bucket_count must still not have moved"
    );
}

#[tokio::test]
async fn drainer_registration_is_the_share_denominator_floored_at_one() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    // No drainer has ever registered: the floor keeps this at 1, not 0.
    let count = claim::count_live_drainers(&client, Duration::from_secs(30))
        .await
        .expect("count live drainers (none registered)");
    assert_eq!(count, 1);

    claim::register_drainer(&client, "w1")
        .await
        .expect("register w1");
    claim::register_drainer(&client, "w2")
        .await
        .expect("register w2");
    claim::register_drainer(&client, "w2")
        .await
        .expect("re-register w2 (upsert, not a duplicate row)");

    let count = claim::count_live_drainers(&client, Duration::from_secs(30))
        .await
        .expect("count live drainers (two registered)");
    assert_eq!(count, 2);
}

#[tokio::test]
async fn owned_bucket_filter_reads_the_claims_table_rather_than_recomputing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    insert_recompute(&client, "seg_0", "k").await;
    let seg_seq = seal_active_segment(&mut client).await;
    client
        .execute(
            "update segments set bucket_count = 8 where seg_seq = $1",
            &[&seg_seq],
        )
        .await
        .expect("widen bucket_count");

    let won = claim::claim(&client, seg_seq, "me", 2)
        .await
        .expect("claim");
    assert!(!won.is_empty());

    let filter = claim::owned_bucket_filter(&client, seg_seq, "me")
        .await
        .expect("owned_bucket_filter");
    // The one key routes to exactly one bucket, which is either in `won`
    // or not; either way the fold must agree with what `seg_claims` says
    // "me" holds. Read before opening the fold transaction below, since
    // `route_of` needs its own immutable borrow of `client`.
    let route = route_of(&client, "k").await;
    let expected_bucket = bucket_of_route(route, 8);

    // The filter must reflect exactly the rows `seg_claims` holds for "me",
    // not e.g. every bucket or a freshly recomputed half.
    let txn = client.transaction().await.expect("begin fold txn");
    let folded = fold::fold(&txn, seg_seq, filter)
        .await
        .expect("fold with owned filter");
    if won.contains(&(expected_bucket as i16)) {
        assert_eq!(
            folded.len(),
            1,
            "the owned filter must include a bucket it holds"
        );
    } else {
        assert_eq!(
            folded.len(),
            0,
            "the owned filter must not include a bucket it does not hold"
        );
    }
}
