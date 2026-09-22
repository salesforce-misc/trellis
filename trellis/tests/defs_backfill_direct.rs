//! Integration tests for the direct, key-range-chunked backfill
//! (`defs::backfill::backfill_definition`, issue #63 milestone 3), run against
//! a real ephemeral Postgres via `testkit::TestCluster`.
//!
//! These exercise the five correctness concerns M3 calls out: exhaustive,
//! non-overlapping chunking (1-1 PK ranges and aggregate group-key ranges),
//! per-field-kind aggregate math (SUM/COUNT/MIN/MAX/AVG), NULL group keys, and
//! idempotent re-runs. The chunk-size constants in the module under test are
//! small enough (50k rows / 10k groups) that these fixtures deliberately cross
//! several chunk boundaries.

use std::collections::HashMap;

use testkit::TestCluster;
use trellis::defs::{
    ValueType, backfill_definition, create_aggregate_target_table,
    create_definition_without_backfill, create_target_table, parse, render_aggregate_select_sql,
    source_primary_key,
};

fn numeric(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

/// Reads a two-column `(key, value)` result into a sorted `Vec` of text pairs
/// for order-independent comparison.
async fn text_pairs(client: &trellis::pool::Client, sql: &str) -> Vec<(String, Option<String>)> {
    let mut rows: Vec<(String, Option<String>)> = client
        .query(sql, &[])
        .await
        .expect("query")
        .into_iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, Option<String>>(1)))
        .collect();
    rows.sort();
    rows
}

#[tokio::test]
async fn one_to_one_build_is_exhaustive_across_chunk_boundaries() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    // Exactly two full 50k chunks: the boundary walk must terminate cleanly
    // when the discovery query lands on the final row (100000) and finds
    // nothing above it, with no row skipped at the 50000/50001 seam.
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table s (id bigint primary key, a numeric); \
             insert into s (id, a) select g, g from generate_series(1, 100000) g",
        )
        .await
        .expect("seed source");
    drop(client);

    let def = parse("TRANSFORM t FROM s SELECT a + a AS x").expect("parse");
    let cols = numeric(&["a"]);
    create_definition_without_backfill(&db.pool, "TRANSFORM t FROM s SELECT a + a AS x", &cols)
        .await
        .expect("create def");
    let pk = source_primary_key(&db.pool, "s").await.expect("pk");
    create_target_table(&db.pool, &def, "public", &pk, &cols, &def.source)
        .await
        .expect("create target");

    backfill_definition(&db.pool, &def, "public", &def.source, &cols)
        .await
        .expect("backfill");

    let client = db.pool.get().await.expect("get connection");
    let count: i64 = client
        .query_one("select count(*) from public.t", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 100_000, "every source row built exactly once");

    // No off-by-one at the chunk seam or the terminating boundary.
    for (id, expected) in [
        (1i64, "2"),
        (50_000, "100000"),
        (50_001, "100002"),
        (100_000, "200000"),
    ] {
        let x: String = client
            .query_one("select x::text from public.t where id = $1", &[&id])
            .await
            .unwrap()
            .get(0);
        assert_eq!(x, expected, "row {id} built with the wrong value");
    }
}

#[tokio::test]
async fn one_to_one_build_handles_pk_gaps() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    // Sparse, non-contiguous keys spanning >1 chunk: half-open (lo, hi] ranges
    // discovered by max()-over-LIMIT must still partition the source exactly,
    // gaps and all.
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table s (id bigint primary key, a numeric); \
             insert into s (id, a) select g * 7, g from generate_series(1, 60000) g",
        )
        .await
        .expect("seed source");
    drop(client);

    let def = parse("TRANSFORM t FROM s SELECT a AS a").expect("parse");
    let cols = numeric(&["a"]);
    create_definition_without_backfill(&db.pool, "TRANSFORM t FROM s SELECT a AS a", &cols)
        .await
        .expect("create def");
    let pk = source_primary_key(&db.pool, "s").await.expect("pk");
    create_target_table(&db.pool, &def, "public", &pk, &cols, &def.source)
        .await
        .expect("create target");

    backfill_definition(&db.pool, &def, "public", &def.source, &cols)
        .await
        .expect("backfill");

    let client = db.pool.get().await.expect("get connection");
    let mismatches: i64 = client
        .query_one(
            "select count(*) from s left join public.t on t.id = s.id \
             where t.id is null or t.a is distinct from s.a",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        mismatches, 0,
        "every gapped source row present with the right value"
    );
}

/// Issue #121: the direct build's `(lo, hi]` PK-range chunking now compares a
/// row-value tuple, not a single scalar column — this is the composite
/// counterpart to `one_to_one_build_is_exhaustive_across_chunk_boundaries`
/// above, deliberately arranged so a chunk boundary falls *inside* a run of
/// rows that share the first key column's value (`a`), varying only the
/// second (`b`): three rows per `a`, 99999 rows total, so the first 50k-row
/// chunk ends mid-group at `a = 16667` (`50000 / 3 = 16666.67`, `b` only
/// reaching `2` of that group's `3`) rather than on a clean group boundary.
/// A backfill that compared only `a` (or only `b`) would either split every
/// group across chunks incorrectly or misdiscover the boundary; genuine
/// `(a, b)` row-value comparison is what makes `discover_pk_ranges` land
/// exactly on `(16667, 2)` and the second chunk correctly start at
/// `(16667, 3)`.
#[tokio::test]
async fn one_to_one_build_with_a_composite_key_is_exhaustive_across_a_boundary_inside_a_group() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table s (a bigint, b bigint, primary key (a, b)); \
             insert into s (a, b) \
             select (g - 1) / 3 + 1, (g - 1) % 3 + 1 from generate_series(1, 99999) g",
        )
        .await
        .expect("seed source with a composite primary key");
    drop(client);

    let def = parse("TRANSFORM t FROM s SELECT a + b AS total").expect("parse");
    let cols = numeric(&["a", "b"]);
    create_definition_without_backfill(&db.pool, "TRANSFORM t FROM s SELECT a + b AS total", &cols)
        .await
        .expect("create def");
    let pk = source_primary_key(&db.pool, "s").await.expect("pk");
    assert_eq!(pk.len(), 2, "s's primary key is genuinely composite");
    create_target_table(&db.pool, &def, "public", &pk, &cols, &def.source)
        .await
        .expect("create target");

    backfill_definition(&db.pool, &def, "public", &def.source, &cols)
        .await
        .expect("backfill");

    let client = db.pool.get().await.expect("get connection");
    let count: i64 = client
        .query_one("select count(*) from public.t", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 99_999, "every source row built exactly once");

    let mismatches: i64 = client
        .query_one(
            "select count(*) from s left join public.t on t.a = s.a and t.b = s.b \
             where t.a is null or t.total is distinct from (s.a + s.b)::numeric",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(mismatches, 0, "every row built with the right value");

    // The exact rows straddling the mid-group chunk boundary: (16667, 1) and
    // (16667, 2) fall in the first chunk, (16667, 3) and (16668, 1) in the
    // second — none skipped, none duplicated.
    for (a, b, expected_total) in [
        (16667i64, 1i64, "16668"),
        (16667, 2, "16669"),
        (16667, 3, "16670"),
        (16668, 1, "16669"),
    ] {
        let total: String = client
            .query_one(
                "select total::text from public.t where a = $1 and b = $2",
                &[&a, &b],
            )
            .await
            .unwrap_or_else(|e| panic!("row ({a}, {b}) missing from target: {e}"))
            .get(0);
        assert_eq!(
            total, expected_total,
            "row ({a}, {b}) built with the wrong value"
        );
    }
}

#[tokio::test]
async fn aggregate_build_matches_oracle_across_group_key_chunks() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    // 25k distinct groups crosses two 10k-group chunk boundaries; two rows per
    // group means a group's rows are only ever fully summed if the group lands
    // wholly within one chunk (which group-key chunking guarantees).
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table s (id bigint primary key, author numeric, sz numeric); \
             alter table s replica identity full; \
             insert into s (id, author, sz) \
             select g, g % 25000, g from generate_series(1, 50000) g",
        )
        .await
        .expect("seed source");
    drop(client);

    let src = "TRANSFORM t FROM s GROUP BY author \
               SELECT author AS author, SUM(sz) AS total, COUNT(*) AS n";
    let def = parse(src).expect("parse");
    let cols = numeric(&["author", "sz"]);
    create_definition_without_backfill(&db.pool, src, &cols)
        .await
        .expect("create def");
    create_aggregate_target_table(&db.pool, &def, "public", &cols)
        .await
        .expect("create target");

    backfill_definition(&db.pool, &def, "public", &def.source, &cols)
        .await
        .expect("backfill");

    assert_aggregate_matches_oracle(&db, &def).await;
}

#[tokio::test]
async fn aggregate_build_handles_null_group_keys() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table s (id bigint primary key, author numeric, sz numeric); \
             alter table s replica identity full; \
             insert into s (id, author, sz) values \
               (1, 1, 10), (2, 1, 20), (3, null, 30), (4, null, 40), (5, 2, 50)",
        )
        .await
        .expect("seed source");
    drop(client);

    let src = "TRANSFORM t FROM s GROUP BY author \
               SELECT author AS author, SUM(sz) AS total, COUNT(*) AS n";
    let def = parse(src).expect("parse");
    let cols = numeric(&["author", "sz"]);
    create_definition_without_backfill(&db.pool, src, &cols)
        .await
        .expect("create def");
    create_aggregate_target_table(&db.pool, &def, "public", &cols)
        .await
        .expect("create target");

    backfill_definition(&db.pool, &def, "public", &def.source, &cols)
        .await
        .expect("backfill");

    // Issue #128: the NULL-author group is now representable (the target's
    // group-by column is keyed by a `UNIQUE NULLS NOT DISTINCT` constraint,
    // not a bare `PRIMARY KEY`) and must be built exactly like any other
    // group — not silently dropped.
    let client = db.pool.get().await.expect("get connection");
    let null_total: String = client
        .query_one("select total::text from public.t where author is null", &[])
        .await
        .expect("the NULL-author group must have a built row")
        .get(0);
    assert_eq!(null_total, "70", "NULL-author group sum = 30 + 40");
    let total_rows: i64 = client
        .query_one("select count(*) from public.t", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        total_rows, 3,
        "both non-NULL groups (authors 1 and 2) plus the NULL group"
    );
    let author1_total: String = client
        .query_one("select total::text from public.t where author = 1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(author1_total, "30", "author 1 sum = 10 + 20");
    assert_aggregate_matches_oracle(&db, &def).await;
}

#[tokio::test]
async fn aggregate_build_computes_min_max_avg() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table s (id bigint primary key, author numeric, sz numeric); \
             alter table s replica identity full; \
             insert into s (id, author, sz) values \
               (1, 1, 10), (2, 1, 30), (3, 1, 20), (4, 2, 5)",
        )
        .await
        .expect("seed source");
    drop(client);

    // MIN/MAX are RecomputeOnly; AVG carries hidden sum/count partials. The
    // direct build computes each with its own aggregate rather than a blanket
    // additive `+` (M3 concern #3).
    let src = "TRANSFORM t FROM s GROUP BY author \
               SELECT author AS author, MIN(sz) AS lo, MAX(sz) AS hi, AVG(sz) AS mean";
    let def = parse(src).expect("parse");
    let cols = numeric(&["author", "sz"]);
    create_definition_without_backfill(&db.pool, src, &cols)
        .await
        .expect("create def");
    create_aggregate_target_table(&db.pool, &def, "public", &cols)
        .await
        .expect("create target");

    backfill_definition(&db.pool, &def, "public", &def.source, &cols)
        .await
        .expect("backfill");

    // Numeric equality (not text): AVG's visible column is maintained as
    // sum/count, whose scale need not match a bare `avg()`'s representation.
    let client = db.pool.get().await.expect("get connection");
    let row = client
        .query_one(
            "select lo = 10, hi = 30, mean = 20, \
             \"__mean_sum\" = 60, \"__mean_count\" = 3 \
             from public.t where author = 1",
            &[],
        )
        .await
        .unwrap();
    assert!(row.get::<_, bool>(0), "MIN = 10");
    assert!(row.get::<_, bool>(1), "MAX = 30");
    assert!(row.get::<_, bool>(2), "AVG = 60/3 = 20");
    assert!(row.get::<_, bool>(3), "AVG sum partial = 60");
    assert!(row.get::<_, bool>(4), "AVG count partial = 3");

    // author = 2 is a single-row group: MIN = MAX = AVG = 5.
    let single = client
        .query_one(
            "select lo = 5, hi = 5, mean = 5 from public.t where author = 2",
            &[],
        )
        .await
        .unwrap();
    assert!(
        single.get::<_, bool>(0) && single.get::<_, bool>(1) && single.get::<_, bool>(2),
        "single-row group MIN/MAX/AVG all = 5"
    );
}

#[tokio::test]
async fn aggregate_build_is_idempotent_on_rerun() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table s (id bigint primary key, author numeric, sz numeric); \
             alter table s replica identity full; \
             insert into s (id, author, sz) \
             select g, g % 50, g from generate_series(1, 5000) g",
        )
        .await
        .expect("seed source");
    drop(client);

    let src = "TRANSFORM t FROM s GROUP BY author \
               SELECT author AS author, SUM(sz) AS total, COUNT(*) AS n";
    let def = parse(src).expect("parse");
    let cols = numeric(&["author", "sz"]);
    create_definition_without_backfill(&db.pool, src, &cols)
        .await
        .expect("create def");
    create_aggregate_target_table(&db.pool, &def, "public", &cols)
        .await
        .expect("create target");

    // Overwrite ON CONFLICT means a re-run (the recovery story for a crash
    // partway through) recomputes to the same values rather than doubling the
    // SUM (M3 concern #4).
    backfill_definition(&db.pool, &def, "public", &def.source, &cols)
        .await
        .expect("first backfill");
    backfill_definition(&db.pool, &def, "public", &def.source, &cols)
        .await
        .expect("second backfill");

    assert_aggregate_matches_oracle(&db, &def).await;
}

#[tokio::test]
async fn aggregate_build_scans_source_once_not_per_chunk() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    // 30k distinct singleton groups over a 30k-row source: three 10k-group
    // write chunks. The M3-review bug filtered the *source* by group-key range
    // per chunk with no group-key index, so each of the (chunks) writes did a
    // full sequential scan of `s` — O(chunks x source_size). The fix aggregates
    // the source once into a staging table and chunk-writes from that, so `s` is
    // sequentially scanned exactly once for the whole build. pg_stat_user_tables
    // records cumulative seq scans per table; we assert the build adds at most a
    // couple (the single aggregation pass, allowing slop for planner/autovacuum),
    // never one-per-chunk.
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table s (id bigint primary key, author numeric, sz numeric); \
             alter table s replica identity full; \
             insert into s (id, author, sz) \
             select g, g, g from generate_series(1, 30000) g",
        )
        .await
        .expect("seed source");
    drop(client);

    let src = "TRANSFORM t FROM s GROUP BY author \
               SELECT author AS author, SUM(sz) AS total, COUNT(*) AS n";
    let def = parse(src).expect("parse");
    let cols = numeric(&["author", "sz"]);
    create_definition_without_backfill(&db.pool, src, &cols)
        .await
        .expect("create def");
    create_aggregate_target_table(&db.pool, &def, "public", &cols)
        .await
        .expect("create target");

    // Reads `s`'s cumulative sequential-scan count. Per-backend stats are flushed
    // to shared memory lazily (rate-limited to ~once/sec unless forced), and the
    // backfill runs on a *different* pooled connection than this reader, so poll
    // a few times letting the collector settle and take the largest observation.
    async fn seq_scans(db: &testkit::TestDatabase) -> i64 {
        let mut max = 0i64;
        for _ in 0..10 {
            let client = db.pool.get().await.expect("get connection");
            client
                .execute("select pg_stat_force_next_flush()", &[])
                .await
                .ok();
            let scans: i64 = client
                .query_one(
                    "select coalesce(seq_scan, 0) from pg_stat_user_tables where relname = 's'",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            max = max.max(scans);
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        }
        max
    }

    let before = seq_scans(&db).await;
    backfill_definition(&db.pool, &def, "public", &def.source, &cols)
        .await
        .expect("backfill");
    let after = seq_scans(&db).await;

    assert_aggregate_matches_oracle(&db, &def).await;

    let delta = after - before;
    // The build scans `s` once (the single-pass aggregation into staging); every
    // chunk write and the boundary scan hit the group-count-sized staging table
    // instead. The M3-review bug re-scanned `s` per write chunk plus once for
    // boundary discovery — with three chunks here that was >= 4, and it grew with
    // cardinality. 2 leaves slop for an incidental autovacuum/analyze scan.
    assert!(
        delta <= 2,
        "aggregate build should scan the source about once, not once per chunk \
         (seq_scan delta on s was {delta})"
    );
}

/// Asserts `public.t` (the aggregate target `def` built) is value-equal to a
/// fresh `GROUP BY` over the source rendered straight from `def`, comparing
/// every group's visible columns — including a NULL-key group (issue #128):
/// the target's grouping columns are keyed by a `UNIQUE NULLS NOT DISTINCT`
/// constraint, not a bare `PRIMARY KEY`, so a NULL-key group is representable
/// and must match the oracle exactly like any other group.
async fn assert_aggregate_matches_oracle(
    db: &testkit::TestDatabase,
    def: &trellis::defs::ast::TransformDef,
) {
    let trellis::defs::ast::KeySpace::Aggregate { group_by } = &def.key_space else {
        panic!("assert_aggregate_matches_oracle called on a non-aggregate def");
    };
    let client = db.pool.get().await.expect("get connection");

    let key_expr = group_by
        .iter()
        .map(|k| format!("coalesce({}::text, '')", k.target_column_name()))
        .collect::<Vec<_>>()
        .join(" || '|' || ");
    let val_expr = def
        .fields
        .iter()
        .filter(|f| !trellis::defs::ast::group_by_contains(group_by, &f.name))
        .map(|f| format!("coalesce({}::text, 'NULL')", f.name))
        .collect::<Vec<_>>()
        .join(" || '|' || ");

    let oracle_sql = render_aggregate_select_sql(def);
    let oracle = text_pairs(
        &client,
        &format!("select {key_expr}, {val_expr} from ({oracle_sql}) o"),
    )
    .await;
    let target = text_pairs(
        &client,
        &format!("select {key_expr}, {val_expr} from public.t"),
    )
    .await;
    assert_eq!(
        target, oracle,
        "built aggregate target must match the oracle"
    );
}
