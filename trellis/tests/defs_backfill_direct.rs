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
    ValueType, backfill_definition, create_definition_without_backfill, create_target_table, parse,
    source_primary_key,
};

fn numeric(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
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
