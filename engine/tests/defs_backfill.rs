//! Integration test for issue #23: creating a definition backfills its
//! source exactly once, regardless of how many calculated fields the
//! definition declares — "N columns, one backfill", not once per column.
//! Run against a real, ephemeral Postgres instance via `testkit::TestCluster`.

use std::collections::HashMap;

use engine::defs::{ValueType, create_definition};
use testkit::TestCluster;

#[tokio::test]
async fn create_definition_backfills_the_source_exactly_once_regardless_of_field_count() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table s (id serial primary key, a numeric, b numeric); \
             insert into s (a, b) values (1, 2), (3, 4), (5, 6)",
        )
        .await
        .expect("create and seed source table");
    drop(client);

    // Two calculated fields off the same source: if the backfill enumerated
    // the source once per field (rather than once per definition), this
    // would stage 6 recompute rows (3 source rows x 2 fields) instead of 3.
    create_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a + a AS x, a + b AS y",
        &HashMap::from([
            ("a".to_string(), ValueType::Numeric),
            ("b".to_string(), ValueType::Numeric),
        ]),
    )
    .await
    .expect("valid definition should be stored and backfilled");

    let client = db.pool.get().await.expect("get connection");
    let staged: Vec<String> = client
        .query(
            "select key, source_relation_oid from seg_0 \
             where op = 'recompute' and src_table = 'trellis.s' \
             order by key",
            &[],
        )
        .await
        .expect("query seg_0")
        .into_iter()
        .map(|r| {
            let source_relation_oid: Option<u32> = r.get(1);
            assert!(
                source_relation_oid.is_some(),
                "backfill must stage the source OID"
            );
            r.get(0)
        })
        .collect();
    assert_eq!(
        staged.len(),
        3,
        "expected exactly one recompute row per source row (3), not one per \
         source row per calculated field; got {staged:?}"
    );
}
