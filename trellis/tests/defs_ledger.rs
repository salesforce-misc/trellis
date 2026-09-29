//! #623 part D2: the one-pass aggregate build writes the target's ledger
//! (`<target>__ledger`, `defs::ledger`), one entry per source row, and builds
//! the group rows from it. Run against a real ephemeral Postgres via
//! `testkit::TestCluster`.
//!
//! Nothing on the apply path reads the ledger yet, so these pin only what the
//! build leaves: the entries equal the source, each carries the snapshot of
//! the read that wrote it, and the groups are sums over the entries.

use std::collections::HashMap;

use testkit::TestCluster;
use trellis::defs::{
    ValueType, backfill_definition, create_aggregate_target_table,
    create_definition_without_backfill, parse,
};

/// Builds `src` directly (no ring), the way the backfill discharge's
/// direct-build job does.
async fn build(db: &testkit::TestDatabase, src: &str, cols: &HashMap<String, ValueType>) {
    let def = parse(src).expect("parse");
    create_definition_without_backfill(&db.pool, src, cols)
        .await
        .expect("create def");
    create_aggregate_target_table(&db.pool, &def, "public", cols)
        .await
        .expect("create target");
    backfill_definition(&db.pool, &def, "public", &def.source, cols)
        .await
        .expect("backfill");
}

async fn rows(client: &trellis::pool::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|r| (0..r.len()).map(|i| r.get(i)).collect())
        .collect()
}

#[tokio::test]
async fn the_aggregate_build_writes_one_ledger_entry_per_source_row_and_the_groups_are_their_sums()
{
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = db.pool.get().await.expect("get connection");
    // A NULL group, a NULL argument, and more than one 10k-group chunk,
    // seeded by one transaction whose id the basis must see.
    let txn = client.transaction().await.unwrap();
    txn.batch_execute(
        "create table s (id bigint primary key, g bigint, a numeric, label text); \
         alter table s replica identity full; \
         insert into s select i, case when i % 997 = 0 then null else i % 12000 end, \
             case when i % 5 = 0 then null else i end, 'l' || (i % 7) \
         from generate_series(1, 30000) i",
    )
    .await
    .expect("seed source");
    let seeded_by: String = txn
        .query_one("select pg_current_xact_id()::text", &[])
        .await
        .unwrap()
        .get(0);
    txn.commit().await.unwrap();

    let cols = HashMap::from([
        ("g".to_string(), ValueType::Numeric),
        ("a".to_string(), ValueType::Numeric),
        ("label".to_string(), ValueType::Text),
    ]);
    build(
        &db,
        "TRANSFORM t FROM s GROUP BY g SELECT g AS g, SUM(a) AS total, AVG(a) AS mean, \
         COUNT(*) AS n, COUNT(label) AS labelled, MIN(label) AS lo",
        &cols,
    )
    .await;

    // One entry per source row, keyed like the ring, with its group and
    // argument values.
    assert_eq!(
        rows(
            &client,
            "select count(*)::text from s full join t__ledger l on l.__from_key = s.id::text \
             where l.__from_key is null or s.id is null \
                or l.g is distinct from s.g or l.__arg0 is distinct from s.a \
                or l.__arg1 is distinct from s.label"
        )
        .await,
        vec![vec![Some("0".to_string())]],
        "the ledger is the source, entry for entry"
    );
    // The build's state for every entry: a live member, never applied, and
    // read at one snapshot.
    assert_eq!(
        rows(
            &client,
            "select count(*)::text, count(distinct __basis::text)::text, \
                 bool_and(__member)::text, bool_or(__tombstone)::text, \
                 count(__applied_lsn)::text, count(__applied_seg)::text, count(__join_key)::text \
             from t__ledger"
        )
        .await,
        vec![vec![
            Some("30000".to_string()),
            Some("1".to_string()),
            Some("true".to_string()),
            Some("false".to_string()),
            Some("0".to_string()),
            Some("0".to_string()),
            Some("0".to_string()),
        ]],
    );
    // The basis is the read's snapshot: the transaction that wrote the rows
    // it read is visible in it, and one that starts after it isn't.
    let seeded_visible: bool = client
        .query_one(
            "select pg_visible_in_snapshot($1::text::xid8, \
                 (select __basis from t__ledger limit 1))",
            &[&seeded_by],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        seeded_visible,
        "the seeding transaction is visible in the basis"
    );
    let txn = client.transaction().await.unwrap();
    let later_visible: bool = txn
        .query_one(
            "select pg_visible_in_snapshot(pg_current_xact_id(), \
                 (select __basis from t__ledger limit 1))",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    txn.rollback().await.unwrap();
    assert!(!later_visible, "a transaction after the read isn't");

    // Every group row is its members' sums: the visible fields, the hidden
    // partials and the member count, NULL group included.
    let from_ledger = rows(
        &client,
        "select g::text, sum(__arg0)::text, count(__arg0)::text, \
             (sum(__arg0) / nullif(count(__arg0), 0)::numeric)::text, count(*)::text, \
             count(__arg1)::text, min(__arg1) \
         from t__ledger where __member and not __tombstone group by g order by g",
    )
    .await;
    let target = rows(
        &client,
        "select g::text, total::text, __total_count::text, mean::text, n::text, \
             labelled::text, lo \
         from t order by g",
    )
    .await;
    assert_eq!(from_ledger.len(), 12_001, "12000 groups and the NULL one");
    assert_eq!(target, from_ledger, "the groups are the ledger's sums");
    assert_eq!(
        rows(
            &client,
            "select count(*)::text from t where __trellis_members is distinct from n"
        )
        .await,
        vec![vec![Some("0".to_string())]],
        "the member count is the group's row count"
    );
    // And both equal a plain GROUP BY over the source.
    assert_eq!(
        rows(
            &client,
            "select g::text, sum(a)::text, count(a)::text, \
                 (case when count(a) = 0 then null else sum(a) / count(a)::numeric end)::text, \
                 count(*)::text, count(label)::text, min(label) \
             from s group by g order by g"
        )
        .await,
        target,
        "the groups equal the source's"
    );
}

/// A rebuild (a resumed definition's) starts the ledger over: an entry
/// whose source row is gone doesn't survive it.
#[tokio::test]
async fn a_rebuild_replaces_the_ledger() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table s (id bigint primary key, g bigint, a numeric); \
             alter table s replica identity full; \
             insert into s select i, i % 3, i from generate_series(1, 30) i",
        )
        .await
        .expect("seed source");
    let src = "TRANSFORM t FROM s GROUP BY g SELECT SUM(a) AS total";
    let cols = HashMap::from([
        ("g".to_string(), ValueType::Numeric),
        ("a".to_string(), ValueType::Numeric),
    ]);
    build(&db, src, &cols).await;
    let first_basis: String = client
        .query_one("select min(__basis::text) from t__ledger", &[])
        .await
        .unwrap()
        .get(0);

    client
        .batch_execute("delete from s where id > 20; update s set a = a * 10 where id = 1")
        .await
        .expect("change the source");
    let def = parse(src).expect("parse");
    backfill_definition(&db.pool, &def, "public", &def.source, &cols)
        .await
        .expect("rebuild");

    assert_eq!(
        rows(
            &client,
            "select count(*)::text, max(__from_key::bigint)::text, \
                 (select __arg0::text from t__ledger where __from_key = '1'), \
                 count(distinct __basis::text)::text from t__ledger"
        )
        .await,
        vec![vec![
            Some("20".to_string()),
            Some("20".to_string()),
            Some("10".to_string()),
            Some("1".to_string()),
        ]],
    );
    let second_basis: String = client
        .query_one("select min(__basis::text) from t__ledger", &[])
        .await
        .unwrap()
        .get(0);
    assert_ne!(first_basis, second_basis, "the rebuild's read is a new one");
}

/// A text argument's ledger column carries the collation the argument has
/// over the source, so `MIN`/`MAX` over the ledger order it the same way. The
/// test clusters' default collation is ICU `en-US`, which sorts `a` before
/// `B`; `"C"` sorts `B` first.
#[tokio::test]
async fn a_text_contribution_keeps_its_arguments_collation() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table s (id bigint primary key, g bigint, label text collate \"C\"); \
             alter table s replica identity full; \
             insert into s values (1, 1, 'a'), (2, 1, 'B')",
        )
        .await
        .expect("seed source");
    build(
        &db,
        "TRANSFORM t FROM s GROUP BY g SELECT MIN(label) AS lo, MAX(label) AS hi",
        &HashMap::from([
            ("g".to_string(), ValueType::Numeric),
            ("label".to_string(), ValueType::Text),
        ]),
    )
    .await;

    assert_eq!(
        rows(
            &client,
            "select c.collname::text from pg_attribute a \
             join pg_collation c on c.oid = a.attcollation \
             where a.attrelid = 't__ledger'::regclass and a.attname = '__arg0'"
        )
        .await,
        vec![vec![Some("C".to_string())]],
    );
    assert_eq!(
        rows(&client, "select lo, hi from t").await,
        rows(&client, "select min(label), max(label) from s").await,
        "the build orders as the source does"
    );
    assert_eq!(
        rows(&client, "select lo, hi from t").await,
        vec![vec![Some("B".to_string()), Some("a".to_string())]],
    );
}
