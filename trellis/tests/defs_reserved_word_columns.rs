//! A source column whose name Postgres reserves (created with quotes) is an
//! ordinary column to Trellis as long as no expression names it: the grammar
//! has no quoted identifiers, so a bare `"order"` can't be written, but a
//! definition over a table that has one defines and builds (ADR-0022).

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::NoTls;
use trellis::defs::{ValueType, install_definition};
use trellis::intake::harness;
use trellis::integer::IntWidth;

fn source_columns() -> HashMap<String, ValueType> {
    let int4 = ValueType::Integer(IntWidth::Int4);
    HashMap::from([
        ("id".to_string(), int4),
        ("grp".to_string(), int4),
        ("val".to_string(), int4),
        ("order".to_string(), int4),
        ("user".to_string(), int4),
        ("group".to_string(), int4),
    ])
}

async fn connect(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

const CREATE: &str = "create table public.rw_src (id integer primary key, grp integer, \
     val integer, \"order\" integer, \"user\" integer, \"group\" integer); \
     insert into public.rw_src values (1, 1, 5, 10, 20, 30), (2, 1, 7, 11, 21, 31), \
     (3, 2, 9, 12, 22, 32)";

/// A 1-1 transform and an aggregate over a table with reserved-word columns
/// define and build; the reserved columns are simply not selected.
#[tokio::test]
async fn a_source_column_named_like_a_reserved_word_does_not_block_a_definition() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect(db.dsn()).await;
    client.batch_execute(CREATE).await.expect("create rw_src");

    install_definition(
        &db.pool,
        "TRANSFORM rw_flat FROM rw_src SELECT (val + 1) AS bumped",
        &source_columns(),
        "public",
    )
    .await
    .expect("a 1-1 definition beside reserved-word columns");
    install_definition(
        &db.pool,
        "TRANSFORM rw_totals FROM rw_src GROUP BY grp WITHOUT SOURCE INDEX \
         SELECT grp AS grp, SUM(val) AS total",
        &source_columns(),
        "public",
    )
    .await
    .expect("an aggregate definition beside reserved-word columns");
    harness::start_registered_builds(&db.pool)
        .await
        .expect("dispatch the builds");
    trellis::staging::build::settle_builds(&db.pool).await;

    let flat: Vec<String> = client
        .query("select bumped::text from public.rw_flat order by id", &[])
        .await
        .expect("read rw_flat")
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(flat, ["6", "8", "10"]);
    let totals: Vec<(i32, String)> = client
        .query(
            "select grp, total::text from public.rw_totals order by grp",
            &[],
        )
        .await
        .expect("read rw_totals")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(totals, [(1, "12".to_string()), (2, "9".to_string())]);
}

/// Naming a reserved-word column in an expression is refused at define with
/// the qualify-or-rename remedy, and nothing is created; a reserved word as the label after `AS` is still fine.
#[tokio::test]
async fn naming_a_reserved_word_column_in_an_expression_is_refused_at_define() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect(db.dsn()).await;
    client.batch_execute(CREATE).await.expect("create rw_src");

    for expr in ["order + 1", "user", "val + group", "t.val + group"] {
        let err = install_definition(
            &db.pool,
            &format!("TRANSFORM rw_bad FROM rw_src SELECT {expr} AS x"),
            &source_columns(),
            "public",
        )
        .await
        .expect_err("a reserved word is not a column name");
        let text = err.to_string();
        assert!(
            text.contains("reserved word")
                && text.contains("qualified with its table")
                && text.contains("rename the column"),
            "{expr}: {text}"
        );
    }
    let exists: bool = client
        .query_one("select to_regclass('public.rw_bad') is not null", &[])
        .await
        .expect("look up the target")
        .get(0);
    assert!(!exists, "a refused definition leaves no target behind");

    // After AS any keyword is a label, as in Postgres; the field becomes a
    // target column of that name.
    install_definition(
        &db.pool,
        "TRANSFORM rw_label FROM rw_src SELECT val AS limit",
        &source_columns(),
        "public",
    )
    .await
    .expect("a reserved word is a fine field name");
}

/// The remedy the refusal names: a reserved-word source column read qualified
/// with its table (`rw_src.order`, which Postgres accepts unquoted too) defines,
/// builds and computes from that column, in a 1-1 field, an aggregate's
/// argument and a `GROUP BY` key.
#[tokio::test]
async fn a_reserved_word_column_is_read_qualified_with_its_table() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect(db.dsn()).await;
    client.batch_execute(CREATE).await.expect("create rw_src");

    install_definition(
        &db.pool,
        "TRANSFORM rw_q FROM rw_src SELECT rw_src.order + 1 AS next_order, \
         public.rw_src.user AS who, val AS limit",
        &source_columns(),
        "public",
    )
    .await
    .expect("qualified reserved-word columns in a 1-1 definition");
    install_definition(
        &db.pool,
        "TRANSFORM rw_q_totals FROM rw_src GROUP BY rw_src.group WITHOUT SOURCE INDEX \
         SELECT SUM(rw_src.order) AS total",
        &source_columns(),
        "public",
    )
    .await
    .expect("a qualified reserved-word column as an aggregate's argument and key");
    harness::start_registered_builds(&db.pool)
        .await
        .expect("dispatch the builds");
    trellis::staging::build::settle_builds(&db.pool).await;

    let rows: Vec<(i32, i32, i32)> = client
        .query(
            "select next_order, who, \"limit\" from public.rw_q order by id",
            &[],
        )
        .await
        .expect("read rw_q")
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    assert_eq!(rows, [(11, 20, 5), (12, 21, 7), (13, 22, 9)]);
    let totals: Vec<(i32, String)> = client
        .query(
            "select \"group\", total::text from public.rw_q_totals order by 1",
            &[],
        )
        .await
        .expect("read rw_q_totals")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(
        totals,
        [
            (30, "10".to_string()),
            (31, "11".to_string()),
            (32, "12".to_string())
        ]
    );
}
