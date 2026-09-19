//! Repro: a 1-1 target's stored primary key must be the source row's *raw*
//! primary-key text, byte-for-byte — never the staging ring's encoded key
//! text.

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::Pool;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::defs::{create_definition, create_target_table, source_primary_key};
use trellis::{Client as TrellisClient, ClientOptions};

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

async fn poll_until<F>(timeout: Duration, interval: Duration, message: &str, mut predicate: F)
where
    F: AsyncFnMut() -> bool,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if predicate().await {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!("poll_until timed out after {timeout:?}: {message}");
        }
        tokio::time::sleep(interval).await;
    }
}

fn totals_def() -> TransformDef {
    TransformDef {
        target: "totals".to_string(),
        source: "orders".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("a".to_string())),
                rhs: Box::new(Expr::Column("b".to_string())),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    }
}

async fn setup(pool: &Pool, raw: &Client) {
    raw.batch_execute("create table orders (id text primary key, a numeric, b numeric)")
        .await
        .expect("create source table");

    let source_columns = HashMap::from([
        ("id".to_string(), ValueType::Text),
        ("a".to_string(), ValueType::Numeric),
        ("b".to_string(), ValueType::Numeric),
    ]);
    create_definition(
        pool,
        "TRANSFORM totals FROM orders SELECT a + b AS total",
        &source_columns,
    )
    .await
    .expect("create definition");

    let pk = trellis::defs::require_single_column_pk(
        source_primary_key(pool, "orders")
            .await
            .expect("introspect source primary key"),
        "orders",
    )
    .expect("single-column pk");
    create_target_table(
        pool,
        &totals_def(),
        "public",
        &pk,
        &source_columns,
        &totals_def().source,
    )
    .await
    .expect("create target table");
}

/// A `text` primary key whose value genuinely contains a U+0001 (SOH) must
/// land in the 1-1 target verbatim: the incremental apply path binds the
/// ring's key text straight in as the target's literal PK value, while
/// backfill (`select {pk} from {source}`), quarantine's resume write-back
/// (`where {pk}::text = $2`) and the oracle all use the *raw* column value.
/// If the key encoding escapes the value, those paths diverge and the row is
/// duplicated/orphaned.
#[tokio::test]
async fn a_one_to_one_target_stores_the_raw_source_pk_even_with_a_control_character() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    setup(&db.pool, &raw).await;

    let options = ClientOptions {
        staging_worker: true,
        application_threads: 2,
        source_tables: vec![format!("{DEFAULT_SCHEMA}.orders")],
        ..Default::default()
    };
    let client = TrellisClient::start(db.dsn(), options).expect("client start");

    let soh_id = "a\u{1}b".to_string();
    let plain_id = "plain".to_string();
    raw.execute(
        "insert into orders (id, a, b) values ($1, 1.00, 2.00), ($2, 3.00, 4.00)",
        &[&soh_id, &plain_id],
    )
    .await
    .expect("insert source rows");

    poll_until(
        Duration::from_secs(20),
        Duration::from_millis(200),
        "the two source rows never drained into the target",
        async || {
            let n: i64 = raw
                .query_one("select count(*) from totals", &[])
                .await
                .expect("count target")
                .get(0);
            n == 2
        },
    )
    .await;

    let ids: Vec<String> = raw
        .query("select id from totals order by id", &[])
        .await
        .expect("read target ids")
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    let mut expected = vec![soh_id.clone(), plain_id.clone()];
    expected.sort();
    assert_eq!(
        ids, expected,
        "the target's stored primary keys must equal the source's raw primary keys \
         (a doubled U+0001 here means the ring's encoded key text leaked into the \
         target's literal PK column)"
    );

    // And the derived row must be *updatable* through the same path: an
    // update that lands under a differently-encoded key would insert a
    // second row instead of updating the first.
    raw.execute("update orders set a = 10.00 where id = $1", &[&soh_id])
        .await
        .expect("update source row");

    poll_until(
        Duration::from_secs(20),
        Duration::from_millis(200),
        "the updated row never converged",
        async || {
            let total: Option<String> = raw
                .query_opt("select total::text from totals where id = $1", &[&soh_id])
                .await
                .expect("read target row")
                .map(|r| r.get(0));
            total == Some("12.00".to_string())
        },
    )
    .await;
    let n: i64 = raw
        .query_one("select count(*) from totals", &[])
        .await
        .expect("count target")
        .get(0);
    assert_eq!(n, 2, "the update must not have inserted a duplicate row");

    // A delete must remove the row it originally wrote, not miss it.
    raw.execute("delete from orders where id = $1", &[&soh_id])
        .await
        .expect("delete source row");
    poll_until(
        Duration::from_secs(20),
        Duration::from_millis(200),
        "the deleted row was never removed from the target",
        async || {
            let n: i64 = raw
                .query_one("select count(*) from totals", &[])
                .await
                .expect("count target")
                .get(0);
            n == 1
        },
    )
    .await;

    client.shutdown().await.expect("clean shutdown");
}
