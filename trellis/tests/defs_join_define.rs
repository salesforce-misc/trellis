//! Define checks a `JOIN` definition against both
//! tables' live columns, then registers it `waiting_to_backfill` with its
//! catalog state: both tables in its row, a `source` and a `join` edge, both
//! version fences bumped, the visible target and its hidden tables. A
//! staging-worker pass starts its build (`join_interleavings.rs` covers the
//! build itself). The pure rules are covered by `defs::validate`'s `join_*`
//! tests; these pin that define reads each side's real types, modifiers,
//! collations and enums, that nothing is left behind by a refusal, and the
//! lifecycle a two-table definition goes through: status, pause, resume
//! (and its refusals on either side) and drop.
//!
//! Every test is stepped by hand: no staging worker runs, and nothing polls
//! for convergence.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::capture::install::{Installed, installed};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{CatalogError, ValidationError, ValueType, install_definition};
use trellis::{Applied, Config, TransformStatus, Trellis, TrellisOptions};

async fn batch(pool: &trellis::pool::Pool, sql: &str) {
    pool.get()
        .await
        .expect("get connection")
        .batch_execute(sql)
        .await
        .expect("set up tables");
}

/// `install_definition`'s refusal for `text`, with the `FROM` table's
/// columns passed the way the app passes them (only the single-table
/// validator reads them, and a JOIN never reaches it).
async fn define_err(pool: &trellis::pool::Pool, text: &str) -> CatalogError {
    let source_columns: HashMap<String, ValueType> = HashMap::new();
    install_definition(pool, text, &source_columns, "public")
        .await
        .expect_err("this JOIN definition is refused")
}

async fn assert_nothing_registered(pool: &trellis::pool::Pool, target: &str) {
    let client = pool.get().await.expect("get connection");
    let exists: bool = client
        .query_one(
            "select pg_catalog.to_regclass($1) is not null",
            &[&format!("public.{target}")],
        )
        .await
        .expect("probe target")
        .get(0);
    assert!(!exists, "a refused definition must not leave its target");
    let definitions: i64 = client
        .query_one(
            "select count(*) from transform_definitions where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("count definitions")
        .get(0);
    assert_eq!(definitions, 0, "a refused definition must not be stored");
}

#[tokio::test]
async fn define_validates_a_join_against_both_tables_then_refuses_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    batch(
        &db.pool,
        "create schema other;
         create type tier as enum ('gold', 'silver');
         create table public.orders (id integer primary key, region text,
                                     total numeric(10,2), big_id bigint, tier tier,
                                     loose numeric);
         create table public.items (id integer primary key, order_id integer, sku text,
                                    qty integer, price numeric, tier tier);
         create table other.lines (id integer primary key, order_id integer, qty integer);
         create index on public.items (order_id); create index on public.items (tier);
         create index on public.orders (tier); create index on other.lines (order_id);",
    )
    .await;

    // A valid JOIN passes every check, from either side, and is registered.
    for (target, text) in [
        (
            "by_region_sku",
            "TRANSFORM by_region_sku FROM orders LEFT JOIN items ON orders.id = items.order_id \
             GROUP BY orders.region, items.sku \
            WITHOUT SOURCE INDEX SELECT orders.region AS region, items.sku AS sku, COUNT(*) AS n, \
                    SUM(items.qty) AS qty, AVG(orders.total) AS avg_total",
        ),
        // An enum join key is text-stable.
        (
            "by_tier",
            "TRANSFORM by_tier FROM orders JOIN items ON orders.tier = items.tier \
             GROUP BY orders.region WITHOUT SOURCE INDEX SELECT COUNT(*) AS n",
        ),
        // An explicitly qualified joined table.
        (
            "by_line",
            "TRANSFORM by_line FROM orders JOIN other.lines ON orders.id = lines.order_id \
             GROUP BY orders.region WITHOUT SOURCE INDEX SELECT SUM(lines.qty) AS qty",
        ),
    ] {
        let definition = install_definition(&db.pool, text, &HashMap::new(), "public")
            .await
            .unwrap_or_else(|err| panic!("{text}: {err:?}"));
        assert_eq!(definition.target_table, format!("public.{target}"));
        assert_eq!(definition.status, TransformStatus::WaitingToBackfill);
        // The public type keeps naming the `FROM` table.
        assert_eq!(definition.source_table, "public.orders");
    }
    let joined: Vec<String> = db
        .pool
        .get()
        .await
        .expect("get connection")
        .query(
            "select join_table from transform_definitions order by id",
            &[],
        )
        .await
        .expect("read join tables")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(joined, ["public.items", "public.items", "other.lines"]);

    // Each check reads the live column, on either side.
    let on = |on: &str, select: &str| {
        format!(
            "TRANSFORM by_region FROM orders JOIN items ON {on} \
             GROUP BY orders.region WITHOUT SOURCE INDEX SELECT {select}"
        )
    };
    let err = define_err(
        &db.pool,
        &on("orders.big_id = items.order_id", "COUNT(*) AS n"),
    )
    .await;
    assert!(
        matches!(
            &err,
            CatalogError::Validate(ValidationError::JoinKeyTypeMismatch { lhs_type, rhs_type, .. })
                if lhs_type == "bigint" && rhs_type == "integer"
        ),
        "{err:?}"
    );
    let err = define_err(
        &db.pool,
        &on("orders.id = items.order_id", "SUM(items.price) AS s"),
    )
    .await;
    assert!(
        matches!(
            err,
            CatalogError::Validate(ValidationError::UnconstrainedNumericAggregate { .. })
        ),
        "{err:?}"
    );
    let err = define_err(
        &db.pool,
        &on("orders.id = items.order_id", "MIN(items.qty) AS m"),
    )
    .await;
    assert!(
        matches!(
            err,
            CatalogError::Validate(ValidationError::DeferredAggregate {
                family: "MIN/MAX",
                ..
            })
        ),
        "{err:?}"
    );
    let err = define_err(&db.pool, &on("orders.id = items.nope", "COUNT(*) AS n")).await;
    assert!(
        matches!(
            err,
            CatalogError::Validate(ValidationError::JoinUnresolvedColumn { .. })
        ),
        "{err:?}"
    );
    // A joined table that doesn't exist.
    let err = define_err(
        &db.pool,
        "TRANSFORM by_region FROM orders JOIN missing ON orders.id = missing.order_id \
         GROUP BY orders.region WITHOUT SOURCE INDEX SELECT COUNT(*) AS n",
    )
    .await;
    assert!(
        matches!(err, CatalogError::SourceTableNotFound(ref table) if table == "missing"),
        "{err:?}"
    );

    // A column named by two ON terms, on either side, is a typed refusal that
    // names the column, not a raw Postgres error from the hidden-table DDL;
    // a repeated pair keeps its own refusal. The second case is refused before
    // its second term's types are compared; the third swaps the tables so the
    // joined side's repeated column pairs with matching types, the shape whose
    // hidden table would name that column twice in its unique constraint.
    for (text, column) in [
        (
            on(
                "orders.id = items.order_id AND orders.id = items.qty",
                "COUNT(*) AS n",
            ),
            "orders.id",
        ),
        (
            on(
                "orders.id = items.order_id AND orders.big_id = items.order_id",
                "COUNT(*) AS n",
            ),
            "items.order_id",
        ),
        (
            "TRANSFORM by_region FROM items JOIN orders ON items.order_id = orders.id \
             AND items.qty = orders.id GROUP BY items.sku WITHOUT SOURCE INDEX \
             SELECT COUNT(*) AS n"
                .to_string(),
            "orders.id",
        ),
    ] {
        let err = define_err(&db.pool, &text).await;
        assert!(
            matches!(
                &err,
                CatalogError::Validate(ValidationError::JoinOnRepeatedColumn { column: named, .. })
                    if named == column
            ),
            "{text}: {err:?}"
        );
        assert!(err.to_string().contains(column), "{err}");
    }
    let err = define_err(
        &db.pool,
        &on(
            "orders.id = items.order_id AND items.order_id = orders.id",
            "COUNT(*) AS n",
        ),
    )
    .await;
    assert!(
        matches!(
            err,
            CatalogError::Validate(ValidationError::JoinOnRepeatedPair { .. })
        ),
        "{err:?}"
    );

    assert_nothing_registered(&db.pool, "by_region").await;
}

#[tokio::test]
async fn define_refuses_a_join_key_with_a_nondeterministic_collation() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    batch(
        &db.pool,
        "create collation case_insensitive \
             (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
         create table customers (id integer primary key,
                                 code text collate case_insensitive, region text);
         create table visits (id integer primary key,
                              code text collate case_insensitive);",
    )
    .await;
    let err = define_err(
        &db.pool,
        "TRANSFORM visits_by_region FROM customers JOIN visits \
         ON customers.code = visits.code GROUP BY customers.region WITHOUT SOURCE INDEX SELECT COUNT(*) AS n",
    )
    .await;
    assert!(
        matches!(
            &err,
            CatalogError::Validate(ValidationError::JoinNondeterministicCollation { collation, .. })
                if collation == "case_insensitive"
        ),
        "{err:?}"
    );
    assert_nothing_registered(&db.pool, "visits_by_region").await;
}

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

async fn define_only(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a define-only Trellis")
}

const ORDERS_ITEMS: &str = "set search_path to public;
     create table orders (id integer primary key, region text, total numeric(10,2));
     create table items (id integer primary key, order_id integer, sku text, qty integer);
     create index on items (order_id);
     insert into orders values (1, 'emea', 10.00), (2, 'apac', 5.50);
     insert into items values (1, 1, 'a', 2), (2, 1, 'b', 3), (3, 2, 'a', 1);
     set search_path to trellis, public;";

const BY_REGION: &str = "TRANSFORM by_region FROM orders LEFT JOIN items \
     ON orders.id = items.order_id GROUP BY orders.region, items.sku \
    WITHOUT SOURCE INDEX SELECT orders.region AS region, items.sku AS sku, COUNT(*) AS n, \
            SUM(items.qty) AS qty, AVG(orders.total) AS avg_total";

async fn status(trellis: &Trellis, target: &str) -> Option<TransformStatus> {
    trellis
        .status(target)
        .await
        .expect("status")
        .map(|status| status.status)
}

async fn definition_id(raw: &Client, target: &str) -> i64 {
    raw.query_one(
        "select id from transform_definitions where split_part(target_table, '.', 2) = $1",
        &[&target],
    )
    .await
    .expect("read the definition's id")
    .get(0)
}

/// The definition's hidden tables in the catalog schema, by name.
async fn hidden_tables(raw: &Client, id: i64) -> Vec<String> {
    raw.query(
        "select c.relname::text from pg_catalog.pg_class c \
         join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
         where n.nspname = $1 and c.relname like $2 and c.relkind = 'r' order by 1",
        &[&DEFAULT_SCHEMA, &format!("join\\_{id}\\_%")],
    )
    .await
    .expect("list hidden tables")
    .into_iter()
    .map(|row| row.get(0))
    .collect()
}

/// One whole staging-worker pass: capture, then the start of ready builds.
async fn pass(raw: &mut Client, pool: &trellis::Pool) {
    trellis::client::reconcile_pass(raw, pool, DEFAULT_SCHEMA)
        .await
        .expect("reconcile pass");
}

async fn count(raw: &Client, sql: &str) -> i64 {
    raw.query_one(sql, &[]).await.expect("count").get(0)
}

#[tokio::test]
async fn a_join_registers_both_tables_and_a_pass_starts_its_build() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;
    raw.batch_execute(ORDERS_ITEMS).await.expect("seed");
    let trellis = define_only(db.dsn()).await;

    let applied = trellis.apply(BY_REGION).await.expect("define the JOIN");
    let target_table = applied.into_transform().expect("a transform is defined");
    assert!(target_table.ends_with(".by_region"), "{target_table}");
    assert_eq!(
        status(&trellis, "by_region").await,
        Some(TransformStatus::WaitingToBackfill)
    );
    let summaries = trellis.definitions().await.expect("definitions");
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].status, TransformStatus::WaitingToBackfill);

    // A lock wait on the joined table's capture, as a pass records one, is
    // what the waiting JOIN reports it waits on, as for its FROM table.
    raw.batch_execute(
        "insert into capture_holdups \
             (table_name, since, operation, lock_mode, observed_at, blockers) \
         values ('public.items', now(), 'install', 'ShareRowExclusiveLock', now(), '{}')",
    )
    .await
    .expect("record a lock wait");
    let reported = trellis
        .status("by_region")
        .await
        .expect("status")
        .expect("defined");
    assert_eq!(
        reported.capture_wait.map(|wait| wait.table).as_deref(),
        Some("public.items"),
        "the joined table's capture holds the JOIN up"
    );
    raw.batch_execute("delete from capture_holdups")
        .await
        .expect("clear the lock wait");

    // The catalog row names both tables, with each side's column types.
    let row = raw
        .query_one(
            "select source_table, join_table, join_columns ->> 'qty', \
                    source_columns ->> 'total' \
             from transform_definitions",
            &[],
        )
        .await
        .expect("read the definition");
    assert_eq!(row.get::<_, String>(0), "public.orders");
    assert_eq!(row.get::<_, String>(1), "public.items");
    assert_eq!(row.get::<_, Option<String>>(2).as_deref(), Some("integer"));
    assert_eq!(row.get::<_, Option<String>>(3).as_deref(), Some("numeric"));

    // A `source` edge from the FROM table and a `join` edge from the joined
    // one, both into the target; and both version fences.
    let edges: Vec<(String, String, String)> = raw
        .query(
            "select f.table_name, t.table_name, e.kind from schema_edges e \
             join schema_nodes f on f.id = e.from_node_id \
             join schema_nodes t on t.id = e.to_node_id order by e.kind",
            &[],
        )
        .await
        .expect("read edges")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    assert_eq!(
        edges,
        [
            (
                "public.items".to_string(),
                "public.by_region".to_string(),
                "join".to_string()
            ),
            (
                "public.orders".to_string(),
                "public.by_region".to_string(),
                "source".to_string()
            ),
        ]
    );
    assert_eq!(
        count(
            &raw,
            "select count(*) from source_table_versions \
             where source_table in ('public.orders', 'public.items') and version = 1"
        )
        .await,
        2
    );

    // The visible target holds the declared columns and its group identity,
    // nothing hidden; the hidden tables sit in the catalog schema, named by
    // the definition's id.
    let columns: Vec<(String, String)> = raw
        .query(
            "select attname::text, pg_catalog.format_type(atttypid, atttypmod) \
             from pg_catalog.pg_attribute \
             where attrelid = 'public.by_region'::regclass and attnum > 0 \
             order by attnum",
            &[],
        )
        .await
        .expect("read the target's columns")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        columns,
        [
            ("region", "text"),
            ("sku", "text"),
            ("n", "bigint"),
            ("qty", "bigint"),
            ("avg_total", "numeric"),
        ]
        .map(|(c, t)| (c.to_string(), t.to_string()))
    );
    let id = definition_id(&raw, "by_region").await;
    assert_eq!(
        hidden_tables(&raw, id).await,
        [
            format!("join_{id}_j"),
            format!("join_{id}_l"),
            format!("join_{id}_m"),
            format!("join_{id}_p"),
            format!("join_{id}_t")
        ]
    );

    // A staging-worker pass captures both tables, each imaged with the
    // columns the definition reads from it, and starts the JOIN's build.
    // The first pass installs both captures, and the joined table's install
    // gate holds the JOIN back; the second finds it ready and starts it.
    pass(&mut raw, &db.pool).await;
    pass(&mut raw, &db.pool).await;
    for (table, columns) in [
        ("public.orders", vec!["id", "region", "total"]),
        ("public.items", vec!["order_id", "qty", "sku"]),
    ] {
        match installed(&raw, DEFAULT_SCHEMA, table)
            .await
            .expect("read the capture")
        {
            Installed::Complete { spec, .. } => {
                for column in columns {
                    assert!(
                        spec.columns().iter().any(|c| c == column)
                            || spec.key().iter().any(|c| c == column),
                        "{table} images {column}: {spec:?}"
                    );
                }
            }
            other => panic!("{table} should be captured: {other:?}"),
        }
    }
    assert_eq!(
        status(&trellis, "by_region").await,
        Some(TransformStatus::Backfilling)
    );
}

#[tokio::test]
async fn a_join_pauses_resumes_and_drops_with_its_hidden_tables() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(ORDERS_ITEMS).await.expect("seed");
    let trellis = define_only(db.dsn()).await;
    trellis.apply(BY_REGION).await.expect("define the JOIN");
    let id = definition_id(&raw, "by_region").await;

    // DROP needs a pause first.
    trellis
        .apply("DROP TRANSFORM by_region")
        .await
        .expect_err("a waiting definition isn't dropped");

    assert!(matches!(
        trellis
            .apply("PAUSE TRANSFORM by_region")
            .await
            .expect("pause"),
        Applied::Paused
    ));
    assert_eq!(
        status(&trellis, "by_region").await,
        Some(TransformStatus::Paused)
    );

    // A resume drops and recreates the hidden tables: whatever was in them
    // is gone.
    raw.batch_execute(&format!(
        "insert into {DEFAULT_SCHEMA}.join_{id}_j (id, __matches) values (1, 2)"
    ))
    .await
    .expect("write a hidden row");
    trellis
        .apply("RESUME TRANSFORM by_region")
        .await
        .expect("resume");
    assert_eq!(
        status(&trellis, "by_region").await,
        Some(TransformStatus::WaitingToBackfill)
    );
    assert_eq!(
        count(
            &raw,
            &format!("select count(*) from {DEFAULT_SCHEMA}.join_{id}_j")
        )
        .await,
        0
    );
    assert_eq!(hidden_tables(&raw, id).await.len(), 5);

    // DROP takes the target, the hidden tables, the row and its edges.
    trellis
        .apply("PAUSE TRANSFORM by_region")
        .await
        .expect("pause");
    assert!(matches!(
        trellis
            .apply("DROP TRANSFORM by_region")
            .await
            .expect("drop"),
        Applied::Dropped
    ));
    assert_eq!(status(&trellis, "by_region").await, None);
    assert!(hidden_tables(&raw, id).await.is_empty());
    assert_eq!(
        count(
            &raw,
            "select count(*) from pg_catalog.pg_class where relname = 'by_region'"
        )
        .await,
        0
    );
    assert_eq!(count(&raw, "select count(*) from schema_edges").await, 0);
    assert_eq!(
        count(&raw, "select count(*) from transform_definitions").await,
        0
    );
}

#[tokio::test]
async fn a_join_resume_revalidates_both_tables() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(ORDERS_ITEMS).await.expect("seed");
    let trellis = define_only(db.dsn()).await;
    trellis.apply(BY_REGION).await.expect("define the JOIN");
    trellis
        .apply("PAUSE TRANSFORM by_region")
        .await
        .expect("pause");

    // Each break, on either side, refuses the resume and leaves the
    // definition paused; undoing it lets the resume through.
    for (break_it, refusal, fix) in [
        (
            "alter table items drop column qty",
            "items.qty",
            "alter table items add column qty integer",
        ),
        (
            "alter table orders drop column region",
            "orders.region",
            "alter table orders add column region text",
        ),
        (
            "alter table items alter column order_id type bigint",
            "bigint",
            "alter table items alter column order_id type integer",
        ),
        (
            "alter table items rename to items_old",
            "items",
            "alter table items_old rename to items",
        ),
        (
            "alter table items drop constraint items_pkey",
            "items",
            "alter table items add primary key (id)",
        ),
    ] {
        raw.batch_execute(break_it).await.expect(break_it);
        let err = trellis
            .apply("RESUME TRANSFORM by_region")
            .await
            .expect_err(break_it);
        let message = err.to_string();
        assert!(
            message.contains("can't resume") && message.contains(refusal),
            "{break_it}: {message}"
        );
        assert_eq!(
            status(&trellis, "by_region").await,
            Some(TransformStatus::Paused),
            "{break_it}"
        );
        raw.batch_execute(fix).await.expect(fix);
    }
    trellis
        .apply("RESUME TRANSFORM by_region")
        .await
        .expect("resume once both tables are whole");
    assert_eq!(
        status(&trellis, "by_region").await,
        Some(TransformStatus::WaitingToBackfill)
    );
}

#[tokio::test]
async fn a_column_lost_or_retyped_on_either_table_pauses_the_whole_join() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;
    raw.batch_execute(ORDERS_ITEMS).await.expect("seed");
    let trellis = define_only(db.dsn()).await;
    trellis.apply(BY_REGION).await.expect("define the JOIN");
    pass(&mut raw, &db.pool).await;

    for (table, break_it, column, fix) in [
        (
            "public.items",
            "alter table items rename column sku to sku_code",
            "sku",
            "alter table items rename column sku_code to sku",
        ),
        (
            "public.orders",
            "alter table orders drop column total",
            "total",
            "alter table orders add column total numeric(10,2)",
        ),
        // A widening Trellis would take on a lone key leaves the `ON` pair
        // of two types.
        (
            "public.items",
            "alter table items alter column order_id type bigint",
            "order_id",
            "alter table items alter column order_id type integer",
        ),
    ] {
        raw.batch_execute(break_it).await.expect(break_it);
        pass(&mut raw, &db.pool).await;
        let status = trellis
            .status("by_region")
            .await
            .expect("status")
            .expect("defined");
        assert_eq!(status.status, TransformStatus::Paused, "{break_it}");
        let failure = status.capture_failure.expect("the pause says why");
        assert_eq!(failure.source_table, table, "{break_it}");
        assert!(
            failure.columns.iter().any(|c| c == column),
            "{break_it}: {failure:?}"
        );
        raw.batch_execute(fix).await.expect(fix);
        trellis
            .apply("RESUME TRANSFORM by_region")
            .await
            .expect("resume once the column is back");
        pass(&mut raw, &db.pool).await;
        assert_eq!(
            trellis
                .status("by_region")
                .await
                .expect("status")
                .map(|s| s.status),
            Some(TransformStatus::Backfilling),
            "{break_it}: the pass after the resume starts the rebuild"
        );
    }
}

/// A JOIN may read another definition's target as either table. That
/// target's drop is refused while the JOIN stands, as it is for a
/// definition reading it as its `FROM`: the `join` edge counts.
#[tokio::test]
async fn a_target_a_join_reads_cannot_be_dropped_under_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(ORDERS_ITEMS).await.expect("seed");
    let trellis = define_only(db.dsn()).await;
    trellis
        .apply(
            "TRANSFORM line_totals FROM items GROUP BY order_id \
            WITHOUT SOURCE INDEX SELECT order_id AS order_id, SUM(qty) AS qty",
        )
        .await
        .expect("define the upstream aggregate");
    trellis::intake::harness::settle_builds(&db.pool).await;
    assert_eq!(
        status(&trellis, "line_totals").await,
        Some(TransformStatus::Live)
    );
    trellis
        .apply(
            "TRANSFORM by_region FROM orders JOIN line_totals \
             ON orders.id = line_totals.order_id GROUP BY orders.region \
            WITHOUT SOURCE INDEX SELECT orders.region AS region, SUM(line_totals.qty) AS qty",
        )
        .await
        .expect("define a JOIN over the upstream's target");

    trellis
        .apply("PAUSE TRANSFORM line_totals")
        .await
        .expect("pause the upstream");
    let err = trellis
        .apply("DROP TRANSFORM line_totals")
        .await
        .expect_err("a JOIN still reads the target");
    assert!(err.to_string().contains("by_region"), "{err}");
    assert_eq!(
        count(
            &raw,
            "select count(*) from pg_catalog.pg_class where relname = 'line_totals'"
        )
        .await,
        1
    );
}

/// `format_type` of `column` of `table` (qualified), with its collation
/// when it isn't the type's default.
async fn column_type(raw: &Client, table: &str, column: &str) -> String {
    raw.query_one(
        "select pg_catalog.format_type(a.atttypid, a.atttypmod) \
                || coalesce(' collate ' || co.collname, '') \
         from pg_catalog.pg_attribute a \
         join pg_catalog.pg_type t on t.oid = a.atttypid \
         left join pg_catalog.pg_collation co \
           on co.oid = a.attcollation and a.attcollation <> t.typcollation \
         where a.attrelid = pg_catalog.to_regclass($1) and a.attname = $2",
        &[&table, &column],
    )
    .await
    .unwrap_or_else(|e| panic!("{table}.{column}: {e}"))
    .get(0)
}

/// `target`'s definition is waiting to be built or building (no build job
/// runs here), not paused.
async fn assert_unpaused(trellis: &Trellis, target: &str) {
    let status = trellis
        .status(target)
        .await
        .expect("status")
        .expect("defined");
    assert!(
        matches!(
            status.status,
            TransformStatus::WaitingToBackfill | TransformStatus::Backfilling
        ),
        "{:?}: {:?}",
        status.status,
        status.capture_failure
    );
}

/// The columns a JOIN copies from either table (its target's `GROUP
/// BY` columns, `P`'s, `M`'s, `J`'s and `T`'s key columns, and its fields)
/// follow a widening as a 1-1 or aggregate target's copies do. A string
/// column's copies are unbounded `varchar`, with the column's
/// collation, so a `varchar(n)` widened, or moved to `text`, re-types
/// nothing and pauses nothing, an `ON` pair included; one that outgrows a column (`SUM(qty)`'s
/// `bigint` after `qty` goes `bigint`) pauses the JOIN, naming the column,
/// and its resume re-types it.
#[tokio::test]
async fn a_widened_string_changes_no_copy_and_an_outgrown_column_pauses_the_join() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "set search_path to public;
         create table orders (id integer primary key, code varchar(10) collate \"C\", \
                              region varchar(10));
         create table items (id integer primary key, order_code varchar(10) collate \"C\", \
                             sku varchar(10), qty integer);
         create index on orders (code); create index on items (order_code);
         set search_path to trellis, public;",
    )
    .await
    .expect("seed");
    let trellis = define_only(db.dsn()).await;
    trellis
        .apply(
            "TRANSFORM by_code FROM orders LEFT JOIN items \
             ON orders.code = items.order_code GROUP BY orders.region, items.sku \
            WITHOUT SOURCE INDEX SELECT orders.region AS region, items.sku AS sku, COUNT(*) AS n, \
                    SUM(items.qty) AS qty",
        )
        .await
        .expect("define the JOIN");
    pass(&mut raw, &db.pool).await;
    let id = definition_id(&raw, "by_code").await;
    let hidden = |part: &str| format!("{DEFAULT_SCHEMA}.join_{id}_{part}");
    // A `GROUP BY` column of each table.
    raw.batch_execute(
        "alter table public.items alter column sku type varchar(40); \
         alter table public.orders alter column region type text",
    )
    .await
    .expect("widen");
    pass(&mut raw, &db.pool).await;
    assert_unpaused(&trellis, "by_code").await;
    for (table, column, ty) in [
        ("public.by_code", "sku", "character varying"),
        (&hidden("m"), "sku", "character varying"),
        (&hidden("t"), "sku", "character varying"),
        ("public.by_code", "region", "character varying"),
        (&hidden("p"), "region", "character varying"),
        (&hidden("t"), "region", "character varying"),
    ] {
        assert_eq!(
            column_type(&raw, table, column).await,
            ty,
            "{table}.{column}"
        );
    }

    // The `ON` pair, widened together, keeps its collation.
    raw.batch_execute(
        "begin; \
         alter table public.orders alter column code type varchar(20) collate \"C\"; \
         alter table public.items alter column order_code type varchar(20) collate \"C\"; \
         commit",
    )
    .await
    .expect("widen the ON pair");
    pass(&mut raw, &db.pool).await;
    assert_unpaused(&trellis, "by_code").await;
    for (table, column) in [
        (hidden("p"), "code"),
        (hidden("j"), "code"),
        (hidden("m"), "order_code"),
    ] {
        assert_eq!(
            column_type(&raw, &table, column).await,
            "character varying collate C",
            "{table}.{column}"
        );
    }

    // A rewriting widening `SUM(items.qty)` outgrows.
    raw.batch_execute("alter table public.items alter column qty type bigint")
        .await
        .expect("widen qty");
    pass(&mut raw, &db.pool).await;
    let status = trellis
        .status("by_code")
        .await
        .expect("status")
        .expect("defined");
    assert_eq!(status.status, TransformStatus::Paused);
    let failure = status.capture_failure.expect("the pause says why");
    assert_eq!(failure.source_table, "public.items");
    assert_eq!(failure.columns, vec!["qty".to_string()]);
    assert!(
        failure.error.contains("public.by_code.qty"),
        "{}",
        failure.error
    );
    assert_eq!(column_type(&raw, "public.by_code", "qty").await, "bigint");
    trellis
        .apply("RESUME TRANSFORM by_code")
        .await
        .expect("resume");
    pass(&mut raw, &db.pool).await;
    assert_eq!(column_type(&raw, "public.by_code", "qty").await, "numeric");
}

/// A column of the joined table retyped without a trigger to a type
/// the JOIN evaluates differently (`timestamp` to `timestamptz` under a
/// `COUNT`, which no copy of it outgrows) pauses the JOIN, as it does when
/// the column is its `FROM` table's: the apply reads the joined table's
/// images by the types define recorded for it.
#[tokio::test]
async fn a_joined_column_retyped_to_another_value_type_pauses_the_join() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "set search_path to public;
         create table orders (id integer primary key, region text, placed timestamp);
         create table items (id integer primary key, order_id integer, sku text, \
                             shipped timestamp);
         create index on items (order_id);
         insert into orders values (1, 'emea', '2026-01-01');
         insert into items values (1, 1, 'a', '2026-01-02');
         set search_path to trellis, public;",
    )
    .await
    .expect("seed");
    let trellis = define_only(db.dsn()).await;
    trellis
        .apply(
            "TRANSFORM shipped FROM orders LEFT JOIN items \
             ON orders.id = items.order_id GROUP BY orders.region \
            WITHOUT SOURCE INDEX SELECT orders.region AS region, COUNT(items.shipped) AS shipped, \
                    COUNT(orders.placed) AS placed",
        )
        .await
        .expect("define the JOIN");
    pass(&mut raw, &db.pool).await;
    assert_unpaused(&trellis, "shipped").await;

    for (table, column) in [("public.items", "shipped"), ("public.orders", "placed")] {
        raw.batch_execute(&format!(
            "alter table {table} alter column {column} type timestamptz"
        ))
        .await
        .expect("retype");
        pass(&mut raw, &db.pool).await;
        let status = trellis
            .status("shipped")
            .await
            .expect("status")
            .expect("defined");
        assert_eq!(status.status, TransformStatus::Paused, "{table}");
        let failure = status.capture_failure.expect("the pause says why");
        assert_eq!(failure.source_table, table);
        assert!(
            failure
                .error
                .contains("changes the type this definition evaluates it as"),
            "{}",
            failure.error
        );
        raw.batch_execute(&format!(
            "alter table {table} alter column {column} type timestamp"
        ))
        .await
        .expect("retype back");
        trellis
            .apply("RESUME TRANSFORM shipped")
            .await
            .expect("resume");
        pass(&mut raw, &db.pool).await;
        assert_unpaused(&trellis, "shipped").await;
    }
}

/// A resume drops and recreates a JOIN's hidden tables, so it never
/// re-types them first. `J` keeps every join key it has seen (ADR-0019), so
/// after both `ON` columns narrow (`bigint` to `integer`, the big keys'
/// rows deleted first) it can still hold a key the narrowed type can't: an
/// `ALTER` of `J` would fail, and the resume with it, where recreating `J`
/// succeeds.
#[tokio::test]
async fn a_join_resume_recreates_its_hidden_tables_without_re_typing_them() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "set search_path to public;
         create table orders (id integer primary key, code bigint, region text);
         create table items (id integer primary key, order_code bigint, qty integer);
         create index on orders (code); create index on items (order_code);
         set search_path to trellis, public;",
    )
    .await
    .expect("seed");
    let trellis = define_only(db.dsn()).await;
    trellis
        .apply(
            "TRANSFORM by_code FROM orders LEFT JOIN items \
             ON orders.code = items.order_code GROUP BY orders.region \
            WITHOUT SOURCE INDEX SELECT orders.region AS region, SUM(items.qty) AS qty",
        )
        .await
        .expect("define the JOIN");
    pass(&mut raw, &db.pool).await;
    let id = definition_id(&raw, "by_code").await;
    let j = format!("{DEFAULT_SCHEMA}.join_{id}_j");
    // A key whose rows are gone: `J` keeps its row.
    raw.batch_execute(&format!("insert into {j} (code) values (5000000000)"))
        .await
        .expect("a key J saw");

    raw.batch_execute(
        "begin; \
         alter table public.orders alter column code type integer; \
         alter table public.items alter column order_code type integer; \
         commit",
    )
    .await
    .expect("narrow the ON pair");
    pass(&mut raw, &db.pool).await;
    trellis
        .apply("PAUSE TRANSFORM by_code")
        .await
        .expect("pause");
    trellis
        .apply("RESUME TRANSFORM by_code")
        .await
        .expect("resume");
    pass(&mut raw, &db.pool).await;
    assert_unpaused(&trellis, "by_code").await;
    assert_eq!(column_type(&raw, &j, "code").await, "integer");
    assert_eq!(count(&raw, &format!("select count(*) from {j}")).await, 0);
}
