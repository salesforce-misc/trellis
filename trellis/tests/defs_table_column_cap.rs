//! Postgres caps a table at 1600 columns (`MaxHeapAttributeNumber`), and a
//! definition's tables carry a column per field: a hidden table of an
//! aggregate's partials has two per summed argument, a `JOIN`'s ledgers copy
//! every column their side reads. A definition whose target or hidden table
//! would be wider used to reach `CREATE TABLE` and fail define with Postgres's
//! raw `54011`; it is now refused at validation, naming the table's role and
//! the limit (#1510).
//!
//! One test per table the width follows from, each pinning the boundary
//! against a real Postgres: a table of exactly 1600 columns is created (and
//! counted in `pg_attribute`, so the width the check computes is the table's),
//! one more column is refused, and a refusal leaves no target, no catalog row
//! and no hidden table behind. `ALTER TRANSFORM ... ADD` is held to the same
//! limit, counting the columns earlier edits dropped.
//!
//! Every test is stepped by hand: no staging worker runs.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio::sync::{Mutex, MutexGuard};
use trellis::defs::join_one_to_one::set_enabled;
use trellis::defs::{CatalogError, ValidationError, ValueType, install_definition};
use trellis::integer::IntWidth;
use trellis::{Config, Trellis, TrellisError, TrellisOptions};

/// The one-to-one `JOIN` knob is process-wide; the test that turns it on holds
/// this for its whole body.
static KNOB: Mutex<()> = Mutex::const_new(());

async fn with_knob(on: bool) -> MutexGuard<'static, ()> {
    let guard = KNOB.lock().await;
    set_enabled(on);
    guard
}

async fn batch(pool: &trellis::pool::Pool, sql: &str) {
    pool.get()
        .await
        .expect("get connection")
        .batch_execute(sql)
        .await
        .expect("set up tables");
}

fn int4_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|c| (c.to_string(), ValueType::Integer(IntWidth::Int4)))
        .collect()
}

async fn define(
    pool: &trellis::pool::Pool,
    text: &str,
    source_columns: &HashMap<String, ValueType>,
) -> Result<(), CatalogError> {
    install_definition(pool, text, source_columns, "public")
        .await
        .map(|_| ())
}

/// How many columns `table` (a regclass text) has, dropped ones excluded.
async fn columns_of(pool: &trellis::pool::Pool, table: &str) -> i64 {
    pool.get()
        .await
        .expect("get connection")
        .query_one(
            "select count(*) from pg_catalog.pg_attribute \
             where attrelid = $1::text::regclass and attnum > 0 and not attisdropped",
            &[&table],
        )
        .await
        .expect("count columns")
        .get(0)
}

/// The widest table in the catalog schema, hidden tables of every definition
/// included.
async fn widest_hidden_table(pool: &trellis::pool::Pool) -> i64 {
    pool.get()
        .await
        .expect("get connection")
        .query_one(
            "select coalesce(max(natts), 0) from ( \
                 select count(*) as natts from pg_catalog.pg_attribute a \
                 join pg_catalog.pg_class c on c.oid = a.attrelid \
                 join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
                 where n.nspname = 'trellis' and c.relkind = 'r' \
                   and c.relname ~ '^(join1?_[0-9]+_|agg_[0-9]+$)' \
                   and a.attnum > 0 and not a.attisdropped \
                 group by c.oid) widths",
            &[],
        )
        .await
        .expect("measure hidden tables")
        .get(0)
}

/// How many hidden tables the registered definitions have, to prove a
/// refusal added none.
async fn hidden_table_count(pool: &trellis::pool::Pool) -> i64 {
    pool.get()
        .await
        .expect("get connection")
        .query_one(
            "select count(*) from pg_catalog.pg_class c \
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
             where n.nspname = 'trellis' and c.relname ~ '^(join1?_[0-9]+_|agg_[0-9]+$)'",
            &[],
        )
        .await
        .expect("count hidden tables")
        .get(0)
}

async fn nothing_registered(pool: &trellis::pool::Pool, target: &str) {
    let client = pool.get().await.expect("get connection");
    let exists: bool = client
        .query_one(
            "select pg_catalog.to_regclass($1) is not null",
            &[&format!("public.{target}")],
        )
        .await
        .expect("probe target")
        .get(0);
    assert!(
        !exists,
        "{target}: a refused definition must not leave its target"
    );
    let definitions: i64 = client
        .query_one(
            "select count(*) from transform_definitions where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("count definitions")
        .get(0);
    assert_eq!(
        definitions, 0,
        "{target}: a refused definition must not be stored"
    );
}

/// Asserts `result` is the typed refusal for `table` with `columns` columns
/// (none of them dropped), and that its message names the table and the
/// limit and not Postgres's raw error.
fn assert_too_wide<T: std::fmt::Debug>(
    result: Result<T, CatalogError>,
    table: &str,
    columns: usize,
) {
    let err = result.expect_err("a table wider than Postgres allows is refused");
    match &err {
        CatalogError::Validate(ValidationError::TableTooWide {
            table: named,
            columns: count,
            limit,
            dropped,
        }) => {
            assert_eq!(named, table, "{err}");
            assert_eq!((*count, *limit, *dropped), (columns, 1600, 0), "{err}");
        }
        other => panic!("expected TableTooWide for {table}, got {other:?}"),
    }
    let message = err.to_string();
    assert!(message.contains(table), "{message}");
    assert!(message.contains("1600"), "{message}");
    assert!(message.contains("fewer"), "{message}");
    assert!(!message.contains("54011"), "{message}");
}

async fn define_join(pool: &trellis::pool::Pool, text: String) -> Result<(), CatalogError> {
    define(pool, &text, &HashMap::new()).await
}

/// `SELECT` items `<expr(i)> AS <name><i>` for `0..n`.
fn items(n: usize, name: &str, expr: impl Fn(usize) -> String) -> Vec<String> {
    (0..n)
        .map(|i| format!("{} AS {name}{i}", expr(i)))
        .collect()
}

const AGGREGATE_HIDDEN: &str = "the hidden table of the aggregate's partial sums";

#[tokio::test]
async fn an_aggregates_hidden_table_is_refused_past_the_column_cap() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    batch(
        &db.pool,
        "create table public.w (id integer primary key, g0 integer, g1 integer, \
         g2 integer, v integer);",
    )
    .await;
    let source_columns = int4_columns(&["id", "g0", "g1", "g2", "v"]);
    let text = |target: &str, groups: &[&str], sums: usize| {
        let mut selected: Vec<String> = groups.iter().map(|g| format!("{g} AS {g}")).collect();
        selected.extend(items(sums, "s", |i| format!("SUM(v + {i})")));
        format!(
            "TRANSFORM {target} FROM public.w GROUP BY {} WITHOUT SOURCE INDEX SELECT {}",
            groups.join(", "),
            selected.join(", ")
        )
    };

    // Two group columns, the row count, two columns per summed argument and
    // three more: 797 arguments fill the 1600 columns.
    define(&db.pool, &text("a_ok", &["g0", "g1"], 797), &source_columns)
        .await
        .expect("a hidden table of exactly 1600 columns is admitted");
    assert_eq!(
        widest_hidden_table(&db.pool).await,
        1600,
        "the width the check computes is the table's"
    );
    let hidden = hidden_table_count(&db.pool).await;

    // One more group column is one more column.
    assert_too_wide(
        define(
            &db.pool,
            &text("a_wide", &["g0", "g1", "g2"], 797),
            &source_columns,
        )
        .await,
        AGGREGATE_HIDDEN,
        1601,
    );
    nothing_registered(&db.pool, "a_wide").await;
    // Each distinct argument is two columns, so one more is 1602.
    assert_too_wide(
        define(
            &db.pool,
            &text("a_wider", &["g0", "g1"], 798),
            &source_columns,
        )
        .await,
        AGGREGATE_HIDDEN,
        1602,
    );
    nothing_registered(&db.pool, "a_wider").await;
    assert_eq!(hidden_table_count(&db.pool).await, hidden);
}

#[tokio::test]
async fn an_aggregates_target_is_refused_past_the_column_cap() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    batch(
        &db.pool,
        "create table public.w (id integer primary key, g integer);",
    )
    .await;
    let source_columns = int4_columns(&["id", "g"]);
    let text = |target: &str, fields: usize| {
        let mut selected = vec!["g AS g".to_string()];
        selected.extend(items(fields, "n", |i| format!("COUNT(*) + {i}")));
        format!(
            "TRANSFORM {target} FROM public.w GROUP BY g WITHOUT SOURCE INDEX SELECT {}",
            selected.join(", ")
        )
    };

    // The group column and 1599 fields; `COUNT(*)` takes no hidden column.
    define(&db.pool, &text("t_ok", 1599), &source_columns)
        .await
        .expect("a target of exactly 1600 columns is admitted");
    assert_eq!(columns_of(&db.pool, "public.t_ok").await, 1600);
    let hidden = hidden_table_count(&db.pool).await;

    assert_too_wide(
        define(&db.pool, &text("t_wide", 1600), &source_columns).await,
        "the target table",
        1601,
    );
    nothing_registered(&db.pool, "t_wide").await;
    assert_eq!(hidden_table_count(&db.pool).await, hidden);
}

#[tokio::test]
async fn a_one_to_one_target_is_refused_past_the_column_cap_counting_its_key() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    batch(
        &db.pool,
        "create table public.one (id integer primary key, v integer); \
         create table public.two (id integer, id2 integer, v integer, primary key (id, id2));",
    )
    .await;
    let text = |target: &str, source: &str, fields: usize| {
        format!(
            "TRANSFORM {target} FROM public.{source} SELECT {}",
            items(fields, "f", |i| format!("v + {i}")).join(", ")
        )
    };
    let one = int4_columns(&["id", "v"]);
    let two = int4_columns(&["id", "id2", "v"]);

    // The key column and 1599 fields.
    define(&db.pool, &text("o_ok", "one", 1599), &one)
        .await
        .expect("a target of exactly 1600 columns is admitted");
    assert_eq!(columns_of(&db.pool, "public.o_ok").await, 1600);
    assert_too_wide(
        define(&db.pool, &text("o_wide", "one", 1600), &one).await,
        "the target table",
        1601,
    );
    nothing_registered(&db.pool, "o_wide").await;

    // A composite key is two columns of the target.
    define(&db.pool, &text("c_ok", "two", 1598), &two)
        .await
        .expect("a two-column key and 1598 fields are 1600 columns");
    assert_eq!(columns_of(&db.pool, "public.c_ok").await, 1600);
    assert_too_wide(
        define(&db.pool, &text("c_wide", "two", 1599), &two).await,
        "the target table",
        1601,
    );
    nothing_registered(&db.pool, "c_wide").await;
}

#[tokio::test]
async fn an_aggregate_joins_tables_are_refused_past_the_column_cap() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    batch(
        &db.pool,
        "create table public.orders (id integer primary key, region integer, zone integer); \
         create table public.items (id integer primary key, order_id integer, qty integer, price integer);",
    )
    .await;
    let join = |target: &str, group: &[&str], selected: Vec<String>| {
        let mut all: Vec<String> = group
            .iter()
            .map(|key| format!("{key} AS {}", key.split_once('.').expect("qualified").1))
            .collect();
        all.extend(selected);
        format!(
            "TRANSFORM {target} FROM public.orders JOIN public.items \
             ON orders.id = items.order_id GROUP BY {} WITHOUT SOURCE INDEX SELECT {}",
            group.join(", "),
            all.join(", ")
        )
    };
    let sums_from = |n: usize| items(n, "f", |i| format!("SUM(orders.id + {i})"));
    let sums_joined = |n: usize| items(n, "j", |i| format!("SUM(items.qty + {i})"));
    // `P` is its two key columns (the `ON` column, which is also a group
    // column, and `region`), the row count, a pair per argument and three
    // more: 797 arguments fill the 1600 columns, and a third key column is
    // the 1601st.
    let two = ["orders.id", "orders.region"];
    let three = ["orders.id", "orders.region", "orders.zone"];
    define_join(&db.pool, join("p_ok", &two, sums_from(797)))
        .await
        .expect("a hidden table of exactly 1600 columns is admitted");
    assert_eq!(widest_hidden_table(&db.pool).await, 1600);
    let hidden = hidden_table_count(&db.pool).await;
    assert_too_wide(
        define_join(&db.pool, join("p_wide", &three, sums_from(797))).await,
        "orders's hidden partials table",
        1601,
    );
    nothing_registered(&db.pool, "p_wide").await;

    // The joined side's table is `M`, keyed by `order_id` and the group
    // columns of `items`.
    let joined_two = ["orders.id", "items.qty"];
    let joined_three = ["orders.id", "items.qty", "items.price"];
    define_join(&db.pool, join("m_ok", &joined_two, sums_joined(797)))
        .await
        .expect("a hidden table of exactly 1600 columns is admitted");
    assert_too_wide(
        define_join(&db.pool, join("m_wide", &joined_three, sums_joined(797))).await,
        "items's hidden partials table",
        1601,
    );
    nothing_registered(&db.pool, "m_wide").await;

    // `T` holds the arguments of both sides: two group columns, the row
    // count, a pair per argument (399 from each side) and the zero flag. A
    // third group column is the 1601st.
    let both = || {
        let mut selected = sums_from(399);
        selected.extend(sums_joined(399));
        selected
    };
    define_join(&db.pool, join("t_ok", &two, both()))
        .await
        .expect("a hidden table of exactly 1600 columns is admitted");
    assert_too_wide(
        define_join(&db.pool, join("t_wide", &three, both())).await,
        "the hidden table of the JOIN's group partials",
        1601,
    );
    nothing_registered(&db.pool, "t_wide").await;

    // The target has a column per field.
    let counts = |n: usize| items(n, "n", |i| format!("COUNT(*) + {i}"));
    let region = ["orders.region"];
    define_join(&db.pool, join("g_ok", &region, counts(1599)))
        .await
        .expect("a target of exactly 1600 columns is admitted");
    assert_eq!(columns_of(&db.pool, "public.g_ok").await, 1600);
    assert_too_wide(
        define_join(&db.pool, join("g_wide", &region, counts(1600))).await,
        "the target table",
        1601,
    );
    nothing_registered(&db.pool, "g_wide").await;
    assert!(hidden_table_count(&db.pool).await > hidden);
}

#[tokio::test]
async fn a_one_to_one_joins_tables_are_refused_past_the_column_cap() {
    let _knob = with_knob(true).await;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    // Each side has 1599 columns, so a field can read any number of them.
    let columns = |prefix: &str| {
        (0..1598)
            .map(|i| format!("{prefix}{i} integer"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    batch(
        &db.pool,
        &format!(
            "create table public.fa (id integer primary key, {}); \
             create table public.fb (fid integer primary key, {});",
            columns("c"),
            columns("d")
        ),
    )
    .await;
    let text = |target: &str, selected: Vec<String>| {
        format!(
            "TRANSFORM {target} FROM public.fa JOIN public.fb ON fa.id = fb.fid SELECT {}",
            selected.join(", ")
        )
    };
    let from_reads = |n: usize| items(n, "f", |i| format!("fa.c{i}"));
    let joined_reads = |n: usize| items(n, "f", |i| format!("fb.d{i}"));

    // `R`: the key, four ordering-state columns, the join key `id` and the
    // `n` columns the fields read. 1594 reads fill the 1600.
    define(&db.pool, &text("r_ok", from_reads(1594)), &HashMap::new())
        .await
        .expect("a ledger of exactly 1600 columns is admitted");
    assert_eq!(widest_hidden_table(&db.pool).await, 1600);
    let hidden = hidden_table_count(&db.pool).await;
    assert_too_wide(
        define(&db.pool, &text("r_wide", from_reads(1595)), &HashMap::new()).await,
        "fa's hidden ledger table",
        1601,
    );
    nothing_registered(&db.pool, "r_wide").await;

    // The joined side's ledger is `M`.
    define(&db.pool, &text("m_ok", joined_reads(1594)), &HashMap::new())
        .await
        .expect("a ledger of exactly 1600 columns is admitted");
    assert_too_wide(
        define(
            &db.pool,
            &text("m_wide", joined_reads(1595)),
            &HashMap::new(),
        )
        .await,
        "fb's hidden ledger table",
        1601,
    );
    nothing_registered(&db.pool, "m_wide").await;

    // The target is the key and a column per field, though the fields read
    // one column.
    let sums = |n: usize| items(n, "f", |i| format!("fa.id + {i}"));
    define(&db.pool, &text("t_ok", sums(1599)), &HashMap::new())
        .await
        .expect("a target of exactly 1600 columns is admitted");
    assert_eq!(columns_of(&db.pool, "public.t_ok").await, 1600);
    assert_too_wide(
        define(&db.pool, &text("t_wide", sums(1600)), &HashMap::new()).await,
        "the target table",
        1601,
    );
    nothing_registered(&db.pool, "t_wide").await;
    assert_eq!(
        hidden_table_count(&db.pool).await,
        hidden * 3,
        "a refusal adds no hidden table"
    );
    set_enabled(false);
}

async fn define_only(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a define-only Trellis")
}

/// Defines `TRANSFORM al FROM public.a` with `fields` fields over a source
/// `a (id, v)`, and runs its build, so `ALTER TRANSFORM` finds it live.
async fn live_wide_target(db: &testkit::TestDatabase, fields: usize) -> Trellis {
    batch(
        &db.pool,
        "create table public.a (id integer primary key, v integer);",
    )
    .await;
    let trellis = define_only(db.dsn()).await;
    trellis
        .apply(&format!(
            "TRANSFORM al FROM public.a SELECT {}",
            items(fields, "f", |i| format!("v + {i}")).join(", ")
        ))
        .await
        .expect("define the wide target");
    trellis::intake::harness::settle_builds(&db.pool).await;
    trellis
}

fn too_wide_alter(err: TrellisError) -> (usize, usize, String) {
    match err {
        TrellisError::Catalog(CatalogError::Validate(ValidationError::TableTooWide {
            table,
            columns,
            limit,
            dropped,
        })) => {
            assert_eq!((table.as_str(), limit), ("the target table", 1600));
            let message = ValidationError::TableTooWide {
                table,
                columns,
                limit,
                dropped,
            }
            .to_string();
            (columns, dropped, message)
        }
        other => panic!("expected TableTooWide, got {other:?}"),
    }
}

#[tokio::test]
async fn an_alter_that_adds_past_the_column_cap_is_refused_and_changes_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    // The key and 1598 fields: 1599 columns.
    let trellis = live_wide_target(&db, 1598).await;
    let text_before = definition_text(&db.pool).await;

    trellis
        .apply("ALTER TRANSFORM al ADD v + 1598 AS f1598")
        .await
        .expect("the 1600th column is admitted");
    assert_eq!(columns_of(&db.pool, "public.al").await, 1600);
    let text_after = definition_text(&db.pool).await;
    assert_ne!(text_before, text_after);

    let err = trellis
        .apply("ALTER TRANSFORM al ADD v + 1599 AS f1599")
        .await
        .expect_err("the 1601st column is refused");
    let (columns, dropped, message) = too_wide_alter(err);
    assert_eq!((columns, dropped), (1601, 0));
    assert!(
        message.contains("1601") && message.contains("fewer"),
        "{message}"
    );
    assert!(!message.contains("54011"), "{message}");
    assert_eq!(columns_of(&db.pool, "public.al").await, 1600);
    assert_eq!(
        definition_text(&db.pool).await,
        text_after,
        "a refused edit leaves the definition alone"
    );

    // An `ADD` of a field the target has does nothing, and is not refused.
    trellis
        .apply("ALTER TRANSFORM al ADD v + 1598 AS f1598")
        .await
        .expect("an idempotent ADD creates no column");
    trellis.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn an_alter_counts_the_columns_postgres_keeps_for_dropped_fields() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let trellis = live_wide_target(&db, 1599).await;
    assert_eq!(columns_of(&db.pool, "public.al").await, 1600);

    // Replacing a field in one edit (`DROP z, ADD ... AS z`, the path for a
    // change of result type) drops its column and adds a new one, so it
    // needs a 1601st attribute although the name is already there.
    let err = trellis
        .apply("ALTER TRANSFORM al DROP f5, ADD v + 5000 AS f5")
        .await
        .expect_err("the replacing column needs a 1601st attribute");
    let (columns, dropped, _) = too_wide_alter(err);
    assert_eq!((columns, dropped), (1601, 1));
    assert_eq!(columns_of(&db.pool, "public.al").await, 1600);

    // Dropping a field frees no attribute number: the 1600 columns' worth of
    // numbers are still taken, so Postgres refuses the added column.
    trellis
        .apply("ALTER TRANSFORM al DROP f0")
        .await
        .expect("a drop adds nothing");
    assert_eq!(columns_of(&db.pool, "public.al").await, 1599);
    let raw = db.pool.get().await.expect("get connection");
    let refused = raw
        .batch_execute("alter table public.al add column probe integer")
        .await
        .expect_err("Postgres counts the dropped column");
    assert_eq!(
        refused.code().map(|c| c.code()),
        Some("54011"),
        "{refused:?}"
    );

    let err = trellis
        .apply("ALTER TRANSFORM al ADD v + 5000 AS extra")
        .await
        .expect_err("the added column needs a 1601st attribute");
    let (columns, dropped, message) = too_wide_alter(err);
    assert_eq!((columns, dropped), (1601, 1));
    assert!(
        message.contains("1 dropped columns") && message.contains("define it again"),
        "{message}"
    );
    assert_eq!(columns_of(&db.pool, "public.al").await, 1599);

    // Dropping and adding in one edit is the same: the drop is made first and
    // still counts.
    let err = trellis
        .apply("ALTER TRANSFORM al DROP f1, ADD v + 5000 AS extra")
        .await
        .expect_err("a dropped column still counts");
    let (columns, dropped, _) = too_wide_alter(err);
    assert_eq!((columns, dropped), (1601, 2));
    assert_eq!(columns_of(&db.pool, "public.al").await, 1599);
    trellis.shutdown().await.expect("shutdown");
}

async fn definition_text(pool: &trellis::pool::Pool) -> String {
    pool.get()
        .await
        .expect("get connection")
        .query_one(
            "select definition_text from transform_definitions where target_table = 'public.al'",
            &[],
        )
        .await
        .expect("read the definition")
        .get(0)
}
